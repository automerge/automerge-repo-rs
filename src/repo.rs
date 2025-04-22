use crate::dochandle::{DocHandle, SharedDocument};
use crate::interfaces::{DocumentId, PeerDocState, RepoId};
use crate::interfaces::{NetworkError, RepoMessage, Storage, StorageError};
use crate::share_policy::ShareDecision;
use crate::{
    share_policy, ConnComplete, ConnFinishedReason, PeerConnectionInfo, SharePolicy,
    SharePolicyError,
};
use automerge::sync::{Message as SyncMessage, State as SyncState, SyncDoc};
use automerge::{Automerge, ChangeHash};
use core::pin::Pin;
use crossbeam_channel::{select, unbounded, Receiver, Sender};
use futures::channel::{mpsc, oneshot};
use futures::future::{BoxFuture, Future};
use futures::stream::Stream;
use futures::task::ArcWake;
use futures::task::{waker_ref, Context, Poll, Waker};
use futures::Sink;
use parking_lot::{Mutex, RwLock};
use std::collections::hash_map::Entry;
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::SystemTime;
use uuid::Uuid;

/// Front-end of the repo.
#[derive(Debug, Clone)]
pub struct RepoHandle {
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// Channel used to send events back to the repo,
    /// such as when a doc is created, and a doc handle acquired.
    repo_sender: Sender<RepoEvent>,
    repo_id: RepoId,
}

#[derive(Debug, Clone)]
pub enum RepoError {
    /// The repo is shutting down.
    Shutdown,
    /// Incorrect use of API. TODO: specify.
    Incorrect(String),
    /// Error coming from storage.
    StorageError(StorageError),
}

/// Create a new document.
fn new_document() -> Automerge {
    Automerge::new()
}

/// Incoming event from the network.
#[derive(Clone)]
enum NetworkEvent {
    /// A repo sent us a sync message,
    // to be applied to a given document.
    Sync {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
        message: SyncMessage,
    },
}

impl std::fmt::Debug for NetworkEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkEvent::Sync {
                from_repo_id,
                to_repo_id,
                document_id,
                message: _,
            } => f
                .debug_struct("NetworkEvent::Sync")
                .field("from_repo_id", from_repo_id)
                .field("to_repo_id", to_repo_id)
                .field("document_id", document_id)
                .finish(),
        }
    }
}

/// Outgoing network message.
#[derive(Debug, Clone)]
enum NetworkMessage {
    /// We're sending a sync message,
    // to be applied by a given repo to a given document.
    Sync {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
        message: SyncMessage,
    },
}

/// Create a pair of repo future and resolver.
pub(crate) fn new_repo_future_with_resolver<F>() -> (RepoFuture<F>, RepoFutureResolver<F>) {
    let result = Arc::new(Mutex::new(None));
    let waker = Arc::new(Mutex::new(None));
    let fut = RepoFuture::new(result.clone(), waker.clone());
    let resolver = RepoFutureResolver::new(result, waker);
    (fut, resolver)
}

impl RepoHandle {
    /// Stop the repo running in the background.
    /// All documents will have been saved when this returns.
    ///
    /// This call will block the current thread.
    /// In an async context, use a variant of `spawn_blocking`.
    ///
    /// How to do clean shutdown:
    /// 1. Stop all the tasks that may still write to a document.
    /// 2. Call this method.
    /// 3. Stop your network and storage implementations.
    pub fn stop(self) -> Result<(), RepoError> {
        let _ = self.repo_sender.send(RepoEvent::Stop);
        if let Some(handle) = self.handle.lock().take() {
            handle.join().expect("Failed to join on repo.");
        }
        Ok(())
    }

    pub fn get_repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    /// Get a list of documents in storage.
    pub fn list_all(&self) -> RepoFuture<Result<Vec<DocumentId>, RepoError>> {
        let (fut, resolver) = new_repo_future_with_resolver();
        self.repo_sender
            .send(RepoEvent::ListAllDocs(resolver))
            .expect("Failed to send repo event.");
        fut
    }

    /// Create a new document.
    pub fn new_document(&self) -> DocHandle {
        let document_id = DocumentId::random();
        let document = new_document();
        let doc_info = self.new_document_info(document, DocState::Sync(vec![]));
        let handle = DocHandle::new(
            self.repo_sender.clone(),
            document_id.clone(),
            doc_info.document.clone(),
            doc_info.handle_count.clone(),
            self.repo_id.clone(),
        );
        self.repo_sender
            .send(RepoEvent::NewDoc(document_id, doc_info))
            .expect("Failed to send repo event.");
        // TODO: return a future to make-up for the unboundedness of the channel.
        handle
    }

    /// Boostrap a document, first from storage, and if not found over the network.
    pub fn request_document(
        &self,
        document_id: DocumentId,
    ) -> RepoFuture<Result<DocHandle, RepoError>> {
        let document = new_document();
        let (fut, resolver) = new_repo_future_with_resolver();
        let doc_info = self.new_document_info(
            document,
            DocState::Bootstrap {
                resolvers: vec![resolver],
                storage_fut: None,
            },
        );
        self.repo_sender
            .send(RepoEvent::NewDoc(document_id, doc_info))
            .expect("Failed to send repo event.");
        fut
    }

    /// Attempt to load the document given by `id` from storage
    ///
    /// If the document is not found in storage then `Ok(None)` is returned.
    ///
    /// Note that this _does not_ attempt to fetch the document from the
    /// network.
    pub fn load(&self, id: DocumentId) -> RepoFuture<Result<Option<DocHandle>, RepoError>> {
        let (fut, resolver) = new_repo_future_with_resolver();
        self.repo_sender
            .send(RepoEvent::LoadDoc(id, resolver))
            .expect("Failed to send repo event.");
        fut
    }

    fn new_document_info(&self, document: Automerge, state: DocState) -> DocumentInfo {
        let document = SharedDocument {
            automerge: document,
        };
        let document = Arc::new(RwLock::new(document));
        let handle_count = Arc::new(AtomicUsize::new(1));
        DocumentInfo::new(state, document, handle_count)
    }

    /// Add a network adapter, representing a connection with a remote repo.
    pub fn new_remote_repo(
        &self,
        repo_id: RepoId,
        stream: Box<dyn Send + Unpin + Stream<Item = Result<RepoMessage, NetworkError>>>,
        sink: Box<dyn Send + Unpin + Sink<RepoMessage, Error = NetworkError>>,
    ) -> ConnComplete {
        let (tx, rx) = oneshot::channel();
        self.repo_sender
            .send(RepoEvent::ConnectRemoteRepo {
                repo_id,
                stream,
                sink,
                on_close: tx,
            })
            .expect("Failed to send repo event.");
        ConnComplete::new(rx)
    }

    pub fn peer_doc_state(
        &self,
        remote_id: RepoId,
        document: DocumentId,
    ) -> RepoFuture<Option<PeerDocState>> {
        let (fut, resolver) = new_repo_future_with_resolver();
        self.repo_sender
            .send(RepoEvent::GetPeerState {
                remote_repo_id: remote_id,
                document_id: document,
                reply: resolver,
            })
            .expect("failed to send repo event");
        fut
    }

    pub fn peer_conn_info_changes(
        &self,
        remote_id: RepoId,
    ) -> impl Stream<Item = PeerConnectionInfo> {
        let (tx, rx) = mpsc::channel(10);
        self.repo_sender
            .send(RepoEvent::ListenToPeerState {
                remote_repo_id: remote_id,
                reply: tx,
            })
            .expect("failed to send repo event");
        rx
    }
}

/// Events sent by repo or doc handles to the repo.
pub(crate) enum RepoEvent {
    /// Start processing a ew document.
    NewDoc(DocumentId, DocumentInfo),
    /// A document changed.
    DocChange(DocumentId),
    /// A document was closed(all doc handles dropped).
    DocClosed(DocumentId),
    /// Add a new change observer to a document, from a given change hash.
    AddChangeObserver(
        DocumentId,
        Vec<ChangeHash>,
        RepoFutureResolver<Result<(), RepoError>>,
    ),
    /// Load a document from storage.
    LoadDoc(
        DocumentId,
        RepoFutureResolver<Result<Option<DocHandle>, RepoError>>,
    ),
    /// List all documents in storage.
    ListAllDocs(RepoFutureResolver<Result<Vec<DocumentId>, RepoError>>),
    /// Connect with a remote repo.
    ConnectRemoteRepo {
        repo_id: RepoId,
        stream: Box<dyn Send + Unpin + Stream<Item = Result<RepoMessage, NetworkError>>>,
        sink: Box<dyn Send + Unpin + Sink<RepoMessage, Error = NetworkError>>,
        on_close: oneshot::Sender<ConnFinishedReason>,
    },
    GetPeerState {
        remote_repo_id: RepoId,
        document_id: DocumentId,
        reply: RepoFutureResolver<Option<PeerDocState>>,
    },
    ListenToPeerState {
        remote_repo_id: RepoId,
        reply: mpsc::Sender<PeerConnectionInfo>,
    },
    /// Stop the repo.
    Stop,
}

impl fmt::Debug for RepoEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RepoEvent::NewDoc(_, _) => f.write_str("RepoEvent::NewDoc"),
            RepoEvent::DocChange(_) => f.write_str("RepoEvent::DocChange"),
            RepoEvent::DocClosed(_) => f.write_str("RepoEvent::DocClosed"),
            RepoEvent::AddChangeObserver(_, _, _) => f.write_str("RepoEvent::AddChangeObserver"),
            RepoEvent::LoadDoc(_, _) => f.write_str("RepoEvent::LoadDoc"),
            RepoEvent::ListAllDocs(_) => f.write_str("RepoEvent::ListAllDocs"),
            RepoEvent::ConnectRemoteRepo { .. } => f.write_str("RepoEvent::ConnectRemoteRepo"),
            RepoEvent::GetPeerState { .. } => f.write_str("RepoEvent::GetPeerState"),
            RepoEvent::ListenToPeerState { .. } => f.write_str("RepoEvent::ListenToPeerState"),
            RepoEvent::Stop => f.write_str("RepoEvent::Stop"),
        }
    }
}

/// Used to resolve repo futures.
#[derive(Debug, Clone)]
pub(crate) struct RepoFutureResolver<T> {
    result: Arc<Mutex<Option<T>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> RepoFutureResolver<T> {
    pub fn new(result: Arc<Mutex<Option<T>>>, waker: Arc<Mutex<Option<Waker>>>) -> Self {
        RepoFutureResolver { result, waker }
    }

    fn resolve_fut(&mut self, result: T) {
        *self.result.lock() = Some(result);
        if let Some(waker) = self.waker.lock().take() {
            waker.wake();
        }
    }
}

/// Futures returned by the public API fo the repo and doc handle.
#[derive(Debug)]
pub struct RepoFuture<T> {
    result: Arc<Mutex<Option<T>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> RepoFuture<T> {
    pub fn new(result: Arc<Mutex<Option<T>>>, waker: Arc<Mutex<Option<Waker>>>) -> Self {
        RepoFuture { result, waker }
    }
}

impl<T> Future for RepoFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        *self.waker.lock() = Some(cx.waker().clone());
        let mut result = self.result.lock();
        result.take().map_or(Poll::Pending, Poll::Ready)
    }
}

type BootstrapStorageFut = Option<BoxFuture<'static, Result<Option<Vec<u8>>, StorageError>>>;

/// The doc info state machine.
pub(crate) enum DocState {
    /// Bootstrapping will resolve into a future doc handle,
    /// the optional storage fut represents first checking storage before the network.
    Bootstrap {
        resolvers: Vec<RepoFutureResolver<Result<DocHandle, RepoError>>>,
        storage_fut: BootstrapStorageFut,
    },
    /// Pending a load from storage, not attempting to sync over network.
    LoadPending {
        resolver: RepoFutureResolver<Result<Option<DocHandle>, RepoError>>,
        storage_fut: BoxFuture<'static, Result<Option<Vec<u8>>, StorageError>>,
    },
    /// The doc is syncing(can be edited locally),
    /// and polling pending storage save operations.
    Sync(Vec<BoxFuture<'static, Result<(), StorageError>>>),
    /// Doc is pending removal,
    /// removal can proceed when the vec of storage futs is empty.
    PendingRemoval(Vec<BoxFuture<'static, Result<(), StorageError>>>),
    /// The document is in a corrupted state,
    /// prune it from memory.
    /// TODO: prune it from storage as well?
    Error,
}

impl fmt::Debug for DocState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DocState::Bootstrap { resolvers, .. } => {
                let input = format!("DocState::Bootstrap {:?}", resolvers.len());
                f.write_str(&input)
            }
            DocState::LoadPending { .. } => f.write_str("DocState::LoadPending"),
            DocState::Sync(_) => f.write_str("DocState::Sync"),
            DocState::PendingRemoval(_) => f.write_str("DocState::PendingRemoval"),
            DocState::Error => f.write_str("DocState::Error"),
        }
    }
}

impl DocState {
    fn is_bootstrapping(&self) -> bool {
        matches!(self, DocState::Bootstrap { .. })
    }

    fn is_pending_load(&self) -> bool {
        matches!(self, DocState::LoadPending { .. })
    }

    fn should_sync(&self) -> bool {
        matches!(self, DocState::Sync(_))
            || matches!(
                self,
                DocState::Bootstrap {
                    storage_fut: None,
                    ..
                }
            )
    }

    fn should_save(&self) -> bool {
        match self {
            DocState::Sync(_) => true,
            DocState::PendingRemoval(futs) => !futs.is_empty(),
            _ => false,
        }
    }

    fn get_bootstrap_resolvers(&mut self) -> Vec<RepoFutureResolver<Result<DocHandle, RepoError>>> {
        match self {
            DocState::Bootstrap { resolvers, .. } => mem::take(resolvers),
            _ => unreachable!(
                "Trying to get boostrap resolvers from a document that cannot have any."
            ),
        }
    }

    fn resolve_bootstrap_fut(&mut self, doc_handle: Result<DocHandle, RepoError>) {
        match self {
            DocState::Bootstrap { resolvers, .. } => {
                for mut resolver in resolvers.drain(..) {
                    resolver.resolve_fut(doc_handle.clone());
                }
            }
            _ => unreachable!(
                "Trying to resolve a boostrap future for a document that does not have one."
            ),
        }
    }

    fn resolve_load_fut(&mut self, doc_handle: Result<Option<DocHandle>, RepoError>) {
        match self {
            DocState::LoadPending {
                resolver,
                storage_fut: _,
            } => resolver.resolve_fut(doc_handle),
            _ => unreachable!(
                "Trying to resolve a load future for a document that does not have one."
            ),
        }
    }

    fn resolve_any_fut_for_shutdown(&mut self) {
        match self {
            DocState::LoadPending {
                resolver,
                storage_fut: _,
            } => resolver.resolve_fut(Err(RepoError::Shutdown)),
            DocState::Bootstrap { resolvers, .. } => {
                for mut resolver in resolvers.drain(..) {
                    resolver.resolve_fut(Err(RepoError::Shutdown));
                }
            }
            _ => {}
        }
    }

    fn poll_pending_load(
        &mut self,
        waker: Arc<RepoWaker>,
    ) -> Poll<Result<Option<Vec<u8>>, StorageError>> {
        assert!(matches!(*waker, RepoWaker::Storage { .. }));
        match self {
            DocState::LoadPending {
                resolver: _,
                storage_fut,
            } => {
                let waker = waker_ref(&waker);
                let pinned = Pin::new(storage_fut);
                pinned.poll(&mut Context::from_waker(&waker))
            }
            DocState::Bootstrap {
                resolvers: _,
                storage_fut: Some(storage_fut),
            } => {
                let waker = waker_ref(&waker);
                let pinned = Pin::new(storage_fut);
                pinned.poll(&mut Context::from_waker(&waker))
            }
            _ => unreachable!(
                "Trying to poll a pending load future for a document that does not have one."
            ),
        }
    }

    fn remove_bootstrap_storage_fut(&mut self) {
        match self {
            DocState::Bootstrap {
                resolvers: _,
                ref mut storage_fut,
            } => {
                *storage_fut = None;
            }
            _ => unreachable!(
                "Trying to remove a boostrap load future for a document that does not have one."
            ),
        }
    }

    fn add_boostrap_storage_fut(
        &mut self,
        fut: BoxFuture<'static, Result<Option<Vec<u8>>, StorageError>>,
    ) {
        match self {
            DocState::Bootstrap {
                resolvers: _,
                ref mut storage_fut,
            } => {
                assert!(storage_fut.is_none());
                *storage_fut = Some(fut);
            }
            _ => unreachable!(
                "Trying to add a boostrap load future for a document that does not need one."
            ),
        }
    }

    fn add_boostrap_resolvers(
        &mut self,
        incoming: &mut Vec<RepoFutureResolver<Result<DocHandle, RepoError>>>,
    ) {
        match self {
            DocState::Bootstrap {
                ref mut resolvers, ..
            } => {
                resolvers.append(incoming);
            }
            _ => unreachable!("Unexpected adding of boostrap resolvers."),
        }
    }

    fn poll_pending_save(&mut self, waker: Arc<RepoWaker>) {
        assert!(matches!(*waker, RepoWaker::Storage { .. }));
        match self {
            DocState::Sync(ref mut storage_futs) => {
                if storage_futs.is_empty() {
                    return;
                }
                let to_poll = mem::take(storage_futs);
                let mut new = to_poll
                    .into_iter()
                    .filter_map(|mut storage_fut| {
                        let waker = waker_ref(&waker);
                        let pinned = Pin::new(&mut storage_fut);
                        match pinned.poll(&mut Context::from_waker(&waker)) {
                            Poll::Ready(Ok(_)) => None,
                            Poll::Ready(Err(e)) => {
                                tracing::error!(err=?e, "error in save operation");
                                // TODO: propagate error to doc handle.
                                // `with_doc_mut` could return a future for this.
                                None
                            }
                            Poll::Pending => Some(storage_fut),
                        }
                    })
                    .collect();
                storage_futs.append(&mut new);
            }
            DocState::PendingRemoval(ref mut storage_futs) => {
                if storage_futs.is_empty() {
                    return;
                }
                let to_poll = mem::take(storage_futs);
                let mut new = to_poll
                    .into_iter()
                    .filter_map(|mut storage_fut| {
                        let waker = waker_ref(&waker);
                        let pinned = Pin::new(&mut storage_fut);
                        let res = match pinned.poll(&mut Context::from_waker(&waker)) {
                            Poll::Ready(Ok(_)) => None,
                            Poll::Ready(Err(e)) => {
                                tracing::error!(err=?e, "error in storage operation");
                                // TODO: propagate error to doc handle.
                                // `with_doc_mut` could return a future for this.
                                None
                            }
                            Poll::Pending => Some(storage_fut),
                        };
                        res
                    })
                    .collect();
                storage_futs.append(&mut new);
            }
            _ => unreachable!(
                "Trying to poll a pending save future for a document that does not have one."
            ),
        }
    }
}

/// Info about a document.
#[derive(Debug)]
pub(crate) struct DocumentInfo {
    /// State of the document.
    state: DocState,
    /// Document used to apply and generate sync messages(shared with handles).
    document: Arc<RwLock<SharedDocument>>,
    /// Ref count for handles(shared with handles).
    handle_count: Arc<AtomicUsize>,
    /// Per repo automerge sync state.
    peer_connections: HashMap<RepoId, PeerConnection>,
    /// Used to resolve futures for DocHandle::changed.
    change_observers: Vec<RepoFutureResolver<Result<(), RepoError>>>,
    /// Counter of changes since last compact,
    /// used to make decisions about full or incemental saves.
    changes_since_last_compact: usize,
    /// The number of changes after which a compaction will be performed.
    allowable_changes_until_compaction: usize,
    /// Last heads obtained from the automerge doc.
    last_heads: Vec<ChangeHash>,
}

#[derive(Debug)]
struct PeerConnection {
    repo_id: RepoId,
    last_recv: Option<SystemTime>,
    last_send: Option<SystemTime>,
    state: PeerConnectionState,
}

impl PeerConnection {
    fn pending(repo_id: RepoId) -> Self {
        Self {
            repo_id,
            last_recv: None,
            last_send: None,
            state: PeerConnectionState::PendingAuth {
                received_messages: vec![],
            },
        }
    }

    fn ready(repo_id: RepoId) -> Self {
        Self {
            repo_id,
            last_recv: None,
            last_send: None,
            state: PeerConnectionState::Accepted(SyncState::new()),
        }
    }

    fn receive_sync_message(
        &mut self,
        doc: &mut Automerge,
        msg: SyncMessage,
    ) -> Result<(), automerge::AutomergeError> {
        self.last_recv = Some(SystemTime::now());
        match &mut self.state {
            PeerConnectionState::Accepted(sync_state) => doc.receive_sync_message(sync_state, msg),
            PeerConnectionState::PendingAuth { received_messages } => {
                received_messages.push(msg);
                Ok(())
            }
        }
    }

    fn generate_first_sync_message(
        &mut self,
        document: &mut automerge::Automerge,
    ) -> Option<SyncMessage> {
        let msg = match &mut self.state {
            PeerConnectionState::PendingAuth { received_messages } => {
                let mut sync_state = SyncState::new();
                for msg in received_messages.drain(..) {
                    document
                        .receive_sync_message(&mut sync_state, msg)
                        .expect("Failed to receive sync message.");
                }
                let message = document.generate_sync_message(&mut sync_state);
                self.state = PeerConnectionState::Accepted(sync_state);
                message
            }
            PeerConnectionState::Accepted(sync_state) => document.generate_sync_message(sync_state),
        };
        if msg.is_some() {
            self.last_send = Some(SystemTime::now());
        }
        msg
    }

    fn generate_sync_message(&mut self, doc: &Automerge) -> Option<SyncMessage> {
        let msg = match &mut self.state {
            PeerConnectionState::Accepted(sync_state) => doc.generate_sync_message(sync_state),
            PeerConnectionState::PendingAuth { .. } => None,
        };
        if msg.is_some() {
            self.last_send = Some(SystemTime::now());
        }
        msg
    }

    fn promote_pending_peer(&mut self) -> Option<Vec<SyncMessage>> {
        if let PeerConnectionState::PendingAuth { received_messages } = &mut self.state {
            let result = std::mem::take(received_messages);
            self.state = PeerConnectionState::Accepted(SyncState::new());
            if !result.is_empty() {
                self.last_send = Some(SystemTime::now());
            }
            Some(result)
        } else {
            tracing::warn!(remote=%self.repo_id, "Tried to promote a peer which was not pending authorization");
            None
        }
    }

    /// Get the state of synchronization with a remote peer and document
    fn peer_state(&self) -> PeerDocState {
        let last_sent_heads = match &self.state {
            PeerConnectionState::Accepted(sync_state) => Some(sync_state.last_sent_heads.clone()),
            PeerConnectionState::PendingAuth {
                received_messages: _,
            } => None,
        };
        let last_acked_heads = match &self.state {
            PeerConnectionState::Accepted(sync_state) => Some(sync_state.shared_heads.clone()),
            PeerConnectionState::PendingAuth {
                received_messages: _,
            } => None,
        };
        PeerDocState {
            last_received: self.last_recv,
            last_sent: self.last_send,
            last_sent_heads,
            last_acked_heads,
        }
    }
}

/// A state machine representing a connection between a remote repo and a particular document
#[derive(Debug)]
enum PeerConnectionState {
    /// we've accepted the peer and are syncing with them
    Accepted(SyncState),
    /// We're waiting for a response from the share policy
    PendingAuth { received_messages: Vec<SyncMessage> },
}

/// A change requested by a peer connection
enum PeerConnCommand {
    /// Request authorization from the share policy
    RequestAuth(RepoId),
}

impl DocumentInfo {
    fn new(
        state: DocState,
        document: Arc<RwLock<SharedDocument>>,
        handle_count: Arc<AtomicUsize>,
    ) -> Self {
        let last_heads = {
            let doc = document.read();
            doc.automerge.get_heads()
        };
        DocumentInfo {
            state,
            document,
            handle_count,
            peer_connections: Default::default(),
            change_observers: Default::default(),
            changes_since_last_compact: 0,
            allowable_changes_until_compaction: 10,
            last_heads,
        }
    }

    fn is_boostrapping(&self) -> bool {
        self.state.is_bootstrapping()
    }

    fn is_pending_load(&self) -> bool {
        self.state.is_pending_load()
    }

    fn start_pending_removal(&mut self) {
        self.state = match &mut self.state {
            DocState::Error | DocState::LoadPending { .. } | DocState::Bootstrap { .. } => {
                assert_eq!(self.changes_since_last_compact, 0);
                DocState::PendingRemoval(vec![])
            }
            DocState::Sync(ref mut storage_fut) => DocState::PendingRemoval(mem::take(storage_fut)),
            DocState::PendingRemoval(_) => return,
        }
    }

    fn poll_storage_operation(
        &mut self,
        document_id: DocumentId,
        wake_sender: &Sender<WakeSignal>,
        repo_sender: &Sender<RepoEvent>,
        repo_id: &RepoId,
    ) {
        let waker = Arc::new(RepoWaker::Storage(wake_sender.clone(), document_id.clone()));
        if matches!(self.state, DocState::LoadPending { .. }) {
            match self.state.poll_pending_load(waker) {
                Poll::Ready(Ok(Some(val))) => {
                    {
                        let res = {
                            let mut doc = self.document.write();
                            doc.automerge.load_incremental(&val)
                        };
                        if let Err(e) = res {
                            self.state
                                .resolve_load_fut(Err(RepoError::Incorrect(format!(
                                    "error loading document: {:?}",
                                    e
                                ))));
                            self.state = DocState::Error;
                            return;
                        }
                    }
                    self.handle_count.fetch_add(1, Ordering::SeqCst);
                    let handle = DocHandle::new(
                        repo_sender.clone(),
                        document_id,
                        self.document.clone(),
                        self.handle_count.clone(),
                        repo_id.clone(),
                    );
                    self.state.resolve_load_fut(Ok(Some(handle)));
                    self.state = DocState::Sync(vec![]);
                    // TODO: send sync messages?
                }
                Poll::Ready(Ok(None)) => {
                    self.state.resolve_load_fut(Ok(None));
                    self.state = DocState::Error;
                }
                Poll::Ready(Err(err)) => {
                    self.state
                        .resolve_load_fut(Err(RepoError::StorageError(err)));
                    self.state = DocState::Error;
                }
                Poll::Pending => {}
            }
        } else if matches!(self.state, DocState::Bootstrap { .. }) {
            match self.state.poll_pending_load(waker) {
                Poll::Ready(Ok(Some(val))) => {
                    {
                        let res = {
                            let mut doc = self.document.write();
                            doc.automerge.load_incremental(&val)
                        };
                        if let Err(e) = res {
                            self.state
                                .resolve_bootstrap_fut(Err(RepoError::Incorrect(format!(
                                    "error loading document: {:?}",
                                    e
                                ))));
                            self.state = DocState::Error;
                            return;
                        }
                    }
                    self.handle_count.fetch_add(1, Ordering::SeqCst);
                    let handle = DocHandle::new(
                        repo_sender.clone(),
                        document_id,
                        self.document.clone(),
                        self.handle_count.clone(),
                        repo_id.clone(),
                    );
                    self.state.resolve_bootstrap_fut(Ok(handle));
                    self.state = DocState::Sync(vec![]);
                    // TODO: send sync messages?
                }
                Poll::Ready(Ok(None)) => {
                    // Switch to a network request.
                    self.state.remove_bootstrap_storage_fut();
                }
                Poll::Ready(Err(err)) => {
                    self.state
                        .resolve_bootstrap_fut(Err(RepoError::StorageError(err)));
                    self.state = DocState::Error;
                }
                Poll::Pending => {}
            }
        } else {
            self.state.poll_pending_save(waker);
        }
    }

    /// Count patches since last save,
    /// returns whether there were any.
    fn note_changes(&mut self) -> bool {
        // TODO, Can we do this without a read lock?
        // I think that if the changes update last_heads and
        // we store `last_heads_since_note` we can get a bool out of this.
        let count = {
            let doc = self.document.read();
            let changes = doc.automerge.get_changes(&self.last_heads);
            tracing::trace!(
                last_heads=?self.last_heads,
                current_heads=?doc.automerge.get_heads(),
                "checking for changes since last save"
            );
            changes.len()
        };
        let has_patches = count > 0;
        if has_patches {
            tracing::debug!("doc has changed");
        }
        self.changes_since_last_compact = self.changes_since_last_compact.saturating_add(count);
        has_patches
    }

    fn resolve_change_observers(&mut self, result: Result<(), RepoError>) {
        for mut observer in mem::take(&mut self.change_observers) {
            observer.resolve_fut(result.clone());
        }
    }

    fn save_document(
        &mut self,
        document_id: DocumentId,
        storage: &dyn Storage,
        wake_sender: &Sender<WakeSignal>,
    ) {
        if !self.state.should_save() {
            return;
        }
        let should_compact =
            self.changes_since_last_compact > self.allowable_changes_until_compaction;
        let (storage_fut, new_heads) = if should_compact {
            tracing::trace!(%document_id, "compacting document");
            let (to_save, new_heads) = {
                let doc = self.document.read();
                (doc.automerge.save(), doc.automerge.get_heads())
            };
            self.changes_since_last_compact = 0;
            (storage.compact(document_id.clone(), to_save), new_heads)
        } else {
            tracing::trace!(%document_id, "writing incremental chunk");
            let (to_save, new_heads) = {
                let doc = self.document.read();
                (
                    doc.automerge.save_after(&self.last_heads),
                    doc.automerge.get_heads(),
                )
            };
            (storage.append(document_id.clone(), to_save), new_heads)
        };
        match self.state {
            DocState::Sync(ref mut futs) => {
                futs.push(storage_fut);
            }
            DocState::PendingRemoval(ref mut futs) => {
                futs.push(storage_fut);
            }
            _ => unreachable!("Unexpected doc state on save."),
        }
        let waker = Arc::new(RepoWaker::Storage(wake_sender.clone(), document_id));
        self.state.poll_pending_save(waker);
        self.last_heads = new_heads;
    }

    /// Apply incoming sync messages,
    ///
    /// # Returns
    ///
    /// A tuple of `(has_changes, commands)` where `has_changes` is true if the document changed as
    /// a result of applying the sync message and `commands` is a list of changes requested by the
    /// peer connections for this document (e.g. requesting authorization from the share policy).
    fn receive_sync_message(
        &mut self,
        per_remote: HashMap<RepoId, VecDeque<SyncMessage>>,
    ) -> (bool, Vec<PeerConnCommand>) {
        let mut commands = Vec::new();
        let (start_heads, new_heads) = {
            let mut document = self.document.write();
            let start_heads = document.automerge.get_heads();
            for (repo_id, messages) in per_remote {
                let conn = match self.peer_connections.entry(repo_id.clone()) {
                    Entry::Vacant(entry) => {
                        // if this is a new peer, request authorization
                        commands.push(PeerConnCommand::RequestAuth(repo_id.clone()));
                        entry.insert(PeerConnection::pending(repo_id.clone()))
                    }
                    Entry::Occupied(entry) => entry.into_mut(),
                };
                for message in messages {
                    conn.receive_sync_message(&mut document.automerge, message)
                        .expect("Failed to receive sync message.");
                }
            }
            let new_heads = document.automerge.get_heads();
            (start_heads, new_heads)
        };
        (start_heads != new_heads, commands)
    }

    /// Promote a peer awaiting authorization to a full peer
    ///
    /// Returns any messages which the peer sent while we were waiting for authorization
    fn promote_pending_peer(&mut self, repo_id: &RepoId) -> Option<Vec<SyncMessage>> {
        self.peer_connections
            .get_mut(repo_id)
            .map(|c| c.promote_pending_peer())
            .unwrap_or_default()
    }

    /// Potentially generate an outgoing sync message.
    fn generate_first_sync_message(&mut self, repo_id: RepoId) -> Option<SyncMessage> {
        let conn = self
            .peer_connections
            .entry(repo_id.clone())
            .or_insert_with(|| PeerConnection::ready(repo_id));
        conn.generate_first_sync_message(&mut self.document.write().automerge)
    }

    /// Generate outgoing sync message for all repos we are syncing with.
    fn generate_sync_messages(&mut self) -> Vec<(RepoId, SyncMessage)> {
        let document = self.document.read();
        self.peer_connections
            .iter_mut()
            .filter_map(|(repo_id, conn)| {
                let message = conn.generate_sync_message(&document.automerge);
                message.map(|msg| (repo_id.clone(), msg))
            })
            .collect()
    }

    fn get_peer_doc_state(&self, peer: &RepoId) -> Option<PeerDocState> {
        self.peer_connections.get(peer).map(|p| p.peer_state())
    }
}

/// Signal that the stream or sink on the network adapter is ready to be polled.
#[derive(Debug)]
enum WakeSignal {
    Stream(RepoId),
    Sink(RepoId),
    PendingCloseSink(RepoId),
    Storage(DocumentId),
    StorageList,
    ShareDecision(RepoId),
}

/// Waking mechanism for stream and sinks.
#[derive(Debug)]
enum RepoWaker {
    Stream(Sender<WakeSignal>, RepoId),
    Sink(Sender<WakeSignal>, RepoId),
    PendingCloseSink(Sender<WakeSignal>, RepoId),
    Storage(Sender<WakeSignal>, DocumentId),
    StorageList(Sender<WakeSignal>),
    ShareDecision(Sender<WakeSignal>, RepoId),
}

/// <https://docs.rs/futures/latest/futures/task/trait.ArcWake.html>
impl ArcWake for RepoWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        // Ignore errors,
        // other side may try to wake after repo shut-down.
        let _ = match &**arc_self {
            RepoWaker::Stream(sender, repo_id) => sender.send(WakeSignal::Stream(repo_id.clone())),
            RepoWaker::Sink(sender, repo_id) => sender.send(WakeSignal::Sink(repo_id.clone())),
            RepoWaker::PendingCloseSink(sender, repo_id) => {
                sender.send(WakeSignal::PendingCloseSink(repo_id.clone()))
            }
            RepoWaker::Storage(sender, doc_id) => sender.send(WakeSignal::Storage(doc_id.clone())),
            RepoWaker::StorageList(sender) => sender.send(WakeSignal::StorageList),
            RepoWaker::ShareDecision(sender, repo_id) => {
                sender.send(WakeSignal::ShareDecision(repo_id.clone()))
            }
        };
    }
}

/// Manages pending `list_all` calls to storage.
/// Note: multiple calls to `list_all` will see all futures resolve
/// using the result of the first call.
struct PendingListAll {
    resolvers: Vec<RepoFutureResolver<Result<Vec<DocumentId>, RepoError>>>,
    storage_fut: BoxFuture<'static, Result<Vec<DocumentId>, StorageError>>,
}

/// A sink and stream pair representing a network connection to a remote repo.
struct RemoteRepo {
    stream: Box<dyn Send + Unpin + Stream<Item = Result<RepoMessage, NetworkError>>>,
    sink: Box<dyn Send + Unpin + Sink<RepoMessage, Error = NetworkError>>,
}

type PendingCloseSinks = Vec<Box<dyn Send + Unpin + Sink<RepoMessage, Error = NetworkError>>>;

struct PendingShareDecision {
    doc_id: DocumentId,
    share_type: ShareType,
    future: BoxFuture<'static, Result<ShareDecision, SharePolicyError>>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum ShareType {
    Request,
    Announce,
    Synchronize,
}

/// The backend of a repo: runs an event-loop in a background thread.
pub struct Repo {
    /// The Id of the repo.
    repo_id: RepoId,

    /// Documents managed in memory by the repo.
    documents: HashMap<DocumentId, DocumentInfo>,

    /// Messages to send on the network adapter sink.
    pending_messages: HashMap<RepoId, VecDeque<NetworkMessage>>,
    /// Events received on the network stream, pending processing.
    pending_events: VecDeque<NetworkEvent>,

    /// Receiver of network stream and sink readiness signals.
    wake_receiver: Receiver<WakeSignal>,
    wake_sender: Sender<WakeSignal>,

    /// Keeping track of streams and sinks
    /// to poll in the current loop iteration.
    streams_to_poll: HashSet<RepoId>,
    sinks_to_poll: HashSet<RepoId>,
    share_decisions_to_poll: HashSet<RepoId>,

    /// Sender and receiver of repo events.
    repo_sender: Sender<RepoEvent>,
    repo_receiver: Receiver<RepoEvent>,

    /// List of documents with changes(to be saved, notify change observers).
    documents_with_changes: Vec<DocumentId>,

    /// Pending storage `list_all` operations.
    pending_storage_list_all: Option<PendingListAll>,

    /// The storage API.
    storage: Box<dyn Storage>,

    /// The network API.
    remote_repos: HashMap<RepoId, RemoteRepo>,

    /// Network sinks that are pending close.
    pending_close_sinks: HashMap<RepoId, PendingCloseSinks>,

    /// The authorization API
    share_policy: Box<dyn SharePolicy>,

    /// Pending share policy futures
    pending_share_decisions: HashMap<RepoId, Vec<PendingShareDecision>>,

    // Peer listeners
    peer_listeners: Vec<PeerInfoListenerState>,

    // Connection closed listeners
    conn_closed_listeners: HashMap<RepoId, oneshot::Sender<ConnFinishedReason>>,
}

impl Repo {
    /// Create a new repo.
    pub fn new(repo_id: Option<String>, storage: Box<dyn Storage>) -> Self {
        let (wake_sender, wake_receiver) = unbounded();
        let (repo_sender, repo_receiver) = unbounded();
        let repo_id = repo_id.map_or_else(|| RepoId(Uuid::new_v4().to_string()), RepoId);
        let share_policy = Box::new(share_policy::Permissive);
        Repo {
            repo_id,
            documents: Default::default(),
            remote_repos: Default::default(),
            wake_receiver,
            wake_sender,
            streams_to_poll: Default::default(),
            sinks_to_poll: Default::default(),
            pending_messages: Default::default(),
            pending_events: Default::default(),
            pending_storage_list_all: Default::default(),
            repo_sender,
            repo_receiver,
            documents_with_changes: Default::default(),
            storage,
            pending_close_sinks: Default::default(),
            share_policy,
            pending_share_decisions: HashMap::new(),
            share_decisions_to_poll: HashSet::new(),
            peer_listeners: Vec::new(),
            conn_closed_listeners: HashMap::new(),
        }
    }

    pub fn with_share_policy(mut self, share_policy: Box<dyn SharePolicy>) -> Self {
        self.share_policy = share_policy;
        self
    }

    fn get_repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    /// Save documents that have changed to storage,
    /// resolve change observers.
    fn process_changed_document(&mut self) {
        for doc_id in mem::take(&mut self.documents_with_changes) {
            if let Some(info) = self.documents.get_mut(&doc_id) {
                info.resolve_change_observers(Ok(()));
                info.save_document(doc_id, self.storage.as_ref(), &self.wake_sender);
            }
        }
    }

    /// Remove sync states for repos for which we do not have an adapter anymore.
    fn remove_unused_sync_states(&mut self) {
        for document_info in self.documents.values_mut() {
            let sync_keys = document_info
                .peer_connections
                .keys()
                .cloned()
                .collect::<HashSet<_>>();
            let live_keys = self.remote_repos.keys().cloned().collect::<HashSet<_>>();
            let delenda = sync_keys.difference(&live_keys).collect::<Vec<_>>();

            for key in delenda {
                document_info.peer_connections.remove(key);
            }
        }
    }

    /// Garbage collect docs.
    fn gc_docs(&mut self) {
        let delenda = self
            .documents
            .iter()
            .filter_map(|(key, info)| match &info.state {
                DocState::Error => Some(key.clone()),
                DocState::PendingRemoval(futs) => {
                    if futs.is_empty() {
                        Some(key.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        for key in delenda {
            self.documents.remove(&key);
        }
    }

    /// Remove pending messages for repos for which we do not have an adapter anymore.
    fn remove_unused_pending_messages(&mut self) {
        let dead_repos = self
            .pending_messages
            .keys()
            .filter(|repo_id| !self.remote_repos.contains_key(repo_id))
            .cloned()
            .collect::<Vec<_>>();
        for dead_repo in dead_repos {
            self.pending_messages.remove(&dead_repo);
        }
    }

    /// Poll the network adapter stream.
    fn collect_network_events(&mut self) {
        // Receive incoming message on streams,
        // discarding streams that error.
        let to_poll = mem::take(&mut self.streams_to_poll);
        for repo_id in to_poll {
            let mut new_messages = Vec::new();
            let fin_reason = if let Some(remote_repo) = self.remote_repos.get_mut(&repo_id) {
                // Collect as many events as possible.
                loop {
                    let stream_waker =
                        Arc::new(RepoWaker::Stream(self.wake_sender.clone(), repo_id.clone()));
                    let waker = waker_ref(&stream_waker);
                    let pinned_stream = Pin::new(&mut remote_repo.stream);
                    let result = pinned_stream.poll_next(&mut Context::from_waker(&waker));
                    match result {
                        Poll::Pending => {
                            break None;
                        }
                        Poll::Ready(Some(repo_message)) => match repo_message {
                            Ok(RepoMessage::Sync {
                                from_repo_id,
                                to_repo_id,
                                document_id,
                                message,
                            }) => match SyncMessage::decode(&message) {
                                Ok(message) => {
                                    let event = NetworkEvent::Sync {
                                        from_repo_id,
                                        to_repo_id,
                                        document_id,
                                        message,
                                    };
                                    new_messages.push(event);
                                }
                                Err(e) => {
                                    tracing::error!(error = ?e, "Error decoding sync message.");
                                    let msg =
                                        format!("error decoding received sync message: {}", e);
                                    break Some(ConnFinishedReason::ErrorReceiving(msg));
                                }
                            },
                            Ok(RepoMessage::Ephemeral { .. }) => {
                                tracing::warn!("received ephemeral message, ignoring.");
                            }
                            Err(e) => {
                                tracing::error!(error = ?e, "Error on network stream.");
                                break Some(ConnFinishedReason::ErrorReceiving(e.to_string()));
                            }
                        },
                        Poll::Ready(None) => {
                            tracing::info!(remote_repo_id=?repo_id, "remote stream closed, removing");
                            break Some(ConnFinishedReason::TheyDisconnected);
                        }
                    }
                }
            } else {
                continue;
            };
            if let Some(fin_reason) = fin_reason {
                self.remove_sink(&repo_id, fin_reason);
            } else {
                self.pending_events.extend(new_messages.into_iter());
            }
        }
    }

    fn process_pending_storage_list(&mut self) {
        if let Some(ref mut pending) = self.pending_storage_list_all {
            let waker = Arc::new(RepoWaker::StorageList(self.wake_sender.clone()));
            let waker = waker_ref(&waker);
            let pinned_fut = Pin::new(&mut pending.storage_fut);
            let result = pinned_fut.poll(&mut Context::from_waker(&waker));
            match result {
                Poll::Pending => {}
                Poll::Ready(res) => {
                    for mut resolver in pending.resolvers.drain(..) {
                        resolver.resolve_fut(res.clone().map_err(RepoError::StorageError));
                    }
                    self.pending_storage_list_all = None;
                }
            }
        }
    }

    fn error_pending_storage_list_for_shutdown(&mut self) {
        if let Some(ref mut pending) = self.pending_storage_list_all {
            for mut resolver in pending.resolvers.drain(..) {
                resolver.resolve_fut(Err(RepoError::Shutdown));
            }
        }
    }

    /// Try to send pending messages on the network sink.
    fn process_outgoing_network_messages(&mut self) {
        // Send outgoing message on sinks,
        // discarding sinks that error.
        let to_poll = mem::take(&mut self.sinks_to_poll);
        for repo_id in to_poll {
            let discard_reason = if let Some(remote_repo) = self.remote_repos.get_mut(&repo_id) {
                // Send as many messages as possible.
                let mut needs_flush = false;
                let mut discard_reason = None;
                loop {
                    let pending_messages =
                        self.pending_messages.entry(repo_id.clone()).or_default();
                    if pending_messages.is_empty() {
                        break;
                    }
                    let sink_waker =
                        Arc::new(RepoWaker::Sink(self.wake_sender.clone(), repo_id.clone()));
                    let waker = waker_ref(&sink_waker);
                    let pinned_sink = Pin::new(&mut remote_repo.sink);
                    let result = pinned_sink.poll_ready(&mut Context::from_waker(&waker));
                    match result {
                        Poll::Pending => break,
                        Poll::Ready(Ok(())) => {
                            let pinned_sink = Pin::new(&mut remote_repo.sink);
                            let NetworkMessage::Sync {
                                from_repo_id,
                                to_repo_id,
                                document_id,
                                message,
                            } = pending_messages
                                .pop_front()
                                .expect("Empty pending messages.");
                            let outgoing = RepoMessage::Sync {
                                from_repo_id,
                                to_repo_id,
                                document_id,
                                message: message.encode(),
                            };
                            let result = pinned_sink.start_send(outgoing);
                            if let Err(e) = result {
                                tracing::error!(error = ?e, "Error on network sink.");
                                discard_reason =
                                    Some(ConnFinishedReason::ErrorSending(e.to_string()));
                                needs_flush = false;
                                break;
                            }
                            needs_flush = true;
                        }
                        Poll::Ready(Err(e)) => {
                            discard_reason = Some(ConnFinishedReason::ErrorSending(e.to_string()));
                            needs_flush = false;
                            break;
                        }
                    }
                }

                // Flush the sink if any messages have been sent.
                if needs_flush {
                    let sink_waker =
                        Arc::new(RepoWaker::Sink(self.wake_sender.clone(), repo_id.clone()));
                    let waker = waker_ref(&sink_waker);
                    let pinned_sink = Pin::new(&mut remote_repo.sink);
                    if let Poll::Ready(Err(e)) =
                        pinned_sink.poll_flush(&mut Context::from_waker(&waker))
                    {
                        discard_reason = Some(ConnFinishedReason::ErrorSending(e.to_string()));
                    }
                }
                discard_reason
            } else {
                continue;
            };
            if let Some(discard_reason) = discard_reason {
                self.remove_sink(&repo_id, discard_reason);
            }
        }
    }

    /// Handle incoming repo events(sent by repo or document handles).
    fn handle_repo_event(&mut self, event: RepoEvent) {
        tracing::trace!(event = ?event, "Handling repo event");
        match event {
            // TODO: simplify handling of `RepoEvent::NewDoc`.
            // `NewDoc` could be broken-up into two events: `RequestDoc` and `NewDoc`,
            // the doc info could be created here.
            RepoEvent::NewDoc(document_id, mut info) => {
                if info.is_boostrapping() {
                    tracing::trace!("adding bootstrapping document");
                    if let Some(existing_info) = self.documents.get_mut(&document_id) {
                        if matches!(existing_info.state, DocState::Bootstrap { .. }) {
                            let mut resolvers = info.state.get_bootstrap_resolvers();
                            existing_info.state.add_boostrap_resolvers(&mut resolvers);
                        } else if matches!(existing_info.state, DocState::Sync(_)) {
                            existing_info.handle_count.fetch_add(1, Ordering::SeqCst);
                            let handle = DocHandle::new(
                                self.repo_sender.clone(),
                                document_id.clone(),
                                existing_info.document.clone(),
                                existing_info.handle_count.clone(),
                                self.repo_id.clone(),
                            );
                            info.state.resolve_bootstrap_fut(Ok(handle));
                        } else {
                            tracing::warn!(state=?info.state, "newdoc event received for existing document with incorrect state");
                            info.state.resolve_bootstrap_fut(Err(RepoError::Incorrect(format!("newdoc event received for existing document with incorrect state: {:?}", info.state))));
                        }
                        return;
                    } else {
                        let storage_fut = self.storage.get(document_id.clone());
                        info.state.add_boostrap_storage_fut(storage_fut);
                        info.poll_storage_operation(
                            document_id.clone(),
                            &self.wake_sender,
                            &self.repo_sender,
                            &self.repo_id,
                        );

                        let share_type = if info.is_boostrapping() {
                            Some(ShareType::Request)
                        } else if info.state.should_sync() {
                            Some(ShareType::Announce)
                        } else {
                            None
                        };
                        if let Some(share_type) = share_type {
                            Self::enqueue_share_decisions(
                                self.remote_repos.keys(),
                                &mut self.pending_share_decisions,
                                &mut self.share_decisions_to_poll,
                                self.share_policy.as_ref(),
                                document_id.clone(),
                                share_type,
                            );
                        }
                    }
                }
                self.documents.insert(document_id, info);
            }
            RepoEvent::DocChange(doc_id) => {
                // Handle doc changes: sync the document.
                let local_repo_id = self.get_repo_id().clone();
                if let Some(info) = self.documents.get_mut(&doc_id) {
                    // only run the documents_with_changes workflow if there
                    // was a change, but always generate potential sync messages
                    // (below)
                    if info.note_changes() {
                        self.documents_with_changes.push(doc_id.clone());
                    }
                    for (to_repo_id, message) in info.generate_sync_messages().into_iter() {
                        let outgoing = NetworkMessage::Sync {
                            from_repo_id: local_repo_id.clone(),
                            to_repo_id: to_repo_id.clone(),
                            document_id: doc_id.clone(),
                            message,
                        };
                        self.pending_messages
                            .entry(to_repo_id.clone())
                            .or_default()
                            .push_back(outgoing);
                        self.sinks_to_poll.insert(to_repo_id);
                    }
                    // Send a sync message to all other repos we are connected with and with
                    // whom we should share this document
                    Self::enqueue_share_decisions(
                        self.remote_repos.keys(),
                        &mut self.pending_share_decisions,
                        &mut self.share_decisions_to_poll,
                        self.share_policy.as_ref(),
                        doc_id.clone(),
                        ShareType::Announce,
                    );
                }
            }
            RepoEvent::DocClosed(doc_id) => {
                if let Some(doc_info) = self.documents.get_mut(&doc_id) {
                    assert_eq!(doc_info.handle_count.load(Ordering::SeqCst), 0);
                    doc_info.save_document(doc_id, self.storage.as_ref(), &self.wake_sender);
                    doc_info.start_pending_removal();
                }
            }
            RepoEvent::ListAllDocs(mut resolver) => match self.pending_storage_list_all {
                Some(ref mut pending) => {
                    pending.resolvers.push(resolver);
                }
                None => {
                    let mut storage_fut = self.storage.list_all();
                    let waker = Arc::new(RepoWaker::StorageList(self.wake_sender.clone()));
                    let waker = waker_ref(&waker);
                    let pinned_fut = Pin::new(&mut storage_fut);
                    let result = pinned_fut.poll(&mut Context::from_waker(&waker));
                    match result {
                        Poll::Pending => {}
                        Poll::Ready(res) => {
                            resolver.resolve_fut(res.map_err(RepoError::StorageError));
                            return;
                        }
                    }
                    self.pending_storage_list_all = Some(PendingListAll {
                        resolvers: vec![resolver],
                        storage_fut,
                    });
                }
            },
            RepoEvent::LoadDoc(doc_id, resolver) => {
                let mut resolver_clone = resolver.clone();
                let info = self.documents.entry(doc_id.clone()).or_insert_with(|| {
                    let storage_fut = self.storage.get(doc_id.clone());
                    let shared_document = SharedDocument {
                        automerge: new_document(),
                    };
                    let state = DocState::LoadPending {
                        storage_fut,
                        resolver,
                    };
                    let document = Arc::new(RwLock::new(shared_document));
                    let handle_count = Arc::new(AtomicUsize::new(0));
                    DocumentInfo::new(state, document, handle_count)
                });

                // Note: unfriendly API.
                //
                // API currently assumes client makes a single `load` call,
                // for a document that has not been created or requested before.
                //
                // If the doc is in memory, we could simply create a new handle for it.
                //
                // If the doc is bootstrapping,
                // the resolver could be added to the list of resolvers.
                if !info.is_pending_load() {
                    resolver_clone.resolve_fut(Err(RepoError::Incorrect(
                        "attempted to load a document which is already loading".to_string(),
                    )));
                    return;
                }
                info.poll_storage_operation(
                    doc_id,
                    &self.wake_sender,
                    &self.repo_sender,
                    &self.repo_id,
                );
            }
            RepoEvent::AddChangeObserver(doc_id, last_heads, mut observer) => {
                if let Some(info) = self.documents.get_mut(&doc_id) {
                    let current_heads = {
                        let state = info.document.read();
                        state.automerge.get_heads()
                    };
                    tracing::trace!(
                        ?current_heads,
                        ?last_heads,
                        "handling AddChangeObserver event"
                    );
                    if current_heads == last_heads {
                        info.change_observers.push(observer);
                    } else {
                        // Resolve now if the document hash already changed.
                        observer.resolve_fut(Ok(()));
                    }
                }
            }
            RepoEvent::ConnectRemoteRepo {
                repo_id,
                stream,
                sink,
                on_close,
            } => {
                if self.remove_sink(&repo_id, ConnFinishedReason::DuplicateConnection) {
                    tracing::debug!(remote_repo=?repo_id, "replacing existing remote repo with new connection");
                    // Reset the sync state.
                    self.remove_unused_sync_states();
                } else {
                    tracing::debug!(remote_repo=?repo_id, "new connection for remote repo");
                }
                assert!(self
                    .remote_repos
                    .insert(repo_id.clone(), RemoteRepo { stream, sink })
                    .is_none());
                // Try to sync all docs we know about.
                for (document_id, info) in self.documents.iter() {
                    if info.state.should_sync() {
                        Self::enqueue_share_decisions(
                            std::iter::once(&repo_id),
                            &mut self.pending_share_decisions,
                            &mut self.share_decisions_to_poll,
                            self.share_policy.as_ref(),
                            document_id.clone(),
                            ShareType::Announce,
                        );
                    }
                }
                self.sinks_to_poll.insert(repo_id.clone());
                self.streams_to_poll.insert(repo_id.clone());
                self.conn_closed_listeners.insert(repo_id, on_close);
            }
            RepoEvent::GetPeerState {
                remote_repo_id,
                document_id,
                mut reply,
            } => {
                reply.resolve_fut(
                    self.documents
                        .get(&document_id)
                        .and_then(|info| info.get_peer_doc_state(&remote_repo_id)),
                );
            }
            RepoEvent::ListenToPeerState {
                remote_repo_id,
                mut reply,
            } => {
                let state = Self::peer_conn_info(&remote_repo_id, &self.documents);
                let do_send = reply.try_send(state.clone());
                if do_send.is_err_and(|e| e.is_disconnected()) {
                    // The listener has already been dropped, doing nothing
                } else {
                    self.peer_listeners.push(PeerInfoListenerState {
                        remote: remote_repo_id,
                        last_sent: state,
                        sender: reply,
                    })
                }
            }
            RepoEvent::Stop => {
                // Handled in the main run loop.
            }
        }
    }

    /// Apply incoming sync messages, and generate outgoing ones.
    fn sync_documents(&mut self) {
        // Re-organize messages so as to acquire the write lock
        // on the document only once per document.
        let mut per_doc_messages: HashMap<DocumentId, HashMap<RepoId, VecDeque<SyncMessage>>> =
            Default::default();
        for event in mem::take(&mut self.pending_events) {
            tracing::trace!(message = ?event, "processing sync message");
            match event {
                NetworkEvent::Sync {
                    from_repo_id,
                    to_repo_id,
                    document_id,
                    message,
                } => {
                    assert_eq!(to_repo_id, self.repo_id);

                    // If we don't know about the document,
                    // create a new sync state and start syncing.
                    // Note: this is the mirror of sending sync messages for
                    // all known documents when a remote repo connects.
                    let info = self
                        .documents
                        .entry(document_id.clone())
                        .or_insert_with(|| {
                            // Note: since the handle count is zero,
                            // the document will not be removed from memory until shutdown.
                            // Perhaps remove this and rely on `request_document` calls.
                            let shared_document = SharedDocument {
                                automerge: new_document(),
                            };
                            let state = DocState::Bootstrap {
                                resolvers: vec![],
                                storage_fut: None,
                            };
                            let document = Arc::new(RwLock::new(shared_document));
                            let handle_count = Arc::new(AtomicUsize::new(0));
                            let mut info = DocumentInfo::new(state, document, handle_count);

                            let storage_fut = self.storage.get(document_id.clone());
                            info.state.add_boostrap_storage_fut(storage_fut);
                            info.poll_storage_operation(
                                document_id.clone(),
                                &self.wake_sender,
                                &self.repo_sender,
                                &self.repo_id,
                            );

                            info
                        });

                    if !info.state.should_sync() {
                        continue;
                    }

                    let per_doc = per_doc_messages.entry(document_id).or_default();
                    let per_remote = per_doc.entry(from_repo_id).or_default();
                    per_remote.push_back(message.clone());
                }
            }
        }

        for (document_id, per_remote) in per_doc_messages {
            let info = self
                .documents
                .get_mut(&document_id)
                .expect("Doc should have an info by now.");

            let (has_changes, peer_conn_commands) = info.receive_sync_message(per_remote);
            if has_changes && info.note_changes() {
                self.documents_with_changes.push(document_id.clone());
            }

            for cmd in peer_conn_commands {
                match cmd {
                    PeerConnCommand::RequestAuth(peer_id) => Self::enqueue_share_decisions(
                        std::iter::once(&peer_id),
                        &mut self.pending_share_decisions,
                        &mut self.share_decisions_to_poll,
                        self.share_policy.as_ref(),
                        document_id.clone(),
                        ShareType::Synchronize,
                    ),
                }
            }

            // Note: since receiving and generating sync messages is done
            // in two separate critical sections,
            // local changes could be made in between those,
            // which is a good thing(generated messages will include those changes).
            let mut ready = true;
            for (to_repo_id, message) in info.generate_sync_messages().into_iter() {
                if message.heads.is_empty() && !message.need.is_empty() {
                    ready = false;
                }
                let outgoing = NetworkMessage::Sync {
                    from_repo_id: self.repo_id.clone(),
                    to_repo_id: to_repo_id.clone(),
                    document_id: document_id.clone(),
                    message,
                };
                self.pending_messages
                    .entry(to_repo_id.clone())
                    .or_default()
                    .push_back(outgoing);
                self.sinks_to_poll.insert(to_repo_id);
            }
            if ready && info.state.is_bootstrapping() {
                info.handle_count.fetch_add(1, Ordering::SeqCst);
                let handle = DocHandle::new(
                    self.repo_sender.clone(),
                    document_id.clone(),
                    info.document.clone(),
                    info.handle_count.clone(),
                    self.repo_id.clone(),
                );
                info.state.resolve_bootstrap_fut(Ok(handle));
                info.state = DocState::Sync(vec![]);
            }
        }
    }

    fn poll_close_sinks(&mut self, repo_id: RepoId) {
        if let Entry::Occupied(mut entry) = self.pending_close_sinks.entry(repo_id.clone()) {
            let sinks = mem::take(entry.get_mut());
            tracing::trace!(remote_repo=?repo_id, num_to_close=sinks.len(), "polling close sinks");
            for mut sink in sinks.into_iter() {
                let result = {
                    let sink_waker = Arc::new(RepoWaker::PendingCloseSink(
                        self.wake_sender.clone(),
                        repo_id.clone(),
                    ));
                    let waker = waker_ref(&sink_waker);
                    let pinned_sink = Pin::new(&mut sink);
                    pinned_sink.poll_close(&mut Context::from_waker(&waker))
                };
                if matches!(result, Poll::Pending) {
                    tracing::trace!(remote_repo=?repo_id, "sink not ready to close");
                    entry.get_mut().push(sink);
                } else {
                    tracing::trace!(remote_repo=?repo_id, "sink closed");
                }
            }
            if entry.get().is_empty() {
                entry.remove_entry();
            }
        }
    }

    fn collect_sharepolicy_responses(&mut self) {
        let mut decisions = Vec::new();
        for repo_id in mem::take(&mut self.share_decisions_to_poll) {
            if let Some(pending) = self.pending_share_decisions.remove(&repo_id) {
                let mut still_pending = Vec::new();
                for PendingShareDecision {
                    doc_id,
                    mut future,
                    share_type,
                } in pending
                {
                    let waker = Arc::new(RepoWaker::ShareDecision(
                        self.wake_sender.clone(),
                        repo_id.clone(),
                    ));
                    let waker = waker_ref(&waker);
                    let pinned_fut = Pin::new(&mut future);
                    let result = pinned_fut.poll(&mut Context::from_waker(&waker));

                    match result {
                        Poll::Pending => {
                            still_pending.push(PendingShareDecision {
                                doc_id,
                                future,
                                share_type,
                            });
                        }
                        Poll::Ready(Ok(res)) => {
                            decisions.push((repo_id.clone(), doc_id, res, share_type))
                        }
                        Poll::Ready(Err(e)) => {
                            tracing::error!(err=?e, "error while polling share policy decision");
                        }
                    }
                }
                if !still_pending.is_empty() {
                    self.pending_share_decisions
                        .insert(repo_id.clone(), still_pending);
                }
            }
        }
        for (peer, doc, share_decision, share_type) in decisions {
            let our_id = self.get_repo_id().clone();
            let Some(info) = self.documents.get_mut(&doc) else {
                tracing::warn!(document=?doc, peer=?peer, "document not found when evaluating share policy decision result");
                return;
            };
            if share_decision == ShareDecision::Share {
                match share_type {
                    ShareType::Announce | ShareType::Request => {
                        tracing::debug!(%doc, remote=%peer, "sharing document with remote");
                        if let Some(pending_messages) = info.promote_pending_peer(&peer) {
                            tracing::trace!(remote=%peer, %doc, "we already had pending messages for this peer when announcing so we just wait to generate a sync message");
                            for message in pending_messages {
                                self.pending_events.push_back(NetworkEvent::Sync {
                                    from_repo_id: peer.clone(),
                                    to_repo_id: our_id.clone(),
                                    document_id: doc.clone(),
                                    message,
                                });
                            }
                        } else if let Some(message) = info.generate_first_sync_message(peer.clone())
                        {
                            tracing::trace!(remote=%peer, %doc, "sending first sync message");
                            let outgoing = NetworkMessage::Sync {
                                from_repo_id: our_id.clone(),
                                to_repo_id: peer.clone(),
                                document_id: doc.clone(),
                                message,
                            };
                            self.pending_messages
                                .entry(peer.clone())
                                .or_default()
                                .push_back(outgoing);
                            self.sinks_to_poll.insert(peer);
                        }
                    }
                    ShareType::Synchronize => {
                        tracing::debug!(%doc, remote=%peer, "synchronizing document with remote");
                        if let Some(pending_messages) = info.promote_pending_peer(&peer) {
                            let events =
                                pending_messages
                                    .into_iter()
                                    .map(|message| NetworkEvent::Sync {
                                        from_repo_id: peer.clone(),
                                        to_repo_id: our_id.clone(),
                                        document_id: doc.clone(),
                                        message,
                                    });
                            self.pending_events.extend(events);
                        }
                    }
                }
            } else {
                tracing::debug!(?doc, ?peer, "refusing to share document with remote");
            }
        }
    }

    /// The event-loop of the repo.
    /// Handles events from handles and adapters.
    /// Returns a handle for optional clean shutdown.
    #[tracing::instrument(skip(self), fields(self_id=%self.repo_id), level=tracing::Level::INFO)]
    pub fn run(mut self) -> RepoHandle {
        tracing::info!("starting repo event loop");
        let repo_sender = self.repo_sender.clone();
        let repo_id = self.repo_id.clone();

        let span = tracing::Span::current();
        // Run the repo's event-loop in a thread.
        // The repo shuts down
        // when the RepoEvent::Stop is received.
        let handle = thread::spawn(move || {
            let _entered = span.entered();
            loop {
                self.collect_sharepolicy_responses();
                self.collect_network_events();
                self.sync_documents();
                self.process_outgoing_network_messages();
                self.process_changed_document();
                self.remove_unused_sync_states();
                self.remove_unused_pending_messages();
                self.gc_docs();
                self.notify_listeners();
                if !self.pending_events.is_empty() || !self.share_decisions_to_poll.is_empty() {
                    continue;
                }
                select! {
                    recv(self.repo_receiver) -> repo_event => {
                        if let Ok(event) = repo_event {
                            match event {
                                RepoEvent::Stop => {
                                    tracing::info!("repo event loop stopping.");
                                    break
                                }
                                event => self.handle_repo_event(event),
                            }
                        } else {
                            // TODO: error in a future returned by `run`.
                            panic!("Repo handle dropped before calling `stop`");
                        }
                    },
                    recv(self.wake_receiver) -> event => {
                        if event.is_err() {
                            // Repo keeps a sender around, should never happen.
                            panic!("Wake senders dropped");
                        }
                        match event.unwrap() {
                            WakeSignal::Stream(repo_id) => {
                                self.streams_to_poll.insert(repo_id);
                            }
                            WakeSignal::Sink(repo_id) => {
                                self.sinks_to_poll.insert(repo_id);
                            }
                            WakeSignal::Storage(doc_id) => {
                                if let Some(info) = self.documents.get_mut(&doc_id) {
                                    info.poll_storage_operation(
                                        doc_id.clone(),
                                        &self.wake_sender,
                                        &self.repo_sender,
                                        &self.repo_id,
                                    );
                                    if info.state.should_sync() {
                                        let remotes = self.remote_repos.keys().filter(|k| !info.peer_connections.contains_key(k));
                                        // Send a sync message to all other repos we are connected
                                        // with and with whom we should share this document
                                        Self::enqueue_share_decisions(
                                            remotes,
                                            &mut self.pending_share_decisions,
                                            &mut self.share_decisions_to_poll,
                                            self.share_policy.as_ref(),
                                            doc_id.clone(),
                                            ShareType::Announce,
                                        );
                                    }
                                }
                            }
                            WakeSignal::PendingCloseSink(repo_id) => self.poll_close_sinks(repo_id),
                            WakeSignal::StorageList => self.process_pending_storage_list(),
                            WakeSignal::ShareDecision(repo_id) => {
                                self.share_decisions_to_poll.insert(repo_id);
                            }
                        }
                    },
                }
            }

            // Start of shutdown.
            self.error_pending_storage_list_for_shutdown();

            // Error all futures for all docs,
            // start to save them,
            // and mark them as pending removal.
            for doc_id in mem::take(&mut self.documents_with_changes) {
                if let Some(info) = self.documents.get_mut(&doc_id) {
                    info.resolve_change_observers(Err(RepoError::Shutdown));
                    info.save_document(doc_id.clone(), self.storage.as_ref(), &self.wake_sender);
                }
            }
            for (_, info) in self.documents.iter_mut() {
                info.state.resolve_any_fut_for_shutdown();
                info.start_pending_removal();
            }

            // close all open sinks
            let sinks_to_close: Vec<RepoId> = self
                .remote_repos
                .drain()
                .map(|(repo_id, remote_repo)| {
                    let pending = self.pending_close_sinks.entry(repo_id.clone()).or_default();
                    pending.push(remote_repo.sink);
                    repo_id
                })
                .collect();
            for repo_id in sinks_to_close {
                self.poll_close_sinks(repo_id);
            }

            // Ensure all docs are saved,
            // and all network sinks are closed.
            loop {
                // Remove docs that have been saved, or that errored.
                self.gc_docs();

                if self.documents.is_empty() && self.pending_close_sinks.is_empty() {
                    // Shutdown is done.
                    break;
                }
                tracing::trace!(
                num_docs_to_gc=?self.documents.len(),
                num_sinks_to_close=?self.pending_close_sinks.len(),
                "waiting for docs to be saved and sinks to close"
                );

                // Repo keeps sender around, should never drop.
                match self.wake_receiver.recv().expect("Wake senders dropped") {
                    WakeSignal::Stream(_) | WakeSignal::Sink(_) | WakeSignal::StorageList => {
                        continue
                    }
                    WakeSignal::PendingCloseSink(repo_id) => self.poll_close_sinks(repo_id),
                    WakeSignal::Storage(doc_id) => {
                        if let Some(info) = self.documents.get_mut(&doc_id) {
                            info.poll_storage_operation(
                                doc_id.clone(),
                                &self.wake_sender,
                                &self.repo_sender,
                                &self.repo_id,
                            );
                        }
                    }
                    WakeSignal::ShareDecision(_) => {}
                }
            }
            // Shutdown finished.
        });

        RepoHandle {
            handle: Arc::new(Mutex::new(Some(handle))),
            repo_id,
            repo_sender,
        }
    }

    /// Remove the sink corresponding to `repo_id` and poll_close it.
    ///
    /// ## Returns
    /// `true` if there was a sink to remove, `false` otherwise
    fn remove_sink(&mut self, repo_id: &RepoId, fin_reason: ConnFinishedReason) -> bool {
        if let Some(RemoteRepo { sink, .. }) = self.remote_repos.remove(repo_id) {
            let pending_sinks = self.pending_close_sinks.entry(repo_id.clone()).or_default();
            pending_sinks.push(sink);
            self.poll_close_sinks(repo_id.clone());
            if let Some(tx) = self.conn_closed_listeners.remove(repo_id) {
                let _ = tx.send(fin_reason);
            }
            true
        } else {
            false
        }
    }

    fn enqueue_share_decisions<'a, I: Iterator<Item = &'a RepoId>>(
        remote_repos: I,
        pending_share_decisions: &mut HashMap<RepoId, Vec<PendingShareDecision>>,
        share_decisions_to_poll: &mut HashSet<RepoId>,
        share_policy: &dyn SharePolicy,
        document_id: DocumentId,
        share_type: ShareType,
    ) {
        let remote_repos = remote_repos.collect::<Vec<_>>();
        match share_type {
            ShareType::Request => {
                tracing::debug!(remotes=?remote_repos, ?document_id, "checking if we should request this document from remotes");
            }
            ShareType::Announce => {
                tracing::debug!(remotes=?remote_repos, ?document_id, "checking if we should announce this document to remotes");
            }
            ShareType::Synchronize => {
                tracing::debug!(remotes=?remote_repos, ?document_id, "checking if we should synchronize this document with remotes");
            }
        }
        for repo_id in remote_repos {
            let future = match share_type {
                ShareType::Request => share_policy.should_request(&document_id, repo_id),
                ShareType::Announce => share_policy.should_announce(&document_id, repo_id),
                ShareType::Synchronize => share_policy.should_sync(&document_id, repo_id),
            };
            pending_share_decisions
                .entry(repo_id.clone())
                .or_default()
                .push(PendingShareDecision {
                    doc_id: document_id.clone(),
                    future,
                    share_type,
                });
            share_decisions_to_poll.insert(repo_id.clone());
        }
    }

    fn peer_conn_info(
        peer: &RepoId,
        documents: &HashMap<DocumentId, DocumentInfo>,
    ) -> PeerConnectionInfo {
        let mut last_received = None;
        let mut last_sent = None;
        let mut doc_states = HashMap::new();
        for (doc_id, doc) in documents {
            if let Some(state) = doc.get_peer_doc_state(peer) {
                last_received = last_received.max(state.last_received);
                last_sent = last_sent.max(state.last_sent);
                doc_states.insert(doc_id.clone(), state);
            }
        }
        PeerConnectionInfo {
            last_received,
            last_sent,
            docs: doc_states,
        }
    }

    fn notify_listeners(&mut self) {
        self.peer_listeners.retain_mut(|listener| {
            let peer_info = Self::peer_conn_info(&listener.remote, &self.documents);
            listener.send(peer_info)
        });
    }
}

struct PeerInfoListenerState {
    remote: RepoId,
    last_sent: PeerConnectionInfo,
    sender: mpsc::Sender<PeerConnectionInfo>,
}

impl PeerInfoListenerState {
    /// Returns false if the listener should be dropped due to a disconnected sender
    fn send(&mut self, new_state: PeerConnectionInfo) -> bool {
        if self.last_sent == new_state {
            return true;
        }
        self.last_sent = new_state.clone();
        !self
            .sender
            .try_send(new_state)
            .is_err_and(|e| e.is_disconnected())
    }
}
