use crate::dochandle::{DocHandle, SharedDocument};
use crate::interfaces::{DocumentId, EphemeralSessionId, RepoId};
use crate::interfaces::{NetworkError, RepoMessage, Storage, StorageError};
use crate::share_policy::ShareDecision;
use crate::{share_policy, EphemeralMessage, SharePolicy, SharePolicyError};
use automerge::sync::{Message as SyncMessage, State as SyncState};
use automerge::ChangeHash;
use core::pin::Pin;
use crossbeam_channel::{select, unbounded, Receiver, Sender};
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
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use uuid::Uuid;

mod request;

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

impl std::fmt::Display for RepoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RepoError::Shutdown => f.write_str("RepoError::Shutdown"),
            RepoError::Incorrect(s) => write!(f, "RepoError::Incorrect({})", s),
            RepoError::StorageError(e) => write!(f, "RepoError::StorageError({:?})", e),
        }
    }
}

impl std::error::Error for RepoError {}

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
    /// A repo requested a document
    Request {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
        message: SyncMessage,
    },
    Unavailable {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
    },
    Ephemeral {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
        message: EphemeralMessage,
        session_id: EphemeralSessionId,
        count: NonZeroU64,
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
            NetworkEvent::Request {
                from_repo_id,
                to_repo_id,
                document_id,
                message: _,
            } => f
                .debug_struct("NetworkEvent::Request")
                .field("from_repo_id", from_repo_id)
                .field("to_repo_id", to_repo_id)
                .field("document_id", document_id)
                .finish(),
            NetworkEvent::Unavailable {
                from_repo_id,
                to_repo_id,
                document_id,
            } => f
                .debug_struct("NetworkEvent::Unavailable")
                .field("from_repo_id", from_repo_id)
                .field("to_repo_id", to_repo_id)
                .field("document_id", document_id)
                .finish(),
            NetworkEvent::Ephemeral {
                from_repo_id,
                to_repo_id,
                document_id,
                message: _,
                session_id,
                count,
            } => f
                .debug_struct("NetworkEvent::Ephemeral")
                .field("from_repo_id", from_repo_id)
                .field("to_repo_id", to_repo_id)
                .field("document_id", document_id)
                .field("session_id", session_id)
                .field("count", count)
                .field("message", &"...")
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
    Request {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
        message: SyncMessage,
    },
    Unavailable {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
    },
    Ephemeral {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
        message: Vec<u8>,
        count: NonZeroU64,
        session_id: String,
    },
}

impl From<NetworkMessage> for RepoMessage {
    fn from(msg: NetworkMessage) -> Self {
        match msg {
            NetworkMessage::Sync {
                from_repo_id,
                to_repo_id,
                document_id,
                message,
            } => RepoMessage::Sync {
                from_repo_id,
                to_repo_id,
                document_id,
                message: message.encode(),
            },
            NetworkMessage::Request {
                from_repo_id,
                to_repo_id,
                document_id,
                message,
            } => RepoMessage::Request {
                sender_id: from_repo_id,
                target_id: to_repo_id,
                document_id,
                sync_message: message.encode(),
            },
            NetworkMessage::Unavailable {
                from_repo_id,
                to_repo_id,
                document_id,
            } => RepoMessage::Unavailable {
                document_id,
                sender_id: from_repo_id,
                target_id: to_repo_id,
            },
            NetworkMessage::Ephemeral {
                from_repo_id,
                to_repo_id,
                document_id,
                message,
                count,
                session_id,
            } => RepoMessage::Ephemeral {
                from_repo_id,
                to_repo_id,
                document_id,
                message,
                session_id: EphemeralSessionId::from(session_id.as_str()),
                count,
            },
        }
    }
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
    pub fn new_document(&self) -> impl Future<Output = DocHandle> {
        let document_id = DocumentId::random();
        let (future, resolver) = new_repo_future_with_resolver();
        self.repo_sender
            .send(RepoEvent::NewDoc(
                document_id,
                SharedDocument::new(),
                resolver,
            ))
            .expect("Failed to send repo event.");
        future
    }

    /// Boostrap a document, first from storage, and if not found over the network.
    pub fn request_document(
        &self,
        document_id: DocumentId,
    ) -> impl Future<Output = Result<Option<DocHandle>, RepoError>> {
        let (fut, resolver) = new_repo_future_with_resolver();
        self.repo_sender
            .send(RepoEvent::RequestDoc(document_id, resolver))
            .expect("Failed to send repo event.");
        fut
    }

    pub async fn find(&self, document_id: DocumentId) -> Result<Option<DocHandle>, RepoError> {
        if let Some(doc) = self.load(document_id.clone()).await? {
            return Ok(Some(doc));
        } else {
            self.request_document(document_id.clone()).await
        }
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

    /// Add a network adapter, representing a connection with a remote repo.
    pub fn new_remote_repo(
        &self,
        repo_id: RepoId,
        stream: Box<dyn Send + Unpin + Stream<Item = Result<RepoMessage, NetworkError>>>,
        sink: Box<dyn Send + Unpin + Sink<RepoMessage, Error = NetworkError>>,
    ) {
        self.repo_sender
            .send(RepoEvent::ConnectRemoteRepo {
                repo_id,
                stream,
                sink,
            })
            .expect("Failed to send repo event.");
    }
}

/// Events sent by repo or doc handles to the repo.
pub(crate) enum RepoEvent {
    /// Start processing a new document.
    NewDoc(DocumentId, SharedDocument, RepoFutureResolver<DocHandle>),
    /// Request a document we don't have
    RequestDoc(
        DocumentId,
        RepoFutureResolver<Result<Option<DocHandle>, RepoError>>,
    ),
    /// A document changed.
    DocChange(DocumentId),
    /// A new ephemeral message was published
    NewEphemeral(DocumentId),
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
    },
    SubscribeEphemeralStream {
        document_id: DocumentId,
        resolver: RepoFutureResolver<EphemeralStream>,
    },
    /// Stop the repo.
    Stop,
}

impl fmt::Debug for RepoEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RepoEvent::NewDoc(_, _, _) => f.write_str("RepoEvent::NewDoc"),
            RepoEvent::RequestDoc(_, _) => f.write_str("RepoEvent::RequestDoc"),
            RepoEvent::DocChange(_) => f.write_str("RepoEvent::DocChange"),
            RepoEvent::NewEphemeral(_) => f.write_str("RepoEvent::NewEphemeral"),
            RepoEvent::DocClosed(_) => f.write_str("RepoEvent::DocClosed"),
            RepoEvent::AddChangeObserver(_, _, _) => f.write_str("RepoEvent::AddChangeObserver"),
            RepoEvent::LoadDoc(_, _) => f.write_str("RepoEvent::LoadDoc"),
            RepoEvent::ListAllDocs(_) => f.write_str("RepoEvent::ListAllDocs"),
            RepoEvent::ConnectRemoteRepo { .. } => f.write_str("RepoEvent::ConnectRemoteRepo"),
            RepoEvent::SubscribeEphemeralStream { .. } => {
                f.write_str("RepoEvent::SubscribeEphemeralStream")
            }
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
        resolvers: Vec<RepoFutureResolver<Result<Option<DocHandle>, RepoError>>>,
        storage_fut: BootstrapStorageFut,
    },
    /// Pending a load from storage, not attempting to sync over network.
    LoadPending {
        resolvers: Vec<RepoFutureResolver<Result<Option<DocHandle>, RepoError>>>,
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

    fn resolve_bootstrap_fut(&mut self, doc_handle: Result<DocHandle, RepoError>) {
        match self {
            DocState::Bootstrap { resolvers, .. } => {
                for mut resolver in resolvers.drain(..) {
                    resolver.resolve_fut(doc_handle.clone().map(Some));
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
                resolvers,
                storage_fut: _,
            } => {
                for mut resolver in resolvers.drain(..) {
                    resolver.resolve_fut(doc_handle.clone());
                }
            }
            _ => unreachable!(
                "Trying to resolve a load future for a document that does not have one."
            ),
        }
    }

    fn resolve_any_fut_for_shutdown(&mut self) {
        match self {
            DocState::LoadPending {
                resolvers,
                storage_fut: _,
            } => {
                for mut resolver in resolvers.drain(..) {
                    resolver.resolve_fut(Err(RepoError::Shutdown))
                }
            }
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
                resolvers: _,
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
                            Poll::Ready(Err(_)) => {
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
                            Poll::Ready(Err(_)) => {
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

    outgoing_ephemera: Arc<Mutex<Vec<EphemeralMessage>>>,
}

/// A state machine representing a connection between a remote repo and a particular document
#[derive(Debug)]
enum PeerConnection {
    /// we've accepted the peer and are syncing with them
    Accepted(SyncState),
    /// We're waiting for a response from the share policy
    PendingAuth {
        /// Messages received while we were waiting for a response from the share policy
        received_messages: Vec<SyncMessage>,
    },
}

impl PeerConnection {
    fn pending() -> Self {
        PeerConnection::PendingAuth {
            received_messages: vec![],
        }
    }

    fn up_to_date(&self, doc: &SharedDocument) -> bool {
        if let Self::Accepted(SyncState {
            their_heads: Some(their_heads),
            ..
        }) = self
        {
            their_heads
                .iter()
                .all(|h| doc.get_change_by_hash(h).is_some())
        } else {
            false
        }
    }
}

/// A change requested by a peer connection
#[derive(Debug)]
enum PeerConnCommand {
    /// Request authorization from the share policy
    RequestAuth(RepoId, ShareType),
    SendRequest {
        message: SyncMessage,
        to: RepoId,
    },
    SendSyncMessage {
        message: SyncMessage,
        to: RepoId,
    },
}

impl DocumentInfo {
    fn new(
        state: DocState,
        document: Arc<RwLock<SharedDocument>>,
        handle_count: Arc<AtomicUsize>,
    ) -> Self {
        let last_heads = {
            let doc = document.read();
            doc.get_heads()
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
            outgoing_ephemera: Arc::new(Mutex::new(Vec::new())),
        }
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
                            doc.load_incremental(&val)
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
                        self.outgoing_ephemera.clone(),
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
                            doc.load_incremental(&val)
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
                        self.outgoing_ephemera.clone(),
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
            let changes = doc.get_changes(&self.last_heads);
            tracing::trace!(
                last_heads=?self.last_heads,
                current_heads=?doc.get_heads(),
                "checking for changes since last save"
            );
            changes.len()
        };
        let has_patches = count > 0;
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
            let (to_save, new_heads) = {
                let doc = self.document.read();
                (doc.save(), doc.get_heads())
            };
            self.changes_since_last_compact = 0;
            (storage.compact(document_id.clone(), to_save), new_heads)
        } else {
            let (to_save, new_heads) = {
                let doc = self.document.read();
                (
                    doc.save_after(&self.last_heads),
                    doc.get_heads(),
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
    /// A `Vec<PeerConnCommand>` which is a list of changes requested by the peer connections for
    /// this document (e.g. requesting authorization from the share policy).
    fn receive_sync_message<P, I>(&mut self, per_remote: P) -> Vec<PeerConnCommand>
    where
        P: IntoIterator<Item = (RepoId, I)>,
        I: IntoIterator<Item = SyncMessage>,
    {
        let mut commands = Vec::new();
        let mut document = self.document.write();
        for (repo_id, messages) in per_remote {
            let conn = match self.peer_connections.entry(repo_id.clone()) {
                Entry::Vacant(entry) => {
                    // if this is a new peer, request authorization
                    commands.push(PeerConnCommand::RequestAuth(
                        repo_id.clone(),
                        ShareType::Synchronize,
                    ));
                    entry.insert(PeerConnection::pending())
                }
                Entry::Occupied(entry) => entry.into_mut(),
            };
            match conn {
                PeerConnection::PendingAuth {
                    ref mut received_messages,
                } => {
                    received_messages.extend(messages);
                }
                PeerConnection::Accepted(ref mut sync_state) => {
                    for message in messages {
                        document
                            .receive_sync_message(sync_state, message)
                            .expect("Failed to receive sync message.");
                    }
                    if let Some(msg) = document.generate_sync_message(sync_state) {
                        commands.push(PeerConnCommand::SendSyncMessage {
                            message: msg,
                            to: repo_id.clone(),
                        });
                    }
                }
            }
        }
        commands
    }

    /// Generate outgoing sync message for all repos we are syncing with.
    fn generate_sync_messages(&mut self) -> Vec<(RepoId, SyncMessage)> {
        let document = self.document.read();
        self.peer_connections
            .iter_mut()
            .filter_map(|(repo_id, conn)| {
                if let PeerConnection::Accepted(ref mut sync_state) = conn {
                    let message = document.generate_sync_message(sync_state);
                    message.map(|msg| (repo_id.clone(), msg))
                } else {
                    None
                }
            })
            .collect()
    }

    fn begin_request(&mut self, remote: &RepoId) -> BeginRequest {
        match self.peer_connections.entry(remote.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(PeerConnection::pending());
                BeginRequest::RequiresAuth
            }
            Entry::Occupied(mut entry) => match entry.get_mut() {
                PeerConnection::PendingAuth { .. } => BeginRequest::AwaitingAuth,
                PeerConnection::Accepted(ref mut sync_state) => {
                    if sync_state.in_flight || sync_state.have_responded {
                        return BeginRequest::AlreadySyncing;
                    }
                    let document = self.document.read();
                    let message = document.generate_sync_message(sync_state);
                    if let Some(msg) = message {
                        BeginRequest::Request(msg)
                    } else {
                        BeginRequest::AlreadySyncing
                    }
                }
            },
        }
    }

    fn begin_requests<'a, I: Iterator<Item = &'a RepoId> + 'a>(
        &'a mut self,
        to_peers: I,
    ) -> impl Iterator<Item = PeerConnCommand> + 'a {
        to_peers.filter_map(|peer| match self.begin_request(peer) {
            BeginRequest::AlreadySyncing => {
                tracing::debug!(remote=%peer, "not sending request as we are already syncing");
                None
            }
            BeginRequest::Request(message) => Some(PeerConnCommand::SendRequest {
                message,
                to: peer.clone(),
            }),
            BeginRequest::AwaitingAuth => None,
            BeginRequest::RequiresAuth => Some(PeerConnCommand::RequestAuth(
                peer.clone(),
                ShareType::Request,
            )),
        })
    }

    fn authorize_peer(&mut self, remote: &RepoId) -> Option<SyncMessage> {
        if let Some(PeerConnection::PendingAuth { received_messages }) =
            self.peer_connections.remove(remote)
        {
            let mut doc = self.document.write();
            let mut sync_state = SyncState::new();
            for msg in received_messages {
                doc.receive_sync_message(&mut sync_state, msg)
                    .expect("Failed to receive sync message.");
            }
            let msg = doc.generate_sync_message(&mut sync_state);
            self.peer_connections
                .insert(remote.clone(), PeerConnection::Accepted(sync_state));
            msg
        } else if !self.peer_connections.contains_key(remote) {
            let mut sync_state = SyncState::new();
            let doc = self.document.write();
            let msg = doc.generate_sync_message(&mut sync_state);
            self.peer_connections
                .insert(remote.clone(), PeerConnection::Accepted(sync_state));
            msg
        } else {
            tracing::warn!(remote=%remote, "tried to authorize a peer which was not pending authorization");
            None
        }
    }

    fn has_up_to_date_peer(&self) -> bool {
        let doc = self.document.read();
        self.peer_connections
            .iter()
            .any(|(_, conn)| conn.up_to_date(&doc))
    }
}

enum BeginRequest {
    AlreadySyncing,
    Request(SyncMessage),
    RequiresAuth,
    AwaitingAuth,
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

    /// Session ID for ephemeral messages
    session_id: EphemeralSessionId,

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

    /// Outstanding requests
    requests: HashMap<DocumentId, request::Request>,

    /// outgoing ephemeral streams
    ephemeral_streams: HashMap<DocumentId, Vec<Weak<Mutex<EphemeralStreamInner>>>>,

    /// ephemeral sessions we have seen
    ephemeral_sessions: HashMap<EphemeralSessionId, NonZeroU64>,

    ephemeral_session_counter: NonZeroU64,
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
            session_id: EphemeralSessionId::new(),
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
            requests: HashMap::new(),
            ephemeral_streams: HashMap::new(),
            ephemeral_sessions: HashMap::new(),
            ephemeral_session_counter: NonZeroU64::new(1).unwrap(),
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
        let mut commands_by_doc = HashMap::new();
        for doc_id in mem::take(&mut self.documents_with_changes) {
            let Some(info) = self.documents.get_mut(&doc_id) else {
                continue;
            };

            if info.has_up_to_date_peer() && info.state.is_bootstrapping() {
                tracing::trace!(%doc_id, "bootstrapping complete");
                info.handle_count.fetch_add(1, Ordering::SeqCst);
                let handle = DocHandle::new(
                    self.repo_sender.clone(),
                    doc_id.clone(),
                    info.document.clone(),
                    info.handle_count.clone(),
                    self.repo_id.clone(),
                    info.outgoing_ephemera.clone(),
                );
                info.state.resolve_bootstrap_fut(Ok(handle));
                info.state = DocState::Sync(vec![]);

                if let Some(req) = self.requests.remove(&doc_id) {
                    tracing::trace!(%doc_id, "resolving request");
                    let awaiting_response = req.fulfilled();
                    let commands = info.receive_sync_message(
                        awaiting_response
                            .into_iter()
                            .map(|(repo, msg)| (repo, std::iter::once(msg))),
                    );
                    commands_by_doc.insert(doc_id.clone(), commands);
                }
            }

            if info.note_changes() {
                info.resolve_change_observers(Ok(()));
                info.save_document(doc_id.clone(), self.storage.as_ref(), &self.wake_sender);
            }
        }
        for (doc_id, commands) in commands_by_doc {
            self.dispatch_peer_conn_commands(&doc_id, commands)
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
            let should_be_removed = if let Some(remote_repo) = self.remote_repos.get_mut(&repo_id) {
                // Collect as many events as possible.
                loop {
                    let stream_waker =
                        Arc::new(RepoWaker::Stream(self.wake_sender.clone(), repo_id.clone()));
                    let waker = waker_ref(&stream_waker);
                    let pinned_stream = Pin::new(&mut remote_repo.stream);
                    let result = pinned_stream.poll_next(&mut Context::from_waker(&waker));
                    match result {
                        Poll::Pending => {
                            break false;
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
                                    break true;
                                }
                            },
                            Ok(RepoMessage::Request {
                                sender_id,
                                target_id,
                                document_id,
                                sync_message,
                            }) => match SyncMessage::decode(&sync_message) {
                                Ok(message) => {
                                    let event = NetworkEvent::Request {
                                        from_repo_id: sender_id,
                                        to_repo_id: target_id,
                                        document_id,
                                        message,
                                    };
                                    new_messages.push(event);
                                }
                                Err(e) => {
                                    tracing::error!(error = ?e, "error decoding sync message");
                                    break true;
                                }
                            },
                            Ok(RepoMessage::Unavailable {
                                document_id,
                                sender_id,
                                target_id,
                            }) => {
                                let event = NetworkEvent::Unavailable {
                                    document_id,
                                    from_repo_id: sender_id,
                                    to_repo_id: target_id,
                                };
                                new_messages.push(event);
                            }
                            Ok(RepoMessage::Ephemeral {
                                from_repo_id,
                                to_repo_id,
                                document_id,
                                message,
                                session_id,
                                count,
                            }) => {
                                let event = NetworkEvent::Ephemeral {
                                    from_repo_id: from_repo_id.clone(),
                                    to_repo_id,
                                    document_id,
                                    message: EphemeralMessage::new(message, from_repo_id),
                                    session_id,
                                    count,
                                };
                                new_messages.push(event);
                            }
                            Err(e) => {
                                tracing::error!(error = ?e, "Error on network stream.");
                                break true;
                            }
                        },
                        Poll::Ready(None) => {
                            tracing::info!(remote_repo_id=?repo_id, "remote stream closed, removing");
                            break true;
                        }
                    }
                }
            } else {
                continue;
            };
            if should_be_removed {
                self.remove_sink(&repo_id);
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
            let should_be_removed = if let Some(remote_repo) = self.remote_repos.get_mut(&repo_id) {
                // Send as many messages as possible.
                let mut needs_flush = false;
                let mut discard = false;
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
                            let msg = pending_messages
                                .pop_front()
                                .expect("Empty pending messages.");
                            let outgoing = RepoMessage::from(msg);
                            tracing::debug!(message = ?outgoing, remote=%repo_id, "sending message.");
                            let result = pinned_sink.start_send(outgoing);
                            if let Err(e) = result {
                                tracing::error!(error = ?e, "Error on network sink.");
                                discard = true;
                                needs_flush = false;
                                break;
                            }
                            needs_flush = true;
                        }
                        Poll::Ready(Err(_)) => {
                            discard = true;
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
                    if let Poll::Ready(Err(_)) =
                        pinned_sink.poll_flush(&mut Context::from_waker(&waker))
                    {
                        discard = true;
                    }
                }
                discard
            } else {
                continue;
            };
            if should_be_removed {
                self.remove_sink(&repo_id);
            }
        }
    }

    /// Handle incoming repo events(sent by repo or document handles).
    fn handle_repo_event(&mut self, event: RepoEvent) {
        tracing::trace!(event = ?event, "Handling repo event");
        match event {
            RepoEvent::NewDoc(document_id, document, mut resolver) => {
                assert!(
                    self.documents.get(&document_id).is_none(),
                    "NewDoc event should be sent with a fresh document ID and only be sent once"
                );
                let shared = Arc::new(RwLock::new(document));
                let handle_count = Arc::new(AtomicUsize::new(1));
                let info =
                    DocumentInfo::new(DocState::Sync(vec![]), shared.clone(), handle_count.clone());
                let outgoing_ephemera = info.outgoing_ephemera.clone();
                self.documents.insert(document_id.clone(), info);
                resolver.resolve_fut(DocHandle::new(
                    self.repo_sender.clone(),
                    document_id.clone(),
                    shared.clone(),
                    handle_count.clone(),
                    self.repo_id.clone(),
                    outgoing_ephemera,
                ));
                Self::enqueue_share_decisions(
                    self.remote_repos.keys(),
                    &mut self.pending_share_decisions,
                    &mut self.share_decisions_to_poll,
                    self.share_policy.as_ref(),
                    document_id,
                    ShareType::Announce,
                );
            }
            RepoEvent::RequestDoc(document_id, mut resolver) => {
                let info = self
                    .documents
                    .entry(document_id.clone())
                    .or_insert_with(|| {
                        let handle_count = Arc::new(AtomicUsize::new(1));
                        let storage_fut = self.storage.get(document_id.clone());
                        let mut info = DocumentInfo::new(
                            DocState::Bootstrap {
                                resolvers: vec![],
                                storage_fut: Some(storage_fut),
                            },
                            Arc::new(RwLock::new(SharedDocument::new())),
                            handle_count.clone(),
                        );
                        info.poll_storage_operation(
                            document_id.clone(),
                            &self.wake_sender,
                            &self.repo_sender,
                            &self.repo_id,
                        );

                        info
                    });

                match &mut info.state {
                    DocState::Bootstrap { resolvers, .. } => resolvers.push(resolver),
                    DocState::Sync(_) => {
                        info.handle_count.fetch_add(1, Ordering::SeqCst);
                        let handle = DocHandle::new(
                            self.repo_sender.clone(),
                            document_id.clone(),
                            info.document.clone(),
                            info.handle_count.clone(),
                            self.repo_id.clone(),
                            info.outgoing_ephemera.clone(),
                        );
                        resolver.resolve_fut(Ok(Some(handle)));
                    }
                    DocState::LoadPending { resolvers, .. } => resolvers.push(resolver),
                    DocState::PendingRemoval(_) => resolver.resolve_fut(Ok(None)),
                    DocState::Error => {
                        resolver.resolve_fut(Err(RepoError::Incorrect(
                            "request event called for document which is in error state".to_string(),
                        )));
                    }
                }

                let req = self.requests.entry(document_id.clone()).or_insert_with(|| {
                    tracing::trace!(%document_id, "creating new local request");
                    request::Request::new(document_id.clone())
                });

                if info.state.is_bootstrapping() {
                    let to_request = req.initiate_local(self.remote_repos.keys());
                    let commands = info.begin_requests(to_request.iter()).collect::<Vec<_>>();
                    self.dispatch_peer_conn_commands(&document_id, commands);
                }
            }
            RepoEvent::DocChange(doc_id) => {
                // Handle doc changes: sync the document.
                let local_repo_id = self.get_repo_id().clone();
                if let Some(info) = self.documents.get_mut(&doc_id) {
                    self.documents_with_changes.push(doc_id.clone());
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
            RepoEvent::NewEphemeral(doc_id) => {
                // broadcast this ephemeral message to all connect repos
                if let Some(info) = self.documents.get_mut(&doc_id) {
                    let mut lock = info.outgoing_ephemera.lock();
                    for message in lock.drain(..) {
                        for (to_repo_id, outbox) in &mut self.pending_messages {
                            let outgoing = NetworkMessage::Ephemeral {
                                from_repo_id: self.repo_id.clone(),
                                to_repo_id: to_repo_id.clone(),
                                document_id: doc_id.clone(),
                                message: message.clone().into_bytes(),
                                count: self.ephemeral_session_counter,
                                session_id: self.session_id.as_ref().into(),
                            };
                            self.ephemeral_session_counter =
                                self.ephemeral_session_counter.saturating_add(1);
                            outbox.push_back(outgoing);
                            self.sinks_to_poll.insert(to_repo_id.clone());
                        }
                    }
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
                let entry = self.documents.entry(doc_id.clone());
                let info = match entry {
                    Entry::Occupied(mut entry) => {
                        let info = entry.get_mut();
                        match &mut info.state {
                            DocState::Error => {
                                resolver_clone.resolve_fut(Err(RepoError::Incorrect(
                                    "load event called for document which is in error state"
                                        .to_string(),
                                )));
                            }
                            DocState::LoadPending { resolvers, .. } => {
                                resolvers.push(resolver_clone);
                            }
                            DocState::Bootstrap { resolvers, .. } => {
                                resolvers.push(resolver_clone);
                            }
                            DocState::Sync(_) => {
                                info.handle_count.fetch_add(1, Ordering::SeqCst);
                                let handle = DocHandle::new(
                                    self.repo_sender.clone(),
                                    doc_id.clone(),
                                    info.document.clone(),
                                    info.handle_count.clone(),
                                    self.repo_id.clone(),
                                    info.outgoing_ephemera.clone(),
                                );
                                resolver_clone.resolve_fut(Ok(Some(handle)));
                            }
                            DocState::PendingRemoval(_) => resolver_clone.resolve_fut(Ok(None)),
                        }
                        entry.into_mut()
                    }
                    Entry::Vacant(entry) => {
                        let storage_fut = self.storage.get(doc_id.clone());
                        let shared_document = SharedDocument::new();
                        let state = DocState::LoadPending {
                            storage_fut,
                            resolvers: vec![resolver_clone],
                        };
                        let document = Arc::new(RwLock::new(shared_document));
                        let handle_count = Arc::new(AtomicUsize::new(0));
                        entry.insert(DocumentInfo::new(state, document, handle_count))
                    }
                };

                if matches!(info.state, DocState::LoadPending { .. }) {
                    info.poll_storage_operation(
                        doc_id,
                        &self.wake_sender,
                        &self.repo_sender,
                        &self.repo_id,
                    );
                }
            }
            RepoEvent::AddChangeObserver(doc_id, last_heads, mut observer) => {
                if let Some(info) = self.documents.get_mut(&doc_id) {
                    let current_heads = {
                        let state = info.document.read();
                        state.get_heads()
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
            } => {
                if self.remove_sink(&repo_id) {
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
                self.streams_to_poll.insert(repo_id);
            }
            RepoEvent::SubscribeEphemeralStream {
                document_id,
                mut resolver,
            } => {
                let stream = EphemeralStream::new();
                self.ephemeral_streams
                    .entry(document_id.clone())
                    .or_default()
                    .push(Arc::downgrade(&stream.inner));
                resolver.resolve_fut(stream);
            }
            RepoEvent::Stop => {
                // Handled in the main run loop.
            }
        }
    }

    fn new_document_info() -> DocumentInfo {
        // Note: since the handle count is zero,
        // the document will not be removed from memory until shutdown.
        // Perhaps remove this and rely on `request_document` calls.
        let shared_document = SharedDocument::new();
        let state = DocState::Bootstrap {
            resolvers: vec![],
            storage_fut: None,
        };
        let document = Arc::new(RwLock::new(shared_document));
        let handle_count = Arc::new(AtomicUsize::new(0));
        DocumentInfo::new(state, document, handle_count)
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
                    let info = self
                        .documents
                        .entry(document_id.clone())
                        .or_insert_with(Self::new_document_info);

                    if !info.state.should_sync() {
                        continue;
                    }

                    let per_doc = per_doc_messages.entry(document_id.clone()).or_default();
                    let per_remote = per_doc.entry(from_repo_id.clone()).or_default();
                    per_remote.push_back(message.clone());
                }
                NetworkEvent::Request {
                    from_repo_id,
                    to_repo_id,
                    document_id,
                    message,
                } => {
                    assert_eq!(to_repo_id, self.repo_id);
                    let info = self
                        .documents
                        .entry(document_id.clone())
                        .or_insert_with(Self::new_document_info);
                    match info.state {
                        DocState::Sync(_) => {
                            tracing::trace!(
                                ?from_repo_id,
                                "responding to request with sync as we have the doc"
                            );
                            // if we have this document then just start syncing
                            Self::enqueue_share_decisions(
                                std::iter::once(&from_repo_id),
                                &mut self.pending_share_decisions,
                                &mut self.share_decisions_to_poll,
                                self.share_policy.as_ref(),
                                document_id.clone(),
                                ShareType::Synchronize,
                            );
                        }
                        _ => {
                            let req =
                                self.requests.entry(document_id.clone()).or_insert_with(|| {
                                    tracing::trace!(?from_repo_id, "creating new remote request");
                                    request::Request::new(document_id.clone())
                                });

                            let request_from = req.initiate_remote(
                                &from_repo_id,
                                message,
                                self.remote_repos.keys(),
                            );
                            let commands =
                                info.begin_requests(request_from.iter()).collect::<Vec<_>>();

                            if req.is_complete() {
                                let req = self.requests.remove(&document_id).unwrap();
                                Self::fail_request(
                                    req,
                                    &mut self.documents,
                                    &mut self.pending_messages,
                                    &mut self.sinks_to_poll,
                                    self.repo_id.clone(),
                                );
                            }

                            self.dispatch_peer_conn_commands(&document_id, commands.into_iter());
                        }
                    }
                }
                NetworkEvent::Unavailable {
                    from_repo_id,
                    to_repo_id: _,
                    document_id,
                } => match self.requests.entry(document_id.clone()) {
                    Entry::Occupied(mut entry) => {
                        let req = entry.get_mut();
                        req.mark_unavailable(&from_repo_id);
                        if req.is_complete() {
                            let req = entry.remove();
                            Self::fail_request(
                                req,
                                &mut self.documents,
                                &mut self.pending_messages,
                                &mut self.sinks_to_poll,
                                self.repo_id.clone(),
                            );
                        }
                    }
                    Entry::Vacant(_) => {
                        tracing::trace!(?from_repo_id, "received unavailable for request we didnt send or are no longer tracking");
                    }
                },
                NetworkEvent::Ephemeral {
                    from_repo_id,
                    to_repo_id,
                    document_id,
                    message,
                    session_id,
                    count,
                } => {
                    assert_eq!(to_repo_id, self.repo_id);
                    match self.ephemeral_sessions.entry(session_id.clone()) {
                        Entry::Occupied(mut entry) => {
                            if entry.get() >= &count {
                                tracing::trace!(
                                    ?from_repo_id,
                                    "received duplicate ephemeral message"
                                );
                                continue;
                            } else {
                                entry.insert(count);
                            }
                        }
                        Entry::Vacant(entry) => {
                            tracing::trace!(
                                ?from_repo_id,
                                "received ephemeral message with unknown session id"
                            );
                            entry.insert(count);
                        }
                    };
                    if let Some(outgoing_streams) = self.ephemeral_streams.get_mut(&document_id) {
                        outgoing_streams.retain_mut(|stream| {
                            if let Some(inner) = stream.upgrade() {
                                let mut inner = inner.lock();
                                inner.outbox.push_back(message.clone());
                                if let Some(waker) = &inner.waker {
                                    waker.wake_by_ref();
                                }
                                true
                            } else {
                                tracing::debug!(%document_id, "removing dead ephemeral stream");
                                false
                            }
                        })
                    }
                    for (repo_id, pending_messages) in self.pending_messages.iter_mut() {
                        if repo_id == &from_repo_id {
                            continue;
                        }
                        let outgoing = NetworkMessage::Ephemeral {
                            from_repo_id: self.repo_id.clone(),
                            to_repo_id: repo_id.clone(),
                            document_id: document_id.clone(),
                            message: message.clone().into_bytes(),
                            count,
                            session_id: session_id.as_ref().into(),
                        };
                        pending_messages.push_back(outgoing);
                        self.sinks_to_poll.insert(repo_id.clone());
                    }
                }
            }
        }

        for (document_id, per_remote) in per_doc_messages {
            let info = self
                .documents
                .get_mut(&document_id)
                .expect("Doc should have an info by now.");

            let peer_conn_commands = info.receive_sync_message(per_remote);
            self.documents_with_changes.push(document_id.clone());

            self.dispatch_peer_conn_commands(&document_id, peer_conn_commands.into_iter());
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

    fn poll_sharepolicy_responses(&mut self) {
        let mut decisions = Vec::new();
        for repo_id in mem::take(&mut self.share_decisions_to_poll) {
            if let Some(pending) = self.pending_share_decisions.remove(&repo_id) {
                let mut still_pending = Vec::new();
                for PendingShareDecision {
                    doc_id,
                    share_type,
                    mut future,
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
                                share_type,
                                future,
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
                let message = info.authorize_peer(&peer);
                self.documents_with_changes.push(doc.clone());
                let outgoing = message.map(|message| match share_type {
                    ShareType::Announce => {
                        tracing::trace!(remote=%peer, %doc, "announcing document to remote");
                        NetworkMessage::Sync {
                            from_repo_id: our_id.clone(),
                            to_repo_id: peer.clone(),
                            document_id: doc.clone(),
                            message,
                        }
                    }
                    ShareType::Request => {
                        tracing::trace!(remote=%peer, %doc, "requesting document from remote");
                        NetworkMessage::Request {
                            from_repo_id: our_id.clone(),
                            to_repo_id: peer.clone(),
                            document_id: doc.clone(),
                            message,
                        }
                    }
                    ShareType::Synchronize => {
                        tracing::debug!(%doc, remote=%peer, "synchronizing document with remote");
                        NetworkMessage::Sync {
                            from_repo_id: our_id.clone(),
                            to_repo_id: peer.clone(),
                            document_id: doc.clone(),
                            message,
                        }
                    }
                });
                if let Some(outgoing) = outgoing {
                    self.pending_messages
                        .entry(peer.clone())
                        .or_default()
                        .push_back(outgoing);
                    self.sinks_to_poll.insert(peer);
                }
            } else {
                match share_type {
                    ShareType::Request => {
                        tracing::debug!(%doc, remote=%peer, "refusing to request document from remote");
                    }
                    ShareType::Announce => {
                        tracing::debug!(%doc, remote=%peer, "refusing to announce document to remote");
                    }
                    ShareType::Synchronize => {
                        tracing::debug!(%doc, remote=%peer, "refusing to synchronize document with remote");
                    }
                }
                if let Some(req) = self.requests.get_mut(&doc) {
                    tracing::trace!(request=?req, "marking request as unavailable due to rejected authorization");
                    req.mark_unavailable(&peer);
                    if req.is_complete() {
                        let req = self.requests.remove(&doc).unwrap();
                        Self::fail_request(
                            req,
                            &mut self.documents,
                            &mut self.pending_messages,
                            &mut self.sinks_to_poll,
                            self.repo_id.clone(),
                        );
                    }
                }
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
                self.poll_sharepolicy_responses();
                self.collect_network_events();
                self.sync_documents();
                self.process_outgoing_network_messages();
                self.process_changed_document();
                self.remove_unused_sync_states();
                self.remove_unused_pending_messages();
                self.gc_docs();
                if !self.share_decisions_to_poll.is_empty() {
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
    fn remove_sink(&mut self, repo_id: &RepoId) -> bool {
        if let Some(RemoteRepo { sink, .. }) = self.remote_repos.remove(repo_id) {
            let pending_sinks = self.pending_close_sinks.entry(repo_id.clone()).or_default();
            pending_sinks.push(sink);
            self.poll_close_sinks(repo_id.clone());
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
        if remote_repos.is_empty() {
            return;
        }
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
                    share_type,
                    future,
                });
            share_decisions_to_poll.insert(repo_id.clone());
        }
    }

    fn fail_request(
        request: request::Request,
        documents: &mut HashMap<DocumentId, DocumentInfo>,
        pending_messages: &mut HashMap<RepoId, VecDeque<NetworkMessage>>,
        sinks_to_poll: &mut HashSet<RepoId>,
        our_repo_id: RepoId,
    ) {
        tracing::debug!(?request, "request is complete");

        match documents.entry(request.document_id().clone()) {
            Entry::Occupied(entry) => {
                if entry.get().state.is_bootstrapping() {
                    let info = entry.remove();
                    if let DocState::Bootstrap { mut resolvers, .. } = info.state {
                        for mut resolver in resolvers.drain(..) {
                            tracing::trace!("resolving local process waiting for request to None");
                            resolver.resolve_fut(Ok(None));
                        }
                    }
                }
            }
            Entry::Vacant(_) => {
                tracing::trace!("no local proess is waiting for this request to complete");
            }
        }

        let document_id = request.document_id().clone();
        for repo_id in request.unavailable() {
            let outgoing = NetworkMessage::Unavailable {
                from_repo_id: our_repo_id.clone(),
                to_repo_id: repo_id.clone(),
                document_id: document_id.clone(),
            };
            pending_messages
                .entry(repo_id.clone())
                .or_default()
                .push_back(outgoing);
            sinks_to_poll.insert(repo_id.clone());
        }
    }

    fn dispatch_peer_conn_commands<I: IntoIterator<Item = PeerConnCommand>>(
        &mut self,
        document_id: &DocumentId,
        commands: I,
    ) {
        for command in commands {
            match command {
                PeerConnCommand::RequestAuth(peer_id, share_type) => {
                    Self::enqueue_share_decisions(
                        std::iter::once(&peer_id),
                        &mut self.pending_share_decisions,
                        &mut self.share_decisions_to_poll,
                        self.share_policy.as_ref(),
                        document_id.clone(),
                        share_type,
                    );
                }
                PeerConnCommand::SendRequest { message, to } => {
                    let outgoing = NetworkMessage::Request {
                        from_repo_id: self.repo_id.clone(),
                        to_repo_id: to.clone(),
                        document_id: document_id.clone(),
                        message,
                    };
                    self.pending_messages
                        .entry(to.clone())
                        .or_default()
                        .push_back(outgoing);
                    self.sinks_to_poll.insert(to);
                }
                PeerConnCommand::SendSyncMessage { message, to } => {
                    let outgoing = NetworkMessage::Sync {
                        from_repo_id: self.repo_id.clone(),
                        to_repo_id: to.clone(),
                        document_id: document_id.clone(),
                        message,
                    };
                    self.pending_messages
                        .entry(to.clone())
                        .or_default()
                        .push_back(outgoing);
                    self.sinks_to_poll.insert(to);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct EphemeralStream {
    inner: Arc<Mutex<EphemeralStreamInner>>,
}

impl EphemeralStream {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(EphemeralStreamInner {
                outbox: VecDeque::new(),
                waker: None,
            })),
        }
    }
}

struct EphemeralStreamInner {
    outbox: VecDeque<EphemeralMessage>,
    waker: Option<std::task::Waker>,
}

impl Stream for EphemeralStream {
    type Item = EphemeralMessage;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut inner = self.inner.lock();
        if let Some(msg) = inner.outbox.pop_front() {
            Poll::Ready(Some(msg))
        } else {
            // store the waker for the repo to call later
            let waker = cx.waker();
            inner.waker = Some(waker.clone());
            Poll::Pending
        }
    }
}
