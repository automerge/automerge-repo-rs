use crate::dochandle::{DocHandle, SharedDocument};
use crate::interfaces::{DocumentId, RepoId};
use crate::interfaces::{
    NetworkAdapter, NetworkError, NetworkEvent, NetworkMessage, StorageAdapter,
};
use automerge::sync::{Message as SyncMessage, State as SyncState, SyncDoc};
use automerge::transaction::Observed;
use automerge::VecOpObserver;
use automerge::{AutoCommit, AutoCommitWithObs};
use core::pin::Pin;
use crossbeam_channel::{select, unbounded, Receiver, Sender};
use futures::future::Future;
use futures::stream::Stream;
use futures::task::ArcWake;
use futures::task::{waker_ref, Context, Poll, Waker};
use futures::Sink;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use uuid::Uuid;

/// Front-end of the repo.
#[derive(Debug, Clone)]
pub struct RepoHandle {
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// Channel used to send events back to the repo,
    /// such as when a doc is created, and a doc handle acquired.
    repo_sender: Sender<RepoEvent>,
    repo_id: RepoId,

    /// Counter to generate unique document ids.
    document_id_counter: Arc<AtomicU64>,
}

#[derive(Debug)]
pub struct RepoError;

// TODO: remove observer.
fn new_document_with_observer() -> AutoCommitWithObs<Observed<VecOpObserver>> {
    let document = AutoCommit::new();
    document.with_observer(VecOpObserver::default())
}

#[derive(Debug)]
pub struct DocHandleFuture {
    doc_handle: Arc<Mutex<Option<DocHandle>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl DocHandleFuture {
    fn new(doc_handle: Arc<Mutex<Option<DocHandle>>>, waker: Arc<Mutex<Option<Waker>>>) -> Self {
        DocHandleFuture { doc_handle, waker }
    }
}

impl Future for DocHandleFuture {
    type Output = DocHandle;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        *self.waker.lock() = Some(cx.waker().clone());
        let mut result = self.doc_handle.lock();
        result.take().map_or(Poll::Pending, Poll::Ready)
    }
}

impl RepoHandle {
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

    /// Create a new document.
    pub fn new_document(&mut self) -> DocHandle {
        let counter = self.document_id_counter.fetch_add(1, Ordering::SeqCst);
        let document_id = DocumentId((self.repo_id.clone(), counter));
        let document = new_document_with_observer();
        let doc_info = self.new_document_info(document, DocState::LocallyCreatedNotEdited);
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
        handle
    }

    /// Boostrap a document using it's ID only.
    /// The returned document should not be edited until ready,
    /// use `DocHandle.wait_ready` to wait for it.
    pub fn request_document(
        &self,
        // TODO: use PeerSpec concept.
        document_id: DocumentId,
    ) -> DocHandleFuture {
        let document = new_document_with_observer();
        let fut_doc_handle = Arc::new(Mutex::new(None));
        let fut_waker = Arc::new(Mutex::new(None));
        let future_handle = DocHandleFuture::new(fut_doc_handle.clone(), fut_waker.clone());
        let doc_info = self.new_document_info(
            document,
            DocState::Bootstrap {
                fut_doc_handle,
                fut_waker,
            },
        );
        self.repo_sender
            .send(RepoEvent::NewDoc(document_id, doc_info))
            .expect("Failed to send repo event.");
        future_handle
    }

    fn new_document_info(
        &self,
        document: AutoCommitWithObs<Observed<VecOpObserver>>,
        state: DocState,
    ) -> DocumentInfo {
        let document = SharedDocument {
            automerge: document,
        };
        let document = Arc::new(Mutex::new(document));
        let handle_count = Arc::new(AtomicUsize::new(1));
        DocumentInfo {
            state,
            document,
            handle_count,
            sync_states: Default::default(),
            patches_since_last_save: 0,
        }
    }

    pub fn new_network_adapter(
        &self,
        repo_id: RepoId,
        network_adapter: Box<dyn NetworkAdapter<Error = NetworkError>>,
    ) {
        self.repo_sender
            .send(RepoEvent::ConnectNetworkAdapter(repo_id, network_adapter))
            .expect("Failed to send repo event.");
    }
}

/// Events sent by repo or doc handles to the repo.
pub(crate) enum RepoEvent {
    /// Start processing a new document.
    NewDoc(DocumentId, DocumentInfo),
    /// A document changed.
    DocChange(DocumentId),
    /// A document was closed(all doc handles dropped).
    DocClosed(DocumentId),
    ConnectNetworkAdapter(RepoId, Box<dyn NetworkAdapter<Error = NetworkError>>),
    Stop,
}

/// The doc info state machine.
#[derive(Clone, Debug)]
pub(crate) enum DocState {
    /// Bootstrapping will resolve into a future doc handle.
    Bootstrap {
        fut_doc_handle: Arc<Mutex<Option<DocHandle>>>,
        fut_waker: Arc<Mutex<Option<Waker>>>,
    },
    /// A document that has been locally created,
    /// and not edited yet,
    /// should not be synced until it has been.
    LocallyCreatedNotEdited,
    /// The doc is syncing(can be edited locally).
    Sync,
}

impl DocState {
    fn is_bootstrapping(&self) -> bool {
        matches!(self, DocState::Bootstrap { .. })
    }

    fn should_sync(&self) -> bool {
        matches!(self, DocState::Sync)
    }

    fn resolve_fut(&mut self, doc_handle: DocHandle) {
        match self {
            DocState::Bootstrap {
                fut_doc_handle,
                fut_waker,
            } => {
                *fut_doc_handle.lock() = Some(doc_handle);
                if let Some(fut_waker) = fut_waker.lock().take() {
                    fut_waker.wake();
                }
            }
            _ => panic!("Trying to resolve a future for a document that is not boostrapping."),
        }
        *self = DocState::Sync
    }
}

/// Info about a document.
#[derive(Debug)]
pub(crate) struct DocumentInfo {
    /// State of the document.
    state: DocState,
    /// Document used to apply and generate sync messages(shared with handles).
    document: Arc<Mutex<SharedDocument>>,
    /// Ref count for handles(shared with handles).
    handle_count: Arc<AtomicUsize>,
    /// Per repo automerge sync state.
    sync_states: HashMap<RepoId, SyncState>,
    patches_since_last_save: usize,
}

impl DocumentInfo {
    fn is_boostrapping(&self) -> bool {
        self.state.is_bootstrapping()
    }

    fn note_changes(&mut self) {
        let mut doc = self.document.lock();
        let observer = doc.automerge.observer();
        let _ = self
            .patches_since_last_save
            .checked_add(observer.take_patches().len());
    }

    fn save_document(&mut self, document_id: DocumentId, storage: &dyn StorageAdapter) {
        if self.patches_since_last_save > 10 {
            storage.compact(document_id);
        } else {
            let to_save = {
                let mut doc = self.document.lock();
                doc.automerge.save_incremental()
            };
            storage.append(document_id, to_save);
        }
        self.patches_since_last_save = 0;
    }

    /// Apply incoming sync messages.
    fn receive_sync_message(&mut self, repo_id: RepoId, message: SyncMessage) {
        let sync_state = self
            .sync_states
            .entry(repo_id)
            .or_insert_with(SyncState::new);
        let mut document = self.document.lock();
        let mut sync = document.automerge.sync();
        sync.receive_sync_message(sync_state, message)
            .expect("Failed to apply sync message.");
    }

    /// Potentially generate an outgoing sync message.
    fn generate_first_sync_message(&mut self, repo_id: RepoId) -> Option<SyncMessage> {
        let sync_state = self
            .sync_states
            .entry(repo_id)
            .or_insert_with(SyncState::new);
        let mut document = self.document.lock();
        let msg = document.automerge.sync().generate_sync_message(sync_state);
        msg
    }

    /// Generate outgoing sync message for all repos we are syncing with.
    fn generate_sync_messages(&mut self) -> Vec<(RepoId, SyncMessage)> {
        self.sync_states
            .iter_mut()
            .filter_map(|(repo_id, sync_state)| {
                let mut document = self.document.lock();
                let message = document.automerge.sync().generate_sync_message(sync_state);
                message.map(|msg| (repo_id.clone(), msg))
            })
            .collect()
    }
}

/// Signal that the stream or sink on the network adapter is ready to be polled.
#[derive(Debug)]
enum WakeSignal {
    Stream(RepoId),
    Sink(RepoId),
}

/// Waking mechanism for stream and sinks.
#[derive(Debug)]
enum RepoWaker {
    Stream(Sender<WakeSignal>, RepoId),
    Sink(Sender<WakeSignal>, RepoId),
}

/// https://docs.rs/futures/latest/futures/task/trait.ArcWake.html
impl ArcWake for RepoWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let res = match &**arc_self {
            RepoWaker::Stream(sender, repo_id) => sender.send(WakeSignal::Stream(repo_id.clone())),
            RepoWaker::Sink(sender, repo_id) => sender.send(WakeSignal::Sink(repo_id.clone())),
        };
        if res.is_err() {
            // TODO: clean shutdown.
            println!("Wake sender disconnected.");
        }
    }
}

/// The backend of a repo: the repo runs an event-loop in a background thread.
pub struct Repo {
    /// The Id of the repo.
    repo_id: RepoId,

    network_adapters: HashMap<RepoId, Box<dyn NetworkAdapter<Error = NetworkError>>>,
    documents: HashMap<DocumentId, DocumentInfo>,

    /// Messages to send on the network adapter sink.
    pending_messages: HashMap<RepoId, VecDeque<NetworkMessage>>,
    /// Events received on the network stream, pending processing.
    pending_events: VecDeque<NetworkEvent>,

    /// Receiver of network stream and sink readiness signals.
    wake_receiver: Receiver<WakeSignal>,
    wake_sender: Sender<WakeSignal>,

    streams_to_poll: HashSet<RepoId>,
    sinks_to_poll: HashSet<RepoId>,

    /// Sender and receiver of repo events.
    repo_sender: Sender<RepoEvent>,
    repo_receiver: Receiver<RepoEvent>,

    pending_saves: Vec<DocumentId>,
    storage: Box<dyn StorageAdapter>,
}

impl Repo {
    /// Create a new repo.
    pub fn new(repo_id: Option<String>, storage: Box<dyn StorageAdapter>) -> Self {
        let (wake_sender, wake_receiver) = unbounded();
        let (repo_sender, repo_receiver) = unbounded();
        let repo_id = repo_id.map_or_else(|| RepoId(Uuid::new_v4().to_string()), RepoId);
        Repo {
            repo_id,
            documents: Default::default(),
            network_adapters: Default::default(),
            wake_receiver,
            wake_sender,
            streams_to_poll: Default::default(),
            sinks_to_poll: Default::default(),
            pending_messages: Default::default(),
            pending_events: Default::default(),
            repo_sender,
            repo_receiver,
            pending_saves: Default::default(),
            storage,
        }
    }

    fn get_repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    /// Save documents that have changed to storage.
    fn save_changed_document(&mut self) {
        for doc_id in mem::take(&mut self.pending_saves) {
            if let Some(info) = self.documents.get_mut(&doc_id) {
                info.save_document(doc_id, self.storage.as_ref());
            }
        }
    }

    /// Remove sync states for repos for which we do not have an adapter anymore.
    fn remove_unused_sync_states(&mut self) {
        for document_info in self.documents.values_mut() {
            document_info
                .sync_states
                .drain_filter(|repo_id, _| !self.network_adapters.contains_key(repo_id));
        }
    }

    /// Remove pending messages for repos for which we do not have an adapter anymore.
    fn remove_unused_pending_messages(&mut self) {
        self.pending_messages
            .drain_filter(|repo_id, _| !self.network_adapters.contains_key(repo_id));
    }

    /// Poll the network adapter stream.
    fn collect_network_events(&mut self) {
        // Receive incoming message on streams,
        // discarding streams that error.
        let to_poll = mem::take(&mut self.streams_to_poll);
        for repo_id in to_poll {
            let should_be_removed =
                if let Some(mut network_adapter) = self.network_adapters.get_mut(&repo_id) {
                    // Collect as many events as possible.
                    loop {
                        let stream_waker =
                            Arc::new(RepoWaker::Stream(self.wake_sender.clone(), repo_id.clone()));
                        let waker = waker_ref(&stream_waker);
                        let pinned_stream = Pin::new(&mut network_adapter);
                        let result = pinned_stream.poll_next(&mut Context::from_waker(&waker));
                        match result {
                            Poll::Pending => break false,
                            Poll::Ready(Some(event)) => self.pending_events.push_back(event),
                            Poll::Ready(None) => break true,
                        }
                    }
                } else {
                    continue;
                };
            if should_be_removed {
                self.network_adapters.remove(&repo_id);
            }
        }
    }

    /// Try to send pending messages on the network sink.
    fn process_outgoing_network_messages(&mut self) {
        // Send outgoing message on sinks,
        // discarding sinks that error.
        let to_poll = mem::take(&mut self.sinks_to_poll);
        for repo_id in to_poll {
            let should_be_removed =
                if let Some(mut network_adapter) = self.network_adapters.get_mut(&repo_id) {
                    // Send as many messages as possible.
                    let mut needs_flush = false;
                    let mut discard = false;
                    loop {
                        let pending_messages = self
                            .pending_messages
                            .entry(repo_id.clone())
                            .or_insert_with(Default::default);
                        if pending_messages.is_empty() {
                            break;
                        }
                        let sink_waker =
                            Arc::new(RepoWaker::Sink(self.wake_sender.clone(), repo_id.clone()));
                        let waker = waker_ref(&sink_waker);
                        let pinned_sink = Pin::new(&mut network_adapter);
                        let result = pinned_sink.poll_ready(&mut Context::from_waker(&waker));
                        match result {
                            Poll::Pending => break,
                            Poll::Ready(Ok(())) => {
                                let pinned_sink = Pin::new(&mut network_adapter);
                                let result = pinned_sink.start_send(
                                    pending_messages
                                        .pop_front()
                                        .expect("Empty pending messages."),
                                );
                                if result.is_err() {
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

                    // Flusk the sink if any messages have been sent.
                    if needs_flush {
                        let sink_waker =
                            Arc::new(RepoWaker::Sink(self.wake_sender.clone(), repo_id.clone()));
                        let waker = waker_ref(&sink_waker);
                        let pinned_sink = Pin::new(&mut network_adapter);
                        let _ = pinned_sink.poll_flush(&mut Context::from_waker(&waker));
                    }
                    discard
                } else {
                    continue;
                };
            if should_be_removed {
                self.network_adapters.remove(&repo_id);
            }
        }
    }

    /// Handle incoming repo events(sent by repo or document handles).
    fn handle_repo_event(&mut self, event: RepoEvent) {
        match event {
            RepoEvent::NewDoc(document_id, mut info) => {
                // If this is a bootsrapped document.
                if info.is_boostrapping() {
                    // TODO: check local storage first.

                    // Send a sync message to all other repos we are connected with.
                    for repo_id in self.network_adapters.keys() {
                        if let Some(message) = info.generate_first_sync_message(repo_id.clone()) {
                            let outgoing = NetworkMessage::Sync {
                                from_repo_id: self.get_repo_id().clone(),
                                to_repo_id: repo_id.clone(),
                                document_id: document_id.clone(),
                                message,
                            };
                            self.pending_messages
                                .entry(repo_id.clone())
                                .or_insert_with(Default::default)
                                .push_back(outgoing);
                            self.sinks_to_poll.insert(repo_id.clone());
                        }
                    }
                }
                self.documents.insert(document_id, info);
            }
            RepoEvent::DocChange(doc_id) => {
                // Handle doc changes: sync the document.
                if let Some(info) = self.documents.get_mut(&doc_id) {
                    info.state = DocState::Sync;
                    info.note_changes();
                    self.pending_saves.push(doc_id.clone());
                    for (to_repo_id, message) in info.generate_sync_messages().into_iter() {
                        let outgoing = NetworkMessage::Sync {
                            from_repo_id: self.get_repo_id().clone(),
                            to_repo_id: to_repo_id.clone(),
                            document_id: doc_id.clone(),
                            message,
                        };
                        self.pending_messages
                            .entry(to_repo_id.clone())
                            .or_insert_with(Default::default)
                            .push_back(outgoing);
                        self.sinks_to_poll.insert(to_repo_id);
                    }
                }
            }
            RepoEvent::DocClosed(id) => {
                // Handle doc closed: remove the document info.
                let doc_info = self
                    .documents
                    .remove(&id)
                    .expect("Document closed but not doc info found.");
                if doc_info.handle_count.load(Ordering::SeqCst) != 0 {
                    panic!("Document closed with outstanding handles.");
                }
            }
            RepoEvent::ConnectNetworkAdapter(repo_id, adapter) => {
                self.network_adapters
                    .entry(repo_id.clone())
                    .and_modify(|_| {
                        // TODO: close the existing stream/sink?
                    })
                    .or_insert(adapter);

                // Try to sync all docs we know about.
                let our_id = self.get_repo_id().clone();
                for (document_id, info) in self.documents.iter_mut() {
                    if !info.state.should_sync() {
                        // Do not sync docs that have been locally created
                        // and not locally edited yet,
                        // as well as those that are bootstrapping.
                        continue;
                    }
                    if let Some(message) = info.generate_first_sync_message(repo_id.clone()) {
                        let outgoing = NetworkMessage::Sync {
                            from_repo_id: our_id.clone(),
                            to_repo_id: repo_id.clone(),
                            document_id: document_id.clone(),
                            message,
                        };
                        self.pending_messages
                            .entry(repo_id.clone())
                            .or_insert_with(Default::default)
                            .push_back(outgoing);
                    }
                }
                self.sinks_to_poll.insert(repo_id.clone());
                self.streams_to_poll.insert(repo_id);
            }
            RepoEvent::Stop => {
                // Handled in the main run loop.
            }
        }
    }

    /// Apply incoming sync messages, and generate outgoing ones.
    fn sync_documents(&mut self) {
        // Process incoming events.
        // Handle events.
        for event in mem::take(&mut self.pending_events) {
            match event {
                NetworkEvent::Sync {
                    from_repo_id,
                    to_repo_id: local_repo_id,
                    document_id,
                    message,
                } => {
                    // If we don't know about the document,
                    // create a new sync state and start syncing.
                    // Note: this is the mirror of sending sync messages for
                    // all known documents when a remote repo connects.
                    let info = self
                        .documents
                        .entry(document_id.clone())
                        .or_insert_with(|| {
                            let shared_document = SharedDocument {
                                automerge: new_document_with_observer(),
                            };
                            let state = DocState::Sync;
                            let document = Arc::new(Mutex::new(shared_document));
                            let handle_count = Arc::new(AtomicUsize::new(0));
                            DocumentInfo {
                                state,
                                document,
                                handle_count,
                                sync_states: Default::default(),
                                patches_since_last_save: 0,
                            }
                        });

                    info.note_changes();
                    self.pending_saves.push(document_id.clone());

                    info.receive_sync_message(from_repo_id, message);
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
                            from_repo_id: local_repo_id.clone(),
                            to_repo_id: to_repo_id.clone(),
                            document_id: document_id.clone(),
                            message,
                        };
                        self.pending_messages
                            .entry(to_repo_id.clone())
                            .or_insert_with(Default::default)
                            .push_back(outgoing);
                        self.sinks_to_poll.insert(to_repo_id);
                    }
                    if ready {
                        // Create a handle and pass it to the sync observer.
                        info.handle_count.fetch_add(1, Ordering::SeqCst);
                        let handle = DocHandle::new(
                            self.repo_sender.clone(),
                            document_id.clone(),
                            info.document.clone(),
                            info.handle_count.clone(),
                            self.repo_id.clone(),
                        );
                        if info.state.is_bootstrapping() {
                            info.state.resolve_fut(handle);
                        }
                    }
                }
            }
        }
    }

    /// The event-loop of the repo.
    /// Handles events from handles and adapters.
    /// Returns a handle for optional clean shutdown.
    pub fn run(mut self) -> RepoHandle {
        let repo_sender = self.repo_sender.clone();
        let repo_id = self.repo_id.clone();
        let document_id_counter = Default::default();

        // Run the repo's event-loop in a thread.
        // The repo shuts down
        // when the RepoEvent::Stop is received.
        let handle = thread::spawn(move || {
            loop {
                // Poll ready streams and sinks at the start of each iteration.
                self.collect_network_events();
                self.sync_documents();
                self.process_outgoing_network_messages();
                self.save_changed_document();
                self.remove_unused_sync_states();
                self.remove_unused_pending_messages();
                select! {
                    recv(self.repo_receiver) -> repo_event => {
                        if let Ok(event) = repo_event {
                            match event {
                                RepoEvent::Stop => break,
                                event => self.handle_repo_event(event),
                            }
                        } else {
                            panic!("Repo handle dropped before calling `stop`");
                        }
                    },
                    recv(self.wake_receiver) -> event => {
                        if event.is_err() {
                            panic!("Wake senders dropped");
                        }
                        match event.unwrap() {
                            WakeSignal::Stream(repo_id) => {
                                self.streams_to_poll.insert(repo_id);
                            },
                            WakeSignal::Sink(repo_id) => {
                                self.sinks_to_poll.insert(repo_id);
                            },
                        }
                    },
                }
            }
            // TODO: close sinks and streams?
        });
        RepoHandle {
            handle: Arc::new(Mutex::new(Some(handle))),
            document_id_counter: Arc::new(document_id_counter),
            repo_id,
            repo_sender,
        }
    }
}
