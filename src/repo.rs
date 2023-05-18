use crate::dochandle::{DocHandle, DocState, SharedDocument};
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
use futures::stream::Stream;
use futures::task::ArcWake;
use futures::task::{waker_ref, Context, Poll};
use futures::Sink;
use parking_lot::{Condvar, Mutex};
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use uuid::Uuid;

/// Front-end of the repo.
#[derive(Debug)]
pub struct RepoHandle {
    handle: JoinHandle<()>,
    /// Channel used to send events back to the repo,
    /// such as when a doc is created, and a doc handle acquired.
    repo_sender: Sender<RepoEvent>,
    repo_id: RepoId,

    /// Counter to generate unique document ids.
    document_id_counter: u64,
}

#[derive(Debug)]
pub struct RepoError;

fn new_document_with_observer() -> AutoCommitWithObs<Observed<VecOpObserver>> {
    let document = AutoCommit::new();
    document.with_observer(VecOpObserver::default())
}

impl RepoHandle {
    pub fn stop(self) -> Result<(), RepoError> {
        self.repo_sender
            .send(RepoEvent::Stop)
            .expect("Failed to send repo event.");
        self.handle.join().expect("Failed to join on repo.");
        Ok(())
    }

    pub fn get_repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    /// Create a new document.
    pub fn new_document(&mut self) -> DocHandle {
        self.document_id_counter = self
            .document_id_counter
            .checked_add(1)
            .expect("Overflowed when creating new document id.");
        let document_id = DocumentId((self.repo_id.clone(), self.document_id_counter));
        let document = new_document_with_observer();
        self.new_document_handle(None, document_id, document, DocState::Sync)
    }

    /// Boostrap a document using it's ID only.
    /// The returned document should not be edited until ready,
    /// use `DocHandle.wait_ready` to wait for it.
    pub fn bootstrap_document_from_id(&self, repo_id: Option<RepoId>, document_id: DocumentId) {
        let document = new_document_with_observer();
        // If no repo id is provided, sync with the creator.
        let repo_id = repo_id.unwrap_or_else(|| document_id.get_repo_id().clone());
        let _ = self.new_document_handle(Some(repo_id), document_id, document, DocState::Bootstrap);
    }

    fn new_document_handle(
        &self,
        repo_id: Option<RepoId>,
        document_id: DocumentId,
        document: AutoCommitWithObs<Observed<VecOpObserver>>,
        state: DocState,
    ) -> DocHandle {
        let is_ready = matches!(state, DocState::Sync);
        let shared_document = SharedDocument {
            state: DocState::Bootstrap,
            automerge: document,
        };
        let state = Arc::new(Mutex::new(shared_document));
        let handle_count = Arc::new(AtomicUsize::new(1));
        let handle = DocHandle::new(
            self.repo_sender.clone(),
            document_id.clone(),
            state.clone(),
            handle_count.clone(),
            self.repo_id.clone(),
        );
        let doc_info = DocumentInfo {
            state,
            handle_count,
            sync_states: Default::default(),
            is_ready,
            patches_since_last_save: 0,
        };
        self.repo_sender
            .send(RepoEvent::NewDoc(repo_id, document_id, doc_info))
            .expect("Failed to send repo event.");
        handle
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
    /// Load a document,
    // repo id is the repo to start syncing with,
    // none if locally created.
    NewDoc(Option<RepoId>, DocumentId, DocumentInfo),
    /// A document changed.
    DocChange(DocumentId),
    /// A document was closed(all doc handles dropped).
    DocClosed(DocumentId),
    ConnectNetworkAdapter(RepoId, Box<dyn NetworkAdapter<Error = NetworkError>>),
    Stop,
}

/// Info about a document.
#[derive(Debug)]
pub(crate) struct DocumentInfo {
    /// State of the document(shared with handles).
    /// Document used to apply and generate sync messages.
    state: Arc<Mutex<SharedDocument>>,
    /// Ref count for handles(shared with handles).
    handle_count: Arc<AtomicUsize>,
    /// Per repo automerge sync state.
    sync_states: HashMap<RepoId, SyncState>,
    /// Flag set to true once the doc reaches `DocState::Sync`.
    is_ready: bool,
    patches_since_last_save: usize,
}

impl DocumentInfo {
    /// Mark the document as ready for editing,
    /// wakes-up all doc handles that are waiting inside `wait_ready`.
    fn set_ready(&mut self) {
        if self.is_ready {
            return;
        }
        let mut state = self.state.lock();
        state.state = DocState::Sync;
        self.is_ready = true;
    }

    fn note_changes(&mut self) {
        let mut state = self.state.lock();
        let observer = state.automerge.observer();
        let _ = self
            .patches_since_last_save
            .checked_add(observer.take_patches().len());
    }

    fn save_document(&mut self, document_id: DocumentId, storage: &dyn StorageAdapter) {
        if self.patches_since_last_save > 10 {
            storage.compact(document_id);
        } else {
            let to_save = {
                let mut state = self.state.lock();
                state.automerge.save_incremental()
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
        let mut state = self.state.lock();
        let mut sync = state.automerge.sync();
        sync.receive_sync_message(sync_state, message)
            .expect("Failed to apply sync message.");
    }

    /// Potentially generate an outgoing sync message.
    fn generate_first_sync_message(&mut self, repo_id: RepoId) -> Option<SyncMessage> {
        let sync_state = self
            .sync_states
            .entry(repo_id)
            .or_insert_with(SyncState::new);
        let mut state = self.state.lock();
        let msg = state.automerge.sync().generate_sync_message(sync_state);
        msg
    }

    /// Generate outgoing sync message for all repos we are syncing with.
    fn generate_sync_messages(&mut self) -> Vec<(RepoId, SyncMessage)> {
        self.sync_states
            .iter_mut()
            .filter_map(|(repo_id, sync_state)| {
                let mut state = self.state.lock();
                let message = state.automerge.sync().generate_sync_message(sync_state);
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
            panic!("Wake sender disconnected.");
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

    /// Callback on applying sync messages.
    sync_observer: Option<Box<dyn Fn(Vec<DocHandle>) + Send>>,

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
    pub fn new(
        sync_observer: Option<Box<dyn Fn(Vec<DocHandle>) + Send>>,
        repo_id: Option<String>,
        storage: Box<dyn StorageAdapter>,
    ) -> Self {
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
            sync_observer,
            pending_saves: Default::default(),
            storage,
        }
    }

    fn get_repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    /// Called on start-up.
    fn load_locally_stored_documents(&mut self) {
        let new_docs = self
            .storage
            .load_all()
            .into_iter()
            .map(|(doc_id, data)| {
                let document = new_document_with_observer();
                let shared_document = SharedDocument {
                    state: DocState::Sync,
                    automerge: document,
                };
                let state = Arc::new(Mutex::new(shared_document));
                let handle_count = Arc::new(AtomicUsize::new(1));
                let handle = DocHandle::new(
                    self.repo_sender.clone(),
                    doc_id.clone(),
                    state.clone(),
                    handle_count.clone(),
                    self.repo_id.clone(),
                );
                let doc_info = DocumentInfo {
                    state,
                    handle_count,
                    sync_states: Default::default(),
                    is_ready: true,
                    patches_since_last_save: 0,
                };
                self.documents.insert(doc_id, doc_info);
                handle
            })
            .collect();
        self.notify_synced(new_docs);
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
            RepoEvent::NewDoc(repo_id, document_id, mut info) => {
                // If this is a bootsrapped document.
                if !(document_id.get_repo_id() == self.get_repo_id()) {
                    // Send a sync message to all other repos we are connected with.
                    let mut repo_ids: Vec<RepoId> = self.network_adapters.keys().cloned().collect();
                    if let Some(repo_id) = repo_id {
                        repo_ids.push(repo_id);
                    }
                    for repo_id in repo_ids {
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
                            self.sinks_to_poll.insert(repo_id);
                        }
                    }
                }
                self.documents.insert(document_id, info);
            }
            RepoEvent::DocChange(doc_id) => {
                // Handle doc changes: sync the document.
                if let Some(info) = self.documents.get_mut(&doc_id) {
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
        let mut synced = vec![];
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
                                state: DocState::Bootstrap,
                                automerge: new_document_with_observer(),
                            };
                            let state = Arc::new(Mutex::new(shared_document));
                            let handle_count = Arc::new(AtomicUsize::new(0));
                            DocumentInfo {
                                state,
                                handle_count,
                                sync_states: Default::default(),
                                is_ready: true,
                                patches_since_last_save: 0,
                            }
                        });

                    // Create a handle and pass it to the sync observer.
                    info.handle_count.fetch_add(1, Ordering::SeqCst);
                    let handle = DocHandle::new(
                        self.repo_sender.clone(),
                        document_id.clone(),
                        info.state.clone(),
                        info.handle_count.clone(),
                        self.repo_id.clone(),
                    );
                    synced.push(handle);
                    info.note_changes();
                    self.pending_saves.push(document_id.clone());

                    info.receive_sync_message(from_repo_id, message);
                    // Note: since receiving and generating sync messages is done
                    // in two separate critical sections,
                    // local changes could be made in between those,
                    // which is a good thing(generated messages will include those changes).
                    for (to_repo_id, message) in info.generate_sync_messages().into_iter() {
                        if !message.heads.is_empty() && message.need.is_empty() {
                            info.set_ready();
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
                }
            }
        }
        self.notify_synced(synced);
    }

    // TODO: better name? Also called when loading from storage on startup.
    fn notify_synced(&self, synced: Vec<DocHandle>) {
        // Notify the client of synced documents.
        if let Some(observer) = self.sync_observer.as_ref().filter(|_| !synced.is_empty()) {
            observer(synced);
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
            self.load_locally_stored_documents();
            loop {
                // Poll ready streams and sinks at the start of each iteration.
                self.collect_network_events();
                self.sync_documents();
                self.process_outgoing_network_messages();
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
            handle,
            document_id_counter,
            repo_id,
            repo_sender,
        }
    }
}
