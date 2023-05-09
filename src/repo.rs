use crate::dochandle::{DocHandle, DocState};
use crate::interfaces::{CollectionId, DocumentId, RepoId};
use crate::interfaces::{NetworkAdapter, NetworkEvent, NetworkMessage};
use automerge::sync::{Message as SyncMessage, State as SyncState, SyncDoc};
use automerge::AutoCommit;
use core::pin::Pin;
use crossbeam_channel::{bounded, select, unbounded, Receiver, Sender, TrySendError};
use futures::stream::Stream;
use futures::task::ArcWake;
use futures::task::{waker_ref, Context, Poll};
use futures::Sink;
use parking_lot::{Condvar, Mutex};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use uuid::Uuid;

/// A collection of documents,
/// through which new docs can be created,
/// and doc handles acquired.
#[derive(Debug)]
pub struct DocCollection {
    /// Channel used to send events back to the repo,
    /// such as when a doc is created, and a doc handle acquired.
    collection_sender: Sender<(CollectionId, CollectionEvent)>,
    collection_id: CollectionId,

    /// Counter to generate unique document ids.
    document_id_counter: u64,
}

impl DocCollection {
    pub fn get_repo_id(&self) -> &RepoId {
        &self.collection_id.0 .0
    }

    /// Create a new document.
    pub fn new_document(&mut self) -> DocHandle {
        self.document_id_counter = self
            .document_id_counter
            .checked_add(1)
            .expect("Overflowed when creating new document id.");
        let document_id = DocumentId((self.collection_id, self.document_id_counter));
        let document = AutoCommit::new();
        self.new_document_handle(None, document_id, document, DocState::Sync)
    }

    /// Load an existing document, assumed to be ready for editing.
    pub fn load_existing_document(
        &self,
        repo_id: RepoId,
        document_id: DocumentId,
        document: AutoCommit,
    ) -> DocHandle {
        self.new_document_handle(Some(repo_id), document_id, document, DocState::Sync)
    }

    /// Boostrap a document using it's ID only.
    /// The returned document should not be edited until ready,
    /// use `DocHandle.wait_ready` to wait for it.
    pub fn bootstrap_document_from_id(
        &self,
        repo_id: Option<RepoId>,
        document_id: DocumentId,
    ) -> DocHandle {
        let document = AutoCommit::new();
        // If no repo id is provided, sync with the creator.
        let repo_id = repo_id.unwrap_or_else(|| *document_id.get_repo_id());
        self.new_document_handle(Some(repo_id), document_id, document, DocState::Bootstrap)
    }

    fn new_document_handle(
        &self,
        repo_id: Option<RepoId>,
        document_id: DocumentId,
        document: AutoCommit,
        state: DocState,
    ) -> DocHandle {
        let is_ready = matches!(state, DocState::Sync);
        let state = Arc::new((Mutex::new((state, document)), Condvar::new()));
        let handle_count = Arc::new(AtomicUsize::new(1));
        let handle = DocHandle::new(
            self.collection_sender.clone(),
            document_id,
            self.collection_id,
            state.clone(),
            handle_count.clone(),
            is_ready,
        );
        let doc_info = DocumentInfo {
            state,
            handle_count,
            sync_states: Default::default(),
            is_ready,
        };
        self.collection_sender
            .send((
                self.collection_id,
                CollectionEvent::NewDocHandle(repo_id, document_id, doc_info),
            ))
            .expect("Failed to send collection event.");
        handle
    }
}

/// Events sent by doc collections or doc handles to the repo.
#[derive(Debug)]
pub(crate) enum CollectionEvent {
    /// Load a document,
    // repo id is the repo to start syncing with,
    // none if locally created.
    NewDocHandle(Option<RepoId>, DocumentId, DocumentInfo),
    /// A document changed.
    DocChange(DocumentId),
    /// A document was closed(all doc handles dropped).
    DocClosed(DocumentId),
}

/// Information on a doc collection held by the repo.
/// Each collection can be configured with different adapters.
struct CollectionInfo<T: NetworkAdapter> {
    network_adapter: Box<T>,
    documents: HashMap<DocumentId, DocumentInfo>,

    stream_waker: Arc<CollectionWaker>,
    sink_waker: Arc<CollectionWaker>,

    /// Messages to send on the network adapter sink.
    pending_messages: VecDeque<NetworkMessage>,
    /// Events received on the network stream, pending processing.
    pending_events: VecDeque<NetworkEvent>,

    /// Callback on applying sync messages.
    sync_observer: Option<Box<dyn Fn(Vec<DocumentId>) + Send>>,
}

/// Info about a document, held by the repo(via CollectionInfo).
#[derive(Debug)]
pub(crate) struct DocumentInfo {
    /// State of the document(shared with handles).
    /// Document used to apply and generate sync messages.
    state: Arc<(Mutex<(DocState, AutoCommit)>, Condvar)>,
    /// Ref count for handles(shared with handles).
    handle_count: Arc<AtomicUsize>,
    /// Per repo automerge sync state.
    sync_states: HashMap<RepoId, SyncState>,
    /// Flag set to true once the doc reaches `DocState::Sync`.
    is_ready: bool,
}

impl DocumentInfo {
    /// Mark the document as ready for editing,
    /// wakes-up all doc handles that are waiting inside `wait_ready`.
    fn set_ready(&mut self) {
        if self.is_ready {
            return;
        }
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock();
        state.0 = DocState::Sync;
        cvar.notify_all();
        self.is_ready = true;
    }

    /// Apply incoming sync messages.
    fn receive_sync_message(&mut self, repo_id: RepoId, message: SyncMessage) {
        let sync_state = self.sync_states.entry(repo_id).or_insert(SyncState::new());
        let (lock, _cvar) = &*self.state;
        let mut state = lock.lock();
        let mut sync = state.1.sync();
        sync.receive_sync_message(sync_state, message)
            .expect("Failed to apply sync message.");
    }

    /// Potentially generate an outgoing sync message.
    fn generate_first_sync_message(&mut self, repo_id: RepoId) -> Option<SyncMessage> {
        let sync_state = self.sync_states.entry(repo_id).or_insert(SyncState::new());
        let (lock, _cvar) = &*self.state;
        lock.lock().1.sync().generate_sync_message(sync_state)
    }

    /// Generate outgoing sync message for all repos we are syncing with.
    fn generate_sync_messages(&mut self) -> Vec<(RepoId, SyncMessage)> {
        self.sync_states
            .iter_mut()
            .filter_map(|(repo_id, sync_state)| {
                let (lock, _cvar) = &*self.state;
                let message = lock.lock().1.sync().generate_sync_message(sync_state);
                message.map(|msg| (*repo_id, msg))
            })
            .collect()
    }
}

/// Signal that for a given collection,
/// the stream or sink on the network adapter is ready to be polled.
enum WakeSignal {
    Stream(CollectionId),
    Sink(CollectionId),
}

/// Waking mechanism for stream and sinks.
enum CollectionWaker {
    Stream(Sender<WakeSignal>, CollectionId),
    Sink(Sender<WakeSignal>, CollectionId),
}

/// https://docs.rs/futures/latest/futures/task/trait.ArcWake.html
impl ArcWake for CollectionWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let err = match &**arc_self {
            CollectionWaker::Stream(sender, id) => sender.try_send(WakeSignal::Stream(*id)),
            CollectionWaker::Sink(sender, id) => sender.try_send(WakeSignal::Sink(*id)),
        };
        if let Err(TrySendError::Disconnected(_)) = err {
            panic!("Wake sender disconnected.");
        }
    }
}

/// The backend of doc collections: the repo runs an event-loop in a background thread.
pub struct Repo<T: NetworkAdapter> {
    /// The Id of the repo.
    repo_id: RepoId,
    /// Counter to generate unique collection ids.
    collection_id_counter: u64,

    /// A map of collections to their info.
    collections: HashMap<CollectionId, CollectionInfo<T>>,

    /// Sender use to signal network stream and sink readiness.
    /// A sender is kept around to clone for multiple calls to `new_collection`.
    wake_sender: Sender<WakeSignal>,
    wake_receiver: Receiver<WakeSignal>,

    /// Sender and receiver of collection events.
    /// A sender is kept around to clone for multiple calls to `new_collection`,
    /// the sender is dropped at the start of the repo's event-loop,
    /// to ensure that the event-loop stops
    /// once all collections and doc handles have been dropped.
    collection_sender: Option<Sender<(CollectionId, CollectionEvent)>>,
    collection_receiver: Receiver<(CollectionId, CollectionEvent)>,
}

impl<T: NetworkAdapter + 'static> Repo<T> {
    /// Create a new repo, specifying a
    /// max number of collections that can be created for this repo.
    pub fn new(max_number_collections: usize) -> Self {
        // Bound the channels by the max number of collections times two,
        // ensuring sending on them as part of polling sink and streams never blocks.
        let (wake_sender, wake_receiver) = bounded(max_number_collections * 2);
        let (collection_sender, collection_receiver) = unbounded();
        Repo {
            repo_id: RepoId(Uuid::new_v4()),
            collection_id_counter: Default::default(),
            collections: Default::default(),
            wake_sender,
            wake_receiver,
            collection_sender: Some(collection_sender),
            collection_receiver,
        }
    }

    pub fn get_repo_id(&self) -> RepoId {
        self.repo_id
    }

    /// Create a new doc collection, with a storage and a network adapter.
    /// Note: all collections must be created before starting to run the repo.
    pub fn new_collection(
        &mut self,
        network_adapter: T,
        sync_observer: Option<Box<dyn Fn(Vec<DocumentId>) + Send>>,
    ) -> DocCollection {
        self.collection_id_counter = self
            .collection_id_counter
            .checked_add(1)
            .expect("Overflowed when creating new collection id.");
        let collection_id = CollectionId((self.repo_id, self.collection_id_counter));
        let collection = DocCollection {
            collection_sender: self
                .collection_sender
                .clone()
                .expect("No collection sender."),
            collection_id,
            document_id_counter: Default::default(),
        };
        let stream_waker = Arc::new(CollectionWaker::Stream(
            self.wake_sender.clone(),
            collection_id,
        ));
        let sink_waker = Arc::new(CollectionWaker::Sink(
            self.wake_sender.clone(),
            collection_id,
        ));
        let collection_info = CollectionInfo {
            network_adapter: Box::new(network_adapter),
            documents: Default::default(),
            stream_waker,
            sink_waker,
            pending_messages: Default::default(),
            pending_events: Default::default(),
            sync_observer,
        };
        self.collections.insert(collection_id, collection_info);
        collection
    }

    /// Poll the network adapter stream for a given collection,
    /// handle the incoming events.
    fn collect_network_events(&mut self, collection_id: &CollectionId) {
        let collection = self
            .collections
            .get_mut(collection_id)
            .expect("Unexpected collection event.");

        // Collect as many events as possible.
        loop {
            let waker = waker_ref(&collection.stream_waker);
            let pinned_stream = Pin::new(&mut collection.network_adapter);
            let result = pinned_stream.poll_next(&mut Context::from_waker(&waker));
            match result {
                Poll::Pending => break,
                Poll::Ready(Some(event)) => collection.pending_events.push_back(event),
                Poll::Ready(None) => return,
            }
        }
    }

    /// Try to send pending messages on network sinks for a given collection.
    fn process_outgoing_network_messages(&mut self, collection_id: &CollectionId) {
        let collection = self
            .collections
            .get_mut(collection_id)
            .expect("Unexpected collection event.");

        // Send as many messages as possible.
        let mut needs_flush = false;
        loop {
            if collection.pending_messages.is_empty() {
                break;
            }
            let waker = waker_ref(&collection.sink_waker);
            let pinned_sink = Pin::new(&mut collection.network_adapter);
            let result = pinned_sink.poll_ready(&mut Context::from_waker(&waker));
            match result {
                Poll::Pending => break,
                Poll::Ready(Ok(())) => {
                    let pinned_sink = Pin::new(&mut collection.network_adapter);
                    let result = pinned_sink.start_send(
                        collection
                            .pending_messages
                            .pop_front()
                            .expect("Empty pending messages."),
                    );
                    if result.is_err() {
                        return;
                    }
                    needs_flush = true;
                }
                Poll::Ready(Err(_)) => return,
            }
        }

        // Flusk the sink if any messages have been sent.
        if needs_flush {
            let waker = waker_ref(&collection.sink_waker);
            let pinned_sink = Pin::new(&mut collection.network_adapter);
            let _ = pinned_sink.poll_flush(&mut Context::from_waker(&waker));
        }
    }

    /// Handle incoming collection events(sent by colleciton or document handles).
    fn handle_collection_event(&mut self, collection_id: &CollectionId, event: CollectionEvent) {
        match event {
            CollectionEvent::NewDocHandle(repo_id, document_id, mut info) => {
                // A new doc handle has been created.
                let collection = self
                    .collections
                    .get_mut(collection_id)
                    .expect("Unexpected collection event.");

                // Send a sync message to the creator, unless it is the local repo.
                if let Some(repo_id) = repo_id {
                    if let Some(message) = info.generate_first_sync_message(repo_id) {
                        let outgoing = NetworkMessage::Sync {
                            from_repo_id: *collection_id.get_repo_id(),
                            to_repo_id: repo_id,
                            document_id,
                            message,
                        };
                        collection.pending_messages.push_back(outgoing);
                    }
                }
                collection.documents.insert(document_id, info);
            }
            CollectionEvent::DocChange(doc_id) => {
                // Handle doc changes: sync the document.
                let collection = self
                    .collections
                    .get_mut(collection_id)
                    .expect("Unexpected collection event.");
                if let Some(info) = collection.documents.get_mut(&doc_id) {
                    for (to_repo_id, message) in info.generate_sync_messages().into_iter() {
                        let outgoing = NetworkMessage::Sync {
                            from_repo_id: *collection_id.get_repo_id(),
                            to_repo_id,
                            document_id: doc_id,
                            message,
                        };
                        collection.pending_messages.push_back(outgoing);
                    }
                }
            }
            CollectionEvent::DocClosed(id) => {
                // Handle doc closed: remove the document info.
                let collection = self
                    .collections
                    .get_mut(collection_id)
                    .expect("Unexpected collection event.");
                let doc_info = collection
                    .documents
                    .remove(&id)
                    .expect("Document closed but not doc info found.");
                if doc_info.handle_count.load(Ordering::SeqCst) != 0 {
                    panic!("Document closed with outstanding handles.");
                }
            }
        }
    }

    /// Apply incoming sync messages, and generate outgoing ones.
    fn sync_document_collection(&mut self, collection_id: &CollectionId) {
        let collection = self
            .collections
            .get_mut(collection_id)
            .expect("Unexpected collection event.");

        // Process incoming events.
        // Handle events.
        let mut synced = vec![];
        for event in mem::take(&mut collection.pending_events) {
            match event {
                NetworkEvent::Sync {
                    from_repo_id,
                    to_repo_id: local_repo_id,
                    document_id,
                    message,
                } => {
                    if let Some(info) = collection.documents.get_mut(&document_id) {
                        info.receive_sync_message(from_repo_id, message);
                        synced.push(document_id);
                        // Note: since receiving and generating sync messages is done
                        // in two separate critical sections,
                        // local changes could be made in between those,
                        // which is a good thing(generated messages will include those changes).
                        for (to_repo_id, message) in info.generate_sync_messages().into_iter() {
                            if !message.heads.is_empty() && message.need.is_empty() {
                                info.set_ready();
                            }
                            let outgoing = NetworkMessage::Sync {
                                from_repo_id: local_repo_id,
                                to_repo_id,
                                document_id,
                                message,
                            };
                            collection.pending_messages.push_back(outgoing);
                        }
                    }
                }
            }
        }

        // Notify the client of synced documents.
        if let Some(observer) = collection.sync_observer.as_ref() {
            if !synced.is_empty() {
                observer(synced);
            }
        }
    }

    /// The event-loop of the repo.
    /// Handles events from collections and adapters.
    /// Returns a `std::thread::JoinHandle` for optional clean shutdown.
    pub fn run(mut self) -> JoinHandle<()> {
        // Run the repo's event-loop in a thread.
        thread::spawn(move || {
            // Drop the repo's clone of the collection sender,
            // ensuring the below loop stops when all collections have been dropped.
            drop(self.collection_sender.take());

            let collection_ids: Vec<CollectionId> = self.collections.keys().cloned().collect();

            loop {
                // Poll streams and sinks at the start of each iteration.
                // Required, in combination with `try_send` on the wakers,
                // to prevent deadlock.
                for collection_id in collection_ids.iter() {
                    self.collect_network_events(collection_id);
                    self.sync_document_collection(collection_id);
                    self.process_outgoing_network_messages(collection_id);
                }
                select! {
                    recv(self.collection_receiver) -> collection_event => {
                        if let Ok((collection_id, event)) = collection_event {
                            self.handle_collection_event(&collection_id, event);
                        } else {
                            // The repo shuts down
                            // once all handles and collections drop.
                            break;
                        }
                    },
                    recv(self.wake_receiver) -> event => {
                        match event {
                            Err(_) => panic!("Wake senders dropped"),
                            Ok(_) => {},
                        }
                    },
                }
            }
        })
    }
}
