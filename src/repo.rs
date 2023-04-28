use crate::dochandle::{DocHandle, DocState};
use crate::interfaces::{CollectionId, DocumentId};
use crate::interfaces::{NetworkAdapter, NetworkEvent, NetworkMessage};
use automerge::sync::{Message as SyncMessage, State as SyncState, SyncDoc};
use automerge::AutoCommit;
use core::pin::Pin;
use crossbeam_channel::{bounded, select, unbounded, Receiver, Sender};
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
pub struct DocCollection {
    /// Channel used to send events back to the repo,
    /// such as when a doc is created, and a doc handle acquired.
    collection_sender: Sender<(CollectionId, CollectionEvent)>,
    collection_id: CollectionId,
}

impl DocCollection {
    /// Public method used in the client context.
    /// Create a new document in a syncing state,
    /// send the info to the repo,
    /// return a handle.
    pub fn new_document(&self) -> DocHandle {
        let document_id = DocumentId(Uuid::new_v4());
        self.new_document_handle(document_id, DocState::Sync)
    }

    /// Load an existing document for local editing.
    /// The document should not be edited until ready,
    /// use `DocHandle.wait_ready` to wait for it.
    pub fn load_existing_document(&self, document_id: DocumentId) -> DocHandle {
        self.new_document_handle(document_id, DocState::Bootstrap)
    }

    fn new_document_handle(&self, document_id: DocumentId, state: DocState) -> DocHandle {
        let is_ready = if matches!(state, DocState::Sync) {
            true
        } else {
            false
        };
        let state = Arc::new((Mutex::new((state, AutoCommit::new())), Condvar::new()));
        let handle_count = Arc::new(AtomicUsize::new(1));
        let handle = DocHandle::new(
            self.collection_sender.clone(),
            document_id.clone(),
            self.collection_id.clone(),
            state.clone(),
            handle_count.clone(),
            is_ready.clone(),
        );
        let doc_info = DocumentInfo {
            state,
            handle_count,
            sync_state: SyncState::new(),
            is_ready,
        };
        self.collection_sender
            .send((
                self.collection_id.clone(),
                CollectionEvent::NewDocHandle(document_id, doc_info),
            ))
            .expect("Failed to send collection event.");
        handle
    }
}

/// Events sent by doc collections or doc handles to the repo.
#[derive(Debug)]
pub(crate) enum CollectionEvent {
    /// A doc was created.
    NewDocHandle(DocumentId, DocumentInfo),
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
}

/// Info about a document, held by the repo(via CollectionInfo).
#[derive(Debug)]
pub(crate) struct DocumentInfo {
    /// State of the document(shared with handles).
    /// Document used to apply and generate sync messages.
    state: Arc<(Mutex<(DocState, AutoCommit)>, Condvar)>,
    /// Ref count for handles(shared with handles).
    handle_count: Arc<AtomicUsize>,
    /// The automerge sync state.
    sync_state: SyncState,
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

    /// Apply incoming sync messages, and potentially generate and outgoing one.
    fn apply_sync_message(&mut self, message: SyncMessage) -> Option<SyncMessage> {
        let (lock, _cvar) = &*self.state;
        let mut state = lock.lock();
        let mut sync = state.1.sync();
        sync.receive_sync_message(&mut self.sync_state, message)
            .expect("Failed to apply sync message.");
        sync.generate_sync_message(&mut self.sync_state)
    }

    /// Potentially generate an outgoing sync message,
    /// called in response to local changes.
    fn generate_sync_for_local_changes(&mut self) -> Option<SyncMessage> {
        let (lock, _cvar) = &*self.state;
        lock.lock()
            .1
            .sync()
            .generate_sync_message(&mut self.sync_state)
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
        match &**arc_self {
            CollectionWaker::Stream(sender, id) => {
                sender
                    .send(WakeSignal::Stream(id.clone()))
                    .expect("Failed to send stream readiness signal.");
            }
            CollectionWaker::Sink(sender, id) => {
                sender
                    .send(WakeSignal::Sink(id.clone()))
                    .expect("Failed to send sink readiness signal.");
            }
        }
    }
}

/// The backend of doc collections: the repo runs an event-loop in a background thread.
pub(crate) struct Repo<T: NetworkAdapter> {
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
            collections: Default::default(),
            wake_sender,
            wake_receiver,
            collection_sender: Some(collection_sender),
            collection_receiver,
        }
    }

    /// Create a new doc collection, with a storage and a network adapter.
    /// Note: all collections must be created before starting to run the repo.
    pub fn new_collection(&mut self, network_adapter: T) -> DocCollection {
        let collection_id = CollectionId(Uuid::new_v4());
        let collection = DocCollection {
            collection_sender: self
                .collection_sender
                .clone()
                .expect("No collection sender."),
            collection_id: collection_id.clone(),
        };
        let stream_waker = Arc::new(CollectionWaker::Stream(
            self.wake_sender.clone(),
            collection_id.clone(),
        ));
        let sink_waker = Arc::new(CollectionWaker::Sink(
            self.wake_sender.clone(),
            collection_id.clone(),
        ));
        let collection_info = CollectionInfo {
            network_adapter: Box::new(network_adapter),
            documents: Default::default(),
            stream_waker,
            sink_waker,
            pending_messages: Default::default(),
            pending_events: Default::default(),
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
            CollectionEvent::NewDocHandle(id, info) => {
                // A new doc handle has been created.
                let collection = self
                    .collections
                    .get_mut(collection_id)
                    .expect("Unexpected collection event.");
                collection.documents.insert(id, info);
            }
            CollectionEvent::DocChange(doc_id) => {
                // Handle doc changes: sync the document.
                let collection = self
                    .collections
                    .get_mut(collection_id)
                    .expect("Unexpected collection event.");
                if let Some(info) = collection.documents.get_mut(&doc_id) {
                    if let Some(outoing_sync_message) = info.generate_sync_for_local_changes() {
                        collection
                            .pending_messages
                            .push_back(NetworkMessage::Sync(doc_id, outoing_sync_message));
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
        for event in mem::take(&mut collection.pending_events) {
            match event {
                NetworkEvent::Sync(doc_id, message) => {
                    if let Some(info) = collection.documents.get_mut(&doc_id) {
                        if let Some(outoing_sync_message) = info.apply_sync_message(message) {
                            if !outoing_sync_message.heads.is_empty() {
                                info.set_ready();
                            }
                            collection
                                .pending_messages
                                .push_back(NetworkMessage::Sync(doc_id, outoing_sync_message));
                        }
                    }
                }
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

            // Initial poll to network streams.
            let collection_ids: Vec<CollectionId> = self.collections.keys().cloned().collect();
            for collection_id in collection_ids {
                self.collect_network_events(&collection_id);
                self.sync_document_collection(&collection_id);
                self.process_outgoing_network_messages(&collection_id);
            }

            loop {
                select! {
                    recv(self.collection_receiver) -> collection_event => {
                        if let Ok((collection_id, event)) = collection_event {
                            self.handle_collection_event(&collection_id, event);
                            self.process_outgoing_network_messages(&collection_id);
                        } else {
                            // The repo shuts down
                            // once all handles and collections drop.
                            break;
                        }
                    },
                    recv(self.wake_receiver) -> event => {
                        match event {
                            Err(_) => panic!("Wake senders dropped"),
                            Ok(WakeSignal::Stream(collection_id)) => {
                                self.collect_network_events(&collection_id);
                                self.sync_document_collection(&collection_id);
                                self.process_outgoing_network_messages(&collection_id);
                            },
                            Ok(WakeSignal::Sink(collection_id)) => {
                                self.process_outgoing_network_messages(&collection_id);
                            }
                        }
                    },
                }
            }
        })
    }
}
