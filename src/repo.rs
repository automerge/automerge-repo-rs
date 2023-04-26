use crate::dochandle::{DocHandle, DocState};
use crate::interfaces::{CollectionId, DocumentId};
use crate::interfaces::{NetworkAdapter, NetworkEvent, NetworkMessage, StorageAdapter};
use core::pin::Pin;
use crossbeam_channel::{bounded, select, unbounded, Receiver, Sender};
use futures::stream::Stream;
use futures::task::ArcWake;
use futures::task::{waker_ref, Context, Poll};
use futures::Sink;
use parking_lot::{Condvar, Mutex};
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
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
    /// Create a new document,
    /// send the info to the repo,
    /// return a handle.
    pub fn new_document(&self) -> DocHandle {
        let document_id = DocumentId(Uuid::new_v4());
        let state = Arc::new((Mutex::new(DocState::Start), Condvar::new()));
        let handle_count = Arc::new(AtomicUsize::new(1));
        let handle = DocHandle::new(
            self.collection_sender.clone(),
            document_id.clone(),
            self.collection_id.clone(),
            state.clone(),
            handle_count.clone(),
        );
        let doc_info = DocumentInfo {
            state,
            handle_count,
        };
        self.collection_sender
            .send((
                self.collection_id.clone(),
                CollectionEvent::NewDoc(document_id, doc_info),
            ))
            .expect("Failed to send collection event.");
        handle
    }
}

/// Events sent by doc collections or doc handles to the repo.
#[derive(Debug)]
pub(crate) enum CollectionEvent {
    /// A doc was created.
    NewDoc(DocumentId, DocumentInfo),
    /// A document changed.
    DocChange(DocumentId),
    /// A document was closed(all doc handles dropped).
    DocClosed(DocumentId),
}

/// Information on a doc collection held by the repo.
/// Each collection can be configured with different adapters.
struct CollectionInfo<T: NetworkAdapter> {
    network_adapter: Box<T>,
    storage_adapter: Box<dyn StorageAdapter>,
    documents: HashMap<DocumentId, DocumentInfo>,

    /// Document data received over the network, but doc no local handle yet.
    data_received: HashSet<DocumentId>,
    stream_waker: Arc<StreamWaker>,
    sink_waker: Arc<SinkWaker>,

    /// Messages to send on the network adapter sink.
    pending_messages: VecDeque<NetworkMessage>,
}

/// Info about a document, held by the repo(via CollectionInfo).
#[derive(Debug)]
pub(crate) struct DocumentInfo {
    /// State of the document(shared with handles).
    state: Arc<(Mutex<DocState>, Condvar)>,
    /// Ref count for handles(shared with handles).
    handle_count: Arc<AtomicUsize>,
}

impl DocumentInfo {
    /// Set the document to a ready state,
    /// wakes-up all doc handles that are waiting inside `wait_ready`.
    pub fn set_ready(&self) {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock();
        *state = DocState::Ready;
        cvar.notify_all();
    }
}

/// The backend of doc collections: the repo runs an event-loop in a background thread.
pub(crate) struct Repo<T: NetworkAdapter> {
    /// A map of collections to their info.
    collections: HashMap<CollectionId, CollectionInfo<T>>,

    /// Sender use to signal network stream readiness.
    /// A sender is kept around to clone for multiple calls to `new_collection`.
    network_stream_sender: Sender<CollectionId>,
    network_stream_receiver: Receiver<CollectionId>,

    /// Sender use to signal network sink readiness.
    /// A sender is kept around to clone for multiple calls to `new_collection`.
    network_sink_sender: Sender<CollectionId>,
    network_sink_receiver: Receiver<CollectionId>,

    /// Sender and receiver of collection events.
    /// A sender is kept around to clone for multiple calls to `new_collection`,
    /// the sender is dropped at the start of the repo's event-loop,
    /// to ensure that the event-loop stops
    /// once all collections and doc handles have been dropped.
    collection_sender: Option<Sender<(CollectionId, CollectionEvent)>>,
    collection_receiver: Receiver<(CollectionId, CollectionEvent)>,

    /// Max number of collections that be created for this repo,
    /// used to configure the buffer of the network channels.
    max_number_collections: usize,
}

struct StreamWaker {
    network_stream_sender: Sender<CollectionId>,
    collection_id: CollectionId,
}

impl ArcWake for StreamWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self
            .network_stream_sender
            .send(arc_self.collection_id.clone())
            .expect("Failed to send network stream readiness signal.");
    }
}

struct SinkWaker {
    network_sink_sender: Sender<CollectionId>,
    collection_id: CollectionId,
}

impl ArcWake for SinkWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self
            .network_sink_sender
            .send(arc_self.collection_id.clone())
            .expect("Failed to send network stream readiness signal.");
    }
}

impl<T: NetworkAdapter + 'static> Repo<T> {
    pub fn new(max_number_collections: usize) -> Self {
        let (network_stream_sender, network_stream_receiver) = bounded(max_number_collections);
        let (network_sink_sender, network_sink_receiver) = bounded(max_number_collections);
        let (collection_sender, collection_receiver) = unbounded();
        Repo {
            collections: Default::default(),
            network_stream_sender,
            network_stream_receiver,
            network_sink_sender,
            network_sink_receiver,
            collection_sender: Some(collection_sender),
            collection_receiver,
            max_number_collections,
        }
    }

    /// Create a new doc collection, with a storage and a network adapter.
    /// Note: all collections must be created before starting to run the repo.
    pub fn new_collection(
        &mut self,
        storage_adapter: Box<dyn StorageAdapter>,
        network_adapter: T,
    ) -> DocCollection {
        let collection_id = CollectionId(Uuid::new_v4());
        let collection = DocCollection {
            collection_sender: self
                .collection_sender
                .clone()
                .expect("No collection sender."),
            collection_id: collection_id.clone(),
        };
        let stream_waker = Arc::new(StreamWaker {
            network_stream_sender: self.network_stream_sender.clone(),
            collection_id: collection_id.clone(),
        });
        let sink_waker = Arc::new(SinkWaker {
            network_sink_sender: self.network_sink_sender.clone(),
            collection_id: collection_id.clone(),
        });
        let collection_info = CollectionInfo {
            network_adapter: Box::new(network_adapter),
            storage_adapter,
            documents: Default::default(),
            data_received: Default::default(),
            stream_waker,
            sink_waker,
            pending_messages: Default::default(),
        };
        self.collections.insert(collection_id, collection_info);
        collection
    }

    /// Poll the network adapter stream for a given collection,
    /// handle the incoming events.
    fn collect_and_handle_network_events(&mut self, collection_id: CollectionId) {
        let collection = self
            .collections
            .get_mut(&collection_id)
            .expect("Unexpected collection event.");

        // Collect as many events as possible.
        let mut events = VecDeque::new();
        loop {
            let waker = waker_ref(&collection.stream_waker);
            let pinned_stream = Pin::new(&mut collection.network_adapter);
            let result = pinned_stream.poll_next(&mut Context::from_waker(&waker));
            match result {
                Poll::Pending => break,
                Poll::Ready(Some(event)) => events.push_back(event),
                Poll::Ready(None) => return,
            }
        }

        // Handle events.
        for event in events {
            match event {
                NetworkEvent::DocFullData(doc_id) => {
                    if let Some(document) = collection.documents.get(&doc_id) {
                        // Set the doc as ready
                        document.set_ready();
                    } else {
                        // keep the data for now,
                        // we haven't received the CollectionEvent::NewDoc yet.
                        collection.data_received.insert(doc_id);
                    }
                }
            }
        }
    }

    /// Process pending messages on network sinks for all collections.
    fn process_outgoing_network_messages(&mut self, collection_id: CollectionId) {
        let collection = self
            .collections
            .get_mut(&collection_id)
            .expect("Unexpected collection event.");

        // Send as many messages as possible.
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
                    let waker = waker_ref(&collection.sink_waker);
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
                }
                Poll::Ready(Err(_)) => return,
            }
        }

        // Flusk the sink.
        let waker = waker_ref(&collection.sink_waker);
        let pinned_sink = Pin::new(&mut collection.network_adapter);
        let _ = pinned_sink.poll_flush(&mut Context::from_waker(&waker));
    }

    fn handle_collection_event(&mut self, collection_id: CollectionId, event: CollectionEvent) {
        match event {
            CollectionEvent::NewDoc(id, info) => {
                // Handle new document
                // (or rather, a new handle for a doc to be synced).
                let collection = self
                    .collections
                    .get_mut(&collection_id)
                    .expect("Unexpected collection event.");

                // We will either already have received the data for this doc,
                // or will eventually receive it
                // (see handling of NetworkEvent below).
                if collection.data_received.remove(&id) {
                    // Set the doc as ready
                    info.set_ready();
                } else {
                    // Ask the other peer for the data.
                    collection
                        .pending_messages
                        .push_back(NetworkMessage::WantDoc(id.clone()));
                }
                collection.documents.insert(id, info);
            }
            CollectionEvent::DocChange(_id) => {
                // Handle doc changes: save the document.
                let collection = self
                    .collections
                    .get_mut(&collection_id)
                    .expect("Unexpected collection event.");
                collection.storage_adapter.save_document(());
            }
            CollectionEvent::DocClosed(id) => {
                // Handle doc closed: remove the document info.
                let collection = self
                    .collections
                    .get_mut(&collection_id)
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
                self.collect_and_handle_network_events(collection_id.clone());
            }

            loop {
                select! {
                    recv(self.collection_receiver) -> collection_event => {
                        if let Ok((id, event)) = collection_event {
                            self.handle_collection_event(id.clone(), event);
                            let collection = self
                                .collections
                                .get(&id)
                                .expect("Unexpected collection event.");
                            if !collection.pending_messages.is_empty() {
                                self.process_outgoing_network_messages(id);
                            }
                        } else {
                            break;
                        }
                    },
                    recv(self.network_stream_receiver) -> event => {
                        match event {
                            Err(_) => panic!("Network senders dropped before closing stream"),
                            Ok(collection_id) => {
                                self.collect_and_handle_network_events(collection_id);
                            }
                        }
                    },
                    recv(self.network_sink_receiver) -> event => {
                        match event {
                            Err(_) => panic!("Network senders dropped before closing stream"),
                            Ok(collection_id) => {
                                self.process_outgoing_network_messages(collection_id);
                            }
                        }
                    },
                }
            }
        })
    }
}
