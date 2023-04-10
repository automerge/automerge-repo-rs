use crate::dochandle::{DocHandle, DocState};
use crate::interfaces::{NetworkAdapter, RepoNetworkSink, StorageAdapter};
use crossbeam_channel::{select, unbounded, Receiver, Sender};
use parking_lot::{Condvar, Mutex};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use uuid::Uuid;

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub(crate) struct DocumentId(Uuid);

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub(crate) struct CollectionId(Uuid);

/// The public interface of the repo,
/// through which new docs can be created,
/// and doc handles acquired.
pub struct DocCollection {
    collection_sender: Sender<(CollectionId, CollectionEvent)>,
    collection_id: CollectionId,
}

impl DocCollection {
    /// Create a new document,
    /// send the info to the repo,
    /// return a handle.
    pub fn new_document(&self) -> DocHandle {
        let document_id = DocumentId(Uuid::new_v4());
        let state = Arc::new((Mutex::new(DocState::Start), Condvar::new()));
        let handle = DocHandle::new(
            self.collection_sender.clone(),
            document_id.clone(),
            self.collection_id.clone(),
            state.clone(),
        );
        let doc_info = DocumentInfo { state };
        self.collection_sender
            .send((
                self.collection_id.clone(),
                CollectionEvent::NewDoc(document_id, doc_info),
            ))
            .expect("Failed to send collection event.");
        handle
    }
}

/// Events sent by doc collections to the repo.
#[derive(Debug)]
pub(crate) enum CollectionEvent {
    NewDoc(DocumentId, DocumentInfo),
    DocChange(DocumentId),
    DocClosed(DocumentId),
}

/// Events sent by the network adapter.
#[derive(Debug)]
pub(crate) enum NetworkEvent {
    NewMessage(Vec<u8>),
}

/// Information on a doc collection held by the repo.
/// Each collection can be configured with different adapters.
struct CollectionInfo {
    network_adapter: Box<dyn NetworkAdapter>,
    storage_adapter: Box<dyn StorageAdapter>,
    documents: HashMap<DocumentId, DocumentInfo>,
}

/// Info about a document, held by the repo(via CollectionInfo).
#[derive(Debug)]
pub(crate) struct DocumentInfo {
    state: Arc<(Mutex<DocState>, Condvar)>,
}

impl DocumentInfo {
    /// Set the document to a ready state,
    /// wakes-up the doc handle if inside `wait_ready`.
    pub fn set_ready(&self) {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock();
        *state = DocState::Ready;
        cvar.notify_one();
    }
}

/// The backend of doc collections: the repo runs an event-loop in a background thread.
pub(crate) struct Repo {
    /// A map of collections to their info.
    collections: HashMap<CollectionId, CollectionInfo>,
    
    /// Sender and receiver of network events.
    /// A sender is kept around to clone for multiple calls to `new_collection`.
    network_sender: Sender<(CollectionId, NetworkEvent)>,
    network_receiver: Receiver<(CollectionId, NetworkEvent)>,
    
    /// Sender and receiver of collection events.
    /// A sender is kept around to clone for multiple calls to `new_collection`,
    /// the sender is dropped at the start of the repo's event-loop,
    /// to ensure that the event-loop stops 
    /// once all collections and doc handles have been dropped.
    collection_sender: Sender<(CollectionId, CollectionEvent)>,
    collection_receiver: Receiver<(CollectionId, CollectionEvent)>,
}

impl Repo {
    pub fn new() -> Self {
        let (network_sender, network_receiver) = unbounded();
        let (collection_sender, collection_receiver) = unbounded();
        Repo {
            collections: Default::default(),
            network_sender,
            network_receiver,
            collection_sender,
            collection_receiver,
        }
    }

    /// Create a new doc collection, with a storage and a network adapter.
    pub fn new_collection(
        &mut self,
        storage_adapter: Box<dyn StorageAdapter>,
        network_adapter: Box<dyn NetworkAdapter>,
    ) -> DocCollection {
        let collection_id = CollectionId(Uuid::new_v4());
        let sink = RepoNetworkSink::new(self.network_sender.clone(), collection_id.clone());
        network_adapter.plug_into_sink(sink);
        let collection = DocCollection {
            collection_sender: self.collection_sender.clone(),
            collection_id: collection_id.clone(),
        };
        let collection_info = CollectionInfo {
            network_adapter,
            storage_adapter,
            documents: Default::default(),
        };
        self.collections.insert(collection_id, collection_info);
        collection
    }

    /// The event-loop of the repo.
    /// Handles events from collections and adapters.
    /// Returns a `std::thread::JoinHandle` for optional clean shutdown.
    pub fn run(mut self) -> JoinHandle<()> {
        // Drop the repo's clone of the collection sender,
        // ensuring the below loop stops when all collections have been dropped.
        drop(self.collection_sender);

        // Run the repo's event-loop in a thread.
        thread::spawn(move || {
            loop {
                select! {
                    recv(self.collection_receiver) -> collection_event => {
                        match collection_event {
                            Err(_) => break,
                            Ok((collection_id, CollectionEvent::NewDoc(id, info))) => {
                                // Handle new document.
                                let mut collection = self
                                    .collections
                                    .get_mut(&collection_id)
                                    .expect("Unexpected collection event.");
                                // Set the doc as ready
                                info.set_ready();
                                collection.documents.insert(id, info);
                            },
                            Ok((collection_id, CollectionEvent::DocChange(_id))) => {
                                // Handle doc changes.
                                let mut collection = self
                                    .collections
                                    .get_mut(&collection_id)
                                    .expect("Unexpected collection event.");
                                collection.storage_adapter.save_document(());
                            },
                            Ok((collection_id, CollectionEvent::DocClosed(id))) => {
                                // Handle doc closed.
                                let mut collection = self
                                    .collections
                                    .get_mut(&collection_id)
                                    .expect("Unexpected collection event.");
                                    collection.documents.remove(&id);
                            }
                        }
                    },
                    recv(self.network_receiver) -> _event => {
                    },
                }
            }
        })
    }
}
