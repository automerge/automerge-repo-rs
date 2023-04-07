use crate::dochandle::DocHandle;
use crate::interfaces::{NetworkAdapter, RepoNetworkSink, StorageAdapter};
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::spawn;
use uuid::Uuid;

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub(crate) struct DocumentId(Uuid);

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub(crate) struct CollectionId(Uuid);

/// The public interface,
/// through which new docs can be created,
/// and doc handles acquired.
pub struct DocCollection {
    collection_sender: Sender<(CollectionId, CollectionEvent)>,
    collection_id: CollectionId,
}

impl DocCollection {
    /// Create a new document,
    /// send a handle to the repo,
    /// return a handle.
    pub async fn new_document(&self) -> DocHandle {
        let document_id = DocumentId(Uuid::new_v4());
        let handle = DocHandle::new(
            self.collection_sender.clone(),
            document_id.clone(),
            self.collection_id.clone(),
        );
        self.collection_sender
            .send((
                self.collection_id.clone(),
                CollectionEvent::NewDoc(document_id, handle.clone()),
            ))
            .await
            .expect("Failed to send collection event.");
        handle
    }
}

/// Events sent by doc collections to the repo.
#[derive(Debug)]
pub(crate) enum CollectionEvent {
    NewDoc(DocumentId, DocHandle),
    DocChange(DocumentId),
}

/// Evnts sent by the network adapter.
#[derive(Debug)]
pub(crate) enum NetworkEvent {
    NewMessage(Vec<u8>),
}

/// Information on a doc collection held by the repo.
/// Each collection can be configured with different adapters.
struct CollectionInfo {
    network_adapter: Box<dyn NetworkAdapter>,
    storage_adapter: Box<dyn StorageAdapter>,
    documents: HashMap<DocumentId, DocHandle>,
}

/// The backend of doc collections: the repo runs an event-loop in a background task.
pub(crate) struct Repo {
    collections: HashMap<CollectionId, CollectionInfo>,
    network_sender: Sender<(CollectionId, NetworkEvent)>,
    network_receiver: Receiver<(CollectionId, NetworkEvent)>,
    collection_sender: Sender<(CollectionId, CollectionEvent)>,
    collection_receiver: Receiver<(CollectionId, CollectionEvent)>,
}

impl Repo {
    pub fn new() -> Self {
        let (network_sender, network_receiver) = channel(1);
        let (collection_sender, collection_receiver) = channel(1);
        Repo {
            collections: Default::default(),
            network_sender,
            network_receiver,
            collection_sender,
            collection_receiver,
        }
    }

    /// Create a new doc collection, with a storage and a network adapter.
    pub async fn new_collection(
        &mut self,
        storage_adapter: Box<dyn StorageAdapter>,
        network_adapter: Box<dyn NetworkAdapter>,
    ) -> DocCollection {
        let collection_id = CollectionId(Uuid::new_v4());
        let sink = RepoNetworkSink::new(self.network_sender.clone(), collection_id.clone());
        network_adapter.plug_into_sink(sink).await;
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
    pub async fn run(mut self) {
        // Drop the repo's clone of the collection sender,
        // ensuring the below loop stops when all collections have been dropped.
        drop(self.collection_sender);

        // Run the repo's event-loop in a task.
        let _join_handle = tokio::spawn(async move {
            loop {
                println!("Start loop");
                tokio::select! {
                    collection_event = self.collection_receiver.recv() => {
                        match collection_event {
                            None => break,
                            Some((collection_id, CollectionEvent::NewDoc(id, handle))) => {
                                println!("Got new doc");
                                // Handle new document.
                                let mut collection = self
                                    .collections
                                    .get_mut(&collection_id)
                                    .expect("Unexpected collection event.");
                                // Set the doc as ready
                                handle.set_ready().await;
                                collection.documents.insert(id, handle);
                                println!("DOne new doc");
                            },
                            Some((collection_id, CollectionEvent::DocChange(_id))) => {
                                // Handle doc changes.
                                let mut collection = self
                                    .collections
                                    .get_mut(&collection_id)
                                    .expect("Unexpected collection event.");
                                    println!("Start saving document");
                                collection.storage_adapter.save_document(()).await;
                                println!("Done saving document");
                            },
                        }
                    },
                    network_event = self.network_receiver.recv() => {
                        if network_event.is_none() {
                            println!("Network event is none");
                            break;
                        }
                    },
                }
            }
            
            println!("Done loop");
        });
    }
}
