use automerge_repo::{DocumentId, StorageAdapter, StorageError};
use futures::future::TryFutureExt;
use futures::Future;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::marker::Unpin;
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot::{channel as oneshot, Sender as OneShot};

pub struct SimpleStorage;

impl StorageAdapter for SimpleStorage {}

#[derive(Clone, Debug)]
pub struct InMemoryStorage {
    documents: Arc<Mutex<HashMap<DocumentId, Vec<u8>>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        InMemoryStorage {
            documents: Arc::new(Mutex::new(Default::default())),
        }
    }

    pub fn add_document(&self, doc_id: DocumentId, doc: Vec<u8>) {
        self.documents.lock().insert(doc_id, doc);
    }
}

impl StorageAdapter for InMemoryStorage {
    fn get(
        &self,
        id: DocumentId,
    ) -> Box<dyn Future<Output = Result<Option<Vec<u8>>, StorageError>> + Send + Unpin> {
        Box::new(futures::future::ready(Ok(self
            .documents
            .lock()
            .get(&id)
            .cloned())))
    }

    fn list_all(&self) -> Box<dyn Future<Output = Result<Vec<DocumentId>, StorageError>>> {
        Box::new(futures::future::ready(Ok(self
            .documents
            .lock()
            .keys()
            .cloned()
            .collect())))
    }

    fn append(&self, _id: DocumentId, _changes: Vec<u8>) {}

    fn compact(&self, _id: DocumentId) {}
}

#[derive(Clone, Debug)]
pub struct AsyncInMemoryStorage {
    chan: Sender<(DocumentId, OneShot<Option<Vec<u8>>>)>,
}

impl AsyncInMemoryStorage {
    pub fn new(documents: HashMap<DocumentId, Vec<u8>>) -> Self {
        let (doc_request_sender, mut doc_request_receiver) =
            channel::<(DocumentId, OneShot<Option<Vec<u8>>>)>(1);
        tokio::spawn(async move {
            loop {
                if let Some((doc_id, sender)) = doc_request_receiver.recv().await {
                    sender.send(documents.get(&doc_id).cloned()).unwrap();
                } else {
                    break;
                }
            }
        });
        AsyncInMemoryStorage {
            chan: doc_request_sender,
        }
    }
}

impl StorageAdapter for AsyncInMemoryStorage {
    fn get(
        &self,
        id: DocumentId,
    ) -> Box<dyn Future<Output = Result<Option<Vec<u8>>, StorageError>> + Send + Unpin> {
        let (tx, rx) = oneshot();
        self.chan.blocking_send((id, tx)).unwrap();
        Box::new(rx.map_err(|_| StorageError::Error))
    }

    fn list_all(&self) -> Box<dyn Future<Output = Result<Vec<DocumentId>, StorageError>>> {
        Box::new(futures::future::ready(Ok(vec![])))
    }

    fn append(&self, _id: DocumentId, _changes: Vec<u8>) {}

    fn compact(&self, _id: DocumentId) {}
}
