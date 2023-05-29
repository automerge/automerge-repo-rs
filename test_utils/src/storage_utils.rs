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
    documents: Arc<Mutex<HashMap<DocumentId, Vec<Vec<u8>>>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        InMemoryStorage {
            documents: Arc::new(Mutex::new(Default::default())),
        }
    }

    pub fn add_document(&self, doc_id: DocumentId, doc: Vec<u8>) {
        let mut documents = self.documents.lock();
        let entry = documents.entry(doc_id).or_insert_with(Default::default);
        entry.push(doc);
    }
}

impl StorageAdapter for InMemoryStorage {
    fn get(
        &self,
        id: DocumentId,
    ) -> Box<dyn Future<Output = Result<Option<Vec<Vec<u8>>>, StorageError>> + Send + Unpin> {
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

    fn append(
        &self,
        _id: DocumentId,
        _changes: Vec<u8>,
    ) -> Box<dyn Future<Output = Result<(), StorageError>> + Send + Unpin> {
        Box::new(futures::future::ready(Ok(())))
    }

    fn compact(
        &self,
        id: DocumentId,
        _full_doc: Vec<u8>,
    ) -> Box<dyn Future<Output = Result<(), StorageError>> + Send + Unpin> {
        Box::new(futures::future::ready(Ok(())))
    }
}

#[derive(Debug)]
enum StorageRequest {
    Load(DocumentId, OneShot<Option<Vec<Vec<u8>>>>),
    Append(DocumentId, Vec<u8>, OneShot<()>),
    Compact(DocumentId, Vec<u8>, OneShot<()>),
}

#[derive(Clone, Debug)]
pub struct AsyncInMemoryStorage {
    chan: Sender<StorageRequest>,
}

impl AsyncInMemoryStorage {
    pub fn new(mut documents: HashMap<DocumentId, Vec<Vec<u8>>>) -> Self {
        let (doc_request_sender, mut doc_request_receiver) = channel::<StorageRequest>(9);
        tokio::spawn(async move {
            loop {
                if let Some(request) = doc_request_receiver.recv().await {
                    match request {
                        StorageRequest::Load(doc_id, sender) => {
                            let result = documents.get(&doc_id).cloned();
                            let _ = sender.send(result);
                        }
                        StorageRequest::Append(doc_id, data, sender) => {
                            let entry = documents.entry(doc_id).or_insert_with(Default::default);
                            entry.push(data);
                            let _ = sender.send(());
                        }
                        StorageRequest::Compact(doc_id, data, sender) => {
                            let entry = documents.entry(doc_id).or_insert_with(Default::default);
                            entry.clear();
                            entry.push(data);
                            let _ = sender.send(());
                        }
                    }
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
    ) -> Box<dyn Future<Output = Result<Option<Vec<Vec<u8>>>, StorageError>> + Send + Unpin> {
        let (tx, rx) = oneshot();
        self.chan
            .blocking_send(StorageRequest::Load(id, tx))
            .unwrap();
        Box::new(rx.map_err(|_| StorageError::Error))
    }

    fn list_all(&self) -> Box<dyn Future<Output = Result<Vec<DocumentId>, StorageError>>> {
        Box::new(futures::future::ready(Ok(vec![])))
    }

    fn append(
        &self,
        id: DocumentId,
        changes: Vec<u8>,
    ) -> Box<dyn Future<Output = Result<(), StorageError>> + Send + Unpin> {
        let (tx, rx) = oneshot();
        self.chan
            .blocking_send(StorageRequest::Append(id, changes, tx))
            .unwrap();
        Box::new(rx.map_err(|_| StorageError::Error))
    }

    fn compact(
        &self,
        id: DocumentId,
        full_doc: Vec<u8>,
    ) -> Box<dyn Future<Output = Result<(), StorageError>> + Send + Unpin> {
        let (tx, rx) = oneshot();
        self.chan
            .blocking_send(StorageRequest::Compact(id, full_doc, tx))
            .unwrap();
        Box::new(rx.map_err(|_| StorageError::Error))
    }
}
