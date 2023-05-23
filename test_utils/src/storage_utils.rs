use automerge_repo::{DocumentId, StorageAdapter};
use futures::Future;
use std::marker::Unpin;
use std::collections::HashMap;

pub struct SimpleStorage;

impl StorageAdapter for SimpleStorage {}


pub struct InMemoryStorage {
    documents: HashMap<DocumentId, Vec<u8>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        InMemoryStorage {
            documents: Default::default(),
        }
    }
}

impl StorageAdapter for InMemoryStorage {
    fn get(&self, _id: DocumentId) -> Box<dyn Future<Output = Option<Vec<u8>>> + Send + Unpin> {
        Box::new(futures::future::ready(None))
    }

    fn list_all(&self) -> Box<dyn Future<Output = Vec<DocumentId>>> {
        Box::new(futures::future::ready(self.documents.keys().cloned().collect()))
    }

    fn append(&self, _id: DocumentId, _changes: Vec<u8>) {}

    fn compact(&self, _id: DocumentId) {}
}
