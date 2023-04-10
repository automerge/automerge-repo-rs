use crate::repo::CollectionEvent;
use crate::repo::{CollectionId, DocumentId};
use crossbeam_channel::Sender;
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;

/// The doc handle state machine.
#[derive(Clone, Debug)]
pub(crate) enum DocState {
    Start,
    Ready,
}

#[derive(Clone, Debug)]
/// A handle to a document, held by the client.
pub struct DocHandle {
    state: Arc<(Mutex<DocState>, Condvar)>,
    collection_sender: Sender<(CollectionId, CollectionEvent)>,
    document_id: DocumentId,
    collection_id: CollectionId,
}

impl DocHandle {
    pub(crate) fn new(
        collection_sender: Sender<(CollectionId, CollectionEvent)>,
        document_id: DocumentId,
        collection_id: CollectionId,
        state: Arc<(Mutex<DocState>, Condvar)>,
    ) -> Self {
        DocHandle {
            state,
            collection_sender,
            document_id,
            collection_id,
        }
    }

    pub fn change(&self) {
        self.collection_sender
            .send((
                self.collection_id.clone(),
                CollectionEvent::DocChange(self.document_id.clone()),
            ))
            .expect("Failed to send doc change event.");
    }

    /// Wait for the document to be ready. 
    /// Note: blocks, should be called only from within a blocking task or thread.
    pub fn wait_ready(&self) {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock();
        loop {
            match *state {
                DocState::Ready => return,
                _ => {}
            }
            cvar.wait(&mut state);
        }
    }
}
