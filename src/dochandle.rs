use crate::interfaces::{CollectionId, DocumentId};
use crate::repo::CollectionEvent;
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
    /// Doc info in repo owns the same state, and sets it to ready.
    state: Arc<(Mutex<DocState>, Condvar)>,
    /// Channel used to send events back to the repo.
    collection_sender: Sender<(CollectionId, CollectionEvent)>,
    document_id: DocumentId,
    collection_id: CollectionId,
}

impl Drop for DocHandle {
    fn drop(&mut self) {
        // Close the document when the handle drops.
        // FIXME: this doesn't work as expected, because handles are cloneable.
        self.collection_sender
            .send((
                self.collection_id.clone(),
                CollectionEvent::DocClosed(self.document_id.clone()),
            ))
            .expect("Failed to send doc close event.");
    }
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

    pub fn get_document_id(&self) -> DocumentId {
        self.document_id.clone()
    }

    /// Send the "doc changed" event to the repo,
    /// which will trigger the `save` call on the StorageAdapter.
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
            if let DocState::Ready = *state {
                return;
            }
            cvar.wait(&mut state);
        }
    }
}
