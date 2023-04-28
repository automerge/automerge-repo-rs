use crate::interfaces::{CollectionId, DocumentId};
use crate::repo::CollectionEvent;
use automerge::AutoCommit;
use crossbeam_channel::Sender;
use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// The doc handle state machine.
#[derive(Clone, Debug)]
pub(crate) enum DocState {
    /// While in bootstrap, the doc should not be edited locally.
    Bootstrap,
    /// The doc is syncing.
    Sync,
}

#[derive(Debug)]
/// A handle to a document, held by the client.
pub struct DocHandle {
    /// Doc info in repo owns the same state, and sets it to ready.
    state: Arc<(Mutex<(DocState, AutoCommit)>, Condvar)>,
    /// Ref count for handles.
    handle_count: Arc<AtomicUsize>,
    /// Channel used to send events back to the repo.
    collection_sender: Sender<(CollectionId, CollectionEvent)>,
    document_id: DocumentId,
    collection_id: CollectionId,
}

impl Clone for DocHandle {
    fn clone(&self) -> Self {
        // Increment handle count.
        self.handle_count.fetch_add(1, Ordering::SeqCst);
        DocHandle::new(
            self.collection_sender.clone(),
            self.document_id.clone(),
            self.collection_id.clone(),
            self.state.clone(),
            self.handle_count.clone(),
        )
    }
}

impl Drop for DocHandle {
    fn drop(&mut self) {
        // Close the document when the last handle drops.
        if self.handle_count.fetch_sub(1, Ordering::SeqCst) == 0 {
            self.collection_sender
                .send((
                    self.collection_id.clone(),
                    CollectionEvent::DocClosed(self.document_id.clone()),
                ))
                .expect("Failed to send doc close event.");
        }
    }
}

impl DocHandle {
    pub(crate) fn new(
        collection_sender: Sender<(CollectionId, CollectionEvent)>,
        document_id: DocumentId,
        collection_id: CollectionId,
        state: Arc<(Mutex<(DocState, AutoCommit)>, Condvar)>,
        handle_count: Arc<AtomicUsize>,
    ) -> Self {
        DocHandle {
            state,
            collection_sender,
            document_id,
            collection_id,
            handle_count,
        }
    }

    /// Run a closure over a mutable reference to the document.
    pub fn with_doc_mut<F>(&self, f: F)
    where
        F: FnOnce(&mut AutoCommit),
    {
        {
            let (lock, _cvar) = &*self.state;
            let mut state = lock.lock();
            f(&mut state.1);
        }
        self.collection_sender
            .send((
                self.collection_id.clone(),
                CollectionEvent::DocChange(self.document_id.clone()),
            ))
            .expect("Failed to send doc change event.");
    }

    /// Wait for the document to be ready to be edited.
    /// Note: blocks, should be called only from within a blocking task or thread.
    pub fn wait_ready(&self) {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock();
        loop {
            match state.0 {
                DocState::Sync => return,
                DocState::Bootstrap => cvar.wait(&mut state),
            }
        }
    }
}
