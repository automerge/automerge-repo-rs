use crate::interfaces::DocumentId;
use crate::repo::RepoEvent;
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
    /// The doc is syncing(can be edited locally).
    Sync,
}

#[derive(Debug)]
/// A handle to a document, held by the client.
pub struct DocHandle {
    /// Doc info in repo owns the same state, and sets it to ready.
    /// Document used by the handle for local editing.
    state: Arc<(Mutex<(DocState, AutoCommit)>, Condvar)>,
    /// Ref count for handles.
    handle_count: Arc<AtomicUsize>,
    /// Channel used to send events back to the repo.
    repo_sender: Sender<RepoEvent>,
    document_id: DocumentId,
    /// Flag set to true once the doc reaches `DocState::Sync`.
    is_ready: bool,
}

impl Clone for DocHandle {
    fn clone(&self) -> Self {
        // Increment handle count.
        self.handle_count.fetch_add(1, Ordering::SeqCst);
        DocHandle::new(
            self.repo_sender.clone(),
            self.document_id.clone(),
            self.state.clone(),
            self.handle_count.clone(),
            self.is_ready,
        )
    }
}

impl Drop for DocHandle {
    fn drop(&mut self) {
        // Close the document when the last handle drops.
        if self.handle_count.fetch_sub(1, Ordering::SeqCst) == 0 {
            self.repo_sender
                .send(RepoEvent::DocClosed(self.document_id.clone()))
                .expect("Failed to send doc close event.");
        }
    }
}

impl DocHandle {
    pub(crate) fn new(
        repo_sender: Sender<RepoEvent>,
        document_id: DocumentId,
        state: Arc<(Mutex<(DocState, AutoCommit)>, Condvar)>,
        handle_count: Arc<AtomicUsize>,
        is_ready: bool,
    ) -> Self {
        DocHandle {
            state,
            repo_sender,
            document_id,
            handle_count,
            is_ready,
        }
    }

    pub fn document_id(&self) -> DocumentId {
        self.document_id.clone()
    }

    /// Run a closure over a mutable reference to the document.
    /// Note: blocks if called on a document that isn't ready,
    /// should be called only from within a blocking task or thread,
    /// or only after having called `wait_ready`.
    pub fn with_doc_mut<F>(&mut self, f: F)
    where
        F: FnOnce(&mut AutoCommit),
    {
        if !self.is_ready {
            self.wait_ready();
        }
        {
            let (lock, _cvar) = &*self.state;
            let mut state = lock.lock();
            f(&mut state.1);
        }
        self.repo_sender
            .send(RepoEvent::DocChange(self.document_id.clone()))
            .expect("Failed to send doc change event.");
    }

    /// Returns whether the document is ready for editing.
    /// If false, `with_doc_mut` will block until the document is ready.
    pub fn is_ready_for_editing(&self) -> bool {
        self.is_ready
    }

    /// Wait for the document to be ready to be edited.
    /// Note: blocks, should be called only from within a blocking task or thread.
    pub fn wait_ready(&mut self) {
        if self.is_ready {
            return;
        }
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock();
        loop {
            match state.0 {
                DocState::Sync => break,
                DocState::Bootstrap => cvar.wait(&mut state),
            }
        }
        self.is_ready = true;
    }
}
