use crate::interfaces::{DocumentId, RepoId};
use crate::repo::RepoEvent;
use automerge::transaction::Observed;
use automerge::{AutoCommitWithObs, VecOpObserver};
use crossbeam_channel::Sender;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// The doc handle state machine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DocState {
    /// While in bootstrap, the doc should not be edited locally.
    Bootstrap,
    /// A document that has been locally created,
    /// and not edited yet,
    /// should not be synced until it has been.
    LocallyCreatedNotEdited,
    /// The doc is syncing(can be edited locally).
    Sync,
}

/// A wrapper around a document shared between a handle and the repo.
#[derive(Clone, Debug)]
pub(crate) struct SharedDocument {
    pub automerge: AutoCommitWithObs<Observed<VecOpObserver>>,
}

#[derive(Debug)]
/// A handle to a document, held by the client.
pub struct DocHandle {
    /// Doc info in repo owns the same state, and sets it to ready.
    /// Document used by the handle for local editing.
    shared_document: Arc<Mutex<SharedDocument>>,
    /// Ref count for handles.
    handle_count: Arc<AtomicUsize>,
    /// Channel used to send events back to the repo.
    repo_sender: Sender<RepoEvent>,
    document_id: DocumentId,
    local_repo_id: RepoId,
}

impl Clone for DocHandle {
    fn clone(&self) -> Self {
        // Increment handle count.
        self.handle_count.fetch_add(1, Ordering::SeqCst);
        DocHandle::new(
            self.repo_sender.clone(),
            self.document_id.clone(),
            self.shared_document.clone(),
            self.handle_count.clone(),
            self.local_repo_id.clone(),
        )
    }
}

impl Drop for DocHandle {
    fn drop(&mut self) {
        // Close the document when the last handle drops.
        // TODO: turn this into a `delete` concept,
        // based on an explicit method call(not drop),
        // which would clear storage as well?
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
        shared_document: Arc<Mutex<SharedDocument>>,
        handle_count: Arc<AtomicUsize>,
        local_repo_id: RepoId,
    ) -> Self {
        DocHandle {
            shared_document,
            repo_sender,
            document_id,
            handle_count,
            local_repo_id,
        }
    }

    pub fn local_repo_id(&self) -> RepoId {
        self.local_repo_id.clone()
    }

    pub fn document_id(&self) -> DocumentId {
        self.document_id.clone()
    }

    /// Run a closure over a mutable reference to the document.
    pub fn with_doc_mut<F>(&mut self, f: F)
    where
        F: FnOnce(&mut AutoCommitWithObs<Observed<VecOpObserver>>),
    {
        {
            let mut state = self.shared_document.lock();
            f(&mut state.automerge);
        }
        self.repo_sender
            .send(RepoEvent::DocChange(self.document_id.clone()))
            .expect("Failed to send doc change event.");
    }
}
