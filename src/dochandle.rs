use crate::interfaces::{DocumentId, RepoId};
use crate::repo::{new_repo_future_with_resolver, RepoError, RepoEvent, RepoFuture};
use crate::EphemeralMessage;
use am::ReadDoc;
use am::sync::SyncDoc;
use automerge::{self as am, Automerge, ChangeHash};
use crossbeam_channel::Sender;
use futures::Stream;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// A wrapper around a document shared between a handle and the repo.
#[derive(Clone, Debug)]
pub(crate) struct SharedDocument {
    automerge: Automerge,
    last_local_change_time: Option<Instant>,
    last_remote_change_time: Option<Instant>,
}

impl SharedDocument {
    pub fn new() -> Self {
        SharedDocument {
            automerge: Automerge::new(),
            last_local_change_time: None,
            last_remote_change_time: None,
        }
    }

    pub(crate) fn get_heads(&self) -> Vec<am::ChangeHash> {
        self.automerge.get_heads()
    }

    pub(crate) fn load_incremental(&mut self, bytes: &[u8]) -> Result<(), am::AutomergeError> {
        let heads_before = self.automerge.get_heads();
        self.automerge
            .load_incremental(bytes)?;
        let heads_after = self.automerge.get_heads();
        if heads_before != heads_after {
            self.last_local_change_time = Some(Instant::now());
        }
        Ok(())
    }

    pub(crate) fn get_changes(&self, have_deps: &[am::ChangeHash]) -> Vec<&am::Change> {
        self.automerge.get_changes(have_deps)
    }

    pub(crate) fn save(&self) -> Vec<u8> {
        self.automerge.save()
    }

    pub(crate) fn save_after(&self, have_deps: &[am::ChangeHash]) -> Vec<u8> {
        self.automerge.save_after(have_deps)
    }

    pub(crate) fn receive_sync_message(&mut self, sync_state: &mut am::sync::State, msg: am::sync::Message) -> Result<(), am::AutomergeError> {
        let heads_before = self.automerge.get_heads();
        self.automerge.receive_sync_message(sync_state, msg)?;
        let heads_after = self.automerge.get_heads();
        if heads_before != heads_after {
            self.last_remote_change_time = Some(Instant::now());
        }
        Ok(())
    }

    pub(crate) fn generate_sync_message(&self, sync_state: &mut am::sync::State) -> Option<am::sync::Message> {
        self.automerge.generate_sync_message(sync_state)
    }

    pub(crate) fn get_change_by_hash(&self, hash: &am::ChangeHash) -> Option<&am::Change> {
        self.automerge.get_change_by_hash(hash)
    }

    pub(crate) fn last_local_change_time(&self) -> Option<Instant> {
        self.last_local_change_time
    }

    pub(crate) fn last_remote_change_time(&self) -> Option<Instant> {
        self.last_remote_change_time
    }

}

#[derive(Debug)]
/// A handle to a document, held by the client(s).
pub struct DocHandle {
    /// Document used by the handle for local editing.
    shared_document: Arc<RwLock<SharedDocument>>,
    /// Ref count for handles.
    handle_count: Arc<AtomicUsize>,
    /// Channel used to send events back to the repo.
    repo_sender: Sender<RepoEvent>,
    document_id: DocumentId,
    local_repo_id: RepoId,
    /// The change hash corresponding to either
    /// the creation of the doc,
    /// or the last time it was access by any of the `with_doc` methods.
    /// Putting it in a mutex to maintain an API
    /// that doesn't require a mutabale reference to the handle.
    /// Note: the mutex is not shared between clones of the same handle.
    last_heads: Mutex<Vec<ChangeHash>>,
    outgoing_ephemera: Arc<Mutex<Vec<EphemeralMessage>>>,
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
            self.outgoing_ephemera.clone(),
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
        shared_document: Arc<RwLock<SharedDocument>>,
        handle_count: Arc<AtomicUsize>,
        local_repo_id: RepoId,
        ephemera: Arc<Mutex<Vec<EphemeralMessage>>>,
    ) -> Self {
        let last_heads = {
            let state = shared_document.read();
            state.automerge.get_heads()
        };
        DocHandle {
            shared_document,
            repo_sender,
            document_id,
            handle_count,
            local_repo_id,
            last_heads: Mutex::new(last_heads),
            outgoing_ephemera: ephemera,
        }
    }

    pub fn local_repo_id(&self) -> RepoId {
        self.local_repo_id.clone()
    }

    pub fn document_id(&self) -> DocumentId {
        self.document_id.clone()
    }

    /// Run a closure over a mutable reference to the document,
    /// returns the result of calling the closure.
    /// Important: if `save` is called on the document inside the closure,
    /// no saving via the storage adapter will be triggered.
    pub fn with_doc_mut<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&mut Automerge) -> T,
    {
        let res = {
            let (start_heads, new_heads, res) = {
                let mut state = self.shared_document.write();
                let start_heads = state.automerge.get_heads();
                let res = f(&mut state.automerge);
                let new_heads = state.automerge.get_heads();
                (start_heads, new_heads, res)
            };

            let doc_changed = start_heads != new_heads;

            // Always note the last heads seen by the handle,
            // for use with `changed`.
            *self.last_heads.lock() = new_heads;

            // If the document wasn't actually mutated,
            // there is no need to send an event.
            if !doc_changed {
                return res;
            }
            res
        };

        self.repo_sender
            .send(RepoEvent::DocChange(self.document_id.clone()))
            .expect("Failed to send doc change event.");
        res
    }

    /// Run a closure over a immutable reference to the document,
    /// returns the result of calling the closure.
    pub fn with_doc<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&Automerge) -> T,
    {
        let (res, new_heads) = {
            let state = self.shared_document.read();
            let res = f(&state.automerge);
            (res, state.automerge.get_heads())
        };
        *self.last_heads.lock() = new_heads;
        res
    }

    /// Returns a future that will resolve when the document has changed,
    /// either via another handle, or by applying a sync messsage.
    /// TODO: check sync message and docs following mutable calls,
    /// and only resolve the future when there was an actual change.
    pub fn changed(&self) -> RepoFuture<Result<(), RepoError>> {
        let (fut, observer) = new_repo_future_with_resolver();
        self.repo_sender
            .send(RepoEvent::AddChangeObserver(
                self.document_id.clone(),
                self.last_heads.lock().clone(),
                observer,
            ))
            .expect("Failed to send doc change event.");
        fut
    }

    pub async fn ephemera(&self) -> impl Stream<Item = EphemeralMessage> + Clone {
        let (fut, resolver) = new_repo_future_with_resolver();
        self.repo_sender
            .send(RepoEvent::SubscribeEphemeralStream {
                document_id: self.document_id.clone(),
                resolver,
            })
            .expect("Failed to send ephemeral observer event.");
        fut.await
    }

    pub fn broadcast_ephemeral(&self, msg: Vec<u8>) -> Result<(), RepoError> {
        let mut outgoing_ephemera = self.outgoing_ephemera.lock();
        outgoing_ephemera.push(EphemeralMessage::new(msg, self.local_repo_id.clone()));
        self.repo_sender
            .send(RepoEvent::NewEphemeral(self.document_id.clone()))
            .expect("failed to send new ephemeral event");
        Ok(())
    }

    pub fn last_local_change_time(&self) -> Option<Instant> {
        let state = self.shared_document.read();
        state.last_local_change_time()
    }

    pub fn last_remote_change_time(&self) -> Option<Instant> {
        let state = self.shared_document.read();
        state.last_remote_change_time()
    }

    pub fn last_change_time(&self) -> Option<Instant> {
        let state = self.shared_document.read();
        let local = state.last_local_change_time();
        let remote = state.last_remote_change_time();
        match (local, remote) {
            (Some(l), Some(r)) => Some(l.max(r)),
            (Some(l), None) => Some(l),
            (None, Some(r)) => Some(r),
            (None, None) => None,
        }
    }
}
