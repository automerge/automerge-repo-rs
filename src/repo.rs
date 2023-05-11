use crate::dochandle::{DocHandle, DocState};
use crate::interfaces::{DocumentId, RepoId};
use crate::interfaces::{NetworkAdapter, NetworkEvent, NetworkMessage};
use automerge::sync::{Message as SyncMessage, State as SyncState, SyncDoc};
use automerge::AutoCommit;
use core::pin::Pin;
use crossbeam_channel::{bounded, select, unbounded, Receiver, Sender, TrySendError};
use futures::stream::Stream;
use futures::task::ArcWake;
use futures::task::{waker_ref, Context, Poll};
use futures::Sink;
use parking_lot::{Condvar, Mutex};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use uuid::Uuid;

/// Front-end of the repo.
#[derive(Debug)]
pub struct RepoHandle {
    handle: JoinHandle<()>,
    /// Channel used to send events back to the repo,
    /// such as when a doc is created, and a doc handle acquired.
    repo_sender: Sender<RepoEvent>,
    repo_id: RepoId,

    /// Counter to generate unique document ids.
    document_id_counter: u64,
}

#[derive(Debug)]
pub struct RepoError;

impl RepoHandle {
    pub fn stop(self) -> Result<(), RepoError> {
        drop(self.repo_sender);
        self.handle.join().expect("Failed to join on repo.");
        Ok(())
    }

    pub fn get_repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    /// Create a new document.
    pub fn new_document(&mut self) -> DocHandle {
        self.document_id_counter = self
            .document_id_counter
            .checked_add(1)
            .expect("Overflowed when creating new document id.");
        let document_id = DocumentId((self.repo_id, self.document_id_counter));
        let document = AutoCommit::new();
        self.new_document_handle(None, document_id, document, DocState::Sync)
    }

    /// Load an existing document, assumed to be ready for editing.
    pub fn load_existing_document(
        &self,
        repo_id: RepoId,
        document_id: DocumentId,
        document: AutoCommit,
    ) -> DocHandle {
        self.new_document_handle(Some(repo_id), document_id, document, DocState::Sync)
    }

    /// Boostrap a document using it's ID only.
    /// The returned document should not be edited until ready,
    /// use `DocHandle.wait_ready` to wait for it.
    pub fn bootstrap_document_from_id(
        &self,
        repo_id: Option<RepoId>,
        document_id: DocumentId,
    ) -> DocHandle {
        let document = AutoCommit::new();
        // If no repo id is provided, sync with the creator.
        let repo_id = repo_id.unwrap_or_else(|| *document_id.get_repo_id());
        self.new_document_handle(Some(repo_id), document_id, document, DocState::Bootstrap)
    }

    fn new_document_handle(
        &self,
        repo_id: Option<RepoId>,
        document_id: DocumentId,
        document: AutoCommit,
        state: DocState,
    ) -> DocHandle {
        let is_ready = matches!(state, DocState::Sync);
        let state = Arc::new((Mutex::new((state, document)), Condvar::new()));
        let handle_count = Arc::new(AtomicUsize::new(1));
        let handle = DocHandle::new(
            self.repo_sender.clone(),
            document_id,
            state.clone(),
            handle_count.clone(),
            is_ready,
        );
        let doc_info = DocumentInfo {
            state,
            handle_count,
            sync_states: Default::default(),
            is_ready,
        };
        self.repo_sender
            .send(RepoEvent::NewDocHandle(repo_id, document_id, doc_info))
            .expect("Failed to send repo event.");
        handle
    }
}

/// Events sent by repo or doc handles to the repo.
#[derive(Debug)]
pub(crate) enum RepoEvent {
    /// Load a document,
    // repo id is the repo to start syncing with,
    // none if locally created.
    NewDocHandle(Option<RepoId>, DocumentId, DocumentInfo),
    /// A document changed.
    DocChange(DocumentId),
    /// A document was closed(all doc handles dropped).
    DocClosed(DocumentId),
}

/// Info about a document.
#[derive(Debug)]
pub(crate) struct DocumentInfo {
    /// State of the document(shared with handles).
    /// Document used to apply and generate sync messages.
    state: Arc<(Mutex<(DocState, AutoCommit)>, Condvar)>,
    /// Ref count for handles(shared with handles).
    handle_count: Arc<AtomicUsize>,
    /// Per repo automerge sync state.
    sync_states: HashMap<RepoId, SyncState>,
    /// Flag set to true once the doc reaches `DocState::Sync`.
    is_ready: bool,
}

impl DocumentInfo {
    /// Mark the document as ready for editing,
    /// wakes-up all doc handles that are waiting inside `wait_ready`.
    fn set_ready(&mut self) {
        if self.is_ready {
            return;
        }
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock();
        state.0 = DocState::Sync;
        cvar.notify_all();
        self.is_ready = true;
    }

    /// Apply incoming sync messages.
    fn receive_sync_message(&mut self, repo_id: RepoId, message: SyncMessage) {
        let sync_state = self.sync_states.entry(repo_id).or_insert(SyncState::new());
        let (lock, _cvar) = &*self.state;
        let mut state = lock.lock();
        let mut sync = state.1.sync();
        sync.receive_sync_message(sync_state, message)
            .expect("Failed to apply sync message.");
    }

    /// Potentially generate an outgoing sync message.
    fn generate_first_sync_message(&mut self, repo_id: RepoId) -> Option<SyncMessage> {
        let sync_state = self.sync_states.entry(repo_id).or_insert(SyncState::new());
        let (lock, _cvar) = &*self.state;
        lock.lock().1.sync().generate_sync_message(sync_state)
    }

    /// Generate outgoing sync message for all repos we are syncing with.
    fn generate_sync_messages(&mut self) -> Vec<(RepoId, SyncMessage)> {
        self.sync_states
            .iter_mut()
            .filter_map(|(repo_id, sync_state)| {
                let (lock, _cvar) = &*self.state;
                let message = lock.lock().1.sync().generate_sync_message(sync_state);
                message.map(|msg| (*repo_id, msg))
            })
            .collect()
    }
}

/// Signal that the stream or sink on the network adapter is ready to be polled.
enum WakeSignal {
    Stream,
    Sink,
}

/// Waking mechanism for stream and sinks.
enum RepoWaker {
    Stream(Sender<WakeSignal>),
    Sink(Sender<WakeSignal>),
}

/// https://docs.rs/futures/latest/futures/task/trait.ArcWake.html
impl ArcWake for RepoWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let err = match &**arc_self {
            RepoWaker::Stream(sender) => sender.try_send(WakeSignal::Stream),
            RepoWaker::Sink(sender) => sender.try_send(WakeSignal::Sink),
        };
        if let Err(TrySendError::Disconnected(_)) = err {
            panic!("Wake sender disconnected.");
        }
    }
}

/// The backend of a repo: the repo runs an event-loop in a background thread.
pub struct Repo<T: NetworkAdapter> {
    /// The Id of the repo.
    repo_id: RepoId,

    network_adapter: Box<T>,
    documents: HashMap<DocumentId, DocumentInfo>,

    stream_waker: Arc<RepoWaker>,
    sink_waker: Arc<RepoWaker>,

    /// Messages to send on the network adapter sink.
    pending_messages: VecDeque<NetworkMessage>,
    /// Events received on the network stream, pending processing.
    pending_events: VecDeque<NetworkEvent>,

    /// Callback on applying sync messages.
    sync_observer: Option<Box<dyn Fn(Vec<DocumentId>) + Send>>,

    /// Receiver of network stream and sink readiness signals.
    wake_receiver: Receiver<WakeSignal>,

    /// Sender and receiver of repo events.
    repo_sender: Option<Sender<RepoEvent>>,
    repo_receiver: Receiver<RepoEvent>,
}

impl<T: NetworkAdapter + 'static> Repo<T> {
    /// Create a new repo.
    pub fn new(
        network_adapter: T,
        sync_observer: Option<Box<dyn Fn(Vec<DocumentId>) + Send>>,
    ) -> Self {
        let (wake_sender, wake_receiver) = bounded(2);
        let (repo_sender, repo_receiver) = unbounded();
        let stream_waker = Arc::new(RepoWaker::Stream(wake_sender.clone()));
        let sink_waker = Arc::new(RepoWaker::Sink(wake_sender));
        Repo {
            repo_id: RepoId(Uuid::new_v4()),
            documents: Default::default(),
            network_adapter: Box::new(network_adapter),
            wake_receiver,
            stream_waker,
            sink_waker,
            pending_messages: Default::default(),
            pending_events: Default::default(),
            repo_sender: Some(repo_sender),
            repo_receiver,
            sync_observer,
        }
    }

    fn get_repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    /// Poll the network adapter stream.
    fn collect_network_events(&mut self) {
        // Collect as many events as possible.
        loop {
            let waker = waker_ref(&self.stream_waker);
            let pinned_stream = Pin::new(&mut self.network_adapter);
            let result = pinned_stream.poll_next(&mut Context::from_waker(&waker));
            match result {
                Poll::Pending => break,
                Poll::Ready(Some(event)) => self.pending_events.push_back(event),
                Poll::Ready(None) => return,
            }
        }
    }

    /// Try to send pending messages on the network sink.
    fn process_outgoing_network_messages(&mut self) {
        // Send as many messages as possible.
        let mut needs_flush = false;
        loop {
            if self.pending_messages.is_empty() {
                break;
            }
            let waker = waker_ref(&self.sink_waker);
            let pinned_sink = Pin::new(&mut self.network_adapter);
            let result = pinned_sink.poll_ready(&mut Context::from_waker(&waker));
            match result {
                Poll::Pending => break,
                Poll::Ready(Ok(())) => {
                    let pinned_sink = Pin::new(&mut self.network_adapter);
                    let result = pinned_sink.start_send(
                        self.pending_messages
                            .pop_front()
                            .expect("Empty pending messages."),
                    );
                    if result.is_err() {
                        return;
                    }
                    needs_flush = true;
                }
                Poll::Ready(Err(_)) => return,
            }
        }

        // Flusk the sink if any messages have been sent.
        if needs_flush {
            let waker = waker_ref(&self.sink_waker);
            let pinned_sink = Pin::new(&mut self.network_adapter);
            let _ = pinned_sink.poll_flush(&mut Context::from_waker(&waker));
        }
    }

    /// Handle incoming repo events(sent by repo or document handles).
    fn handle_repo_event(&mut self, event: RepoEvent) {
        match event {
            RepoEvent::NewDocHandle(repo_id, document_id, mut info) => {
                // Send a sync message to the creator, unless it is the local repo.
                if let Some(repo_id) = repo_id {
                    if let Some(message) = info.generate_first_sync_message(repo_id) {
                        let outgoing = NetworkMessage::Sync {
                            from_repo_id: *self.get_repo_id(),
                            to_repo_id: repo_id,
                            document_id,
                            message,
                        };
                        self.pending_messages.push_back(outgoing);
                    }
                }
                self.documents.insert(document_id, info);
            }
            RepoEvent::DocChange(doc_id) => {
                // Handle doc changes: sync the document.
                if let Some(info) = self.documents.get_mut(&doc_id) {
                    for (to_repo_id, message) in info.generate_sync_messages().into_iter() {
                        let outgoing = NetworkMessage::Sync {
                            from_repo_id: *self.get_repo_id(),
                            to_repo_id,
                            document_id: doc_id,
                            message,
                        };
                        self.pending_messages.push_back(outgoing);
                    }
                }
            }
            RepoEvent::DocClosed(id) => {
                // Handle doc closed: remove the document info.
                let doc_info = self
                    .documents
                    .remove(&id)
                    .expect("Document closed but not doc info found.");
                if doc_info.handle_count.load(Ordering::SeqCst) != 0 {
                    panic!("Document closed with outstanding handles.");
                }
            }
        }
    }

    /// Apply incoming sync messages, and generate outgoing ones.
    fn sync_documents(&mut self) {
        // Process incoming events.
        // Handle events.
        let mut synced = vec![];
        for event in mem::take(&mut self.pending_events) {
            match event {
                NetworkEvent::Sync {
                    from_repo_id,
                    to_repo_id: local_repo_id,
                    document_id,
                    message,
                } => {
                    if let Some(info) = self.documents.get_mut(&document_id) {
                        info.receive_sync_message(from_repo_id, message);
                        synced.push(document_id);
                        // Note: since receiving and generating sync messages is done
                        // in two separate critical sections,
                        // local changes could be made in between those,
                        // which is a good thing(generated messages will include those changes).
                        for (to_repo_id, message) in info.generate_sync_messages().into_iter() {
                            if !message.heads.is_empty() && message.need.is_empty() {
                                info.set_ready();
                            }
                            let outgoing = NetworkMessage::Sync {
                                from_repo_id: local_repo_id,
                                to_repo_id,
                                document_id,
                                message,
                            };
                            self.pending_messages.push_back(outgoing);
                        }
                    }
                }
            }
        }

        // Notify the client of synced documents.
        if let Some(observer) = self.sync_observer.as_ref() {
            if !synced.is_empty() {
                observer(synced);
            }
        }
    }

    /// The event-loop of the repo.
    /// Handles events from handles and adapters.
    /// Returns a handle for optional clean shutdown.
    pub fn run(mut self) -> RepoHandle {
        let repo_sender = self.repo_sender.take().unwrap();
        let repo_id = self.repo_id;
        let document_id_counter = Default::default();

        // Run the repo's event-loop in a thread.
        let handle = thread::spawn(move || {
            loop {
                // Poll streams and sinks at the start of each iteration.
                // Required, in combination with `try_send` on the wakers,
                // to prevent deadlock.
                self.collect_network_events();
                self.sync_documents();
                self.process_outgoing_network_messages();
                select! {
                    recv(self.repo_receiver) -> repo_event => {
                        if let Ok(event) = repo_event {
                            self.handle_repo_event(event);
                        } else {
                            // The repo shuts down
                            // once all handles and collections drop.
                            break;
                        }
                    },
                    recv(self.wake_receiver) -> event => {
                        if event.is_err() {
                            panic!("Wake senders dropped");
                        }
                    },
                }
            }
        });
        RepoHandle {
            handle,
            document_id_counter,
            repo_id,
            repo_sender,
        }
    }
}
