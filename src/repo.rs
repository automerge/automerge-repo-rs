use crate::dochandle::{DocHandle, DocState};
use crate::interfaces::{DocumentId, RepoId};
use crate::interfaces::{NetworkAdapter, NetworkEvent, NetworkMessage};
use automerge::sync::{Message as SyncMessage, State as SyncState, SyncDoc};
use automerge::AutoCommit;
use core::pin::Pin;
use crossbeam_channel::{bounded, select, unbounded, Receiver, Sender, TrySendError};
use futures::task::ArcWake;
use futures::task::{waker_ref, Context, Poll, Waker};
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt, TryStreamExt};
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

    outboxes: Arc<Mutex<HashMap<RepoId, Arc<Mutex<Outbox>>>>>,
    inboxes: Arc<Mutex<HashMap<RepoId, Arc<Mutex<Inbox>>>>>,
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

    pub async fn connect_remote<RecvErr, SendErr, S>(
        &self,
        repo_id: RepoId,
        stream: S,
    ) -> Result<(), error::RunConnection<SendErr, RecvErr>>
    where
        RecvErr: std::fmt::Debug,
        SendErr: std::error::Error,
        S: Sink<NetworkMessage, Error = SendErr> + Stream<Item = Result<NetworkMessage, RecvErr>>,
    {
        let (mut sink, stream) = stream.split();
        let outbox = self
            .outboxes
            .lock()
            .entry(repo_id)
            .or_insert_with(|| Arc::new(Mutex::new(Default::default())))
            .clone();
        let inbox = self
            .inboxes
            .lock()
            .entry(repo_id)
            .or_insert_with(|| Arc::new(Mutex::new(Default::default())))
            .clone();
        let sending = OutgoingStream(outbox)
            .map(|m| Ok::<_, SendErr>(m))
            .forward(&mut sink)
            .fuse();

        let recv_msg = self.repo_sender.clone();
        let recving = stream
            .map_err(error::RunConnection::<SendErr, RecvErr>::Recv)
            .try_for_each({
                move |m| {
                    inbox.lock().messages.push_back(m);
                    let _ = recv_msg.try_send(RepoEvent::IncomingSyncMessage);
                    futures::future::ready(Ok(()))
                }
            })
            .fuse();

        let finished = futures::future::select(sending, recving).await;
        match finished {
            futures::future::Either::Left((result, _recving)) => match result {
                Ok(_) => Ok(()),
                Err(e) => Err(error::RunConnection::Send(e)),
            },
            futures::future::Either::Right((result, _sending)) => {
                if let Err(_e) = sink.close().await {}
                match result {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            }
        }
    }
}

struct OutgoingStream(Arc<Mutex<Outbox>>);

impl Stream for OutgoingStream {
    type Item = NetworkMessage;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Outbox {
            waker,
            ref mut messages,
        } = &mut *self.0.lock();
        if let Some(msg) = messages.pop_front() {
            Poll::Ready(Some(msg))
        } else {
            *waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[derive(Debug, Default)]
struct Outbox {
    waker: Option<Waker>,
    messages: VecDeque<NetworkMessage>,
}

#[derive(Debug, Default)]
struct Inbox {
    messages: VecDeque<NetworkMessage>,
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
    /// Incoming sync message.
    IncomingSyncMessage,
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

/// The backend of a repo: the repo runs an event-loop in a background thread.
pub struct Repo {
    /// The Id of the repo.
    repo_id: RepoId,

    documents: HashMap<DocumentId, DocumentInfo>,

    /// Messages to send on the network adapter sink.
    outgoing_messages: VecDeque<NetworkMessage>,
    /// Events received on the network stream, pending processing.
    incoming_messages: VecDeque<NetworkMessage>,

    /// Callback on applying sync messages.
    sync_observer: Option<Box<dyn Fn(Vec<DocumentId>) + Send>>,
    /// Sender and receiver of repo events.
    repo_sender: Option<Sender<RepoEvent>>,
    repo_receiver: Receiver<RepoEvent>,

    outboxes: Arc<Mutex<HashMap<RepoId, Arc<Mutex<Outbox>>>>>,
    inboxes: Arc<Mutex<HashMap<RepoId, Arc<Mutex<Inbox>>>>>,
}

impl Repo {
    /// Create a new repo.
    pub fn new(sync_observer: Option<Box<dyn Fn(Vec<DocumentId>) + Send>>) -> Self {
        let (repo_sender, repo_receiver) = unbounded();
        Repo {
            repo_id: RepoId(Uuid::new_v4()),
            documents: Default::default(),
            outgoing_messages: Default::default(),
            incoming_messages: Default::default(),
            repo_sender: Some(repo_sender),
            repo_receiver,
            sync_observer,
            outboxes: Default::default(),
            inboxes: Default::default(),
        }
    }

    fn get_repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    /// Poll the network adapter stream.
    fn collect_network_messages(&mut self) {
        for (_syncing_with_repo_id, inbox) in self.inboxes.lock().iter() {
            self.incoming_messages.append(&mut inbox.lock().messages);
        }
    }

    /// Try to send pending messages on the network sink.
    fn process_outgoing_network_messages(&mut self) {
        for message in mem::take(&mut self.outgoing_messages) {
            match message {
                NetworkMessage::Sync { to_repo_id, .. } => {
                    if let Some(waker) = {
                        let outboxes = self.outboxes.lock();
                        let outbox = outboxes.get(&to_repo_id).expect("No outbox for repo");
                        let mut outbox = outbox.lock();
                        outbox.messages.push_back(message);
                        outbox.waker.take()
                    } {
                        waker.wake();
                    }
                }
            }
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
                        self.outgoing_messages.push_back(outgoing);
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
                        self.outgoing_messages.push_back(outgoing);
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
            RepoEvent::IncomingSyncMessage => {}
        }
    }

    /// Apply incoming sync messages, and generate outgoing ones.
    fn sync_documents(&mut self) {
        // Process incoming events.
        // Handle events.
        let mut synced = vec![];
        for event in mem::take(&mut self.incoming_messages) {
            match event {
                NetworkMessage::Sync {
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
                            self.outgoing_messages.push_back(outgoing);
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
        let outboxes = self.outboxes.clone();
        let inboxes = self.inboxes.clone();

        // Run the repo's event-loop in a thread.
        let handle = thread::spawn(move || {
            loop {
                // Poll streams and sinks at the start of each iteration.
                // Required, in combination with `try_send` on the wakers,
                // to prevent deadlock.
                self.collect_network_messages();
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
                }
            }
        });
        RepoHandle {
            handle,
            document_id_counter,
            repo_id,
            repo_sender,
            outboxes,
            inboxes,
        }
    }
}

pub mod error {

    #[derive(thiserror::Error, Debug)]
    pub enum RunConnection<Send, Recv> {
        Send(Send),
        Recv(Recv),
        RecvCollection,
    }
}
