use crate::dochandle::{DocHandle, DocState};
use crate::interfaces::{CollectionId, DocumentId, RepoId};
use crate::interfaces::{NetworkEvent, NetworkMessage};
use automerge::sync::{Message as SyncMessage, State as SyncState, SyncDoc};
use automerge::AutoCommit;
use crossbeam_channel::{bounded, select, unbounded, Receiver, Sender};
use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use parking_lot::{Condvar, Mutex};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::mem;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};
use uuid::Uuid;

/// A collection of documents,
/// through which new docs can be created,
/// and doc handles acquired.
pub struct DocCollection {
    /// Channel used to send events back to the repo,
    /// such as when a doc is created, and a doc handle acquired.
    collection_sender: Sender<(CollectionId, CollectionEvent)>,
    collection_id: CollectionId,

    /// Counter to generate unique document ids.
    document_id_counter: u64,
}

impl DocCollection {
    pub fn get_repo_id(&self) -> RepoId {
        self.collection_id.0 .0
    }

    pub fn id(&self) -> CollectionId {
        self.collection_id
    }

    /// Create a new document.
    pub fn new_document(&mut self) -> DocHandle {
        self.document_id_counter = self
            .document_id_counter
            .checked_add(1)
            .expect("Overflowed when creating new document id.");
        let document_id = DocumentId((self.collection_id, self.document_id_counter));
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
            self.collection_sender.clone(),
            document_id,
            self.collection_id,
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
        self.collection_sender
            .send((
                self.collection_id,
                CollectionEvent::NewDocHandle(repo_id, document_id, doc_info),
            ))
            .expect("Failed to send collection event.");
        handle
    }
}

/// Events sent by doc collections or doc handles to the repo.
#[derive(Debug)]
pub(crate) enum CollectionEvent {
    /// Load a document,
    // repo id is the repo to start syncing with,
    // none if locally created.
    NewDocHandle(Option<RepoId>, DocumentId, DocumentInfo),
    /// A document changed.
    DocChange(DocumentId),
    /// A document was closed(all doc handles dropped).
    DocClosed(DocumentId),
    /// Network event
    Network(NetworkEvent),
}

/// Information on a doc collection held by the repo.
/// Each collection can be configured with different adapters.
struct CollectionInfo {
    documents: HashMap<DocumentId, DocumentInfo>,

    /// Messages to send on the network adapter sink.
    pending_messages: VecDeque<NetworkMessage>,
    /// Events received on the network stream, pending processing.
    pending_events: VecDeque<NetworkEvent>,

    /// Callback on applying sync messages.
    sync_observer: Option<Box<dyn Fn(Vec<DocumentId>) + Send>>,
}

/// Info about a document, held by the repo(via CollectionInfo).
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

enum ControlEvent {
    CreateCollection {
        observer: Option<Box<dyn Fn(Vec<DocumentId>) + Send>>,
        reply: Sender<DocCollection>,
    },
    Shutdown,
}

struct Outbox {
    waker: Option<Waker>,
    messages: Vec<NetworkMessage>,
}

impl Outbox {
    fn new() -> Self {
        Self {
            waker: None,
            messages: Vec::new(),
        }
    }
}

/// The backend of doc collections: the repo runs an event-loop in a background thread.
pub struct Repo {
    control_sender: Option<Sender<ControlEvent>>,
    control_receiver: Receiver<ControlEvent>,
    /// The Id of the repo.
    repo_id: RepoId,
    /// Counter to generate unique collection ids.
    collection_id_counter: u64,

    /// A map of collections to their info.
    collections: HashMap<CollectionId, CollectionInfo>,

    /// Sender and receiver of collection events.
    /// A sender is kept around to clone for multiple calls to `new_collection`,
    /// the sender is dropped at the start of the repo's event-loop,
    /// to ensure that the event-loop stops
    /// once all collections and doc handles have been dropped.
    collection_sender: Option<Sender<(CollectionId, CollectionEvent)>>,
    collection_receiver: Receiver<(CollectionId, CollectionEvent)>,
    outboxes: Arc<Mutex<HashMap<CollectionId, Outbox>>>,
}

impl Repo {
    /// Create a new repo, specifying a
    /// max number of collections that can be created for this repo.
    pub fn new(max_number_collections: usize) -> Self {
        let (collection_sender, collection_receiver) = unbounded();
        let (control_sender, control_receiver) = unbounded();
        Repo {
            repo_id: RepoId(Uuid::new_v4()),
            collection_id_counter: Default::default(),
            collections: Default::default(),
            collection_sender: Some(collection_sender),
            control_sender: Some(control_sender),
            control_receiver,
            collection_receiver,
            outboxes: Default::default(),
        }
    }

    /// Create a new doc collection
    /// Note: all collections must be created before starting to run the repo.
    fn new_collection(
        &mut self,
        sync_observer: Option<Box<dyn Fn(Vec<DocumentId>) + Send>>,
    ) -> DocCollection {
        self.collection_id_counter = self
            .collection_id_counter
            .checked_add(1)
            .expect("Overflowed when creating new collection id.");
        let collection_id = CollectionId((self.repo_id, self.collection_id_counter));
        let collection = DocCollection {
            collection_sender: self
                .collection_sender
                .clone()
                .expect("no collection sender"),
            collection_id,
            document_id_counter: Default::default(),
        };
        let collection_info = CollectionInfo {
            documents: Default::default(),
            pending_messages: Default::default(),
            pending_events: Default::default(),
            sync_observer,
        };
        self.collections.insert(collection_id, collection_info);
        collection
    }

    /// Handle incoming collection events(sent by colleciton or document handles).
    fn handle_collection_event(&mut self, collection_id: &CollectionId, event: CollectionEvent) {
        match event {
            CollectionEvent::NewDocHandle(repo_id, document_id, mut info) => {
                // A new doc handle has been created.
                let collection = self
                    .collections
                    .get_mut(collection_id)
                    .expect("Unexpected collection event.");

                // Send a sync message to the creator, unless it is the local repo.
                if let Some(repo_id) = repo_id {
                    if let Some(message) = info.generate_first_sync_message(repo_id) {
                        tracing::trace!("generating initial sync message");
                        let outgoing = NetworkMessage::Sync {
                            from_repo_id: *collection_id.get_repo_id(),
                            to_repo_id: repo_id,
                            document_id,
                            message,
                        };
                        tracing::trace!(?outgoing, "sending initial sync message");
                        collection.pending_messages.push_back(outgoing);
                    }
                }
                collection.documents.insert(document_id, info);
            }
            CollectionEvent::DocChange(doc_id) => {
                // Handle doc changes: sync the document.
                let collection = self
                    .collections
                    .get_mut(collection_id)
                    .expect("Unexpected collection event.");
                if let Some(info) = collection.documents.get_mut(&doc_id) {
                    for (to_repo_id, message) in info.generate_sync_messages().into_iter() {
                        let outgoing = NetworkMessage::Sync {
                            from_repo_id: *collection_id.get_repo_id(),
                            to_repo_id,
                            document_id: doc_id,
                            message,
                        };
                        collection.pending_messages.push_back(outgoing);
                    }
                }
            }
            CollectionEvent::DocClosed(id) => {
                // Handle doc closed: remove the document info.
                let collection = self
                    .collections
                    .get_mut(collection_id)
                    .expect("Unexpected collection event.");
                let doc_info = collection
                    .documents
                    .remove(&id)
                    .expect("Document closed but not doc info found.");
                if doc_info.handle_count.load(Ordering::SeqCst) != 0 {
                    panic!("Document closed with outstanding handles.");
                }
            }
            CollectionEvent::Network(network_evt) => {
                let collection = self
                    .collections
                    .get_mut(collection_id)
                    .expect("Unexpected collection event.");
                collection.pending_events.push_back(network_evt);
            }
        }
    }

    /// Apply incoming sync messages, and generate outgoing ones.
    fn sync_document_collection(&mut self, collection_id: &CollectionId) {
        let collection = self
            .collections
            .get_mut(collection_id)
            .expect("Unexpected collection event.");

        // Process incoming events.
        // Handle events.
        let mut synced = vec![];
        for event in mem::take(&mut collection.pending_events) {
            match event {
                NetworkEvent::Sync {
                    from_repo_id,
                    to_repo_id: local_repo_id,
                    document_id,
                    message,
                } => {
                    if let Some(info) = collection.documents.get_mut(&document_id) {
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
                            collection.pending_messages.push_back(outgoing);
                        }
                    }
                }
            }
        }

        // Notify the client of synced documents.
        if let Some(observer) = collection.sync_observer.as_ref() {
            if !synced.is_empty() {
                observer(synced);
            }
        }
    }

    fn fill_outboxes(&mut self) {
        for (collection_id, collection) in self.collections.iter_mut() {
            let mut outboxes = self.outboxes.lock();
            let outbox = outboxes.entry(*collection_id).or_insert_with(Outbox::new);
            tracing::trace!(
                num_messages = collection.pending_messages.len(),
                "enqueing outgoing messages"
            );
            outbox
                .messages
                .extend(collection.pending_messages.drain(..));
            if let Some(waker) = outbox.waker.take() {
                waker.wake();
            }
        }
    }

    /// The event-loop of the repo.
    /// Handles events from collections and adapters.
    /// Returns a `std::thread::JoinHandle` for optional clean shutdown.
    pub fn run(mut self) -> JoinHandle<()> {
        // Run the repo's event-loop in a thread.
        thread::spawn(move || {
            // Drop the repo's clone of the collection sender,
            // ensuring the below loop stops when all collections have been dropped.

            loop {
                select! {
                    recv(self.collection_receiver) -> collection_event => {
                        if let Ok((collection_id, event)) = collection_event {
                            tracing::trace!(?collection_id, ?event, "received collection event");
                            self.handle_collection_event(&collection_id, event);
                            self.sync_document_collection(&collection_id);
                            self.fill_outboxes();
                        } else {
                            // The repo shuts down
                            // once all handles and collections drop.
                            break;
                        }
                    },
                    recv(self.control_receiver) -> control_event => {
                        match control_event {
                            Ok(ControlEvent::CreateCollection{observer, reply}) => {
                                let collection = self.new_collection(observer);
                                reply.send(collection).unwrap();
                            },
                            Ok(ControlEvent::Shutdown) => {
                                tracing::info!("shutting down repo");
                                break;
                            },
                            Err(e) => {
                                tracing::error!("Error receiving control event: {:?}", e);
                            }
                        }
                    },
                }
            }
        })
    }

    pub fn handle(&self) -> RepoHandle {
        RepoHandle {
            control_sender: self.control_sender.clone().expect("Repo has shut down."),
            collection_sender: self.collection_sender.clone().expect("Repo has shut down."),
            outboxes: self.outboxes.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RepoHandle {
    control_sender: Sender<ControlEvent>,
    collection_sender: Sender<(CollectionId, CollectionEvent)>,
    outboxes: Arc<Mutex<HashMap<CollectionId, Outbox>>>,
}

impl RepoHandle {
    pub fn create_collection(
        &self,
        observer: Option<Box<dyn Fn(Vec<DocumentId>) + Send>>,
    ) -> DocCollection {
        let (reply, receiver) = bounded(1);
        self.control_sender
            .send(ControlEvent::CreateCollection { observer, reply })
            .unwrap();
        receiver.recv().unwrap()
    }

    pub async fn connect_stream<RecvErr, SendErr, S>(
        &self,
        collection: CollectionId,
        stream: S,
    ) -> Result<(), error::RunConnection<SendErr, RecvErr>>
    where
        RecvErr: std::fmt::Debug,
        SendErr: std::error::Error,
        S: Sink<NetworkMessage, Error = SendErr> + Stream<Item = Result<NetworkMessage, RecvErr>>,
    {
        let (mut sink, stream) = stream.split();
        let to_send = self.outgoing(collection).map(|m| Ok::<_, SendErr>(m));
        let sending = to_send.forward(&mut sink).fuse();

        let recv_msg = self.collection_sender.clone();
        let recving = stream
            .map_err(error::RunConnection::<SendErr, RecvErr>::Recv)
            .try_for_each({
                move |m| {
                    futures::future::ready(
                        recv_msg
                            .try_send((collection, CollectionEvent::Network(m.into())))
                            .map_err(|_| error::RunConnection::<SendErr, RecvErr>::RecvCollection),
                    )
                }
            })
            .fuse();

        let finished = futures::future::select(sending, recving).await;
        match finished {
            futures::future::Either::Left((result, _recving)) => match result {
                Ok(_) => {
                    tracing::trace!("we're closing the connection");
                    Ok(())
                }
                Err(e) => {
                    tracing::trace!(err=?e, "error while sending");
                    Err(error::RunConnection::Send(e))
                }
            },
            futures::future::Either::Right((result, _sending)) => {
                if let Err(e) = sink.close().await {
                    tracing::trace!(err=?e, "error while closing sink");
                }
                match result {
                    Ok(_) => {
                        tracing::trace!("the remote closed the connection");
                        Ok(())
                    }
                    Err(e) => {
                        tracing::trace!(err=?e, "error while receiving");
                        Err(e)
                    }
                }
            }
        }
    }

    fn outgoing(&self, collection: CollectionId) -> impl Stream<Item = NetworkMessage> {
        OutgoingMessages {
            outboxes: self.outboxes.clone(),
            id: collection,
        }
    }

    pub fn shutdown(&self) {
        self.control_sender.send(ControlEvent::Shutdown).unwrap();
    }
}

struct OutgoingMessages {
    outboxes: Arc<Mutex<HashMap<CollectionId, Outbox>>>,
    id: CollectionId,
}

impl Stream for OutgoingMessages {
    type Item = NetworkMessage;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut outboxes = self.outboxes.lock();
        if let Some(Outbox { waker, messages }) = outboxes.get_mut(&self.id) {
            if let Some(msg) = messages.pop() {
                Poll::Ready(Some(msg))
            } else {
                *waker = Some(cx.waker().clone());
                Poll::Pending
            }
        } else {
            outboxes.insert(
                self.id,
                Outbox {
                    messages: Vec::new(),
                    waker: Some(cx.waker().clone()),
                },
            );
            Poll::Pending
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
