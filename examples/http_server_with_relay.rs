use automerge::sync::Message as SyncMessage;
use automerge::transaction::Transactable;
use automerge::ReadDoc;
use automerge_repo::{
    DocHandle, DocumentId, NetworkAdapter, NetworkError, NetworkEvent, NetworkMessage, Repo,
    RepoHandle, RepoId, StorageAdapter,
};
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::Parser;
use core::pin::Pin;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::task::{Context, Poll, Waker};
use futures::FutureExt;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    run_ip: String,
    #[arg(long)]
    relay_ip: String,
}

#[derive(Clone)]
struct Network<NetworkMessage> {
    buffer: Arc<Mutex<VecDeque<NetworkEvent>>>,
    stream_waker: Arc<Mutex<Option<Waker>>>,
    outgoing: Arc<Mutex<VecDeque<NetworkMessage>>>,
    sink_waker: Arc<Mutex<Option<Waker>>>,
    sender: Sender<()>,
}

impl Network<NetworkMessage> {
    pub fn new(sender: Sender<()>) -> Self {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let stream_waker = Arc::new(Mutex::new(None));
        let sink_waker = Arc::new(Mutex::new(None));
        let outgoing = Arc::new(Mutex::new(VecDeque::new()));
        Network {
            buffer: buffer.clone(),
            stream_waker: stream_waker.clone(),
            outgoing: outgoing.clone(),
            sender,
            sink_waker: sink_waker.clone(),
        }
    }

    pub fn receive_incoming(&self, event: NetworkEvent) {
        self.buffer.lock().push_back(event);
        if let Some(waker) = self.stream_waker.lock().take() {
            waker.wake();
        }
    }

    pub fn take_outgoing(&self) -> Option<NetworkMessage> {
        let message = self.outgoing.lock().pop_front();
        if let Some(waker) = self.sink_waker.lock().take() {
            waker.wake();
        }
        message
    }
}

impl Stream for Network<NetworkMessage> {
    type Item = NetworkEvent;
    fn poll_next(
        self: Pin<&mut Network<NetworkMessage>>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<NetworkEvent>> {
        *self.stream_waker.lock() = Some(cx.waker().clone());
        if let Some(event) = self.buffer.lock().pop_front() {
            Poll::Ready(Some(event))
        } else {
            Poll::Pending
        }
    }
}

impl Sink<NetworkMessage> for Network<NetworkMessage> {
    type Error = NetworkError;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        *self.sink_waker.lock() = Some(cx.waker().clone());
        if self.outgoing.lock().is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn start_send(self: Pin<&mut Self>, item: NetworkMessage) -> Result<(), Self::Error> {
        self.outgoing.lock().push_back(item);
        self.sender.blocking_send(()).unwrap();
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        *self.sink_waker.lock() = Some(cx.waker().clone());
        if self.outgoing.lock().is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        *self.sink_waker.lock() = Some(cx.waker().clone());
        if self.outgoing.lock().is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl NetworkAdapter for Network<NetworkMessage> {}

struct Storage;

impl StorageAdapter for Storage {}

fn main() {
    let args = Args::parse();
    let (sender, mut network_receiver) = channel(1);
    let doc_handles: Arc<Mutex<HashMap<DocumentId, DocHandle>>> =
        Arc::new(Mutex::new(Default::default()));
    let peers = Arc::new(Mutex::new(Default::default()));
    let adapter = Network::new(sender.clone());

    // Create the repo.
    let repo = Repo::new(
        Some(Box::new(move |synced| {
            for doc_handle in synced {
                println!("Synced {:?}", doc_handle.document_id());
            }
        })),
        None,
        Box::new(Storage),
    );

    // Run the repo in the background.
    let repo_handle = repo.run();

    // The client code uses tokio and axum.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Task that handles outgoing network messages,
    // which are all sent to the relay server.
    let adapter_clone = adapter.clone();
    let relay_ip = args.relay_ip.clone();
    rt.spawn(async move {
        let client = reqwest::Client::new();
        loop {
            if network_receiver.recv().await.is_none() {
                return;
            }
            let message = adapter_clone.take_outgoing();
            println!("Outoing message: {:?}", message);
            if let Some(NetworkMessage::Sync {
                from_repo_id,
                to_repo_id,
                document_id,
                message,
            }) = message
            {
                let message = message.encode();
                let url = format!("http://{}/relay_sync", relay_ip);
                client
                    .post(url)
                    .json(&(from_repo_id, to_repo_id, document_id, message))
                    .send()
                    .await
                    .unwrap();
            }
        }
    });

    struct AppState {
        repo_handle: Arc<Mutex<Option<RepoHandle>>>,
        doc_handles: Arc<Mutex<HashMap<DocumentId, DocHandle>>>,
        adapter: Network<NetworkMessage>,
        connected_adapters: Arc<Mutex<HashSet<RepoId>>>,
        peers: Arc<Mutex<HashMap<RepoId, String>>>,
    }

    async fn new_doc(State(state): State<Arc<AppState>>) -> Json<DocumentId> {
        let doc_handle = state.repo_handle.lock().as_mut().unwrap().new_document();
        let doc_id = doc_handle.document_id();
        state.doc_handles.lock().insert(doc_id.clone(), doc_handle);
        Json(doc_id)
    }

    async fn load_doc(State(state): State<Arc<AppState>>, Json(document_id): Json<DocumentId>) {
        state
            .repo_handle
            .lock()
            .as_mut()
            .unwrap()
            .new_network_adapter(
                document_id.get_repo_id().clone(),
                Box::new(state.adapter.clone()),
            );
        let doc_handle = state
            .repo_handle
            .lock()
            .as_mut()
            .unwrap()
            .bootstrap_document_from_id(None, document_id);
        state
            .doc_handles
            .lock()
            .insert(doc_handle.document_id(), doc_handle);
    }

    async fn edit_doc(
        State(state): State<Arc<AppState>>,
        Path(string): Path<String>,
        Json(id): Json<DocumentId>,
    ) {
        let mut handle_clone = {
            let mut handles = state.doc_handles.lock();
            let doc_handle = handles.get_mut(&id).unwrap();
            if doc_handle.is_ready_for_editing() {
                // Make the edit and return.
                doc_handle.with_doc_mut(|doc| {
                    doc.put(automerge::ROOT, "key", string)
                        .expect("Failed to change the document.");
                    doc.commit();
                });
                return;
            } else {
                // Clone the handle and wait for it to be ready.
                // Need to clone so as not to hold the lock across await.
                doc_handle.clone()
            }
        };
        Handle::current()
            .spawn_blocking(move || {
                // Wait for the document
                // to get into the `ready` state.
                handle_clone.wait_ready();
            })
            .await
            .unwrap();

        // Here we are sure the doc is ready.
        let mut handles = state.doc_handles.lock();
        let doc_handle = handles.get_mut(&id).unwrap();
        doc_handle.with_doc_mut(|doc| {
            doc.put(automerge::ROOT, "key", string)
                .expect("Failed to change the document.");
            doc.commit();
        });
    }

    async fn register_relay(
        State(state): State<Arc<AppState>>,
        Json((repo_id, peer)): Json<(RepoId, String)>,
    ) {
        state.peers.lock().insert(repo_id, peer);
    }

    async fn relay_sync(
        State(state): State<Arc<AppState>>,
        Json((from_repo_id, to_repo_id, document_id, message)): Json<(
            RepoId,
            RepoId,
            DocumentId,
            Vec<u8>,
        )>,
    ) {
        let client = reqwest::Client::new();
        let url = format!(
            "http://{}/sync_doc",
            state.peers.lock().get(&to_repo_id).unwrap()
        );
        client
            .post(url)
            .json(&(from_repo_id, to_repo_id, document_id, message))
            .send()
            .await
            .unwrap();
    }

    async fn sync_doc(
        State(state): State<Arc<AppState>>,
        Json((from_repo_id, to_repo_id, document_id, message)): Json<(
            RepoId,
            RepoId,
            DocumentId,
            Vec<u8>,
        )>,
    ) {
        // Connect the adapter for the peer if we haven't already.
        if state.connected_adapters.lock().insert(from_repo_id.clone()) {
            state
                .repo_handle
                .lock()
                .as_mut()
                .unwrap()
                .new_network_adapter(from_repo_id.clone(), Box::new(state.adapter.clone()));
        }
        let message = SyncMessage::decode(&message).expect("Failed to decode sync mesage.");
        state.adapter.receive_incoming(NetworkEvent::Sync {
            from_repo_id,
            to_repo_id,
            document_id,
            message,
        });
    }

    async fn print_doc(State(state): State<Arc<AppState>>, Json(id): Json<DocumentId>) {
        let mut handles = state.doc_handles.lock();
        let doc_handle = handles.get_mut(&id).unwrap();
        doc_handle.with_doc_mut(|doc| {
            println!("Doc: {:?}", doc.get(automerge::ROOT, "key"));
        });
    }

    let repo_id = repo_handle.get_repo_id().clone();
    let repo_handle = Arc::new(Mutex::new(Some(repo_handle)));
    let app_state = Arc::new(AppState {
        adapter,
        repo_handle: repo_handle.clone(),
        doc_handles,
        peers,
        connected_adapters: Arc::new(Mutex::new(Default::default())),
    });

    let (done_sender, mut done_receiver) = channel(1);
    let app = Router::new()
        .route("/new_doc", get(new_doc))
        .route("/load_doc", post(load_doc))
        .route("/edit_doc/:string", post(edit_doc))
        .route("/sync_doc", post(sync_doc))
        .route("/relay_sync", post(relay_sync))
        .route("/register_relay", post(register_relay))
        .route("/print_doc", post(print_doc))
        .with_state(app_state);

    rt.spawn(async move {
        if args.relay_ip != args.run_ip {
            // Register with relay server.
            let client = reqwest::Client::new();
            let url = format!("http://{}/register_relay", args.relay_ip);
            client
                .post(url)
                .json(&(repo_id, args.run_ip.clone()))
                .send()
                .await
                .unwrap();
        }
        let serve =
            axum::Server::bind(&args.run_ip.parse().unwrap()).serve(app.into_make_service());
        tokio::select! {
            _ = serve.fuse() => {},
            _ = tokio::signal::ctrl_c().fuse() => {
                repo_handle.lock().take().unwrap().stop().unwrap();
                done_sender.send(()).await.unwrap();
            }
        }
    });

    done_receiver.blocking_recv().unwrap();
    println!("Stopped");
}
