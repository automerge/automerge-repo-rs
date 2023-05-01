mod dochandle;
mod interfaces;
mod repo;
mod simulation;

use crate::dochandle::DocHandle;
use crate::interfaces::DocumentId;
use crate::interfaces::{NetworkAdapter, NetworkError, NetworkEvent, NetworkMessage};
use crate::repo::DocCollection;
use crate::repo::Repo;
use automerge::sync::Message as SyncMessage;
use automerge::transaction::Transactable;
use automerge::ReadDoc;
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use core::pin::Pin;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::task::{Context, Poll, Waker};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};
use uuid::Uuid;

fn main() {
    struct Network<NetworkMessage> {
        buffer: Arc<Mutex<VecDeque<NetworkEvent>>>,
        stream_waker: Arc<Mutex<Option<Waker>>>,
        outgoing: Arc<Mutex<VecDeque<NetworkMessage>>>,
        sink_waker: Arc<Mutex<Option<Waker>>>,
        sender: Sender<()>,
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

    let buffer = Arc::new(Mutex::new(VecDeque::new()));
    let stream_waker = Arc::new(Mutex::new(None));
    let sink_waker = Arc::new(Mutex::new(None));
    let outgoing = Arc::new(Mutex::new(VecDeque::new()));
    let (sender, mut network_receiver) = channel(1);
    let network = Network {
        buffer: buffer.clone(),
        stream_waker: stream_waker.clone(),
        outgoing: outgoing.clone(),
        sender,
        sink_waker: sink_waker.clone(),
    };

    // Create the repo.
    let mut repo = Repo::new(1);

    // Create a new collection with a network and a storage adapters.
    let collection = repo.new_collection(network);

    // Run the repo in the background.
    let repo_join_handle = repo.run();

    // The client code uses tokio and axum.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Task that handles outgoing network messages.
    rt.spawn(async move {
        loop {
            let message = network_receiver.recv().await;
            let message = outgoing.lock().pop_front();
            if let Some(NetworkMessage::Sync(id, sync_message)) = message {
                let sync_message = sync_message.encode();
                let client = reqwest::Client::new();

                if let Some(waker) = sink_waker.lock().take() {
                    waker.wake();
                }
            }
        }
    });

    struct AppState {
        incoming: Arc<Mutex<VecDeque<NetworkEvent>>>,
        collection: DocCollection,
        doc_handles: Arc<Mutex<HashMap<DocumentId, DocHandle>>>,
        stream_waker: Arc<Mutex<Option<Waker>>>,
    }

    async fn new_doc(State(state): State<Arc<AppState>>) -> Json<DocumentId> {
        let doc_handle = state.collection.new_document();
        let doc_id = doc_handle.document_id();
        state.doc_handles.lock().insert(doc_id.clone(), doc_handle);
        Json(doc_id)
    }

    async fn load_doc(State(state): State<Arc<AppState>>, Path(id): Path<DocumentId>) {
        let doc_handle = state.collection.load_existing_document(id);
        state
            .doc_handles
            .lock()
            .insert(doc_handle.document_id(), doc_handle);
    }

    async fn edit_doc(
        State(state): State<Arc<AppState>>,
        Path(id): Path<DocumentId>,
        Path(string): Path<String>,
    ) {
        let mut handles = state.doc_handles.lock();
        if let Some(doc_handle) = handles.get_mut(&id) {
            doc_handle.with_doc_mut(|doc| {
                doc.put(automerge::ROOT, "key", string)
                    .expect("Failed to change the document.");
                doc.commit();
            });
        }
    }

    async fn sync_doc(
        State(state): State<Arc<AppState>>,
        Path(id): Path<DocumentId>,
        Path(msg): Path<Vec<u8>>,
    ) {
        let sync_message = SyncMessage::decode(&msg).expect("Failed to decode sync mesage.");
        state
            .incoming
            .lock()
            .push_back(NetworkEvent::Sync(id, sync_message));
        if let Some(waker) = state.stream_waker.lock().take() {
            waker.wake();
        }
    }

    let app_state = Arc::new(AppState {
        incoming: buffer,
        collection,
        doc_handles: Arc::new(Mutex::new(Default::default())),
        stream_waker,
    });

    let app = Router::new()
        .route("/new_doc", get(new_doc))
        .route("/load_doc/:id", get(load_doc))
        .route("/edit_doc/:id/:string", post(edit_doc))
        .route("/sync_doc/:id/:msg", post(sync_doc))
        .with_state(app_state);

    rt.spawn(async move {
        // run it with hyper on localhost:3000
        axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    repo_join_handle.join().unwrap();
    println!("Stopped");
}
