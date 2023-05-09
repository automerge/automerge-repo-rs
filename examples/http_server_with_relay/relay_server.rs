use std::{
    collections::{hash_map::Entry, HashMap},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use automerge::sync::Message;
use automerge_repo::{DocumentId, NetworkMessage, RepoId};
use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use futures::{select, FutureExt};
use serde_json::json;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    oneshot,
};
use tower_http::trace::TraceLayer;

#[derive(Clone)]
struct Relay {
    peers: Arc<Mutex<HashMap<RepoId, SocketAddr>>>,
    task_sender: Sender<SyncTask>,
}

impl Relay {
    async fn forward_sync(
        task_sender: Sender<SyncTask>,
        dest: SocketAddr,
        message: NetworkMessage,
    ) {
        let (tx, rx) = oneshot::channel();
        let task = SyncTask {
            dest,
            message,
            reply: tx,
        };
        task_sender.send(task).await.unwrap();
        rx.await.unwrap();
    }
}

#[derive(Debug)]
struct SyncTask {
    message: NetworkMessage,
    dest: SocketAddr,
    reply: oneshot::Sender<()>,
}

// Register a socket address to receive relayed messages
async fn register(
    State(state): State<Relay>,
    Json((repo_id, addr)): Json<(RepoId, SocketAddr)>,
    //Json((repo_id, _peer)): Json<(RepoId, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    tracing::info!(?repo_id, ?addr, "registering peer");
    let mut peers = state.peers.lock().unwrap();
    if let Entry::Vacant(e) = peers.entry(repo_id) {
        tracing::info!(
            ?repo_id,
            ?addr,
            "registering peer to receive relayed messages"
        );
        e.insert(addr);
        (StatusCode::OK, Json(json!({"message": "peer registered"})))
    } else {
        tracing::warn!(?repo_id, ?addr, "peer already registered");
        (
            StatusCode::CONFLICT,
            Json(json!({"message": "peer already registered"})),
        )
    }
}

async fn relay_sync(
    State(state): State<Relay>,
    Json((from_repo_id, to_repo_id, document_id, message)): Json<(
        RepoId,
        RepoId,
        DocumentId,
        Vec<u8>,
    )>,
) -> (StatusCode, Json<serde_json::Value>) {
    let dest = if let Some(peer_addr) = state.peers.lock().unwrap().get(&to_repo_id) {
        *peer_addr
    } else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"message": "peer not found"})),
        );
    };
    tracing::info!(?dest, "forwarding message");
    let message = Message::decode(&message).expect("Failed to decode sync message.");
    Relay::forward_sync(
        state.task_sender.clone(),
        dest,
        NetworkMessage::Sync {
            from_repo_id,
            to_repo_id,
            document_id,
            message,
        },
    )
    .await;
    (StatusCode::OK, Json(json!({"message": "message relayed"})))
}

async fn sync_tasks(mut receiver: Receiver<SyncTask>) {
    while let Some(task) = receiver.recv().await {
        let client = reqwest::Client::new();
        let url = format!("http://{}/sync_doc", task.dest);
        let NetworkMessage::Sync {
            from_repo_id,
            to_repo_id,
            document_id,
            message,
        } = task.message;
        client
            .post(url)
            .json(&(from_repo_id, to_repo_id, document_id, message.encode()))
            .send()
            .await
            .unwrap();
        task.reply.send(()).unwrap();
    }
}

pub async fn run_relay(ip: &SocketAddr) {
    let (sender, receiver) = tokio::sync::mpsc::channel(10);
    let relay = Relay {
        peers: Default::default(),
        task_sender: sender,
    };

    let sync = sync_tasks(receiver);

    let app = Router::new()
        .route("/relay_sync", post(relay_sync))
        .route("/register_relay", post(register))
        .layer(TraceLayer::new_for_http())
        .with_state(relay);
    let serve = axum::Server::bind(ip).serve(app.into_make_service());

    tracing::info!("relay server running at {}", ip);

    select! {
        _ = serve.fuse() => {
            tracing::info!("relay server stopped");
        }
        _ = sync.fuse() => {
            tracing::info!("sync task stopped");
        }
        _ = tokio::signal::ctrl_c().fuse() => {
            tracing::info!("ctrl-c received, shutting down");
        }
    }
}
