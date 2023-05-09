use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use automerge::{sync::Message, transaction::Transactable, AutoSerde};
use automerge_repo::{DocCollection, DocHandle, DocumentId, NetworkMessage, Repo, RepoId};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use futures::{select, FutureExt, StreamExt};
use tokio::runtime::Handle;
use tower_http::trace::TraceLayer;

struct AppState {
    sync_channel: super::http_sync_channel::IncomingHandle,
    collection: Arc<Mutex<DocCollection>>,
    doc_handles: Arc<Mutex<HashMap<DocumentId, DocHandle>>>,
}

pub(crate) async fn run_peer(ip: &SocketAddr, relay_ip: &SocketAddr) {
    let doc_handles: Arc<Mutex<HashMap<DocumentId, DocHandle>>> =
        Arc::new(Mutex::new(Default::default()));

    // Create the repo.
    let repo = Repo::new(1);
    let repo_handle = repo.handle();

    // Run the repo in the background.
    let repo_join_handle = repo.run();

    // Create a new collection with a network and a storage adapters.
    let doc_handles_clone = doc_handles.clone();
    let collection = repo_handle.create_collection(Some(Box::new(move |synced| {
        for doc_id in synced {
            let handles = doc_handles_clone.lock().unwrap();
            let doc_handle = handles.get(&doc_id).unwrap();
            println!("Synced {:?}", doc_handle.document_id());
        }
    })));
    let repo_id = collection.get_repo_id();
    let coll_id = collection.id();
    tracing::info!(?repo_id, "repo created");

    // Create the subroutine which communicates with the relay server
    let super::http_sync_channel::RunChannel {
        incoming, // We use this to handle messages from the relay server
        stream, // We give this to the repo
        task, // We drive this to send messages to the relay server
    } = super::http_sync_channel::run(*relay_ip);
    // Now give the relay stream to the repo
    let run_connection = repo_handle.connect_stream(coll_id, stream.map(Ok::<_, ()>));

    let app_state = Arc::new(AppState {
        sync_channel: incoming,
        collection: Arc::new(Mutex::new(collection)),
        doc_handles,
    });

    let app = Router::new()
        .route("/new_doc", get(new_doc))
        .route("/load_doc", post(load_doc))
        .route("/edit_doc/:string", post(edit_doc))
        .route("/sync_doc", post(sync_doc))
        .route("/print_doc", post(print_doc))
        .layer(TraceLayer::new_for_http())
        .with_state(app_state);

    let serve = axum::Server::bind(ip).serve(app.into_make_service());

    let resp = reqwest::Client::new()
        .post(format!("http://{}/register_relay", relay_ip))
        .json(&(repo_id, ip))
        .send()
        .await
        .unwrap();
    if resp.status() != StatusCode::OK {
        let body = resp.text().await.unwrap();
        tracing::error!(err=?body, "Failed to register with relay");
        return;
    }

    tracing::info!("peer running at {}", ip);
    tracing::info!("configured relay is {}", relay_ip);

    // Now run all the subroutines until something dies
    select! {
        _ = serve.fuse() => {
            tracing::warn!("Server exited.");
        },
        _ = tokio::signal::ctrl_c().fuse() => {
            tracing::info!("Ctrl-C received, shutting down.");
        },
        _ = task.fuse() => {
            tracing::warn!("http sync channel exited.");
        },
        _ = run_connection.fuse() => {
            tracing::warn!("connection exited.");
        },
    }

    repo_handle.shutdown();
    repo_join_handle.join().unwrap();
}

async fn new_doc(State(state): State<Arc<AppState>>) -> Json<DocumentId> {
    let doc_handle = state.collection.lock().unwrap().new_document();
    let doc_id = doc_handle.document_id();
    state.doc_handles.lock().unwrap().insert(doc_id, doc_handle);
    Json(doc_id)
}

async fn load_doc(State(state): State<Arc<AppState>>, Json(document_id): Json<DocumentId>) {
    let doc_handle = state
        .collection
        .lock()
        .unwrap()
        .bootstrap_document_from_id(None, document_id);
    state
        .doc_handles
        .lock()
        .unwrap()
        .insert(doc_handle.document_id(), doc_handle);
}

async fn edit_doc(
    State(state): State<Arc<AppState>>,
    Path(string): Path<String>,
    Json(id): Json<DocumentId>,
) {
    let mut handle_clone = {
        let mut handles = state.doc_handles.lock().unwrap();
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
    let mut handles = state.doc_handles.lock().unwrap();
    let doc_handle = handles.get_mut(&id).unwrap();
    doc_handle.with_doc_mut(|doc| {
        doc.put(automerge::ROOT, "key", string)
            .expect("Failed to change the document.");
        doc.commit();
    });
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
    let message = Message::decode(&message).expect("Failed to decode sync mesage.");
    state
        .sync_channel
        .recv(NetworkMessage::Sync {
            from_repo_id,
            to_repo_id,
            document_id,
            message,
        })
        .await;
}

async fn print_doc(
    State(state): State<Arc<AppState>>,
    Json(id): Json<DocumentId>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut handles = state.doc_handles.lock().unwrap();
    let doc_handle = handles.get_mut(&id).unwrap();
    (
        StatusCode::OK,
        doc_handle.with_doc_mut(|doc| {
            Json(serde_json::to_value(AutoSerde::from(doc.document())).unwrap())
        }),
    )
}
