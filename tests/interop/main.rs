use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use automerge::{transaction::Transactable, ReadDoc};
use automerge_repo::{ConnDirection, Repo, RepoHandle};
use futures::StreamExt;
use test_log::test;
use test_utils::storage_utils::InMemoryStorage;

mod js_wrapper;
use js_wrapper::JsWrapper;

#[test(tokio::test)]
async fn sync_rust_clients_via_js_server() {
    let js = JsWrapper::create().await.unwrap();
    let js_server = js.start_server().await.unwrap();
    let port = js_server.port;

    let repo1 = repo_connected_to_js_server(port, Some("repo1".to_string())).await;

    let doc_handle_repo1 = repo1.handle.new_document().await;
    doc_handle_repo1
        .with_doc_mut(|doc| {
            doc.transact(|tx| {
                tx.put(automerge::ROOT, "key", "value")?;
                Ok::<_, automerge::AutomergeError>(())
            })
        })
        .unwrap();

    tokio::time::sleep(Duration::from_millis(1000)).await;

    let repo2 = repo_connected_to_js_server(port, Some("repo2".to_string())).await;

    let doc_handle_repo2 = repo2
        .handle
        .request_document(doc_handle_repo1.document_id())
        .await
        .unwrap()
        .unwrap();
    doc_handle_repo2.with_doc(|doc| {
        assert_eq!(
            doc.get::<_, &str>(automerge::ROOT, "key")
                .unwrap()
                .unwrap()
                .0
                .into_string()
                .unwrap()
                .as_str(),
            "value"
        );
    });
}

#[test(tokio::test)]
async fn send_ephemeral_messages_from_rust_clients_via_js_server() {
    let js = JsWrapper::create().await.unwrap();
    let js_server = js.start_server().await.unwrap();
    let port = js_server.port;

    let repo1 = repo_connected_to_js_server(port, Some("repo1".to_string())).await;

    let doc_handle_repo1 = repo1.handle.new_document().await;

    tokio::time::sleep(Duration::from_millis(1000)).await;

    let repo2 = repo_connected_to_js_server(port, Some("repo2".to_string())).await;

    let doc_handle_repo2 = repo2
        .handle
        .request_document(doc_handle_repo1.document_id())
        .await
        .unwrap()
        .unwrap();

    let mut ephemera = doc_handle_repo2.ephemera().await;

    // A cbor array of two integers
    let msg: Vec<u8> = vec![0x82, 0x01, 0x02];

    doc_handle_repo1.broadcast_ephemeral(msg).unwrap();

    let msg = tokio::time::timeout(Duration::from_millis(1000), ephemera.next())
        .await
        .expect("timed out waiting for ephemeral message")
        .expect("no ephemeral message received");

    assert_eq!(msg.bytes(), &msg.bytes()[..]);
    
}

#[test(tokio::test)]
async fn two_js_clients_can_sync_through_rust_server() {
    let server = start_rust_server().await;
    let js = JsWrapper::create().await.unwrap();
    let (doc_id, heads, _child1) = js.create_doc(server.port).await.unwrap();

    let fetched_heads = js.fetch_doc(server.port, doc_id).await.unwrap();

    assert_eq!(heads, fetched_heads);
}

#[test(tokio::test)]
async fn two_js_clients_can_send_ephemera_through_rust_server() {
    let js = JsWrapper::create().await.unwrap();
    let server = start_rust_server().await;

    let (doc_id, _heads, _child1) = js.create_doc(server.port).await.unwrap();

    let mut listening = js.receive_ephemera(server.port, doc_id.clone()).await.unwrap();

    js.send_ephemeral_message(server.port, doc_id, "hello").await.unwrap();

    let msg = tokio::time::timeout(Duration::from_millis(1000), listening.next())
        .await
        .expect("timed out waiting for ephemeral message")
        .expect("no ephemeral message received")
        .expect("error reading ephemeral message");

    assert_eq!(msg, "hello");
    
}

struct RunningRepo {
    handle: RepoHandle,
}

impl Drop for RunningRepo {
    fn drop(&mut self) {
        self.handle.clone().stop().unwrap();
    }
}

async fn repo_connected_to_js_server(port: u16, id: Option<String>) -> RunningRepo {
    let repo = Repo::new(id, Box::<InMemoryStorage>::default());
    let handle = repo.run();
    let (conn, _) = tokio_tungstenite::connect_async(format!("ws://localhost:{}", port))
        .await
        .unwrap();

    let driver = handle
        .connect_tungstenite(conn, ConnDirection::Outgoing)
        .await
        .expect("error connecting connection 1");

    tokio::spawn(async {
        if let Err(e) = driver.await {
            tracing::error!("Error running connection: {}", e);
        }
    });
    RunningRepo { handle }
}


struct RunningRustServer {
    port: u16,
    handle: RepoHandle,
    running_connections: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl Drop for RunningRustServer {
    fn drop(&mut self) {
        for handle in self.running_connections.lock().unwrap().drain(..) {
            handle.abort();
        }
        self.handle.clone().stop().unwrap();
    }
}

async fn start_rust_server() -> RunningRustServer {
    let handle = Repo::new(None, Box::<InMemoryStorage>::default()).run();
    let running_connections = Arc::new(Mutex::new(Vec::new()));
    let app = axum::Router::new()
        .route("/", axum::routing::get(websocket_handler))
        .with_state((handle.clone(), running_connections.clone()));
    let server = axum::Server::bind(&"0.0.0.0:0".parse().unwrap()).serve(app.into_make_service());
    let port = server.local_addr().port();
    tokio::spawn(server);
    RunningRustServer {
        port,
        handle,
        running_connections,
    }
}

async fn websocket_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    axum::extract::State((handle, running_connections)): axum::extract::State<(
        RepoHandle,
        Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    )>,
) -> axum::response::Response {
    ws.on_upgrade(|socket| handle_socket(socket, handle, running_connections))
}

async fn handle_socket(
    socket: axum::extract::ws::WebSocket,
    repo: RepoHandle,
    running_connections: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
) {
    let driver = repo
        .accept_axum(socket)
        .await
        .unwrap();
    let handle = tokio::spawn(async {
        if let Err(e) = driver.await {
            tracing::error!("Error running connection: {}", e);
        }
    });
    running_connections.lock().unwrap().push(handle);
}
