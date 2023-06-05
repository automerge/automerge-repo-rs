use automerge::transaction::Transactable;
use automerge::ReadDoc;
use automerge_repo::{ConnDirection, Repo, StorageAdapter};
use automerge_repo::{DocumentId, StorageError};
use clap::Parser;
use futures::future::TryFutureExt;
use futures::Future;
use futures::FutureExt;
use std::collections::HashMap;
use std::marker::Unpin;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot::{channel as oneshot, Sender as OneShot};

#[derive(Debug)]
enum StorageRequest {
    Load(DocumentId, OneShot<Option<Vec<u8>>>),
    Append(DocumentId, Vec<u8>, OneShot<()>),
    Compact(DocumentId, Vec<u8>, OneShot<()>),
    ListAll(OneShot<Vec<DocumentId>>),
}

#[derive(Clone, Debug)]
pub struct AsyncInMemoryStorage {
    chan: Sender<StorageRequest>,
}

impl AsyncInMemoryStorage {
    pub fn new() -> Self {
        let mut documents: HashMap<DocumentId, Vec<u8>> = Default::default();
        let (doc_request_sender, mut doc_request_receiver) = channel::<StorageRequest>(1);
        tokio::spawn(async move {
            loop {
                while let Some(request) = doc_request_receiver.recv().await {
                    match request {
                        StorageRequest::ListAll(sender) => {
                            let result = documents.keys().cloned().collect();
                            let _ = sender.send(result);
                        }
                        StorageRequest::Load(doc_id, sender) => {
                            let result = documents.get(&doc_id).cloned();
                            let _ = sender.send(result);
                        }
                        StorageRequest::Append(doc_id, mut data, sender) => {
                            let entry = documents.entry(doc_id).or_insert_with(Default::default);
                            entry.append(&mut data);
                            let _ = sender.send(());
                        }
                        StorageRequest::Compact(doc_id, data, sender) => {
                            let _entry = documents
                                .entry(doc_id)
                                .and_modify(|entry| *entry = data)
                                .or_insert_with(Default::default);
                            let _ = sender.send(());
                        }
                    }
                }
            }
        });
        AsyncInMemoryStorage {
            chan: doc_request_sender,
        }
    }
}

impl StorageAdapter for AsyncInMemoryStorage {
    fn get(
        &self,
        id: DocumentId,
    ) -> Box<dyn Future<Output = Result<Option<Vec<u8>>, StorageError>> + Send + Unpin> {
        let (tx, rx) = oneshot();
        self.chan
            .blocking_send(StorageRequest::Load(id, tx))
            .unwrap();
        Box::new(rx.map_err(|_| StorageError::Error))
    }

    fn list_all(
        &self,
    ) -> Box<dyn Future<Output = Result<Vec<DocumentId>, StorageError>> + Send + Unpin> {
        let (tx, rx) = oneshot();
        self.chan
            .blocking_send(StorageRequest::ListAll(tx))
            .unwrap();
        Box::new(rx.map_err(|_| StorageError::Error))
    }

    fn append(
        &self,
        id: DocumentId,
        changes: Vec<u8>,
    ) -> Box<dyn Future<Output = Result<(), StorageError>> + Send + Unpin> {
        let (tx, rx) = oneshot();
        self.chan
            .blocking_send(StorageRequest::Append(id, changes, tx))
            .unwrap();
        Box::new(rx.map_err(|_| StorageError::Error))
    }

    fn compact(
        &self,
        id: DocumentId,
        full_doc: Vec<u8>,
    ) -> Box<dyn Future<Output = Result<(), StorageError>> + Send + Unpin> {
        let (tx, rx) = oneshot();
        self.chan
            .blocking_send(StorageRequest::Compact(id, full_doc, tx))
            .unwrap();
        Box::new(rx.map_err(|_| StorageError::Error))
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    run_ip: Option<String>,
    #[arg(long)]
    other_ip: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let run_ip = args.run_ip;
    let other_ip = args.other_ip;

    // Create a repo.
    let repo = Repo::new(None, Box::new(AsyncInMemoryStorage::new()));
    let repo_handle = repo.run();
    let repo_handle_clone = repo_handle.clone();

    // Create a document.
    {
        let mut doc_handle = repo_handle.new_document();
        let our_id = repo_handle.get_repo_id();
        doc_handle.with_doc_mut(|doc| {
            doc.put(automerge::ROOT, "repo_id", format!("{}", our_id))
                .expect("Failed to change the document.");
            doc.commit();
        });
    }

    if let Some(run_ip) = run_ip {
        // Start a server.
        let listener = TcpListener::bind(run_ip).await.unwrap();
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    repo_handle
                        .connect_tokio_io(addr, socket, ConnDirection::Incoming)
                        .await
                        .unwrap();
                }
                Err(e) => println!("couldn't get client: {:?}", e),
            }
        }
    } else {
        // Start a client.
        // Spawn a task connecting to the other peer.
        let other_ip = other_ip.unwrap();
        let stream = loop {
            // Try to connect to a peer
            let res = TcpStream::connect(other_ip.clone()).await;
            if res.is_err() {
                continue;
            }
            break res.unwrap();
        };
        repo_handle
            .connect_tokio_io(other_ip, stream, ConnDirection::Outgoing)
            .await
            .unwrap();
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c().fuse() => {
            let synced_docs = repo_handle_clone.list_all().await.unwrap();
            for doc_id in synced_docs {
                let doc = repo_handle_clone
                    .request_document(doc_id.clone())
                    .await
                    .unwrap();
                doc.with_doc(|doc| {
                    let val = doc
                        .get(automerge::ROOT, "repo_id")
                        .expect("Failed to read the document.")
                        .unwrap();
                    let val = val.0.to_str().clone().unwrap();
                    println!("Synced: {:?} to {:?}", doc_id, val);
                });
            }
            repo_handle_clone.stop().unwrap();
        }
    }
}
