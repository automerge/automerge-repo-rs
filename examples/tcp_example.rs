use automerge::sync::Message as SyncMessage;
use automerge::transaction::Transactable;
use automerge::ReadDoc;
use automerge_repo::{
    DocHandle, DocumentId, NetworkAdapter, NetworkError, NetworkEvent, NetworkMessage, Repo,
    RepoHandle, RepoId,
};
use clap::Parser;
use core::pin::Pin;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::task::{Context, Poll, Waker};
use futures::FutureExt;
use futures::SinkExt;
use futures::TryStreamExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};
use tokio_serde::formats::*;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

#[derive(Debug, Clone)]
struct Network<NetworkMessage> {
    buffer: Arc<Mutex<VecDeque<NetworkEvent>>>,
    stream_waker: Arc<Mutex<Option<Waker>>>,
    outgoing: Arc<Mutex<VecDeque<NetworkMessage>>>,
    sink_waker: Arc<Mutex<Option<Waker>>>,
    sender: Sender<(RepoId, RepoId)>,
    closed: Arc<Mutex<bool>>,
}

impl Network<NetworkMessage> {
    pub fn new(sender: Sender<(RepoId, RepoId)>) -> Self {
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
            closed: Arc::new(Mutex::new(false)),
        }
    }

    pub fn receive_incoming(&self, event: NetworkEvent) {
        self.buffer.lock().push_back(event);
        if let Some(waker) = self.stream_waker.lock().take() {
            waker.wake();
        }
    }

    pub fn take_outgoing(&self) -> NetworkMessage {
        let message = self.outgoing.lock().pop_front().unwrap();
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
        if *self.closed.lock() {
            return Poll::Ready(None);
        }
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
        if *self.closed.lock() {
            return Poll::Ready(Err(NetworkError::Error));
        }
        *self.sink_waker.lock() = Some(cx.waker().clone());
        if self.outgoing.lock().is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn start_send(self: Pin<&mut Self>, item: NetworkMessage) -> Result<(), Self::Error> {
        let (from_repo_id, to_repo_id) = match &item {
            NetworkMessage::Sync {
                from_repo_id,
                to_repo_id,
                ..
            } => (from_repo_id.clone(), to_repo_id.clone()),
        };

        self.outgoing.lock().push_back(item);
        if self
            .sender
            .blocking_send((from_repo_id, to_repo_id))
            .is_err()
        {
            return Err(NetworkError::Error);
        }
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

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    run_ip: String,
    #[arg(long)]
    other_ip: String,
}

#[derive(Deserialize, Serialize, Debug)]
struct Message {
    from_repo_id: RepoId,
    to_repo_id: RepoId,
    document_id: DocumentId,
    message: Vec<u8>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let run_ip = args.run_ip;
    let other_ip = args.other_ip;

    // Create a repo, the id is the local ip.
    let repo = Repo::new(None, Some(run_ip.clone()));
    let repo_handle = Arc::new(Mutex::new(Some(repo.run())));
    let repo_handle_clone = repo_handle.clone();

    let (sender, mut network_receiver) = channel(1);
    let peers = Arc::new(Mutex::new(HashMap::new()));
    let mut synced_docs = Arc::new(Mutex::new(vec![]));

    // Spawn a listening task.
    let handle_clone = repo_handle.clone();
    let peers_clone = peers.clone();
    let synced_docs_clone = synced_docs.clone();
    Handle::current().spawn(async move {
        let listener = TcpListener::bind(run_ip).await.unwrap();

        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    println!("new client: {:?}", addr);
                    let adapter = Network::new(sender.clone());
                    let repo_id = RepoId(addr.to_string());
                    repo_handle
                        .lock()
                        .as_mut()
                        .unwrap()
                        .new_network_adapter(repo_id.clone(), Box::new(adapter.clone()));
                    peers_clone.lock().insert(repo_id, adapter.clone());
                    let inner_sync_docs_clone = synced_docs_clone.clone();
                    Handle::current().spawn(async move {
                        // Delimit frames using a length header
                        let length_delimited = FramedRead::new(socket, LengthDelimitedCodec::new());

                        // Deserialize frames
                        let mut deserialized = tokio_serde::SymmetricallyFramed::new(
                            length_delimited,
                            SymmetricalJson::<Message>::default(),
                        );
                        while let Some(Message {
                            from_repo_id,
                            to_repo_id,
                            document_id,
                            message,
                        }) = deserialized.try_next().await.unwrap()
                        {
                            println!("GOT message from: {:?}", from_repo_id);
                            inner_sync_docs_clone.lock().push(document_id.clone());
                            let message = SyncMessage::decode(&message)
                                .expect("Failed to decode sync mesage.");
                            let incoming = NetworkEvent::Sync {
                                from_repo_id,
                                to_repo_id,
                                document_id,
                                message,
                            };
                            adapter.receive_incoming(incoming);
                        }

                        *adapter.closed.lock() = true;
                    });
                }
                Err(e) => println!("couldn't get client: {:?}", e),
            }
        }
    });

    // Spawn a task connecting to the other peer.
    Handle::current().spawn(async move {
        let mut stream = loop {
            // Try to connect to a peer
            let res = TcpStream::connect(other_ip.clone()).await;
            if res.is_err() {
                continue;
            }
            break res.unwrap();
        };

        // Delimit frames using a length header
        let length_delimited = FramedWrite::new(stream, LengthDelimitedCodec::new());

        // Serialize frames with JSON
        let mut serialized =
            tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());

        loop {
            tokio::select! {
               msg = network_receiver.recv() => {
                   let (_from_repo_id, to_repo_id) = msg.unwrap();
                   let NetworkMessage::Sync {
                       from_repo_id,
                       to_repo_id,
                       document_id,
                       message,
                   } = {

                       let mut peers = peers.lock();
                       let adapter = peers.get_mut(&to_repo_id).unwrap();
                       adapter.take_outgoing()
                   };

                   // Send the value
                   serialized
                       .send(json!(Message {
                           from_repo_id,
                           to_repo_id,
                           document_id,
                           message: message.encode()
                       }))
                       .await
                       .unwrap();
               },
            }
        }
    });

    // Create a document.
    {
        let mut handle = repo_handle_clone.lock();
        let mut doc_handle = handle.as_mut().unwrap().new_document();
        let our_id = handle.as_mut().unwrap().get_repo_id();
        doc_handle.with_doc_mut(|doc| {
            doc.put(automerge::ROOT, "repo_id", format!("{}", our_id))
                .expect("Failed to change the document.");
            doc.commit();
        });
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c().fuse() => {
            for id in synced_docs.lock().iter() {
                // TODO: add API to get handle to an existing doc in the repo.
                println!("Synced: {:?}", id);
            }
            repo_handle_clone.lock().take().unwrap().stop().unwrap();
        }
    }
}
