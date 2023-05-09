use automerge::transaction::Transactable;
use automerge::ReadDoc;
use automerge_repo::{NetworkAdapter, NetworkError, NetworkEvent, NetworkMessage, Repo, RepoId};
use core::pin::Pin;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::task::{Context, Poll, Waker};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};

#[derive(Debug, Clone)]
struct Network<NetworkMessage> {
    buffer: Arc<Mutex<VecDeque<NetworkEvent>>>,
    stream_waker: Arc<Mutex<Option<Waker>>>,
    outgoing: Arc<Mutex<VecDeque<NetworkMessage>>>,
    sink_waker: Arc<Mutex<Option<Waker>>>,
    sender: Sender<RepoId>,
    repo_id: RepoId,
}

impl Network<NetworkMessage> {
    pub fn new(sender: Sender<RepoId>, repo_id: RepoId) -> Self {
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
            repo_id,
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
        if self.sender.blocking_send(self.repo_id).is_err() {
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

#[test]
fn test_repo_stop() {
    let (sender, _network_receiver) = channel(1);

    // Create the repo.
    let mut repo = Repo::new(1);

    // Create the network adapter.
    let network = Network::new(sender.clone(), repo.get_repo_id());

    // Create a new collection with a network and a storage adapters.
    let collection = repo.new_collection(network, None);

    // Run the repo in the background.
    let repo_join_handle = repo.run();

    drop(collection);

    // All collections and doc handles have been dropped,
    // repo should stop running.
    repo_join_handle.join().unwrap();
}

#[test]
fn test_simple_sync() {
    let (sender, mut network_receiver) = channel(1);
    let mut repo_handles = vec![];
    let mut documents = vec![];
    let mut docs_to_sync = HashMap::new();
    let mut peers = HashMap::new();
    let mut collections = HashMap::new();

    for _ in 1..10 {
        // Create the repo.
        let mut repo = Repo::new(1);

        // Create the network adapter.
        let network = Network::new(sender.clone(), repo.get_repo_id());

        // Create a new collection with a network and a storage adapters.
        let mut collection = repo.new_collection(network.clone(), None);

        // Create a document.
        let mut doc_handle = collection.new_document();
        doc_handle.with_doc_mut(|doc| {
            doc.put(
                automerge::ROOT,
                "repo_id",
                format!("{}", repo.get_repo_id()),
            )
            .expect("Failed to change the document.");
            doc.commit();
        });

        peers.insert(repo.get_repo_id(), network);
        collections.insert(repo.get_repo_id(), collection);
        documents.push(doc_handle);

        // Run the repo in the background.
        repo_handles.push(repo.run());
    }

    // We want each repo to sync the documents created by all other repos.
    for collection in collections.values_mut() {
        let mut doc_handles = vec![];
        let repo_id = *collection.get_repo_id();
        for doc_handle in documents.iter() {
            let doc_id = doc_handle.document_id();
            if doc_id.get_repo_id() == &repo_id {
                continue;
            }
            doc_handles.push(collection.bootstrap_document_from_id(None, doc_id));
        }
        docs_to_sync.insert(repo_id, doc_handles);
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.spawn(async move {
        let (done_sync_sender, mut done_sync_receiver) = channel(1);
        Handle::current().spawn_blocking(move || {
            // Keep looping until all docs are in sync.
            let mut synced = 0;
            let total_expected = docs_to_sync
                .iter()
                .fold(0, |acc, (_, docs)| acc + docs.len());
            loop {
                if synced == total_expected {
                    done_sync_sender.blocking_send(()).unwrap();
                    return;
                }
                for (_, doc_handles) in docs_to_sync.iter_mut() {
                    for doc_handle in doc_handles {
                        let expected_repo_id =
                            format!("{}", doc_handle.document_id().get_repo_id());
                        doc_handle.with_doc_mut(|doc| {
                            let value = doc.get(automerge::ROOT, "repo_id");
                            if value.unwrap().unwrap().0.to_str().unwrap() == expected_repo_id {
                                synced = synced + 1;
                            }
                        });
                    }
                }
            }
        });

        // A router of network messages.
        loop {
            tokio::select! {
               outgoing_repo = network_receiver.recv() => {
                   let incoming = {
                       let peer = peers.get_mut(&outgoing_repo.unwrap()).unwrap();
                       peer.take_outgoing()
                   };
                   match incoming {
                       NetworkMessage::Sync {
                           from_repo_id,
                           to_repo_id,
                           document_id,
                           message,
                       } => {
                           let peer = peers.get_mut(&to_repo_id).unwrap();
                           peer.receive_incoming(NetworkEvent::Sync {
                               from_repo_id,
                               to_repo_id,
                               document_id,
                               message,
                           });
                       }
                   }
               },
               _ = done_sync_receiver.recv() => {
                   // Shutdown the repo.
                   drop(documents);
                   drop(collections);
                   break;
               }
            }
        }
    });

    // All collections and doc handles have been dropped,
    // repo should stop running.
    for handle in repo_handles.into_iter() {
        handle.join().unwrap();
    }
}
