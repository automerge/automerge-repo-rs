mod dochandle;
mod interfaces;
mod repo;

use crate::interfaces::DocumentId;
use crate::interfaces::{NetworkAdapter, NetworkError, NetworkEvent, NetworkMessage};
use crate::repo::Repo;
use automerge::sync::{State as SyncState, SyncDoc};
use automerge::transaction::Transactable;
use automerge::ReadDoc;
use core::pin::Pin;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::task::{Context, Poll, Waker};
use parking_lot::Mutex;
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

    // The client code uses tokio.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // A channel used to block the main function
    // until the async system here has shut down.
    let (done_sender, mut done_receiver) = channel(1);

    // Spawn the backend for the client code.
    rt.spawn(async move {
        // Create a new document
        // (or rather acquire a handle to an existing doc
        // to be synced over the network).
        let document_id = DocumentId(Uuid::new_v4());
        let handle = collection.load_existing_document(document_id.clone());
        let handle_clone = handle.clone();
        let another_clone = handle.clone();

        // Spawn a task that receives data from the "other peer".
        Handle::current().spawn(async move {
            // Create a doc, change it, and send a sync message.
            let mut peer2 = automerge::AutoCommit::new();
            let mut peer2_state = SyncState::new();
            peer2.put(automerge::ROOT, "key", "value").unwrap();
            let first_message = peer2.sync().generate_sync_message(&mut peer2_state).unwrap();
            buffer.lock().push_back(NetworkEvent::Sync(document_id.clone(), first_message));
            if let Some(waker) = stream_waker.lock().take() {
                waker.wake();
            }
            loop {
                tokio::select! {
                        _ = network_receiver.recv() => {
                            let message = outgoing.lock().pop_front();
                            if let Some(network_message) = message {
                                match network_message {
                                    NetworkMessage::Sync(_, message) => {
                                        peer2.sync().receive_sync_message(&mut peer2_state, message).unwrap();
                                        if let Some(message) = peer2.sync().generate_sync_message(&mut peer2_state) {
                                            buffer.lock().push_back(NetworkEvent::Sync(document_id.clone(), message));
                                            if let Some(waker) = stream_waker.lock().take() {
                                                waker.wake();
                                            }
                                        }
                                    },
                                }
                                if let Some(waker) = sink_waker.lock().take() {
                                    waker.wake();
                                }
                            }
                        },
                };
                if peer2.get(automerge::ROOT, "key").unwrap().unwrap().0.to_str() ==  Some("test") {
                    // Signal task being done to main.
                    done_sender.send(()).await.unwrap();
                    break;
                }
            }
            // Drop the last handle, stopping the repo.
            drop(another_clone);
        });

        // Spawn, and await,
        // a blocking task to wait for the document to be ready.
        Handle::current()
            .spawn_blocking(move || {
                // Wait for the document
                // to get into the `ready` state.
                handle_clone.wait_ready();
            })
            .await
            .unwrap();
        // Change the document.
        handle.with_doc_mut(|doc| {
            doc.put(automerge::ROOT, "key", "test")
                .expect("Failed to change the document.");
            doc.commit();
        });
    });
    done_receiver.blocking_recv().unwrap();

    // All collections and doc handles have been dropped,
    // repo should stop running.
    repo_join_handle.join().unwrap();

    println!("Stopped");
}
