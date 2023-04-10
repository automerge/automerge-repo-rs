#![feature(hash_drain_filter)]

mod dochandle;
mod interfaces;
mod repo;

use crate::interfaces::{NetworkAdapter, NetworkEvent, RepoNetworkSink, StorageAdapter};
use crate::repo::Repo;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Receiver, Sender};

fn main() {
    enum NetworkMessage {
        SinkWantsEvents,
        NewSink(RepoNetworkSink),
    }

    struct Network {
        network_sender: Sender<NetworkMessage>,
    }

    impl NetworkAdapter for Network {
        fn send_message(&self) {}

        fn sink_wants_events(&self) {
            self.network_sender
                .blocking_send(NetworkMessage::SinkWantsEvents);
        }

        fn plug_into_sink(&self, sink: RepoNetworkSink) {
            self.network_sender
                .blocking_send(NetworkMessage::NewSink(sink));
        }
    }

    struct Storage {
        sender: Sender<()>,
    }

    impl StorageAdapter for Storage {
        fn save_document(&self, document: ()) {
            self.sender.blocking_send(()).unwrap();
        }
    }

    let (sender, mut receiver) = channel(1);
    let storage = Storage { sender };

    let (network_sender, mut network_receiver) = channel(1);
    let mut network = Network { network_sender };

    // Create the repo.
    let mut repo = Repo::new();

    // Create a new collection with a network and a storage adapters.
    let collection = repo.new_collection(Box::new(storage), Box::new(network));

    // Run the repo in the background.
    let repo_join_handle = repo.run();

    let (other_peer_sender, mut other_peer_receiver) = channel(1);

    // Pretend the client uses tokio.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let (done_sender, mut done_receiver) = channel(1);
    rt.spawn(async move {
        // Create a new document.
        let handle = collection.new_document();
        let document_id = handle.get_document_id();
        let handle_clone = handle.clone();

        // Spawn a task simulating another peer sending data over the network.   
        Handle::current().spawn(async move {
            other_peer_sender.send(document_id).await;
        });

        // The state of the network sink.
        enum SinkState {
            None,
            WantsEvents(RepoNetworkSink),
            Wait(RepoNetworkSink),
        }

        // Spawn a task to await for notification that the sinks wants event,
        // and send some when it does.
        Handle::current().spawn(async move {
            let mut sink_state = SinkState::None;
            let mut buffer = VecDeque::new();
            loop {
                tokio::select! {
                        msg = network_receiver.recv() => {
                            match msg {
                                Some(NetworkMessage::NewSink(new_sink)) => {
                                    sink_state = SinkState::Wait(new_sink);
                                },
                                Some(NetworkMessage::SinkWantsEvents) =>  {
                                    match sink_state {
                                        SinkState::Wait(new_sink) => {
                                            sink_state = SinkState::WantsEvents(new_sink)
                                        },
                                        SinkState::WantsEvents(_) => {},
                                        SinkState::None => panic!("Unepxected NetworkMessage::SinkWantsEvents"),
                                    }
                                },
                                None => break,
                            }
                        }
                        data = other_peer_receiver.recv() => {
                            if data.is_some() {
                                buffer.push_back(data.unwrap());
                            }
                        }
                };

                // Send events if possible.
                sink_state = match (sink_state, buffer.is_empty()) {
                    (SinkState::WantsEvents(sink), false) => {
                        let data = buffer.pop_front().unwrap();
                        sink.send_event(NetworkEvent::DocFullData(data));
                        SinkState::Wait(sink)
                    },
                    (sink_state, _) => sink_state,
                }
            }
        });

        // Spawn, and await, a blocking task to wait for the document to be ready.
        Handle::current()
            .spawn_blocking(move || {
                // Wait for the document to get into the `ready` state.
                handle_clone.wait_ready();
            })
            .await
            .unwrap();

        // Change the document.
        handle.change();

        // Signal task being done to main.
        done_sender.send(()).await.unwrap();
    });
    done_receiver.blocking_recv().unwrap();

    // Wait for the `save_document` call, which happens in response to the change call above.
    receiver.blocking_recv().unwrap();

    repo_join_handle.join().unwrap();

    println!("Stopped");
}
