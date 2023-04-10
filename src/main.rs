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
    
    // Events used internally by the network adapter.
    #[derive(Debug)]
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
            // Note use of blocking send, 
            // an easy way for the adapter to integrate 
            // with an async backend(see tasks below).
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

    let (sender, mut storage_receiver) = channel(1);
    let storage = Storage { sender };

    let (network_sender, mut network_receiver) = channel(1);
    let mut network = Network { network_sender };

    // Create the repo.
    let mut repo = Repo::new(1);

    // Create a new collection with a network and a storage adapters.
    let collection = repo.new_collection(Box::new(storage), Box::new(network));

    // Run the repo in the background.
    let repo_join_handle = repo.run();

    // A channel used to model another peer over the network.
    let (other_peer_sender, mut other_peer_receiver) = channel(1);

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
        // Create a new document.
        let handle = collection.new_document();
        let document_id = handle.get_document_id();
        let handle_clone = handle.clone();

        // Simulate another peer sending data over the network.   
        other_peer_sender.send(document_id).await;

        // The state of the network sink.
        // Changed in response to the `sink_wants_events` 
        // method call of the NetworkAdapter.
        #[derive(Debug)]
        enum SinkState {
            None,
            WantsEvents(RepoNetworkSink),
            Wait(RepoNetworkSink),
        }

        // Spawn a task that receives data from the "other peer",
        // and buffers it until the repo sink is ready to receive events.
        Handle::current().spawn(async move {
            // Start in the None state.
            let mut sink_state = SinkState::None;
            let mut buffer = VecDeque::new();
            let mut should_stop = false;
            loop {
                tokio::select! {
                        msg = network_receiver.recv() => {
                            match msg {
                                Some(NetworkMessage::NewSink(new_sink)) => {
                                    // We have now received the sink,
                                    // via the `plug_into_sink` 
                                    // method of the NetworkAdapter.
                                    sink_state = SinkState::Wait(new_sink);
                                },
                                Some(NetworkMessage::SinkWantsEvents) =>  {
                                    // The repo tells it is ready 
                                    // to receive an event(could be a batch instead).
                                    match sink_state {
                                        SinkState::Wait(new_sink) => {
                                            sink_state = SinkState::WantsEvents(new_sink)
                                        },
                                        SinkState::WantsEvents(_) => {},
                                        SinkState::None => panic!("Unepxected NetworkMessage"),
                                    }
                                },
                                None => {
                                    should_stop = true;
                                },
                            }
                        }
                        data = other_peer_receiver.recv() => {
                            // The "other peer" sends a message,
                            // assumed here to be the data for a document.
                            if data.is_some() {
                                buffer.push_back(data.unwrap());
                            } else {
                                should_stop = true;
                            }
                        }
                };
                // Send events if possible.
                sink_state = match (sink_state, buffer.is_empty()) {
                    (SinkState::WantsEvents(sink), false) => {
                        let data = buffer.pop_front().unwrap();
                        // Pretend the peer sent us the data of a document,
                        // the repo will mark the document as ready, via the handle,
                        // upon receiving that event.
                        sink.send_event(NetworkEvent::DocFullData(data));
                        
                        // Set the sink back to waiting mode.
                        SinkState::Wait(sink)
                    },
                    (sink_state, _) => sink_state,
                };
                if should_stop {
                    break;
                }
            }
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
        handle.change();

        // Signal task being done to main.
        done_sender.send(()).await.unwrap();
    });
    done_receiver.blocking_recv().unwrap();

    // Wait for the `save_document` call, 
    // which happens in response to the change call in the task above.
    storage_receiver.blocking_recv().unwrap();

    // All collections and doc handles have been dropped,
    // repo should stop running.
    repo_join_handle.join().unwrap();

    println!("Stopped");
}
