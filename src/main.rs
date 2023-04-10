#![feature(hash_drain_filter)]

mod dochandle;
mod interfaces;
mod repo;

use crate::interfaces::{NetworkAdapter, RepoNetworkSink, StorageAdapter};
use crate::repo::Repo;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Receiver, Sender};

fn main() {
    struct Network {
        sink: Arc<Mutex<Option<RepoNetworkSink>>>,
    }

    impl NetworkAdapter for Network {
        fn send_message(&self) {}

        fn plug_into_sink(&self, sink: RepoNetworkSink) {
            *self.sink.lock() = Some(sink);
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

    let network = Network {
        sink: Default::default(),
    };

    // Create the repo.
    let mut repo = Repo::new();

    // Create a new collection with a network and a storage adapters.
    let collection = repo.new_collection(Box::new(storage), Box::new(network));

    // Run the repo in the background.
    let repo_join_handle = repo.run();

    // Pretend the client uses tokio.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let (done_sender, mut done_receiver) = channel(1);
    rt.spawn(async move {
        // Create a new document.
        let handle = collection.new_document();
        let handle_clone = handle.clone();

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
