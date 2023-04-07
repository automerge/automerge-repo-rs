#![feature(hash_drain_filter)]

mod dochandle;
mod interfaces;
mod repo;

use crate::interfaces::{NetworkAdapter, RepoNetworkSink, StorageAdapter};
use crate::repo::Repo;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() {
    struct Network {
        sink: Arc<Mutex<Option<RepoNetworkSink>>>,
    }

    #[async_trait]
    impl NetworkAdapter for Network {
        async fn send_message(&self) {}

        async fn plug_into_sink(&self, sink: RepoNetworkSink) {
            *self.sink.lock().await = Some(sink);
        }
    }
    
    struct Storage {
        sender: Sender<()>
    }
    
    #[async_trait]
    impl StorageAdapter for Storage {
        async fn save_document(&self, document: ()) {
            self.sender.send(()).await.unwrap();
        }
    }
    
    let (sender, mut receiver) = channel(1);
    let storage = Storage {
        sender,
    };
    
    let network = Network {
        sink: Default::default()
    };
    
    // Create the repo.
    let mut repo = Repo::new();
    
    // Create a new collection with a network and a storage adapters.
    let collection = repo.new_collection(Box::new(storage), Box::new(network)).await;
    
    // Run the repo in the background.
    let repo_join_handle = repo.run().await;
    
    // Create a new document.
    let handle = collection.new_document().await;
    
    // Wait for the document to get into the `ready` state.
    handle.wait_ready().await;
    
    // Change the document.
    handle.change().await;
    
    // Wait for the `save_document` call, which happens in response to the change call above.
    assert!(receiver.recv().await.is_some());
    
    repo_join_handle.join().unwrap();
    println!("Stopped");
}
