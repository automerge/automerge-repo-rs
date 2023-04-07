use crate::repo::{CollectionId, DocumentId, NetworkEvent};
use async_trait::async_trait;
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub struct RepoNetworkSink {
    collection_id: CollectionId,
    network_sender: Sender<(CollectionId, NetworkEvent)>,
}

impl RepoNetworkSink {
    pub(crate) fn new(
        network_sender: Sender<(CollectionId, NetworkEvent)>,
        collection_id: CollectionId,
    ) -> Self {
        RepoNetworkSink {
            network_sender,
            collection_id,
        }
    }
    pub async fn new_message(&self, message: Vec<u8>) {
        let event = NetworkEvent::NewMessage(message);
        self.network_sender
            .send((self.collection_id.clone(), event))
            .await
            .expect("Failed to send network event.");
    }
}

#[async_trait]
pub trait NetworkAdapter: Send + Sync {
    async fn send_message(&self);

    async fn plug_into_sink(&self, sink: RepoNetworkSink);
}

#[async_trait]
pub trait StorageAdapter: Send + Sync {
    async fn save_document(&self, document: ());
}
