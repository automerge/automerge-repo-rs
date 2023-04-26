use crossbeam_channel::Sender;
use futures::sink::Sink;
use futures::stream::Stream;
use std::marker::Unpin;
use uuid::Uuid;

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct DocumentId(pub Uuid);

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct CollectionId(pub Uuid);

/// Events sent by the network adapter.
#[derive(Debug)]
pub enum NetworkEvent {
    /// A peer sent us the "full data" for the doc,
    /// which will set the doc handle state to ready.
    DocFullData(DocumentId),
}

/// Messages sent into the network sink.
#[derive(Debug)]
pub enum NetworkMessage {
    /// We want data for a doc.
    WantDoc(DocumentId),
}

/// Network errors used by the sink.
#[derive(Debug)]
pub enum NetworkError {
    Error,
}

pub trait NetworkAdapter:
    Send + Unpin + Stream<Item = NetworkEvent> + Sink<NetworkMessage>
{
}

pub trait StorageAdapter: Send {
    fn save_document(&self, document: ());
}
