use automerge::sync::Message as SyncMessage;
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
    /// A peer sent us a sync message,
    // to be applied to a given document.
    Sync(DocumentId, SyncMessage),
    DoneSync(DocumentId),
}

/// Messages sent into the network sink.
#[derive(Debug)]
pub enum NetworkMessage {
    /// We're sending a sync message,
    // to be applied to a peer to a given document.
    Sync(DocumentId, SyncMessage),
    DoneSync(DocumentId),
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
