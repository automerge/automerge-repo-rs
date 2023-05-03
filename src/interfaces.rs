use automerge::sync::Message as SyncMessage;
use futures::sink::Sink;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result};
use std::marker::Unpin;
use uuid::Uuid;

#[derive(Debug, Eq, Hash, PartialEq, Clone, Deserialize, Serialize)]
pub struct RepoId(pub Uuid);

impl Display for RepoId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Eq, Hash, PartialEq, Clone, Deserialize, Serialize)]
pub struct DocumentId(pub (CollectionId, u64));

impl Display for DocumentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}::{}", self.0 .0, self.0 .1)
    }
}

#[derive(Debug, Eq, Hash, PartialEq, Clone, Deserialize, Serialize)]
pub struct CollectionId(pub (RepoId, u64));

impl Display for CollectionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}::{}", self.0 .0, self.0 .1)
    }
}

/// Events sent by the network adapter.
#[derive(Debug)]
pub enum NetworkEvent {
    /// A peer sent us a sync message,
    // to be applied to a given document.
    Sync(DocumentId, SyncMessage),
}

/// Messages sent into the network sink.
#[derive(Debug)]
pub enum NetworkMessage {
    /// We're sending a sync message,
    // to be applied to a peer to a given document.
    Sync(DocumentId, SyncMessage),
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
