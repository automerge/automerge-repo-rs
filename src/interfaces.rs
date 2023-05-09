use automerge::sync::Message as SyncMessage;
use futures::sink::Sink;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result};
use std::marker::Unpin;
use uuid::Uuid;

#[derive(Debug, Eq, Hash, PartialEq, Copy, Clone, Deserialize, Serialize)]
pub struct RepoId(pub Uuid);

impl Display for RepoId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Eq, Hash, PartialEq, Copy, Clone, Deserialize, Serialize)]
pub struct DocumentId(pub (CollectionId, u64));

impl Display for DocumentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}::{}", self.0 .0, self.0 .1)
    }
}

impl DocumentId {
    pub fn get_repo_id(&self) -> &RepoId {
        &self.0 .0 .0 .0
    }
}

#[derive(Debug, Eq, Hash, PartialEq, Copy, Clone, Deserialize, Serialize)]
pub struct CollectionId(pub (RepoId, u64));

impl CollectionId {
    pub fn get_repo_id(&self) -> &RepoId {
        &self.0 .0
    }
}

impl Display for CollectionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}::{}", self.0 .0, self.0 .1)
    }
}

/// Events sent by the network adapter.
#[derive(Debug)]
pub enum NetworkEvent {
    /// A repo sent us a sync message,
    // to be applied to a given document.
    Sync {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
        message: SyncMessage,
    },
}

impl From<NetworkMessage> for NetworkEvent {
    fn from(msg: NetworkMessage) -> Self {
        match msg {
            NetworkMessage::Sync {
                from_repo_id,
                to_repo_id,
                document_id,
                message,
            } => NetworkEvent::Sync {
                from_repo_id,
                to_repo_id,
                document_id,
                message,
            },
        }
    }
}

/// Messages sent into the network sink.
#[derive(Debug)]
pub enum NetworkMessage {
    /// We're sending a sync message,
    // to be applied by a given repo to a given document.
    Sync {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
        message: SyncMessage,
    },
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
