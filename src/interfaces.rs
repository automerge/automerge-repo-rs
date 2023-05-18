use automerge::sync::Message as SyncMessage;
use futures::sink::Sink;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result};
use std::marker::Unpin;

#[derive(Debug, Eq, Hash, PartialEq, Clone, Deserialize, Serialize)]
pub struct RepoId(pub String);

impl Display for RepoId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Eq, Hash, PartialEq, Clone, Deserialize, Serialize)]
pub struct DocumentId(pub (RepoId, u64));

impl Display for DocumentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}::{}", self.0 .0, self.0 .1)
    }
}

impl DocumentId {
    pub fn get_repo_id(&self) -> &RepoId {
        &self.0 .0
    }
}

/// Events sent by the network adapter.
#[derive(Debug, Clone)]
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

/// Messages sent into the network sink.
#[derive(Debug, Clone)]
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

pub trait StorageAdapter: Send {
    fn get(&self, id: DocumentId) -> Option<Vec<u8>> {
        None
    }

    fn load_all(&self) -> Vec<(DocumentId, Vec<u8>)> {
        vec![]
    }

    fn append(&self, id: DocumentId, changes: Vec<u8>) {}

    fn compact(&self, id: DocumentId) {}
}
