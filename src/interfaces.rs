use automerge::sync::Message as SyncMessage;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::Future;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::marker::Unpin;

#[derive(Debug, Eq, Hash, PartialEq, Clone, Deserialize, Serialize)]
pub struct RepoId(pub String);

impl Display for RepoId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Eq, Hash, PartialEq, Clone, Deserialize, Serialize)]
pub struct DocumentId(pub (RepoId, u64));

impl Display for DocumentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

/// Errors used by storage.
#[derive(Clone, Debug)]
pub enum StorageError {
    Error,
}

// TODO: return futures.
pub trait StorageAdapter: Send {
    fn get(
        &self,
        _id: DocumentId,
    ) -> Box<dyn Future<Output = Result<Option<Vec<u8>>, StorageError>> + Send + Unpin> {
        Box::new(futures::future::ready(Ok(None)))
    }

    fn list_all(
        &self,
    ) -> Box<dyn Future<Output = Result<Vec<DocumentId>, StorageError>> + Send + Unpin> {
        Box::new(futures::future::ready(Ok(vec![])))
    }

    fn append(
        &self,
        _id: DocumentId,
        _changes: Vec<u8>,
    ) -> Box<dyn Future<Output = Result<(), StorageError>> + Send + Unpin> {
        Box::new(futures::future::ready(Ok(())))
    }

    fn compact(
        &self,
        _id: DocumentId,
        _full_doc: Vec<u8>,
    ) -> Box<dyn Future<Output = Result<(), StorageError>> + Send + Unpin> {
        Box::new(futures::future::ready(Ok(())))
    }
}
