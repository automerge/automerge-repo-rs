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

/// Network errors used by the sink.
#[derive(Debug)]
pub enum NetworkError {
    Error,
}

pub trait NetworkAdapter: Send + Unpin + Stream<Item = RepoMessage> + Sink<RepoMessage> {}

#[derive(Debug, Clone)]
pub enum RepoMessage {
    /// A sync message for a particular document
    Sync {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
        message: Vec<u8>,
    },
    /// An ephemeral message for a particular document.
    Ephemeral {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
        message: Vec<u8>,
    },
}

/// The messages of the multi-document sync protocol
///
/// The multi-doc sync protocol works like this:
///
/// 1. The connecting peer sends a `Message::Join` containing its repo ID
/// 2. The accepting peer sends a `Message::Peer` containing its repo ID
/// 3. Sync message exchange can proceed, by exchanging Message::Repo(_).
pub enum Message {
    /// Sent by the connecting peer on opening a connection to tell the other
    /// end their repo ID
    Join(RepoId),
    /// Sent by the accepting peer after having received [`Join`] to tell the
    /// connecting peer their repo ID.
    Joined(RepoId),
    /// A repo message for a particular document
    Repo(RepoMessage),
}

/// Errors used by storage.
#[derive(Clone, Debug)]
pub enum StorageError {
    Error,
}

/// The Storage API.
pub trait Storage: Send {
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
