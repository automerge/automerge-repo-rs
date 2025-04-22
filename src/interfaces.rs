use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
    time::SystemTime,
};

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct RepoId(pub String);

impl Display for RepoId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'a> From<&'a str> for RepoId {
    fn from(s: &'a str) -> Self {
        Self(s.to_string())
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Deserialize, Serialize)]
pub struct DocumentId([u8; 16]);

impl DocumentId {
    pub fn random() -> Self {
        Self(uuid::Uuid::new_v4().into_bytes())
    }

    // This is necessary to make the interop tests work, we'll remove it once
    // we upgrade to the latest version of automerge-repo for the interop tests
    pub fn as_uuid_str(&self) -> String {
        uuid::Uuid::from_slice(self.0.as_ref()).unwrap().to_string()
    }
}

impl AsRef<[u8]> for DocumentId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid document ID: {0}")]
pub struct BadDocumentId(String);

impl TryFrom<Vec<u8>> for DocumentId {
    type Error = BadDocumentId;

    fn try_from(v: Vec<u8>) -> Result<Self, Self::Error> {
        match uuid::Uuid::from_slice(v.as_slice()) {
            Ok(id) => Ok(Self(id.into_bytes())),
            Err(e) => Err(BadDocumentId(format!("invalid uuid: {}", e))),
        }
    }
}

impl FromStr for DocumentId {
    type Err = BadDocumentId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match bs58::decode(s).with_check(None).into_vec() {
            Ok(bytes) => Self::try_from(bytes),
            Err(_) => {
                // attempt to parse legacy UUID format
                let uuid = uuid::Uuid::parse_str(s).map_err(|_| {
                    BadDocumentId(
                        "expected either a bs58-encoded document ID or a UUID".to_string(),
                    )
                })?;
                Ok(Self(uuid.into_bytes()))
            }
        }
    }
}

impl std::fmt::Debug for DocumentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let as_string = bs58::encode(&self.0).with_check().into_string();
        write!(f, "{}", as_string)
    }
}

impl Display for DocumentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let as_string = bs58::encode(&self.0).with_check().into_string();
        write!(f, "{}", as_string)
    }
}

/// Network errors used by the sink.
#[derive(Debug, thiserror::Error, Clone)]
pub enum NetworkError {
    Error(String),
}

impl Display for NetworkError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkError::Error(e) => write!(f, "NetworkError: {}", e),
        }
    }
}

#[derive(Clone, PartialEq)]
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

impl std::fmt::Debug for RepoMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RepoMessage::Sync {
                from_repo_id,
                to_repo_id,
                document_id,
                message: _,
            } => write!(
                f,
                "Sync {{ from_repo_id: {:?}, to_repo_id: {:?}, document_id: {:?} }}",
                from_repo_id, to_repo_id, document_id
            ),
            RepoMessage::Ephemeral {
                from_repo_id,
                to_repo_id,
                document_id,
                message: _,
            } => write!(
                f,
                "Ephemeral {{ from_repo_id: {:?}, to_repo_id: {:?}, document_id: {:?} }}",
                from_repo_id, to_repo_id, document_id
            ),
        }
    }
}

/// The messages of the multi-document sync protocol
///
/// The multi-doc sync protocol works like this:
///
/// 1. The connecting peer sends a `Message::Join` containing its repo ID
/// 2. The accepting peer sends a `Message::Joined` containing its repo ID
/// 3. Sync message exchange can proceed, by exchanging Message::Repo(_).
#[derive(Debug, Clone)]
pub enum Message {
    /// Sent by the connecting peer on opening a connection to tell the other
    /// end their repo ID
    Join(RepoId),
    /// Sent by the accepting peer after having received [`Self::Join`] to tell the
    /// connecting peer their repo ID.
    Peer(RepoId),
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
        //) -> Box<dyn Future<Output = Result<Option<Vec<u8>>, StorageError>> + Send + Unpin>;
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, StorageError>>;

    fn list_all(&self) -> BoxFuture<'static, Result<Vec<DocumentId>, StorageError>>;

    fn append(
        &self,
        _id: DocumentId,
        _changes: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), StorageError>>;

    fn compact(
        &self,
        _id: DocumentId,
        _full_doc: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), StorageError>>;
}

/// The state of sycnhronization of a document with a remote peer obtained via [`RepoHandle::peer_doc_state`](crate::RepoHandle::peer_doc_state)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct PeerDocState {
    /// When we last received a message from this peer
    pub last_received: Option<SystemTime>,
    /// When we last sent a message to this peer
    pub last_sent: Option<SystemTime>,
    /// The heads of the document when we last sent a message
    pub last_sent_heads: Option<Vec<automerge::ChangeHash>>,
    /// The last heads of the document that the peer said they had
    pub last_acked_heads: Option<Vec<automerge::ChangeHash>>,
}
