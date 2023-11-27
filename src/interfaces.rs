use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    num::NonZeroU64,
    str::FromStr,
};

#[cfg(test)]
use arbitrary::Arbitrary;

#[derive(Debug, Eq, Hash, PartialEq, Clone)]
#[cfg_attr(test, derive(Arbitrary))]
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
#[cfg_attr(test, derive(Arbitrary))]
pub struct DocumentId(Vec<u8>);

impl From<Vec<u8>> for DocumentId {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl DocumentId {
    pub fn random() -> Self {
        Self(uuid::Uuid::new_v4().as_bytes().to_vec())
    }
}

impl AsRef<[u8]> for DocumentId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid document ID: {0}")]
pub struct BadDocumentId(String);

impl FromStr for DocumentId {
    type Err = BadDocumentId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match bs58::decode(s).with_check(None).into_vec() {
            Ok(bytes) => Ok(Self(bytes)),
            Err(_) => Err(BadDocumentId(s.to_string())),
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
        write!(f, "NetworkError")
    }
}

#[derive(Clone, PartialEq)]
#[cfg_attr(test, derive(Arbitrary))]
pub enum RepoMessage {
    /// A sync message for a particular document
    Sync {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
        message: Vec<u8>,
    },
    /// A request to begin sync for a document the sender does not have
    Request {
        sender_id: RepoId,
        target_id: RepoId,
        /// The document ID to request
        document_id: DocumentId,
        /// The initial sync message
        sync_message: Vec<u8>,
    },
    /// Notify a peer who has requsted a document that we don't have it and
    /// none of our peers have it either.
    Unavailable {
        document_id: DocumentId,
        sender_id: RepoId,
        target_id: RepoId,
    },
    /// An ephemeral message for a particular document.
    Ephemeral {
        from_repo_id: RepoId,
        to_repo_id: RepoId,
        document_id: DocumentId,
        message: Vec<u8>,
        session_id: EphemeralSessionId,
        count: NonZeroU64,
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
            RepoMessage::Request {
                sender_id,
                target_id,
                document_id,
                sync_message: _,
            } => write!(
                f,
                "Request {{ sender_id: {:?}, target_id: {:?}, document_id: {:?} }}",
                sender_id, target_id, document_id
            ),
            RepoMessage::Ephemeral {
                from_repo_id,
                to_repo_id,
                document_id,
                message: _,
                session_id,
                count,
            } => write!(
                f,
                "Ephemeral {{ from_repo_id: {:?}, to_repo_id: {:?}, document_id: {:?}, session_id: {:?}, count: {:?} }}",
                from_repo_id, to_repo_id, document_id, session_id, count,
            ),
            RepoMessage::Unavailable {
                document_id,
                sender_id,
                target_id,
            } => write!(
                f,
                "Unavailable {{ document_id: {:?}, sender_id: {:?}, target_id: {:?} }}",
                document_id, sender_id, target_id
            ),
        }
    }
}

/// The messages of the multi-document sync protocol
///
/// The multi-doc sync protocol works like this:
///
/// 1. The connecting peer sends a `Message::Join` containing its repo ID
/// 2. The accepting peer sends a `Message::Peer` containing its repo ID
/// 3. Sync message exchange can proceed, by exchanging Message::Repo(_).
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(test, derive(Arbitrary))]
pub enum Message {
    /// Sent by the connecting peer on opening a connection to tell the other
    /// end their repo ID
    Join {
        sender: RepoId,
        supported_protocol_versions: Vec<ProtocolVersion>,
    },
    /// Sent by the accepting peer after having received [`Self::Join`] to tell the
    /// connecting peer their repo ID.
    Peer {
        sender: RepoId,
        selected_protocol_version: ProtocolVersion,
    },
    /// A repo message for a particular document
    Repo(RepoMessage),
    /// An error report
    Error { message: String },
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(Arbitrary))]
pub struct EphemeralSessionId(String);

impl EphemeralSessionId {
    pub(crate) fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

impl<'a> From<&'a str> for EphemeralSessionId {
    fn from(s: &'a str) -> Self {
        Self(s.to_string())
    }
}

impl AsRef<str> for EphemeralSessionId {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolVersion {
    V1,
    Other(String),
}

#[cfg(test)]
impl<'a> Arbitrary<'a> for ProtocolVersion {
    fn arbitrary(u: &mut arbitrary::Unstructured<'_>) -> arbitrary::Result<Self> {
        let is_v1 = bool::arbitrary(u)?;
        if is_v1 {
            Ok(ProtocolVersion::V1)
        } else {
            let s = String::arbitrary(u)?;
            if s == "1" {
                Ok(ProtocolVersion::Other("2".to_string()))
            } else {
                Ok(s.into())
            }
        }
    }
}

impl From<String> for ProtocolVersion {
    fn from(s: String) -> Self {
        match s.as_str() {
            "1" => ProtocolVersion::V1,
            _ => ProtocolVersion::Other(s),
        }
    }
}

impl From<ProtocolVersion> for String {
    fn from(p: ProtocolVersion) -> Self {
        p.as_ref().to_string()
    }
}

impl AsRef<str> for ProtocolVersion {
    fn as_ref(&self) -> &str {
        match self {
            ProtocolVersion::V1 => "1",
            ProtocolVersion::Other(s) => s.as_ref(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct EphemeralMessage {
    from_repo_id: RepoId,
    message: Vec<u8>,
}

impl EphemeralMessage {
    pub fn new(bytes: Vec<u8>, sender: RepoId) -> Self {
        Self {
            from_repo_id: sender,
            message: bytes,
        }
    }

    pub fn sender(&self) -> &RepoId {
        &self.from_repo_id
    }

    pub fn bytes(&self) -> &[u8] {
        &self.message
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.message
    }
}
