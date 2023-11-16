use futures::{future::BoxFuture, FutureExt};

use crate::{DocumentId, RepoId};

/// A policy for deciding whether to share a document with a peer
///
/// There are three situations when we need to decide whether to share a document with a peer:
///
/// 1. When we receive a sync message from a peer, we need to decide whether to incorporate the
///    changes in the sync message and whether to respond to the sync message with our own changes
/// 2. When we are trying to find a document that we don't have locally we need to decide whether
///    to request the document from other peers we are connected to
/// 3. When we need to decide whether to announce a document to another peer. This happens either
///    when a document is created locally in which case we need to decide which of our connected
///    peers to announce to; or when a peer connects for the first time in which case we need to
///    decide whether to announce any of the documents we have locally to the new peer.
///
/// This trait is implemented for `Fn(&RepoId, &DocumentId) -> ShareDecision` so if you don't need
/// to make different decisions for these three situations you can just pass a boxed async closure
/// to the repo.
///
/// ## Examples
///
/// ### Using the `Fn(&RepoId, &DocumentId) -> ShareDecision` implementation
///
/// ```no_run
/// use automerge_repo::{Repo, RepoId, DocumentId, share_policy::ShareDecision, Storage};
///
/// let storage: Box<dyn Storage> = unimplemented!();
/// let repo = Repo::new(None, storage)
///     .with_share_policy(Box::new(|peer, document| {
///         // A share policy which only responds to peers with a particular peer ID
///         if peer == &RepoId::from("some-peer-id") {
///             ShareDecision::Share
///         } else {
///             ShareDecision::DontShare
///         }
///     }));
/// ```
///
/// ### Using a custom share policy
///
/// ```no_run
/// use automerge_repo::{Repo, RepoId, DocumentId, share_policy::{SharePolicy, ShareDecision, SharePolicyError}, Storage};
/// use futures::future::BoxFuture;
/// use std::sync::Arc;
///
/// /// A sync policy which only allows request to a particular peer
/// struct OnlyRequestFrom(RepoId);
///
/// impl SharePolicy for OnlyRequestFrom {
///     fn should_sync(
///         &self,
///         document_id: &DocumentId,
///         with_peer: &RepoId,
///     ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>> {
///         Box::pin(async move { Ok(ShareDecision::Share) })
///     }
///
///     fn should_request(
///         &self,
///         document_id: &DocumentId,
///         from_peer: &RepoId,
///     ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>> {
///         let us = self.0.clone();
///         let them = from_peer.clone();
///         Box::pin(async move {
///             if them == us {
///                 Ok(ShareDecision::Share)
///             } else {
///                 Ok(ShareDecision::DontShare)
///             }
///         })
///     }
///
///     fn should_announce(
///         &self,
///         document_id: &DocumentId,
///         to_peer: &RepoId,
///     ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>> {
///         Box::pin(async move { Ok(ShareDecision::Share) })
///     }
/// }
/// ```
///
pub trait SharePolicy: Send {
    /// Whether we should incorporate changes from this peer into our local document and respond to
    /// sync messages from them
    fn should_sync(
        &self,
        document_id: &DocumentId,
        with_peer: &RepoId,
    ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>>;

    /// Whether we should request this document from this peer if we don't have the document
    /// locally
    fn should_request(
        &self,
        document_id: &DocumentId,
        from_peer: &RepoId,
    ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>>;

    /// Whether we should announce this document to this peer
    fn should_announce(
        &self,
        document_id: &DocumentId,
        to_peer: &RepoId,
    ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareDecision {
    Share,
    DontShare,
}

pub struct SharePolicyError(String);

impl From<String> for SharePolicyError {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl<'a> From<&'a str> for SharePolicyError {
    fn from(s: &'a str) -> Self {
        Self(s.to_string())
    }
}

impl std::fmt::Display for SharePolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::fmt::Debug for SharePolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::error::Error for SharePolicyError {}

impl<F> SharePolicy for F
where
    F: for<'a, 'b> Fn(&'a RepoId, &'b DocumentId) -> ShareDecision,
    F: Send + Sync + 'static,
{
    fn should_sync(
        &self,
        document_id: &DocumentId,
        with_peer: &RepoId,
    ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>> {
        let result = self(with_peer, document_id);
        std::future::ready(Ok(result)).boxed()
    }

    fn should_request(
        &self,
        document_id: &DocumentId,
        from_peer: &RepoId,
    ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>> {
        let result = self(from_peer, document_id);
        std::future::ready(Ok(result)).boxed()
    }

    fn should_announce(
        &self,
        document_id: &DocumentId,
        to_peer: &RepoId,
    ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>> {
        let result = self(to_peer, document_id);
        std::future::ready(Ok(result)).boxed()
    }
}

/// A share policy which always shares documents with all peers
pub struct Permissive;

impl SharePolicy for Permissive {
    fn should_sync(
        &self,
        _document_id: &DocumentId,
        _with_peer: &RepoId,
    ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>> {
        Box::pin(async move { Ok(ShareDecision::Share) })
    }

    fn should_request(
        &self,
        _document_id: &DocumentId,
        _from_peer: &RepoId,
    ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>> {
        Box::pin(async move { Ok(ShareDecision::Share) })
    }

    fn should_announce(
        &self,
        _document_id: &DocumentId,
        _to_peer: &RepoId,
    ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>> {
        Box::pin(async move { Ok(ShareDecision::Share) })
    }
}
