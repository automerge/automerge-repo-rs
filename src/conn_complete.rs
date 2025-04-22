use std::future::Future;

use futures::{channel::oneshot, FutureExt};

pub struct ConnComplete(oneshot::Receiver<ConnFinishedReason>);

impl ConnComplete {
    pub(crate) fn new(rx: oneshot::Receiver<ConnFinishedReason>) -> Self {
        Self(rx)
    }
}

/// The result of the future returned by [`RepoHandle::new_remote_repo`](crate::RepoHandle::new_remote_repo)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnFinishedReason {
    /// This repository is shutting down
    RepoShutdown,
    /// Another connection started with the same peer ID as this one, we're
    /// closing this connection and we'll carry on with the new one
    DuplicateConnection,
    /// The other end disconnected gracefully
    TheyDisconnected,
    /// There was some error on the network transport when receiving data
    ErrorReceiving(String),
    /// There was some error on the network transport when sending data
    ErrorSending(String),
}

impl std::fmt::Display for ConnFinishedReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnFinishedReason::RepoShutdown => write!(f, "Repository shutdown"),
            ConnFinishedReason::DuplicateConnection => write!(f, "Duplicate connection"),
            ConnFinishedReason::TheyDisconnected => write!(f, "They disconnected"),
            ConnFinishedReason::ErrorReceiving(msg) => write!(f, "Error receiving: {}", msg),
            ConnFinishedReason::ErrorSending(msg) => write!(f, "Error sending: {}", msg),
        }
    }
}

impl Future for ConnComplete {
    type Output = ConnFinishedReason;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.0.poll_unpin(cx).map(|result| match result {
            Ok(reason) => reason,
            Err(_) => ConnFinishedReason::RepoShutdown,
        })
    }
}
