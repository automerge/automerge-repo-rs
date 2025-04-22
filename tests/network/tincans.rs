use std::sync::{atomic::AtomicBool, Arc};

use automerge_repo::{ConnComplete, NetworkError, RepoHandle, RepoMessage};
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::PollSender;

/// A pair of [`TinCan`]s, one for each direction of a connection.
///
/// You know that thing you did as a kid where you connect two tin cans with a wire and then talk
/// into them? This is that, but with `tokio::sync::mpsc::Channel`s.
///
/// ## Example
///
/// ```no_run
/// use futures::{SinkExt, StreamExt};
///
/// // lets say you already have some repos around
/// let repo1: Repo = todo!();
/// let repo2: Repo = todo!();
///
/// let repo1_handle = repo1.run();
/// let repo2_handle = repo2.run();
///
/// // make some tincans and use them to connect the repos to each other
/// let TinCans{
///    left: TinCan{send: mut left_send, recv: mut left_recv, ..},
///    right: TinCan{send: mut right_send, recv: mut right_recv, ..},
/// };
///
/// repo1_handle.new_remote_repo(repo2_handle.get_repo_id().clone(), left_recv, left_send);
/// repo2_handle.new_remote_repo(repo1_handle.get_repo_id().clone(), right_recv, right_send);
///
/// ```
pub(crate) struct TinCans {
    pub left: TinCan,
    pub right: TinCan,
}

/// One side of a connection
pub(crate) struct TinCan {
    /// Send messages to the other side of the connection
    pub send: Box<dyn Send + Unpin + Sink<RepoMessage, Error = NetworkError>>,
    /// Receive messages from the other side of the connection
    pub recv: Box<dyn Send + Unpin + Stream<Item = Result<RepoMessage, NetworkError>>>,
    /// Whether anyone has ever called `poll_close` on `recv`
    pub sink_closed: Arc<AtomicBool>,
}

/// Create a pair of [`TinCan`]s, one for each direction of a connection.
pub(crate) fn tincans() -> TinCans {
    let (left_send, right_recv) = tokio::sync::mpsc::channel::<RepoMessage>(1);
    let left_sink_closed = Arc::new(AtomicBool::new(false));
    let (right_send, left_recv) = tokio::sync::mpsc::channel::<RepoMessage>(1);
    let right_sink_closed = Arc::new(AtomicBool::new(false));
    TinCans {
        left: TinCan {
            send: Box::new(RecordCloseSink::new(
                PollSender::new(left_send).sink_map_err(|e| {
                    NetworkError::Error(format!(
                        "unexpected failure to send on blocking channel: {:?}",
                        e
                    ))
                }),
                left_sink_closed.clone(),
            )),
            recv: Box::new(ReceiverStream::new(left_recv).map(Ok)),
            sink_closed: left_sink_closed,
        },
        right: TinCan {
            send: Box::new(RecordCloseSink::new(
                PollSender::new(right_send).sink_map_err(|e| {
                    NetworkError::Error(format!(
                        "unexpected failure to send on blocking channel: {:?}",
                        e
                    ))
                }),
                right_sink_closed.clone(),
            )),
            recv: Box::new(ReceiverStream::new(right_recv).map(Ok)),
            sink_closed: right_sink_closed,
        },
    }
}

/// Create a single tincan which sends messages to nowhere and receives nothing.
pub(crate) fn tincan_to_nowhere() -> TinCan {
    let sink_closed = Arc::new(AtomicBool::new(false));
    TinCan {
        send: Box::new(RecordCloseSink::new(
            futures::sink::drain().sink_map_err(|_e| unreachable!()),
            sink_closed.clone(),
        )),
        recv: Box::new(futures::stream::pending()),
        sink_closed,
    }
}

pub(crate) struct Connected {
    pub(crate) left_complete: ConnComplete,
    pub(crate) right_complete: ConnComplete,
}

pub(crate) fn connect_repos(left: &RepoHandle, right: &RepoHandle) -> Connected {
    let TinCans {
        left:
            TinCan {
                send: left_send,
                recv: left_recv,
                ..
            },
        right:
            TinCan {
                send: right_send,
                recv: right_recv,
                ..
            },
    } = tincans();
    let left_complete = left.new_remote_repo(right.get_repo_id().clone(), left_recv, left_send);
    let right_complete = right.new_remote_repo(left.get_repo_id().clone(), right_recv, right_send);
    Connected {
        left_complete,
        right_complete,
    }
}

/// A wrapper around a `Sink` which records whether `poll_close` has ever been called
struct RecordCloseSink<S> {
    inner: S,
    closed: Arc<AtomicBool>,
}

impl<S> RecordCloseSink<S> {
    fn new(inner: S, closed: Arc<AtomicBool>) -> Self {
        Self { inner, closed }
    }
}

impl<S> Sink<RepoMessage> for RecordCloseSink<S>
where
    S: Sink<RepoMessage, Error = NetworkError> + Unpin,
{
    type Error = NetworkError;
    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let inner = unsafe { std::pin::Pin::new_unchecked(&mut self.get_unchecked_mut().inner) };
        inner.poll_ready(cx)
    }
    fn start_send(self: std::pin::Pin<&mut Self>, item: RepoMessage) -> Result<(), Self::Error> {
        let inner = unsafe { std::pin::Pin::new_unchecked(&mut self.get_unchecked_mut().inner) };
        inner.start_send(item)
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let inner = unsafe { std::pin::Pin::new_unchecked(&mut self.get_unchecked_mut().inner) };
        inner.poll_flush(cx)
    }
    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let closed = self.closed.clone();
        let inner = unsafe { std::pin::Pin::new_unchecked(&mut self.get_unchecked_mut().inner) };
        let result = inner.poll_close(cx);
        if result.is_ready() {
            closed.store(true, std::sync::atomic::Ordering::Release);
        }
        result
    }
}
