use std::{
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use automerge::transaction::Transactable;
use automerge_repo::{NetworkError, Repo, RepoMessage};
use futures::{Sink, SinkExt};
use pin_project::pin_project;
use test_utils::storage_utils::SimpleStorage;

use crate::{tincans, TinCan, TinCans};

#[tokio::test]
#[test_log::test]
async fn test_errored_sinks_do_not_crash_repo_loop() {
    // This test exercises a bug in the repo code where a sink which returned
    // an error would sometimes result in the repo loop panicking. The root
    // cause of this is that we would call `poll_close` on a sink after `poll_flush`
    // had returned an error.
    //
    // Actually observing this bug requires a bit of setup as we have to orchestrate
    // a `poll_flush` call which returns an error followed by a `poll_close` call,
    // this is achieved by the `FailableSink` struct below.
    //
    // Once we've set up the failure, we can detect that the repo loop has panicked
    // by trying to call any suspending method as they all attempt to send a message
    // to the repo loop, which has now crashed and dropped it's end of the channel.

    // Create two repos.
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));

    // Run the repos in the background.
    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();

    let TinCans {
        left:
            TinCan {
                send: left_sink,
                recv: left_stream,
                sink_closed: _,
            },
        right:
            TinCan {
                send: right_sink,
                recv: right_stream,
                sink_closed: _,
            },
    } = tincans();
    let (left_sink, left_sink_failed) = FailableSink::new(left_sink);
    let left_sink = left_sink.sink_map_err(|err| {
        // This combinator is just used to ensure that we use SinkMapErr, which
        // has this behavior of panicking if poll methods are called after an
        // error is returned
        err
    });
    repo_handle_1.new_remote_repo(
        repo_handle_2.get_repo_id().clone(),
        left_stream,
        Box::new(left_sink),
    );
    repo_handle_2.new_remote_repo(
        repo_handle_1.get_repo_id().clone(),
        right_stream,
        right_sink,
    );

    // wait a bit for them to connect
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create two document on repo1
    let document_handle_1 = repo_handle_1.new_document();

    // Now fail the left sink
    left_sink_failed.store(true, std::sync::atomic::Ordering::Relaxed);

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });

    // wait a bit for messages to go out
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check that we are still able to communicate with the repo loop. This will
    // panic with a SendError if the repo loop has crashed
    let _ = tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_1.peer_doc_state(
            repo_handle_2.get_repo_id().clone(),
            document_handle_1.document_id(),
        ),
    )
    .await
    .expect("timed out waiting for peer doc state");

    // // Now try and retrieve doc 2 on repo 1
    // let _document_handle_2 = tokio::time::timeout(
    //     Duration::from_millis(1000),
    //     repo_handle_1.request_document(document_handle_2.document_id()),
    // )
    // .await
    // .expect("timed out waiting for request");
}

// The purpose of this stream is to expose a particular sequence of events:
//
// * The repo calls poll_flush on the sink
// * The sink fails, so it returns an error
// * The repo doesn't discard the sink immediately, but instead attempts to call
//   `poll_close` on it before discarding it
// * This is an error as the repo internally uses `SinkMapErr` which relies on
//   the caller never calling any of the poll_* methods after one of them has
//   returned an error
//
// To reliably reproduce this we use an `AtomicBool` to signal that the sink
// should fail, then a boolean to track whether `poll_flush` has returned an
// error yet. This means we can make sure that the `poll_flush` method is
// the first one to return an error.
#[pin_project]
struct FailableSink {
    failed: Arc<AtomicBool>,
    #[pin]
    wrapped: Box<dyn Send + Unpin + Sink<RepoMessage, Error = NetworkError>>,
    failed_flush_happened: bool,
}

impl FailableSink {
    fn new(
        wrapped: Box<dyn Send + Unpin + Sink<RepoMessage, Error = NetworkError>>,
    ) -> (Self, Arc<AtomicBool>) {
        let failed = Arc::new(AtomicBool::new(false));
        (
            Self {
                failed: failed.clone(),
                wrapped,
                failed_flush_happened: false,
            },
            failed,
        )
    }
}

impl Sink<RepoMessage> for FailableSink {
    type Error = NetworkError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        if self.failed.load(std::sync::atomic::Ordering::Relaxed) && self.failed_flush_happened {
            return std::task::Poll::Ready(Err(NetworkError::Error("Sink is failed".to_string())));
        }
        self.project().wrapped.poll_ready(cx)
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: RepoMessage) -> Result<(), Self::Error> {
        if self.failed.load(std::sync::atomic::Ordering::Relaxed) && self.failed_flush_happened {
            return Err(NetworkError::Error("Sink is failed".to_string()));
        }
        self.project().wrapped.start_send(item)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        if self.failed.load(std::sync::atomic::Ordering::Relaxed) {
            self.failed_flush_happened = true;
            return std::task::Poll::Ready(Err(NetworkError::Error("Sink is failed".to_string())));
        }
        self.project().wrapped.poll_flush(cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        if self.failed.load(std::sync::atomic::Ordering::Relaxed) && self.failed_flush_happened {
            return std::task::Poll::Ready(Err(NetworkError::Error("Sink is failed".to_string())));
        }
        self.project().wrapped.poll_close(cx)
    }
}
