use automerge_repo::{NetworkAdapter, NetworkError, NetworkEvent, NetworkMessage, RepoId};
use core::pin::Pin;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::task::{Context, Poll, Waker};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone)]
pub struct Network<NetworkMessage> {
    buffer: Arc<Mutex<VecDeque<NetworkEvent>>>,
    stream_waker: Arc<Mutex<Option<Waker>>>,
    outgoing: Arc<Mutex<VecDeque<NetworkMessage>>>,
    sink_waker: Arc<Mutex<Option<Waker>>>,
    sender: Sender<(RepoId, RepoId)>,
}

impl Network<NetworkMessage> {
    pub fn new(sender: Sender<(RepoId, RepoId)>) -> Self {
        let buffer = Arc::new(Mutex::new(VecDeque::new()));
        let stream_waker = Arc::new(Mutex::new(None));
        let sink_waker = Arc::new(Mutex::new(None));
        let outgoing = Arc::new(Mutex::new(VecDeque::new()));
        Network {
            buffer,
            stream_waker,
            outgoing,
            sender,
            sink_waker,
        }
    }

    pub fn receive_incoming(&self, event: NetworkEvent) {
        self.buffer.lock().push_back(event);
        if let Some(waker) = self.stream_waker.lock().take() {
            waker.wake();
        }
    }

    pub fn take_outgoing(&self) -> NetworkMessage {
        let message = self.outgoing.lock().pop_front().unwrap();
        if let Some(waker) = self.sink_waker.lock().take() {
            waker.wake();
        }
        message
    }
}

impl Stream for Network<NetworkMessage> {
    type Item = NetworkEvent;
    fn poll_next(
        self: Pin<&mut Network<NetworkMessage>>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<NetworkEvent>> {
        *self.stream_waker.lock() = Some(cx.waker().clone());
        if let Some(event) = self.buffer.lock().pop_front() {
            Poll::Ready(Some(event))
        } else {
            Poll::Pending
        }
    }
}

impl Sink<NetworkMessage> for Network<NetworkMessage> {
    type Error = NetworkError;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        *self.sink_waker.lock() = Some(cx.waker().clone());
        if self.outgoing.lock().is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn start_send(self: Pin<&mut Self>, item: NetworkMessage) -> Result<(), Self::Error> {
        let (from_repo_id, to_repo_id) = match &item {
            NetworkMessage::Sync {
                from_repo_id,
                to_repo_id,
                ..
            } => (from_repo_id.clone(), to_repo_id.clone()),
        };

        self.outgoing.lock().push_back(item);
        if self
            .sender
            .blocking_send((from_repo_id, to_repo_id))
            .is_err()
        {
            return Err(NetworkError::Error);
        }
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        *self.sink_waker.lock() = Some(cx.waker().clone());
        if self.outgoing.lock().is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        *self.sink_waker.lock() = Some(cx.waker().clone());
        if self.outgoing.lock().is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl NetworkAdapter for Network<NetworkMessage> {}
