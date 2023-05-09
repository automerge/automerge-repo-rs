use std::net::SocketAddr;

use automerge_repo::NetworkMessage;
use futures::{Future, Sink, Stream};
use pin_project::pin_project;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::{PollSendError, PollSender};

/// The result of calling `run`
pub(crate) struct RunChannel<F> {
    /// The interface to receive messages from the relay server
    pub(crate) incoming: IncomingHandle,
    /// The stream and sink of `NetworkMessage`s to give to the repo
    pub(crate) stream: Handle,
    /// The task which must be driven to send messages to the relay server
    pub(crate) task: F,
}

/// An adapter which provides a stream of incoming messages through a relay server
///
/// The relay server provides a simple http interface, you POST messages to 
/// <relay_ip>/relay_sync and it forwards those messages to the other peer, and 
/// likewise when the relay peer receives a message from the other peer it
/// forwards those messages to us.
///
/// However, automerge_repo expects a stream of messages, so we need this 
/// adapter. The adapter consists of three components:
///
/// * An implementation of Sink (`Handle`) which pushes messages into a tokio channel
/// * An async task which pulls messages out of the sink channel and sends them
///   to the relay server
/// * A type (`IncomingHandle`) which allowws us to push messages into a tokio
///   channel whenever we receive an http request from the relay server
/// * An implementation of Stream (also `Handle`) which pulls messages out of
///   the `IncomingHandle` channel
///
/// That looks like this:
///
/// ```text
/// +-----------------+   +-----------------+   +-----------------+  +------------------+
/// |                 |   |                 |   |                 |  |                  |
/// | relay server    |   | http interface  |   | IncomingHandle  |  |     Handle       |
/// |                 |   |                 |   |                 |  |------------------|
/// |                 |   |                 |   |                 |  | reciever | sender|
/// +-----------------+   +-----------------+   +-----------------+  +------------------+
///         |                     |                     |                 |         |
///         |                     |                     |                 |         |
///         |---- sync message--->|                     |                 |         |
///         |                     |                     |                 |         |
///         |                     |------sync message-->|                 |         |
///         |                     |                     |---------------->|         |        |
///         |                     |                     |                 |         |
///         |<------------------------- sync message   -----------------------------|
///
/// ```
///
/// As you can see, because the `Handle` contains both a receiver and a sender, it can  implement
/// both `Sink` and `Stream`, as automerge_repo requires.
///
/// ## How to use this
///
/// ```rust,no_run
/// let relay_ip: SocketAddr = "0.0.0.0:1234".parse().unwrap();
/// let channel = Channel::new(relay_ip);
/// let RunChannel { incoming, stream, task } = channel.run();
///
/// // Incoming is where we send messages we receive from the relay server. So somewhere
/// // we'll have something like the following
/// async fn handle_request(msg: NetworkMessage) {
///     incoming.send(msg).await.unwrap();
/// }
///
/// // Stream is what we pass to automerge repo
/// let repo_handle: automerge_repo::RepoHandle = todo!();
/// let collection = repo_handle.create_collection(None);
/// let connect_task = collection.connect_stream(stream);
///
/// // Task is the subroutine which must be driven to send messages to the relay server
/// tokio::spawn(task);
/// ```
pub fn run(relay_ip: SocketAddr) -> RunChannel<impl Future<Output = ()>> {
    let (send_to_relay, mut messages_for_relay) = tokio::sync::mpsc::channel(10);
    let (send_to_repo, messages_for_repo) = tokio::sync::mpsc::channel(10);
    let handle = Handle {
        send_to_relay: PollSender::new(send_to_relay),
        recv_from_relay: messages_for_repo,
    };
    let run = async move {
        let client = reqwest::Client::new();
        while let Some(message) = messages_for_relay.recv().await {
            let NetworkMessage::Sync {
                from_repo_id,
                to_repo_id,
                document_id,
                message,
            } = message;
            let message = message.encode();
            let url = format!("http://{}/relay_sync", relay_ip);
            client
                .post(url)
                .json(&(from_repo_id, to_repo_id, document_id, message))
                .send()
                .await
                .unwrap();
        }
    };
    RunChannel {
        incoming: IncomingHandle(send_to_repo),
        stream: handle,
        task: run,
    }
}

pub(crate) struct IncomingHandle(Sender<NetworkMessage>);

impl IncomingHandle {
    pub async fn recv(&self, message: NetworkMessage) {
        self.0.send(message).await.unwrap();
    }
}

#[pin_project]
pub struct Handle {
    #[pin]
    send_to_relay: PollSender<NetworkMessage>,
    recv_from_relay: Receiver<NetworkMessage>,
}

impl Sink<NetworkMessage> for Handle {
    type Error = PollSendError<NetworkMessage>;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.project();
        this.send_to_relay.poll_ready(cx)
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: NetworkMessage) -> Result<(), Self::Error> {
        let this = self.project();
        this.send_to_relay.start_send(item)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.project();
        this.send_to_relay.poll_flush(cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.project();
        this.send_to_relay.poll_close(cx)
    }
}

impl Stream for Handle {
    type Item = NetworkMessage;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<NetworkMessage>> {
        let this = self.project();
        this.recv_from_relay.poll_recv(cx)
    }
}
