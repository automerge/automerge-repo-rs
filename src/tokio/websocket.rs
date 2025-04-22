use futures::{Future, FutureExt, Sink, SinkExt, Stream, StreamExt};
use tokio_util::sync::PollSender;

use crate::{ConnDirection, ConnFinishedReason, Message, NetworkError, RepoHandle};

/// A copy of tungstenite::Message
///
/// This is necessary because axum uses tungstenite::Message internally but exposes it's own
/// version so in order to have the logic which handles tungstenite clients and axum servers
/// written in the same function we have to map both the tungstenite `Message` and the axum
/// `Message` to our own type.
pub(crate) enum WsMessage {
    Binary(Vec<u8>),
    Text(String),
    Close,
    Ping(Vec<u8>),
    Pong(Vec<u8>),
}

#[cfg(feature = "tungstenite")]
impl From<WsMessage> for tungstenite::Message {
    fn from(msg: WsMessage) -> Self {
        match msg {
            WsMessage::Binary(data) => tungstenite::Message::Binary(data),
            WsMessage::Text(data) => tungstenite::Message::Text(data),
            WsMessage::Close => tungstenite::Message::Close(None),
            WsMessage::Ping(data) => tungstenite::Message::Ping(data),
            WsMessage::Pong(data) => tungstenite::Message::Pong(data),
        }
    }
}

#[cfg(feature = "tungstenite")]
impl From<tungstenite::Message> for WsMessage {
    fn from(msg: tungstenite::Message) -> Self {
        match msg {
            tungstenite::Message::Binary(data) => WsMessage::Binary(data),
            tungstenite::Message::Text(data) => WsMessage::Text(data),
            tungstenite::Message::Close(_) => WsMessage::Close,
            tungstenite::Message::Ping(data) => WsMessage::Ping(data),
            tungstenite::Message::Pong(data) => WsMessage::Pong(data),
            tungstenite::Message::Frame(_) => unreachable!("unexpected frame message"),
        }
    }
}

#[cfg(feature = "axum")]
impl From<WsMessage> for axum::extract::ws::Message {
    fn from(msg: WsMessage) -> Self {
        match msg {
            WsMessage::Binary(data) => axum::extract::ws::Message::Binary(data),
            WsMessage::Text(data) => axum::extract::ws::Message::Text(data),
            WsMessage::Close => axum::extract::ws::Message::Close(None),
            WsMessage::Ping(data) => axum::extract::ws::Message::Ping(data),
            WsMessage::Pong(data) => axum::extract::ws::Message::Pong(data),
        }
    }
}

#[cfg(feature = "axum")]
impl From<axum::extract::ws::Message> for WsMessage {
    fn from(msg: axum::extract::ws::Message) -> Self {
        match msg {
            axum::extract::ws::Message::Binary(data) => WsMessage::Binary(data),
            axum::extract::ws::Message::Text(data) => WsMessage::Text(data),
            axum::extract::ws::Message::Close(_) => WsMessage::Close,
            axum::extract::ws::Message::Ping(data) => WsMessage::Ping(data),
            axum::extract::ws::Message::Pong(data) => WsMessage::Pong(data),
        }
    }
}

impl RepoHandle {
    /// Connect a websocket
    ///
    /// This function waits until the initial handshake is complete and then returns a future which
    /// must be driven to completion to keep the connection alive. The returned future is required
    /// so that we continue to respond to pings from the server which will otherwise disconnect us.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// use automerge_repo::{Repo, RepoHandle, ConnDirection, Storage};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///    let storage: Box<dyn Storage> = unimplemented!();
    ///    let repo = Repo::new(None, storage);
    ///    let handle = repo.run();
    ///    let (conn, _) = tokio_tungstenite::connect_async("ws://localhost:8080").await.unwrap();
    ///    let conn_driver = handle.connect_tungstenite(conn, ConnDirection::Outgoing).await.unwrap();
    ///    tokio::spawn(async move {
    ///        let finished_reason = conn_driver.await;
    ///        eprintln!("Repo connection finished: {}", finished_reason);
    ///    });
    ///    // ...
    /// }
    /// ```
    #[cfg(feature = "tungstenite")]
    pub async fn connect_tungstenite<S>(
        &self,
        stream: S,
        direction: ConnDirection,
    ) -> Result<impl Future<Output = ConnFinishedReason>, NetworkError>
    where
        S: Sink<tungstenite::Message, Error = tungstenite::Error>
            + Stream<Item = Result<tungstenite::Message, tungstenite::Error>>
            + Send
            + 'static,
    {
        use futures::TryStreamExt;

        let stream = stream
            .map_err(|e| NetworkError::Error(format!("error receiving websocket message: {}", e)))
            .sink_map_err(|e| {
                NetworkError::Error(format!("error sending websocket message: {}", e))
            });
        self.connect_tokio_websocket(stream, direction).await
    }

    /// Accept a websocket in an axum handler
    ///
    /// This function waits until the initial handshake is complete and then returns a future which
    /// must be driven to completion to keep the connection alive.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// use automerge_repo::{Repo, RepoHandle, ConnDirection, Storage};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let storage: Box<dyn Storage> = unimplemented!();
    ///     let handle = Repo::new(None, storage).run();
    ///     let app = axum::Router::new()
    ///         .route("/", axum::routing::get(websocket_handler))
    ///         .with_state(handle.clone());
    ///     let server = axum::Server::bind(&"0.0.0.0:0".parse().unwrap()).serve(app.into_make_service());
    ///     let port = server.local_addr().port();
    ///     tokio::spawn(server);
    /// }
    ///
    /// async fn websocket_handler(
    ///     ws: axum::extract::ws::WebSocketUpgrade,
    ///     axum::extract::State(handle): axum::extract::State<RepoHandle>,
    /// ) -> axum::response::Response {
    ///     ws.on_upgrade(|socket| handle_socket(socket, handle))
    /// }
    ///
    /// async fn handle_socket(
    ///     socket: axum::extract::ws::WebSocket,
    ///     repo: RepoHandle,
    /// ) {
    ///     let driver = repo
    ///         .accept_axum(socket)
    ///         .await
    ///         .unwrap();
    ///     tokio::spawn(async {
    ///         let finished_reason = driver.await;
    ///         eprintln!("Repo connection finished: {}", finished_reason);
    ///     });
    /// }
    /// ```
    #[cfg(feature = "axum")]
    pub async fn accept_axum<S>(
        &self,
        stream: S,
    ) -> Result<impl Future<Output = ConnFinishedReason>, NetworkError>
    where
        S: Sink<axum::extract::ws::Message, Error = axum::Error>
            + Stream<Item = Result<axum::extract::ws::Message, axum::Error>>
            + Send
            + 'static,
    {
        use futures::TryStreamExt;

        let stream = stream
            .map_err(|e| NetworkError::Error(format!("error receiving websocket message: {}", e)))
            .sink_map_err(|e| {
                NetworkError::Error(format!("error sending websocket message: {}", e))
            });
        self.connect_tokio_websocket(stream, ConnDirection::Incoming)
            .await
    }

    pub(crate) async fn connect_tokio_websocket<S, M>(
        &self,
        stream: S,
        direction: ConnDirection,
    ) -> Result<impl Future<Output = ConnFinishedReason>, NetworkError>
    where
        M: Into<WsMessage> + From<WsMessage> + Send + 'static,
        S: Sink<M, Error = NetworkError> + Stream<Item = Result<M, NetworkError>> + Send + 'static,
    {
        // We have to do a bunch of acrobatics here. The `connect_stream` call expects a stream and
        // a sink. However, we need to intercept the stream of websocket messages and if we see a
        // ping we need to immediately send a pong. Intercepting the stream is straightforward, we
        // just filter it and only forward decoded binary messages but as a side effect send pongs
        // back to the server whenever we see a ping. This last part makes the sink side of things
        // tricky though. We can't just use the sink from the filter because we also have to pass
        // the sink to the `connect_stream` call.
        //
        // To get around this we create a channel. We can then create a sink from the sender side
        // of the channel and pass that to `connect_stream` and we can also push to the channel
        // from the filter. Then we create a future which pulls from the other end of the channel
        // and sends to the websocket sink. This is the future that we return from this function.

        let (mut sink, stream) = stream.split();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<WsMessage>(1);

        let msg_stream = stream
            .filter_map::<_, Result<Message, NetworkError>, _>({
                let tx = tx.clone();
                move |msg| {
                    let tx = tx.clone();
                    async move {
                        let msg = match msg {
                            Ok(m) => m,
                            Err(e) => {
                                return Some(Err(NetworkError::Error(format!(
                                    "websocket receive error: {}",
                                    e
                                ))));
                            }
                        };
                        match msg.into() {
                            WsMessage::Binary(data) => Some(Message::decode(&data).map_err(|e| {
                                tracing::error!(err=?e, msg=%hex::encode(data), "error decoding message");
                                NetworkError::Error(format!("error decoding message: {}", e))
                            })),
                            WsMessage::Close => {
                                tracing::debug!("websocket closing");
                                None
                            }
                            WsMessage::Ping(ping_data) => {
                                let pong_response = WsMessage::Pong(ping_data);
                                tx.send(pong_response).await.ok();
                                None
                            },
                            WsMessage::Pong(_) => None,
                            WsMessage::Text(_) => {
                                Some(Err(NetworkError::Error("unexpected string message on websocket".to_string())))
                            },
                        }
                    }
                }
            }).boxed();

        let msg_sink = PollSender::new(tx.clone())
            .sink_map_err(|e| NetworkError::Error(format!("websocket send error: {}", e)))
            .with(|msg: Message| {
                futures::future::ready(Ok::<_, NetworkError>(WsMessage::Binary(msg.encode())))
            });

        let connecting = self.connect_stream(msg_stream, msg_sink, direction);

        let mut do_send = Box::pin(
            async move {
                while let Some(msg) = rx.recv().await {
                    if let Err(e) = sink.send(msg.into()).await {
                        tracing::error!(err=?e, "error sending message");
                        return Err(NetworkError::Error(format!("error sending message: {}", e)));
                    }
                }
                Ok(())
            }
            .fuse(),
        );

        let complete = futures::select! {
            res = connecting.fuse() => {
                res?
            },
            res = do_send => {
                match res {
                    Err(e) => {
                        tracing::error!(err=?e, "error sending message");
                        return Err(NetworkError::Error(format!("error sending message: {}", e)));
                    }
                    Ok(()) => {
                        tracing::error!("websocket send loop unexpectedly stopped");
                        return Err(NetworkError::Error("websocket send loop unexpectedly stopped".to_string()));
                    }
                }
            },
        };

        Ok(async move {
            use futures::future::Either;
            match futures::future::select(complete, do_send).await {
                Either::Left((reason, _)) => reason,
                Either::Right((_, complete)) => {
                    // Assume that the error will be reported by the complete future
                    complete.await
                }
            }
        })
    }
}
