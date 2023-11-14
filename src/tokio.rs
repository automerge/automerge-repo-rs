use std::fmt::Debug;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder};

use crate::{repo::RepoHandle, ConnDirection};
use crate::{Message, NetworkError};

#[cfg(feature = "tungstenite")]
use futures::{Future, FutureExt, Sink, SinkExt, Stream, StreamExt};

mod fs_storage;
pub use fs_storage::FsStorage;

impl RepoHandle {
    /// Connect a tokio io object
    pub async fn connect_tokio_io<Io, Source>(
        &self,
        _source: Source,
        io: Io,
        direction: ConnDirection,
    ) -> Result<(), CodecError>
    where
        Io: AsyncRead + AsyncWrite + Send + 'static,
        Source: Debug,
    {
        let codec = Codec::new();
        let framed = tokio_util::codec::Framed::new(io, codec);

        let (sink, stream) = framed.split();

        self.connect_stream(stream, sink, direction).await?;

        Ok(())
    }

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
    ///        if let Err(e) = conn_driver.await {
    ///            eprintln!("Error running repo connection: {}", e);
    ///        }
    ///    });
    ///    // ...
    /// }
    /// ```
    #[cfg(feature = "tungstenite")]
    pub async fn connect_tungstenite<S>(
        &self,
        stream: S,
        direction: ConnDirection,
    ) -> Result<impl Future<Output = Result<(), CodecError>>, CodecError>
    where
        S: Sink<tungstenite::Message, Error = tungstenite::Error>
            + Stream<Item = Result<tungstenite::Message, tungstenite::Error>>
            + Send
            + 'static,
    {
        // We have to do a bunch of acrobatics here. The `connect_stream` call expects a stream and
        // a sink. However, we need to intercept the stream of websocket messages and if we see a
        // ping we need to immediately send a pong. Intercepting the stream is straightforward, we
        // just filter it and only forward decoded binary messages but as a side effect send pongs
        // back to the server whenever we see one. This last part makes the sink side of things
        // tricky though. We can't just use the sink from the filter because we also have to pass
        // the sink to the `connect_stream` call.
        //
        // To get around this we create a channel. We can then create a sink from the sender side
        // of the channel and pass that to `connect_stream` and we can also push to the channel
        // from the filter. Then we create a future which pulls from the other end of the channel
        // and sends to the websocket sink. This is the future that we return from this function.
        use tokio_util::sync::PollSender;

        let (mut sink, stream) = stream.split();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<tungstenite::Message>(1);

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
                        match msg {
                            tungstenite::Message::Binary(data) => Some(Message::decode(&data).map_err(|e| {
                                tracing::error!(err=?e, msg=%hex::encode(data), "error decoding message");
                                NetworkError::Error(format!("error decoding message: {}", e))
                            })),
                            tungstenite::Message::Close(_) => {
                                tracing::debug!("websocket closing");
                                None
                            }
                            tungstenite::Message::Ping(ping_data) => {
                                let pong_response = tungstenite::Message::Pong(ping_data);
                                tx.send(pong_response).await.ok();
                                None
                            },
                            tungstenite::Message::Pong(_) => None,
                            tungstenite::Message::Text(_) => {
                                Some(Err(NetworkError::Error("unexpected string message on websocket".to_string())))
                            },
                            tungstenite::Message::Frame(_) => unreachable!(),
                        }
                    }
                }
            }).boxed();

        let msg_sink = PollSender::new(tx.clone())
            .sink_map_err(|e| NetworkError::Error(format!("websocket send error: {}", e)))
            .with(|msg: Message| {
                futures::future::ready(Ok::<_, NetworkError>(tungstenite::Message::Binary(
                    msg.encode(),
                )))
            });

        let connecting = self.connect_stream(msg_stream, msg_sink, direction);

        let mut do_send = Box::pin(
            async move {
                while let Some(msg) = rx.recv().await {
                    if let Err(e) = sink.send(msg).await {
                        tracing::error!(err=?e, "error sending message");
                        return Err(CodecError::Network(NetworkError::Error(format!(
                            "error sending message: {}",
                            e
                        ))));
                    }
                }
                Ok(())
            }
            .fuse(),
        );

        futures::select! {
            res = connecting.fuse() => {
                res?;
            },
            res = do_send => {
                match res {
                    Err(e) => {
                        tracing::error!(err=?e, "error sending message");
                        return Err(CodecError::Network(NetworkError::Error(format!("error sending message: {}", e))));
                    }
                    Ok(()) => {
                        tracing::error!("websocket send loop unexpectedly stopped");
                        return Err(CodecError::Network(NetworkError::Error("websocket send loop unexpectedly stopped".to_string())));
                    }
                }
            },
        }

        Ok(do_send)
    }
}

/// A simple length prefixed codec over `crate::Message` for use over stream oriented transports
pub(crate) struct Codec;

impl Codec {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Decode(#[from] crate::message::DecodeError),
    #[error(transparent)]
    Network(#[from] NetworkError),
}

impl From<CodecError> for NetworkError {
    fn from(err: CodecError) -> Self {
        NetworkError::Error(err.to_string())
    }
}

impl Decoder for Codec {
    type Item = Message;

    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        // Read the length prefix
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&src[..4]);
        let len = u32::from_be_bytes(len_bytes) as usize;

        // Check if we have enough data for this message
        if src.len() < len + 4 {
            src.reserve(len + 4 - src.len());
            return Ok(None);
        }

        // Parse the message
        let data = src[4..len + 4].to_vec();
        src.advance(len + 4);
        Message::decode(&data).map(Some).map_err(Into::into)
    }
}

impl Encoder<Message> for Codec {
    type Error = CodecError;

    fn encode(&mut self, msg: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded = msg.encode();
        let len = encoded.len() as u32;
        let len_slice = len.to_be_bytes();
        dst.reserve(4 + len as usize);
        dst.extend_from_slice(&len_slice);
        dst.extend_from_slice(&encoded);
        Ok(())
    }
}
