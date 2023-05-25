use crate::repo::RepoHandle;


/// Which direction a connection passed to [`Repo::connect`] is going
pub enum ConnDirection {
    Incoming,
    Outgoing,
}

#[derive(Debug)]
pub enum CodecError {}

impl RepoHandle {

    /// Connect a tokio io object
    ///
    /// This implements a simple length prefixed framing protocol and is intended for use with
    /// stream oriented transports
    ///
    /// Will panic if not called from a tokio context.
    #[cfg(feature = "tokio")]
    pub async fn connect_tokio_io<Io, Source>(
        self,
        source: Source,
        io: Io,
        direction: ConnDirection,
    ) -> Result<(), CodecError>
    where
        Io: tokio::io::AsyncRead + tokio::io::AsyncWrite,
        Source: tokio::net::ToSocketAddrs,
    {
        todo!()
    }
}