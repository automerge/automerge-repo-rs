use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use futures::{future::BoxFuture, FutureExt, TryFutureExt};

use crate::{DocumentId, Storage, StorageError};

/// A wrapper around [`crate::fs_store::FsStore`] that implements [`crate::Storage`]
#[derive(Debug)]
pub struct FsStorage {
    inner: Arc<Mutex<crate::fs_store::FsStore>>,
    handle: tokio::runtime::Handle,
}

impl FsStorage {
    /// Creates a new [`FsStorage`] from a [`Path`].
    ///
    /// # Errors
    ///
    /// This will attempt to create the root directory and throw an error if
    /// it does not exist.
    ///
    /// # Panics
    ///
    /// If there is not a tokio runtime available
    pub fn open<P: AsRef<Path>>(root: P) -> Result<Self, std::io::Error> {
        let handle = tokio::runtime::Handle::current();
        let inner = Arc::new(Mutex::new(crate::fs_store::FsStore::open(root)?));
        Ok(Self { inner, handle })
    }

    /// Overrides the tmpdir directory used for temporary files.
    ///
    /// The default is to use the root directory passed to [`FsStorage::open`].
    ///
    /// The tmpdir used must be on the same mount point as the root directory,
    /// otherwise the storage will throw an error on writing data.
    ///
    /// # Errors
    ///
    /// This will attempt to create the tmpdir directory and throw an error if
    /// it does not exist.
    pub fn with_tmpdir<P: AsRef<Path>>(self, tmpdir: P) -> Option<Result<Self, std::io::Error>> {
        let Self { inner, handle } = self;
        let inner = Arc::into_inner(inner)?.into_inner().ok()?;
        let inner = inner.with_tmpdir(tmpdir);
        let Ok(inner) = inner else {
            let e = inner.unwrap_err();
            return Some(Err(e));
        };
        let inner = Arc::new(Mutex::new(inner));
        Some(Ok(Self { inner, handle }))
    }
}

impl Storage for FsStorage {
    fn get(
        &self,
        id: crate::DocumentId,
    ) -> BoxFuture<'static, Result<Option<Vec<u8>>, StorageError>> {
        let inner = Arc::clone(&self.inner);
        let inner_id = id.clone();
        self.handle
            .spawn_blocking(move || inner.lock().unwrap().get(&inner_id))
            .map(handle_joinerror)
            .map_err(move |e| {
                tracing::error!(err=?e, doc=?id, "error reading chunks from filesystem");
                StorageError::Error
            })
            .boxed()
    }

    fn list_all(&self) -> BoxFuture<'static, Result<Vec<DocumentId>, StorageError>> {
        let inner = Arc::clone(&self.inner);
        self.handle
            .spawn_blocking(move || inner.lock().unwrap().list())
            .map(handle_joinerror)
            .map_err(move |e| {
                tracing::error!(err=?e, "error listing all documents");
                StorageError::Error
            })
            .boxed()
    }

    fn append(
        &self,
        id: crate::DocumentId,
        changes: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), StorageError>> {
        let inner = Arc::clone(&self.inner);
        let inner_id = id.clone();
        self.handle
            .spawn_blocking(move || inner.lock().unwrap().append(&inner_id, &changes))
            .map(handle_joinerror)
            .map_err(move |e| {
                tracing::error!(err=?e, doc=?id, "error appending chunk to filesystem");
                StorageError::Error
            })
            .boxed()
    }

    fn compact(
        &self,
        id: crate::DocumentId,
        full_doc: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), StorageError>> {
        let inner = Arc::clone(&self.inner);
        let inner_id = id.clone();
        self.handle
            .spawn_blocking(move || inner.lock().unwrap().compact(&inner_id, &full_doc))
            .map(handle_joinerror)
            .map_err(move |e| {
                tracing::error!(err=?e, doc=?id, "error compacting chunk to filesystem");
                StorageError::Error
            })
            .boxed()
    }
}

fn handle_joinerror<T>(
    result: Result<Result<T, crate::fs_store::Error>, tokio::task::JoinError>,
) -> Result<T, Error> {
    match result {
        Ok(r) => r.map_err(Error::FsStorage),
        Err(e) => {
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            } else {
                Err(Error::SpawnBlockingCancelled)
            }
        }
    }
}

enum Error {
    SpawnBlockingCancelled,
    FsStorage(crate::fs_store::Error),
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::SpawnBlockingCancelled => write!(f, "tokio spawn_blocking cancelled"),
            Error::FsStorage(e) => e.fmt(f),
        }
    }
}
