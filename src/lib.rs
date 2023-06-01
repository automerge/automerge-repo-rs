#![feature(hash_drain_filter)]

mod dochandle;
mod interfaces;
mod repo;

pub use crate::dochandle::DocHandle;
pub use crate::interfaces::{
    DocumentId, NetworkAdapter, NetworkError, NetworkEvent, NetworkMessage, RepoId, StorageAdapter,
    StorageError,
};
pub use crate::repo::{Repo, RepoError, RepoHandle};
