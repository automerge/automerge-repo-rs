#![feature(hash_drain_filter)]

mod dochandle;
mod interfaces;
mod network_connect;
mod repo;

pub use crate::dochandle::DocHandle;
pub use crate::interfaces::{
    DocumentId, NetworkAdapter, NetworkError, RepoId, RepoMessage, Storage, StorageError,
};
pub use crate::repo::{Repo, RepoError, RepoHandle};
