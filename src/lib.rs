#![feature(hash_drain_filter)]

mod dochandle;
mod interfaces;
mod repo;
mod network_connect;

pub use crate::dochandle::DocHandle;
pub use crate::interfaces::{
    DocumentId, NetworkAdapter, NetworkError, NetworkEvent, NetworkMessage, RepoId, Storage,
    StorageError,
};
pub use crate::repo::{Repo, RepoError, RepoHandle};
