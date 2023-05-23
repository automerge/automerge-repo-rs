extern crate test_utils;

use automerge::transaction::Transactable;
use automerge_repo::{NetworkEvent, NetworkMessage, Repo};
use test_utils::storage_utils::InMemoryStorage;
use std::collections::HashMap;
use tokio::sync::mpsc::channel;

#[test]
fn test_loading_document_found() {
    // Create one repo.
    let repo = Repo::new(None, Box::new(InMemoryStorage::new()));
}
