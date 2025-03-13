extern crate test_utils;

use std::time::Duration;

use automerge::transaction::Transactable;
use automerge_repo::{DocHandle, DocumentId, Repo, RepoHandle};
use test_log::test;
use test_utils::storage_utils::AsyncInMemoryStorage;

use crate::tincans::connect_repos;

struct Scenario {
    repo_handle_1: RepoHandle,
    repo_handle_2: RepoHandle,
    document_handle_1: DocHandle,
    #[allow(dead_code)]
    document_handle_2: DocHandle,
    document_id: DocumentId,
}

async fn scenario() -> Scenario {
    let storage = AsyncInMemoryStorage::new(Default::default(), false);
    let storage2 = AsyncInMemoryStorage::new(Default::default(), false);

    // Create one repo.
    let repo = Repo::new(Some("repo1".to_string()), Box::new(storage.clone()));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(storage2.clone()));
    let repo_handle_1 = repo.run();
    let repo_handle_2 = repo_2.run();
    connect_repos(&repo_handle_1, &repo_handle_2);

    // Create a document for one repo.
    let document_handle_1 = repo_handle_1.new_document();
    let document_id = document_handle_1.document_id();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "foo", "bar")
            .expect("Failed to change the document.");
        tx.commit();
    });

    let document_handle_2 = tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_2.request_document(document_handle_1.document_id()),
    )
    .await
    .expect("timed out waiting to fetch document")
    .expect("failed to fetch document");

    Scenario {
        repo_handle_1,
        repo_handle_2,
        document_handle_1,
        document_handle_2,
        document_id,
    }
}

#[test(tokio::test)]
async fn test_read_peer_state() {
    let Scenario {
        repo_handle_1,
        repo_handle_2,
        document_id,
        ..
    } = scenario().await;

    let peer_state = tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_1.peer_doc_state(repo_handle_2.get_repo_id().clone(), document_id),
    )
    .await
    .expect("timed out getting peer state");
    assert!(peer_state.is_some());
}

#[test(tokio::test)]
async fn test_peer_state_last_send() {
    let Scenario {
        repo_handle_1,
        repo_handle_2,
        document_handle_1,
        ..
    } = scenario().await;

    let peer_state = tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_1.peer_doc_state(
            repo_handle_2.get_repo_id().clone(),
            document_handle_1.document_id(),
        ),
    )
    .await
    .expect("timed out getting peer state")
    .expect("peer state was none");
    let last_send_before_change = peer_state
        .last_sent
        .expect("last send before should be some");

    // Now make a change on doc 1
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "foo", "baz")
            .expect("Failed to change the document.");
        tx.commit();
    });

    let peer_state_after = tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_1.peer_doc_state(
            repo_handle_2.get_repo_id().clone(),
            document_handle_1.document_id(),
        ),
    )
    .await
    .expect("timed out getting peer state")
    .expect("peer state was none");

    let last_sent_after_change = peer_state_after
        .last_sent
        .expect("last send after should be some");
    assert!(last_sent_after_change > last_send_before_change);
}

#[test(tokio::test)]
async fn test_peer_state_last_recv() {
    let Scenario {
        repo_handle_1,
        repo_handle_2,
        document_handle_1,
        document_id,
        ..
    } = scenario().await;

    // Get the peer state on repo 2
    let peer_state = tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_2.peer_doc_state(repo_handle_1.get_repo_id().clone(), document_id.clone()),
    )
    .await
    .expect("timed out getting peer state")
    .expect("peer state was none");
    let last_recv_before_change = peer_state
        .last_received
        .expect("last recv before should be some");

    // Now make a change on repo 1
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "foo", "baz")
            .expect("Failed to change the document.");
        tx.commit();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let peer_state_after = tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_2.peer_doc_state(repo_handle_1.get_repo_id().clone(), document_id),
    )
    .await
    .expect("timed out getting peer state")
    .expect("peer state was none");

    let last_received_after_change = peer_state_after
        .last_received
        .expect("last received after should be some");
    assert!(last_received_after_change > last_recv_before_change);
}

#[test(tokio::test)]
async fn test_peer_state_last_sent_heads() {
    let Scenario {
        repo_handle_1,
        repo_handle_2,
        document_handle_1,
        document_id,
        ..
    } = scenario().await;

    let heads_before = document_handle_1.with_doc(|d| d.get_heads());

    // Get the peer state on repo 1
    let peer_state = tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_1.peer_doc_state(
            repo_handle_2.get_repo_id().clone(),
            document_handle_1.document_id(),
        ),
    )
    .await
    .expect("timed out getting peer state")
    .expect("peer state was none");
    let last_sent_heads_before_change = peer_state
        .last_sent_heads
        .expect("last sent heads before should be some");

    assert_eq!(heads_before, last_sent_heads_before_change);

    // Now make a change on repo 1
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "foo", "baz")
            .expect("Failed to change the document.");
        tx.commit();
    });

    let heads_after_change = document_handle_1.with_doc(|d| d.get_heads());

    let peer_state_after = tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_1.peer_doc_state(repo_handle_2.get_repo_id().clone(), document_id),
    )
    .await
    .expect("timed out getting peer state")
    .expect("peer state was none");

    let last_sent_heads_after = peer_state_after
        .last_sent_heads
        .expect("last received after should be some");
    assert_eq!(last_sent_heads_after, heads_after_change);
}

#[test(tokio::test)]
async fn test_last_acked_heads() {
    let Scenario {
        repo_handle_1,
        repo_handle_2,
        document_handle_1,
        document_id,
        ..
    } = scenario().await;

    let heads_before = document_handle_1.with_doc(|d| d.get_heads());

    // Wait for the sync to run a bit
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get the peer state on repo 1
    let peer_state = tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_1.peer_doc_state(repo_handle_2.get_repo_id().clone(), document_id),
    )
    .await
    .expect("timed out getting peer state")
    .expect("peer state was none");
    let last_acked_heads_before_change = peer_state
        .last_acked_heads
        .expect("last acked heads before should be some");

    assert_eq!(heads_before, last_acked_heads_before_change);

    // Now make a change on repo 1
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "foo", "baz")
            .expect("Failed to change the document.");
        tx.commit();
    });
}
