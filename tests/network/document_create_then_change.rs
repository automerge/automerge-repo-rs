extern crate test_utils;

use std::time::Duration;

use automerge::transaction::Transactable;
use automerge_repo::Repo;
use test_log::test;
use test_utils::storage_utils::{InMemoryStorage, SimpleStorage};

use super::tincans::connect_repos;

#[test(tokio::test)]
async fn test_create_document_then_change_is_synced() {
    let repo2_storage = InMemoryStorage::default();
    // Create two repos.
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(repo2_storage.clone()));

    // Run the repos in the background.
    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();
    connect_repos(&repo_handle_1, &repo_handle_2);

    // Spawn a task that creates a document and makes a change to id
    let doc_handle = tokio::spawn({
        let repo_handle_1 = repo_handle_1.clone();
        async move {
            let document_handle_1 = repo_handle_1.new_document();

            let repo_id = repo_handle_1.get_repo_id().clone();
            let document_handle_1 = document_handle_1.clone();
            // Edit the document.
            document_handle_1.with_doc_mut(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "repo_id", format!("{}", repo_id))
                    .expect("Failed to change the document.");
                tx.commit();
            });
            document_handle_1
        }
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Now stop both repos
    repo_handle_1.stop().unwrap();
    repo_handle_2.stop().unwrap();

    assert!(repo2_storage.contains_document(doc_handle.document_id()));
}
