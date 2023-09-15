extern crate test_utils;

use automerge::transaction::Transactable;
use automerge::ReadDoc;
use automerge_repo::Repo;
use test_log::test;
use test_utils::storage_utils::SimpleStorage;

use super::tincans::connect_repos;

#[test(tokio::test)]
async fn test_document_changed_over_sync() {
    // Create two repos.
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));

    // Run the repos in the background.
    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();
    connect_repos(&repo_handle_1, &repo_handle_2);

    let repo_handle_2_clone = repo_handle_2.clone();

    // Create a document for one repo.
    let document_handle_1 = repo_handle_1.new_document();

    // Spawn a task that awaits the requested doc handle,
    // and then edits the document.
    let doc_id = document_handle_1.document_id();
    tokio::spawn(async move {
        // Request the document.
        let doc_handle = repo_handle_2.request_document(doc_id).await.unwrap();
        doc_handle.with_doc_mut(|doc| {
            let val = doc
                .get(automerge::ROOT, "repo_id")
                .expect("Failed to read the document.")
                .unwrap();
            tracing::debug!(heads=?doc.get_heads(), ?val, "before repo_handle_2 makes edit");
            {
                let mut tx = doc.transaction();
                tx.put(
                    automerge::ROOT,
                    "repo_id",
                    format!("{}", repo_handle_2.get_repo_id()),
                )
                .expect("Failed to change the document.");
                tx.commit();
            }
            tracing::debug!(heads=?doc.get_heads(), "after repo_handle_2 makes edit");
        });
    });

    // Spawn a task that makes a change the document change.
    tokio::spawn({
        let repo_id = repo_handle_1.get_repo_id().clone();
        let document_handle_1 = document_handle_1.clone();
        async move {
            // Edit the document.
            document_handle_1.with_doc_mut(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "repo_id", format!("{}", repo_id))
                    .expect("Failed to change the document.");
                tx.commit();
            });
        }
    });

    // Await changes until the edit comes through over sync.
    document_handle_1.changed().await.unwrap();
    document_handle_1.with_doc(|doc| {
        let val = doc
            .get(automerge::ROOT, "repo_id")
            .expect("Failed to read the document.")
            .unwrap();
        tracing::debug!(?val, "after repo_handle_1 received sync");
        assert_eq!(val.0.to_str().unwrap(), "repo1".to_string());
    });

    // Stop the repos.
    tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
        repo_handle_2_clone.stop().unwrap();
    })
    .await
    .unwrap();
}

#[test(tokio::test)]
async fn test_document_changed_locally() {
    // Create two repos.
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));

    // Run the repo in the background.
    let repo_handle_1 = repo_1.run();
    let expected_repo_id = repo_handle_1.get_repo_id().clone();

    // Create a document for the repo.
    let doc_handle = repo_handle_1.new_document();

    // spawn a task which edits the document
    tokio::spawn({
        let doc_handle = doc_handle.clone();
        async move {
            // Edit the document.
            doc_handle.with_doc_mut(|doc| {
                let mut tx = doc.transaction();
                tx.put(
                    automerge::ROOT,
                    "repo_id",
                    format!("{}", expected_repo_id.clone()),
                )
                .expect("Failed to change the document.");
                tx.commit();
            });
        }
    });

    // wait for the change
    doc_handle.changed().await.unwrap();
    doc_handle.with_doc(|doc| {
        let val = doc
            .get(automerge::ROOT, "repo_id")
            .expect("Failed to read the document.")
            .unwrap();
        assert_eq!(val.0.to_str().unwrap(), "repo1".to_string());
    });

    // Stop the repo.
    tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
    })
    .await
    .unwrap();
}
