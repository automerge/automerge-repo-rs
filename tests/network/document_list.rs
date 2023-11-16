extern crate test_utils;

use automerge::transaction::Transactable;
use automerge_repo::{Repo, RepoError};
use test_log::test;
use test_utils::storage_utils::AsyncInMemoryStorage;

#[test(tokio::test)]
async fn test_list_all() {
    let storage = AsyncInMemoryStorage::new(Default::default(), false);

    // Create one repo.
    let repo = Repo::new(None, Box::new(storage.clone()));
    let repo_handle = repo.run();

    // Create a document for one repo.
    let document_handle = repo_handle.new_document();
    let document_id = document_handle.document_id();

    // Edit the document.
    let expected_value = format!("{}", repo_handle.get_repo_id());
    document_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "repo_id", expected_value.clone())
            .expect("Failed to change the document.");
        tx.commit();
    });

    // Shut down the first repo
    tokio::task::spawn_blocking(|| {
        repo_handle.stop().unwrap();
    })
    .await
    .unwrap();

    // Create another repo, using the same storage.
    let repo = Repo::new(None, Box::new(storage.clone()));
    let repo_handle = repo.run();

    let mut list = repo_handle.list_all().await.unwrap();
    assert_eq!(list.pop().unwrap(), document_id);
    assert!(list.is_empty());

    // Shut down the repo.
    tokio::task::spawn_blocking(|| {
        repo_handle.stop().unwrap();
    })
    .await
    .unwrap();
}

#[test(tokio::test)]
async fn test_list_all_errors_on_shutdown() {
    let storage = AsyncInMemoryStorage::new(Default::default(), false);

    // Create one repo.
    let repo = Repo::new(None, Box::new(storage.clone()));
    let repo_handle = repo.run();

    // Create a document for one repo.
    let document_handle = repo_handle.new_document();
    let document_id = document_handle.document_id();

    // Edit the document.
    let expected_value = format!("{}", repo_handle.get_repo_id());
    document_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "repo_id", expected_value.clone())
            .expect("Failed to change the document.");
        tx.commit();
    });

    // Shut down the repo.
    tokio::task::spawn_blocking(|| {
        repo_handle.stop().unwrap();
    })
    .await
    .unwrap();

    // Create another repo, using the same storage.
    let repo = Repo::new(None, Box::new(storage.clone()));
    let repo_handle = repo.run();

    let list_all_fut = repo_handle.list_all();

    // Shut down the repo.
    tokio::task::spawn_blocking(|| {
        repo_handle.stop().unwrap();
    })
    .await
    .unwrap();

    let res = list_all_fut.await;

    // Whether the future is resolved or not before shutdown is not deterministic,
    // hence the conditional. Could be fixed with a storage backend
    // that would only send a result when told to.
    if let Err(res) = res {
        assert!(matches!(res, RepoError::Shutdown));
    } else {
        let mut list = res.unwrap();
        assert_eq!(list.pop().unwrap(), document_id);
        assert!(list.is_empty());
    }
}
