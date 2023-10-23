extern crate test_utils;

use automerge::transaction::Transactable;
use automerge::ReadDoc;
use automerge_repo::{DocumentId, Repo};
use std::collections::HashMap;
use test_log::test;
use test_utils::storage_utils::{AsyncInMemoryStorage, InMemoryStorage};

#[test(tokio::test)]
async fn test_loading_document_found_immediately() {
    let storage = InMemoryStorage::default();
    // Create one repo.
    let repo = Repo::new(Some("repo1".to_string()), Box::new(storage.clone()));
    let repo_handle = repo.run();

    // Create a document for one repo.
    let document_handle = repo_handle.new_document();

    // Edit the document.
    let doc_data = document_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
        doc.save()
    });

    // Add the doc to storage, out of band.
    let doc_id = document_handle.document_id();
    storage.add_document(document_handle.document_id(), doc_data);
    drop(document_handle);

    // Shut down the repo.
    tokio::task::spawn_blocking(|| repo_handle.stop().unwrap())
        .await
        .unwrap();

    // Create another repo.
    let repo = Repo::new(None, Box::new(storage));
    let repo_handle = repo.run();

    let doc = repo_handle.load(doc_id).await.unwrap().unwrap();
    doc.with_doc(|doc| {
        let val = doc
            .get(automerge::ROOT, "repo_id")
            .expect("Failed to read the document.")
            .unwrap();
        assert_eq!(val.0.to_str().unwrap(), "repo1".to_string());
    });
    // Shut down the repo.
    tokio::task::spawn_blocking(|| repo_handle.stop().unwrap())
        .await
        .unwrap();
}

#[test(tokio::test)]
async fn test_loading_document_found_async() {
    let storage = InMemoryStorage::default();
    // Create one repo.
    let repo = Repo::new(Some("repo1".to_string()), Box::new(storage));
    let repo_handle = repo.run();

    // Create a document for one repo.
    let document_handle = repo_handle.new_document();

    // Edit the document.
    let doc_data = document_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
        doc.save()
    });

    // Add the doc to storage, out of band.
    let doc_id = document_handle.document_id();
    let mut docs = HashMap::new();
    docs.insert(doc_id.clone(), doc_data);

    // Shut down the repo.
    drop(document_handle);
    repo_handle.stop().unwrap();

    // Create another repo, using the async storage.
    let async_storage = AsyncInMemoryStorage::new(docs, false);
    let repo = Repo::new(None, Box::new(async_storage));
    let repo_handle = repo.run();

    // Spawn a task that awaits the requested doc handle.
    repo_handle
        .load(doc_id)
        .await
        .unwrap()
        .unwrap()
        .with_doc(|doc| {
            let val = doc
                .get(automerge::ROOT, "repo_id")
                .expect("Failed to read the document.")
                .unwrap();
            assert_eq!(val.0.to_str().unwrap(), "repo1".to_string());
        });

    tokio::task::spawn_blocking(|| {
        repo_handle.stop().unwrap();
    })
    .await
    .unwrap();
}

#[test(tokio::test)]
async fn test_loading_document_immediately_not_found() {
    // Empty storage.
    let storage = InMemoryStorage::default();

    // Create a repo.
    let repo = Repo::new(None, Box::new(storage));
    let repo_handle = repo.run();

    // Spawn a task that awaits the requested doc handle.
    let doc_id = DocumentId::random();
    assert!(repo_handle.load(doc_id).await.unwrap().is_none());
    // Shut down the repo.
    repo_handle.stop().unwrap();
}

#[test(tokio::test)]
async fn test_loading_document_not_found_async() {
    // Empty docs.
    let docs = HashMap::new();

    let async_storage = AsyncInMemoryStorage::new(docs, false);
    let repo = Repo::new(None, Box::new(async_storage));
    let repo_handle = repo.run();

    // Spawn a task that awaits the requested doc handle.
    let doc_id = DocumentId::random();
    assert!(repo_handle.load(doc_id).await.unwrap().is_none());
    // Shut down the repo.
    tokio::task::spawn_blocking(|| {
        repo_handle.stop().unwrap();
    })
    .await
    .unwrap();
}
