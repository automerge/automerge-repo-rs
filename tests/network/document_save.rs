extern crate test_utils;

use automerge::transaction::Transactable;
use automerge::ReadDoc;
use automerge_repo::Repo;
use test_log::test;
use test_utils::storage_utils::AsyncInMemoryStorage;

#[test(tokio::test)]
async fn test_simple_save() {
    let storage = AsyncInMemoryStorage::new(Default::default(), false);

    // Create one repo.
    let repo = Repo::new(Some("repo1".to_string()), Box::new(storage.clone()));
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

    drop(document_handle);
    // Shut down the repo.
    tokio::task::spawn_blocking(|| {
        repo_handle.stop().unwrap();
    })
    .await
    .unwrap();

    // Create another repo, using the same storage.
    let repo = Repo::new(Some("repo2".to_string()), Box::new(storage.clone()));
    let repo_handle = repo.run();

    // Load the document
    // and check that its content matches the saved edit.
    repo_handle
        .load(document_id)
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
async fn test_save_on_shutdown() {
    // Step by step storage.
    let storage = AsyncInMemoryStorage::new(Default::default(), true);
    // Create one repo.
    let repo = Repo::new(Some("repo1".to_string()), Box::new(storage.clone()));
    let repo_handle = repo.run();

    // Create a document for one repo.
    let document_handle = repo_handle.new_document();

    // Edit the document.
    let expected_value = format!("{}", repo_handle.get_repo_id());
    document_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "repo_id", expected_value.clone())
            .expect("Failed to change the document.");
        tx.commit();
    });
    drop(document_handle);

    use tokio::sync::oneshot::channel as oneshot;

    let (tx, rx) = oneshot();

    tokio::spawn(async move {
        rx.await.unwrap();
        // Allow storage to perform the saves.
        storage.process_results().await;
    });

    // Shut down the repo.
    tokio::task::spawn_blocking(|| {
        // Not completely deterministic, but good enough.
        tx.send(()).unwrap();
        repo_handle.stop().unwrap();
    })
    .await
    .unwrap();
}

#[test(tokio::test)]
async fn test_multiple_save() {
    let storage = AsyncInMemoryStorage::new(Default::default(), false);

    // Create one repo.
    let repo = Repo::new(Some("repo1".to_string()), Box::new(storage.clone()));
    let repo_handle = repo.run();

    // Create a document for one repo.
    let document_handle = repo_handle.new_document();
    let document_id = document_handle.document_id();

    // Edit the document, once.
    document_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}::1", repo_handle.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });

    // Edit the document, twice.
    let expected_value = format!("{}::2", repo_handle.get_repo_id());
    document_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, "repo_id", expected_value.clone())
            .expect("Failed to change the document.");
        tx.commit();
    });

    // Shut down the repo.
    drop(document_handle);
    tokio::task::spawn_blocking(|| {
        repo_handle.stop().unwrap();
    })
    .await
    .unwrap();

    // Create another repo, using the same storage.
    let repo = Repo::new(None, Box::new(storage.clone()));
    let repo_handle = repo.run();

    // Load the document
    // and check that its content matches the saved edit.
    repo_handle
        .load(document_id)
        .await
        .unwrap()
        .unwrap()
        .with_doc(|doc| {
            let val = doc
                .get(automerge::ROOT, "repo_id")
                .expect("Failed to read the document.")
                .unwrap();
            assert_eq!(val.0.to_str().unwrap(), "repo1::2".to_string())
        });

    tokio::task::spawn_blocking(|| {
        repo_handle.stop().unwrap();
    })
    .await
    .unwrap();
}

#[test(tokio::test)]
async fn test_compact_save() {
    let storage = AsyncInMemoryStorage::new(Default::default(), false);
    // Create one repo.
    let repo = Repo::new(Some("repo1".to_string()), Box::new(storage.clone()));
    let repo_handle = repo.run();

    // Create a document for one repo.
    let document_handle = repo_handle.new_document();
    let document_id = document_handle.document_id();

    // Edit the document, once.
    let change_fut = document_handle.changed();
    document_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}::1", repo_handle.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });
    change_fut.await.unwrap();

    // Edit the document, again.
    let change_fut = document_handle.changed();
    document_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}::2", repo_handle.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });
    change_fut.await.unwrap();

    // Edit the document, enough times to trigger a compact.
    let change_fut = document_handle.changed();
    document_handle.with_doc_mut(|doc| {
        for i in 0..12 {
            let mut tx = doc.transaction();
            tx.put(
                automerge::ROOT,
                "repo_id",
                format!("{}::{}", repo_handle.get_repo_id(), i),
            )
            .expect("Failed to change the document.");
            tx.commit();
        }
    });
    change_fut.await.unwrap();

    drop(document_handle);

    // Shut down the repo.
    tokio::task::spawn_blocking(|| {
        repo_handle.stop().unwrap();
    })
    .await
    .unwrap();

    // Create another repo, using the same storage.
    let repo = Repo::new(Some("repo2".to_string()), Box::new(storage.clone()));
    let repo_handle = repo.run();

    // Load the document
    // and check that its content matches the saved edit.
    repo_handle
        .load(document_id)
        .await
        .unwrap()
        .unwrap()
        .with_doc(|doc| {
            let val = doc
                .get(automerge::ROOT, "repo_id")
                .expect("Failed to read the document.")
                .unwrap();
            assert_eq!(val.0.to_str().unwrap(), "repo1::11")
        });

    tokio::task::spawn_blocking(|| {
        repo_handle.stop().unwrap();
    })
    .await
    .unwrap();
}
