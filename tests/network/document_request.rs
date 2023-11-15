extern crate test_utils;

use automerge::transaction::Transactable;
use automerge_repo::Repo;
use test_log::test;
use test_utils::storage_utils::{InMemoryStorage, SimpleStorage};

use crate::tincans::connect_repos;

#[test(tokio::test)]
async fn test_requesting_document_connected_peers() {
    // Create two repos.
    let repo_1 = Repo::new(None, Box::new(SimpleStorage));

    // Keeping a handle to the storage of repo_2,
    // to later assert requested doc is saved.
    let storage = InMemoryStorage::default();
    let repo_2 = Repo::new(None, Box::new(storage.clone()));

    // Run the repos in the background.
    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();
    connect_repos(&repo_handle_1, &repo_handle_2);

    // Create a document for one repo.
    let document_handle_1 = repo_handle_1.new_document();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });

    // Request the document.
    let doc_handle_future =
        tokio::spawn(repo_handle_2.request_document(document_handle_1.document_id()));
    let load = repo_handle_2.load(document_handle_1.document_id());

    assert_eq!(
        doc_handle_future.await.unwrap().unwrap().document_id(),
        document_handle_1.document_id()
    );
    let _ = tokio::task::spawn_blocking(move || {
        // Check that the document has been saved in storage.
        // TODO: replace the loop with an async notification mechanism.
        loop {
            if storage.contains_document(document_handle_1.document_id()) {
                break;
            }
        }
    })
    .await;

    // Load following a request fails, but this API should be improved.
    // See comment at handling of `RepoEvent::LoadDoc`.
    assert!(load.await.is_err());

    // Stop the repos.
    tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
        repo_handle_2.stop().unwrap();
    })
    .await
    .unwrap();
}

#[test(tokio::test)]
async fn test_requesting_document_unconnected_peers() {
    // Create two repos.
    let repo_1 = Repo::new(None, Box::new(SimpleStorage));
    let repo_2 = Repo::new(None, Box::new(SimpleStorage));

    // Run the repos in the background.
    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();
    connect_repos(&repo_handle_1, &repo_handle_2);

    // Create a document for one repo.
    let document_handle_1 = repo_handle_1.new_document();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });

    // Note: requesting the document while peers aren't connected yet.

    // Request the document.
    let doc_id = repo_handle_2
        .request_document(document_handle_1.document_id())
        .await
        .unwrap()
        .document_id();
    assert_eq!(doc_id, document_handle_1.document_id());

    // Stop the repos.
    tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
        repo_handle_2.stop().unwrap();
    })
    .await
    .unwrap();
}

#[test(tokio::test)]
async fn test_requesting_document_unconnected_peers_with_storage_load() {
    let repo_1 = Repo::new(None, Box::new(SimpleStorage));
    let repo_handle_1 = repo_1.run();

    // Create a document for one repo.
    let document_handle_1 = repo_handle_1.new_document();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });

    // Add document to storage.
    let storage = InMemoryStorage::default();
    storage.add_document(
        document_handle_1.document_id(),
        document_handle_1.with_doc_mut(|doc| doc.save()),
    );
    let repo_2 = Repo::new(None, Box::new(storage));
    let repo_handle_2 = repo_2.run();

    // Request the document.
    let doc_id = repo_handle_2
        .request_document(document_handle_1.document_id())
        .await
        .unwrap()
        .document_id();
    assert_eq!(doc_id, document_handle_1.document_id());

    // Stop the repos.
    tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
        repo_handle_2.stop().unwrap();
    })
    .await
    .unwrap();
}

#[test(tokio::test)]
async fn test_request_with_repo_stop() {
    // Create two repos.
    let repo_1 = Repo::new(None, Box::new(SimpleStorage));
    let repo_2 = Repo::new(None, Box::new(SimpleStorage));

    // Run the repos in the background.
    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();

    // Create a document for one repo.
    let document_handle_1 = repo_handle_1.new_document();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });

    // Note: requesting the document while peers aren't connected yet.
    let doc_handle_future = repo_handle_2.request_document(document_handle_1.document_id());

    // Stop the repos.
    let stop = tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
        repo_handle_2.stop().unwrap();
    });

    // Now run the future
    // Since the repo is stopping, the future should error.
    assert!(doc_handle_future.await.is_err());

    // Make sure everything stopped okay
    stop.await.unwrap();
}

#[test(tokio::test)]
async fn test_request_twice_ok_bootstrap() {
    // Create a repo.
    let repo_1 = Repo::new(None, Box::new(SimpleStorage));

    // Run in the background.
    let repo_handle_1 = repo_1.run();

    // Create a document for one repo.
    let document_handle_1 = repo_handle_1.new_document();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });

    // Add document to storage(out-of-band).
    let storage = InMemoryStorage::default();
    storage.add_document(
        document_handle_1.document_id(),
        document_handle_1.with_doc_mut(|doc| doc.save()),
    );

    // Create another repo, with the storage containing the doc.
    let repo_2 = Repo::new(None, Box::new(storage));
    let repo_handle_2 = repo_2.run();

    // Note: requesting the document while peers aren't connected yet.

    // Request the document, twice.
    let _doc_handle_future = repo_handle_2.request_document(document_handle_1.document_id());
    let doc_handle_future = repo_handle_2.request_document(document_handle_1.document_id());

    // Future should resolve from storage load(no peers are connected).
    assert_eq!(
        doc_handle_future.await.unwrap().document_id(),
        document_handle_1.document_id()
    );

    // Stop the repos.
    tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
        repo_handle_2.stop().unwrap();
    })
    .await
    .unwrap();
}

#[test(tokio::test)]
async fn test_request_twice_ok() {
    // Create one repo.
    let repo = Repo::new(None, Box::new(SimpleStorage));

    // Run the repos in the background.
    let repo_handle = repo.run();

    // Create a document for one repo.
    let document_handle = repo_handle.new_document();

    // Edit the document.
    document_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });

    // Note: requesting the document while peers aren't connected yet.

    // Request the document, twice.
    let _doc_handle_future = repo_handle.request_document(document_handle.document_id());
    let doc_handle_future = repo_handle.request_document(document_handle.document_id());

    // Since the request was made twice,
    // but the document is ready, the future should resolve to ok.
    assert_eq!(
        doc_handle_future.await.unwrap().document_id(),
        document_handle.document_id()
    );

    // Stop the repo.
    repo_handle.stop().unwrap();
}

#[test(tokio::test)]
async fn test_request_unavailable_point_to_point() {
    // Test that requesting a document which the other end doesn't have
    // immediately returns unavailable rather than waiting for a timeout.

    // Create two repos.
    let repo_1 = Repo::new(None, Box::new(SimpleStorage));
    let repo_2 = Repo::new(None, Box::new(SimpleStorage));

    // Run the repos in the background.
    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();

    // Create a document for one repo.
    let document_handle_1 = repo_handle_1.new_document();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });

    // Note: requesting the document while peers aren't connected yet.

    // Request the document.
    let doc_handle_future = repo_handle_2.request_document(document_handle_1.document_id());

    // Stop the repos.
    repo_handle_1.stop().unwrap();
    repo_handle_2.stop().unwrap();

    // Since the repo is stopping, the future should error.
    assert!(doc_handle_future.await.is_err());
}
