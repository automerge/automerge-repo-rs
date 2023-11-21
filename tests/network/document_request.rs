extern crate test_utils;

use std::time::Duration;

use automerge::{transaction::Transactable, ReadDoc};
use automerge_repo::{
    share_policy::ShareDecision, DocumentId, Repo, RepoHandle, RepoId, SharePolicy,
    SharePolicyError,
};
use futures::{future::BoxFuture, FutureExt};
use test_log::test;
use test_utils::storage_utils::{InMemoryStorage, SimpleStorage};

use crate::tincans::{connect_repos, connect_to_nowhere};

#[test(tokio::test)]
async fn test_requesting_document_connected_peers() {
    // Create two repos.
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));

    // Keeping a handle to the storage of repo_2,
    // to later assert requested doc is saved.
    let storage = InMemoryStorage::default();
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(storage.clone()));

    // Run the repos in the background.
    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();
    connect_repos(&repo_handle_1, &repo_handle_2);

    // Create a document for one repo.
    let document_handle_1 = repo_handle_1.new_document().await;

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
    let _load = repo_handle_2.load(document_handle_1.document_id());

    assert_eq!(
        doc_handle_future
            .await
            .expect("load future timed out")
            .unwrap()
            .expect("document should be found")
            .document_id(),
        document_handle_1.document_id()
    );

    let _ = tokio::task::spawn(async move {
        // Check that the document has been saved in storage.
        // TODO: replace the loop with an async notification mechanism.
        loop {
            if storage.contains_document(document_handle_1.document_id()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

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
    let document_handle_1 = repo_handle_1.new_document().await;

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
        .expect("document should be found")
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
    let document_handle_1 = repo_handle_1.new_document().await;

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
        .expect("document should be found")
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
    let document_handle_1 = repo_handle_1.new_document().await;

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
    let document_handle_1 = repo_handle_1.new_document().await;

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
        doc_handle_future
            .await
            .unwrap()
            .expect("document should be found")
            .document_id(),
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
    let document_handle = repo_handle.new_document().await;

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
        doc_handle_future
            .await
            .unwrap()
            .expect("document should be found")
            .document_id(),
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
    let document_handle_1 = repo_handle_1.new_document().await;

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

#[test(tokio::test)]
async fn request_doc_which_is_not_shared_does_not_announce() {
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage)).with_share_policy(
        Box::new(|_peer: &RepoId, _doc_id: &DocumentId| {
            automerge_repo::share_policy::ShareDecision::DontShare
        }),
    );
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));

    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();

    connect_repos(&repo_handle_1, &repo_handle_2);

    let document_id = create_doc_with_contents(&repo_handle_1, "peer", "repo1").await;

    // Wait for the announcement to have (maybe) taken place
    tokio::time::sleep(Duration::from_millis(100)).await;

    // now try and resolve the document  from storage of repo 2
    let doc_handle = repo_handle_2.load(document_id).await.unwrap();
    assert!(doc_handle.is_none());
}

struct DontAnnounce;

impl SharePolicy for DontAnnounce {
    fn should_announce(
        &self,
        _doc_id: &DocumentId,
        _with_peer: &RepoId,
    ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>> {
        futures::future::ready(Ok(ShareDecision::DontShare)).boxed()
    }

    fn should_sync(
        &self,
        _document_id: &DocumentId,
        _with_peer: &RepoId,
    ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>> {
        futures::future::ready(Ok(ShareDecision::Share)).boxed()
    }

    fn should_request(
        &self,
        _document_id: &DocumentId,
        _from_peer: &RepoId,
    ) -> BoxFuture<'static, Result<ShareDecision, SharePolicyError>> {
        futures::future::ready(Ok(ShareDecision::Share)).boxed()
    }
}

#[test(tokio::test)]
async fn request_document_transitive() {
    // Test that requesting a document from a peer who doesn't have that document but who is
    // connected to another peer that does have the document eventually resolves

    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));
    let repo_3 = Repo::new(Some("repo3".to_string()), Box::new(SimpleStorage))
        .with_share_policy(Box::new(DontAnnounce));

    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();
    let repo_handle_3 = repo_3.run();

    connect_repos(&repo_handle_1, &repo_handle_2);
    connect_repos(&repo_handle_2, &repo_handle_3);

    let document_id = create_doc_with_contents(&repo_handle_3, "peer", "repo3").await;

    let doc_handle = match tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_1.request_document(document_id),
    )
    .await
    {
        Ok(d) => d.unwrap(),
        Err(_e) => {
            panic!("Request timed out");
        }
    };

    doc_handle.expect("doc should exist").with_doc(|doc| {
        let val = doc.get(&automerge::ROOT, "peer").unwrap();
        assert_eq!(val.unwrap().0.into_string().unwrap(), "repo3");
    });

    tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
        repo_handle_2.stop().unwrap();
        repo_handle_3.stop().unwrap();
    })
    .await
    .unwrap();
}

#[test(tokio::test)]
async fn request_document_which_no_peer_has_returns_unavailable() {
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));
    let repo_3 = Repo::new(Some("repo3".to_string()), Box::new(SimpleStorage));

    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();
    let repo_handle_3 = repo_3.run();

    connect_repos(&repo_handle_1, &repo_handle_2);
    connect_repos(&repo_handle_2, &repo_handle_3);

    let document_id = DocumentId::random();

    let doc_handle = match tokio::time::timeout(
        Duration::from_millis(1000),
        repo_handle_1.request_document(document_id),
    )
    .await
    {
        Ok(d) => d.unwrap(),
        Err(_e) => {
            panic!("Request timed out");
        }
    };

    assert!(doc_handle.is_none());

    tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
        repo_handle_2.stop().unwrap();
        repo_handle_3.stop().unwrap();
    })
    .await
    .unwrap();
}

#[test(tokio::test)]
async fn request_document_which_no_peer_has_but_peer_appears_after_request_starts_resolves_to_some()
{
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));
    let repo_3 = Repo::new(Some("repo3".to_string()), Box::new(SimpleStorage))
        .with_share_policy(Box::new(DontAnnounce));

    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();
    let repo_handle_3 = repo_3.run();

    // note: repo 3 is not connected
    connect_repos(&repo_handle_1, &repo_handle_2);
    // This connection will never respond and so we will hang around waiting until someone has the
    // document
    connect_to_nowhere(&repo_handle_1);

    let document_id = create_doc_with_contents(&repo_handle_3, "peer", "repo3").await;

    let doc_handle_fut = repo_handle_1.request_document(document_id);

    // wait a little bit
    tokio::time::sleep(Duration::from_millis(100)).await;

    //connect repo3
    connect_repos(&repo_handle_1, &repo_handle_3);

    let handle = match tokio::time::timeout(Duration::from_millis(100), doc_handle_fut).await {
        Ok(d) => d.unwrap(),
        Err(_e) => {
            panic!("Request timed out");
        }
    };

    handle.expect("doc should exist").with_doc(|doc| {
        let val = doc.get(&automerge::ROOT, "peer").unwrap();
        assert_eq!(val.unwrap().0.into_string().unwrap(), "repo3");
    });

    tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
        repo_handle_2.stop().unwrap();
        repo_handle_3.stop().unwrap();
    })
    .await
    .unwrap();
}

#[test(tokio::test)]
async fn request_document_which_no_peer_has_but_transitive_peer_appears_after_request_starts_resolves_to_some(
) {
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));
    let repo_3 = Repo::new(Some("repo3".to_string()), Box::new(SimpleStorage))
        .with_share_policy(Box::new(DontAnnounce));

    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();
    let repo_handle_3 = repo_3.run();

    // note: repo 3 is not connected
    connect_repos(&repo_handle_1, &repo_handle_2);
    // This connection will never respond and so we will hang around waiting until someone has the
    // document
    connect_to_nowhere(&repo_handle_2);

    let document_id = create_doc_with_contents(&repo_handle_3, "peer", "repo3").await;

    let doc_handle_fut = repo_handle_1.request_document(document_id);

    // wait a little bit
    tokio::time::sleep(Duration::from_millis(100)).await;

    //connect repo3
    connect_repos(&repo_handle_2, &repo_handle_3);

    let handle = match tokio::time::timeout(Duration::from_millis(1000), doc_handle_fut).await {
        Ok(d) => d.unwrap(),
        Err(_e) => {
            panic!("Request timed out");
        }
    };

    let handle = handle.expect("doc should exist");

    // wait for the doc to sync up
    // TODO: add an API for saying "wait until we're in sync with <peer>"
    tokio::time::sleep(Duration::from_millis(100)).await;

    handle.with_doc(|doc| {
        let val = doc.get(&automerge::ROOT, "peer").unwrap();
        assert_eq!(val.unwrap().0.into_string().unwrap(), "repo3");
    });

    tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
        repo_handle_2.stop().unwrap();
        repo_handle_3.stop().unwrap();
    })
    .await
    .unwrap();
}

async fn create_doc_with_contents(handle: &RepoHandle, key: &str, value: &str) -> DocumentId {
    let document_handle = handle.new_document().await;
    document_handle.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(automerge::ROOT, key, value)
            .expect("Failed to change the document.");
        tx.commit();
    });
    document_handle.document_id()
}
