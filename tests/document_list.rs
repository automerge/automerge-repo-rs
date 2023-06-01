extern crate test_utils;

use automerge::transaction::Transactable;
use automerge_repo::{Repo, RepoError};
use test_utils::storage_utils::AsyncInMemoryStorage;
use tokio::sync::mpsc::channel;

#[test]
fn test_list_all() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        let storage = AsyncInMemoryStorage::new(Default::default());
        let (_expected_value, document_id) = {
            // Create one repo.
            let repo = Repo::new(None, Box::new(storage.clone()));
            let repo_handle = repo.run();

            // Create a document for one repo.
            let mut document_handle = repo_handle.new_document();
            let document_id = document_handle.document_id();

            // Edit the document.
            let expected_value = format!("{}", repo_handle.get_repo_id());
            document_handle.with_doc_mut(|doc| {
                doc.put(automerge::ROOT, "repo_id", expected_value.clone())
                    .expect("Failed to change the document.");
                doc.commit();
            });

            // Shut down the repo.
            drop(document_handle);
            let _ = tokio::task::spawn_blocking(|| {
                repo_handle.stop().unwrap();
            })
            .await;
            (expected_value, document_id)
        };

        // Create another repo, using the same storage.
        let repo = Repo::new(None, Box::new(storage.clone()));
        let repo_handle = repo.run();

        let mut list = repo_handle.list_all().await.unwrap();
        assert_eq!(list.pop().unwrap(), document_id);
        assert!(list.is_empty());
        // Shut down the repo.
        let _ = tokio::task::spawn_blocking(|| {
            repo_handle.stop().unwrap();
        })
        .await;
        done_sync_sender.send(()).await.unwrap();
    });

    done_sync_receiver.blocking_recv().unwrap();
}

#[test]
fn test_list_all_errors_on_shutdown() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        let storage = AsyncInMemoryStorage::new(Default::default());
        let (_expected_value, document_id) = {
            // Create one repo.
            let repo = Repo::new(None, Box::new(storage.clone()));
            let repo_handle = repo.run();

            // Create a document for one repo.
            let mut document_handle = repo_handle.new_document();
            let document_id = document_handle.document_id();

            // Edit the document.
            let expected_value = format!("{}", repo_handle.get_repo_id());
            document_handle.with_doc_mut(|doc| {
                doc.put(automerge::ROOT, "repo_id", expected_value.clone())
                    .expect("Failed to change the document.");
                doc.commit();
            });

            // Shut down the repo.
            drop(document_handle);
            repo_handle.stop().unwrap();
            (expected_value, document_id)
        };

        // Create another repo, using the same storage.
        let repo = Repo::new(None, Box::new(storage.clone()));
        let repo_handle = repo.run();

        let list_all_fut = repo_handle.list_all();

        // Shut down the repo.
        let _ = tokio::task::spawn_blocking(|| {
            repo_handle.stop().unwrap();
        })
        .await;

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
        done_sync_sender.send(()).await.unwrap();
    });

    done_sync_receiver.blocking_recv().unwrap();
}
