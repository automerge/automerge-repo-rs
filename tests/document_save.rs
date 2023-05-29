extern crate test_utils;

use automerge::transaction::Transactable;
use automerge::ReadDoc;
use automerge_repo::{DocumentId, Repo, RepoId};
use std::collections::HashMap;
use test_utils::storage_utils::AsyncInMemoryStorage;
use tokio::sync::mpsc::channel;

#[test]
fn test_simple_save() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        let storage = AsyncInMemoryStorage::new(Default::default());
        let (expected_value, document_id) = {
            // Create one repo.
            let repo = Repo::new(None, Box::new(storage.clone()));
            let mut repo_handle = repo.run();

            // Create a document for one repo.
            let mut document_handle = repo_handle.new_document();
            let document_id = document_handle.document_id();

            // Edit the document.
            let expected_value = format!("{}", repo_handle.get_repo_id());
            let doc_data = document_handle.with_doc_mut(|doc| {
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
        let mut repo_handle = repo.run();

        // Load the document
        // and check that its content matches the saved edit.
        let equals = repo_handle
            .load(document_id)
            .await
            .unwrap()
            .unwrap()
            .with_doc(|doc| {
                let val = doc
                    .get(automerge::ROOT, "repo_id")
                    .expect("Failed to read the document.")
                    .unwrap();
                val.0.to_str().clone().unwrap() == expected_value
            });
        if equals {
            // Shut down the repo.
            repo_handle.stop().unwrap();
            done_sync_sender.send(()).await.unwrap();
        }
    });

    done_sync_receiver.blocking_recv().unwrap();
}

#[test]
fn test_multiple_save() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        let storage = AsyncInMemoryStorage::new(Default::default());
        let (expected_value, document_id) = {
            // Create one repo.
            let repo = Repo::new(None, Box::new(storage.clone()));
            let mut repo_handle = repo.run();

            // Create a document for one repo.
            let mut document_handle = repo_handle.new_document();
            let document_id = document_handle.document_id();

            // Edit the document, once.
            let doc_data = document_handle.with_doc_mut(|doc| {
                doc.put(
                    automerge::ROOT,
                    "repo_id",
                    format!("{}::1", repo_handle.get_repo_id()),
                )
                .expect("Failed to change the document.");
                doc.commit();
            });

            // Edit the document, twice.
            let expected_value = format!("{}::2", repo_handle.get_repo_id());
            let doc_data = document_handle.with_doc_mut(|doc| {
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
        let mut repo_handle = repo.run();

        // Load the document
        // and check that its content matches the saved edit.
        let equals = repo_handle
            .load(document_id)
            .await
            .unwrap()
            .unwrap()
            .with_doc(|doc| {
                let val = doc
                    .get(automerge::ROOT, "repo_id")
                    .expect("Failed to read the document.")
                    .unwrap();
                val.0.to_str().clone().unwrap() == expected_value
            });
        if equals {
            // Shut down the repo.
            repo_handle.stop().unwrap();
            done_sync_sender.send(()).await.unwrap();
        }
    });

    done_sync_receiver.blocking_recv().unwrap();
}

#[test]
fn test_compact_save() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        let storage = AsyncInMemoryStorage::new(Default::default());
        let (expected_value, document_id) = {
            // Create one repo.
            let repo = Repo::new(None, Box::new(storage.clone()));
            let mut repo_handle = repo.run();

            // Create a document for one repo.
            let mut document_handle = repo_handle.new_document();
            let document_id = document_handle.document_id();

            // Edit the document, once.
            let change_fut = document_handle.changed();
            document_handle.with_doc_mut(|doc| {
                doc.put(
                    automerge::ROOT,
                    "repo_id",
                    format!("{}::1", repo_handle.get_repo_id()),
                )
                .expect("Failed to change the document.");
                doc.commit();
            });
            change_fut.await.unwrap();

            // Edit the document, again.
            let change_fut = document_handle.changed();
            document_handle.with_doc_mut(|doc| {
                doc.put(
                    automerge::ROOT,
                    "repo_id",
                    format!("{}::2", repo_handle.get_repo_id()),
                )
                .expect("Failed to change the document.");
                doc.commit();
            });
            change_fut.await.unwrap();

            // Edit the document, and again.
            let change_fut = document_handle.changed();
            document_handle.with_doc_mut(|doc| {
                doc.put(
                    automerge::ROOT,
                    "repo_id",
                    format!("{}::3", repo_handle.get_repo_id()),
                )
                .expect("Failed to change the document.");
                doc.commit();
            });
            change_fut.await.unwrap();

            // Edit the document, enough times to trigger a concat.
            let change_fut = document_handle.changed();
            let expected_value = format!("{}::11", repo_handle.get_repo_id());
            document_handle.with_doc_mut(|doc| {
                for i in 0..12 {
                    doc.put(
                        automerge::ROOT,
                        "repo_id",
                        format!("{}::{}", repo_handle.get_repo_id(), i),
                    )
                    .expect("Failed to change the document.");
                    doc.commit();
                }
            });
            change_fut.await.unwrap();

            // Shut down the repo.
            drop(document_handle);
            repo_handle.stop().unwrap();
            (expected_value, document_id)
        };

        // Create another repo, using the same storage.
        let repo = Repo::new(None, Box::new(storage.clone()));
        let mut repo_handle = repo.run();

        // Load the document
        // and check that its content matches the saved edit.
        let equals = repo_handle
            .load(document_id)
            .await
            .unwrap()
            .unwrap()
            .with_doc(|doc| {
                let val = doc
                    .get(automerge::ROOT, "repo_id")
                    .expect("Failed to read the document.")
                    .unwrap();
                val.0.to_str().clone().unwrap() == expected_value
            });
        if equals {
            // Shut down the repo.
            repo_handle.stop().unwrap();
            done_sync_sender.send(()).await.unwrap();
        }
    });

    done_sync_receiver.blocking_recv().unwrap();
}
