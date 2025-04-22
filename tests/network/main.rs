extern crate test_utils;

use automerge::transaction::Transactable;
use automerge_repo::{share_policy::ShareDecision, DocumentId, Repo, RepoId};
use futures::{select, FutureExt};
use std::time::Duration;
use test_utils::storage_utils::SimpleStorage;

mod tincans;
use tincans::{tincan_to_nowhere, tincans, TinCan, TinCans};
mod conn_complete;
mod document_changed;
mod document_create_then_change;
mod document_list;
mod document_load;
mod document_request;
mod document_save;
mod listen_to_peer_conn_info;
mod peer_doc_state;

use test_log::test;

use crate::tincans::connect_repos;

#[test]
fn test_repo_stop() {
    // Create the repo.
    let repo = Repo::new(None, Box::new(SimpleStorage));

    // Run the repo in the background.
    let repo_handle = repo.run();

    // Stop the repo.
    repo_handle.stop().unwrap();
}

#[test(tokio::test)]
async fn test_simple_sync() {
    let mut repo_handles = vec![];
    let mut documents = vec![];

    for i in 1..10 {
        // Create the repo.
        let repo_id = format!("repo{}", i);
        let repo = Repo::new(Some(repo_id), Box::new(SimpleStorage));
        let repo_handle = repo.run();

        // Create a document.
        let doc_handle = repo_handle.new_document();
        doc_handle.with_doc_mut(|doc| {
            let mut tx = doc.transaction();
            tx.put(
                automerge::ROOT,
                "repo_id",
                format!("{}", repo_handle.get_repo_id()),
            )
            .expect("Failed to change the document.");
            tx.commit();
        });
        documents.push(doc_handle);

        repo_handles.push(repo_handle);
    }

    let repo_handles_clone = repo_handles.clone();

    //connect all the repos to each other
    for left_idx in 0..repo_handles.len() {
        for right_idx in 0..repo_handles.len() {
            let left_handle = &repo_handles[left_idx];
            let right_handle = &repo_handles[right_idx];
            connect_repos(left_handle, right_handle);
        }
    }

    tokio::spawn(async move {
        let mut synced = 0;
        for doc_handle in documents {
            for repo_handle in repo_handles_clone.iter() {
                repo_handle
                    .request_document(doc_handle.document_id())
                    .await
                    .unwrap();
                synced += 1;
            }
        }
        assert_eq!(synced, 81);
    })
    .await
    .unwrap();

    // Stop repo.
    for handle in repo_handles.into_iter() {
        tokio::task::spawn_blocking(|| handle.stop().unwrap())
            .await
            .unwrap();
    }
}

#[test(tokio::test)]
async fn test_sinks_closed_on_shutdown() {
    // Create two repos.
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));

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

    let TinCans {
        left:
            TinCan {
                send: left_sink,
                recv: left_stream,
                sink_closed: left_closed,
            },
        right:
            TinCan {
                send: right_sink,
                recv: right_stream,
                sink_closed: right_closed,
            },
    } = tincans();
    repo_handle_1.new_remote_repo(repo_handle_2.get_repo_id().clone(), left_stream, left_sink);
    repo_handle_2.new_remote_repo(
        repo_handle_1.get_repo_id().clone(),
        right_stream,
        right_sink,
    );

    // Request the document.
    let doc_handle_future = repo_handle_2.request_document(document_handle_1.document_id());

    let id = doc_handle_future.await.unwrap().document_id();
    assert_eq!(id, document_handle_1.document_id());

    // Stop the repos.
    let mut stopped = tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
        repo_handle_2.stop().unwrap();
    })
    .fuse();

    select! {
        r = stopped => {
            r.unwrap();
        },
        _ = tokio::time::sleep(Duration::from_secs(1)).fuse() => {
            panic!("Repo stop timed out.");
        }
    }

    assert!(left_closed.load(std::sync::atomic::Ordering::Acquire));
    assert!(right_closed.load(std::sync::atomic::Ordering::Acquire));
}

#[test(tokio::test)]
async fn test_sinks_closed_on_replacement() {
    // Create two repos.
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));

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

    let TinCans {
        left:
            TinCan {
                send: left_sink,
                recv: left_stream,
                sink_closed: left_closed,
            },
        right:
            TinCan {
                send: right_sink,
                recv: right_stream,
                sink_closed: right_closed,
            },
    } = tincans();
    repo_handle_1.new_remote_repo(repo_handle_2.get_repo_id().clone(), left_stream, left_sink);
    repo_handle_2.new_remote_repo(
        repo_handle_1.get_repo_id().clone(),
        right_stream,
        right_sink,
    );

    // Request the document.
    let doc_handle_future = repo_handle_2.request_document(document_handle_1.document_id());
    let doc_id = doc_handle_future.await.unwrap().document_id();
    assert_eq!(doc_id, document_handle_1.document_id());

    // Replace the peers.
    let TinCans {
        left:
            TinCan {
                send: left_sink,
                recv: left_stream,
                sink_closed: new_left_closed,
            },
        right:
            TinCan {
                send: right_sink,
                recv: right_stream,
                sink_closed: new_right_closed,
            },
    } = tincans();
    repo_handle_1.new_remote_repo(repo_handle_2.get_repo_id().clone(), left_stream, left_sink);
    repo_handle_2.new_remote_repo(
        repo_handle_1.get_repo_id().clone(),
        right_stream,
        right_sink,
    );

    // Stop the repos.
    tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
        repo_handle_2.stop().unwrap();
    })
    .await
    .unwrap();

    assert!(left_closed.load(std::sync::atomic::Ordering::Acquire));
    assert!(right_closed.load(std::sync::atomic::Ordering::Acquire));
    assert!(new_left_closed.load(std::sync::atomic::Ordering::Acquire));
    assert!(new_right_closed.load(std::sync::atomic::Ordering::Acquire));
}

#[test(tokio::test)]
async fn test_streams_chained_on_replacement() {
    // Create two repos.
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));

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

    let TinCan {
        send: left_sink,
        recv: left_stream,
        sink_closed: old_left_closed,
    } = tincan_to_nowhere();
    repo_handle_1.new_remote_repo(repo_handle_2.get_repo_id().clone(), left_stream, left_sink);

    let TinCan {
        send: right_sink,
        recv: right_stream,
        sink_closed: old_right_closed,
    } = tincan_to_nowhere();
    repo_handle_2.new_remote_repo(
        repo_handle_1.get_repo_id().clone(),
        right_stream,
        right_sink,
    );

    // Request the document.
    let doc_handle_future =
        tokio::spawn(repo_handle_2.request_document(document_handle_1.document_id()));

    // Replace the peers.
    let TinCans {
        left:
            TinCan {
                send: left_sink,
                recv: left_stream,
                sink_closed: new_left_closed,
            },
        right:
            TinCan {
                send: right_sink,
                recv: right_stream,
                sink_closed: new_right_closed,
            },
    } = tincans();
    repo_handle_1.new_remote_repo(repo_handle_2.get_repo_id().clone(), left_stream, left_sink);
    repo_handle_2.new_remote_repo(
        repo_handle_1.get_repo_id().clone(),
        right_stream,
        right_sink,
    );

    let doc_id = doc_handle_future.await.unwrap().unwrap().document_id();
    assert_eq!(doc_id, document_handle_1.document_id());

    // Stop the repos.
    tokio::task::spawn_blocking(|| {
        repo_handle_1.stop().unwrap();
        repo_handle_2.stop().unwrap();
    })
    .await
    .unwrap();

    assert!(old_left_closed.load(std::sync::atomic::Ordering::Acquire));
    assert!(old_right_closed.load(std::sync::atomic::Ordering::Acquire));
    assert!(new_left_closed.load(std::sync::atomic::Ordering::Acquire));
    assert!(new_right_closed.load(std::sync::atomic::Ordering::Acquire));
}

#[test(tokio::test)]
async fn sync_with_unauthorized_peer_never_occurs() {
    let repo_handle_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage))
        .with_share_policy(Box::new(|repo: &RepoId, _: &DocumentId| {
            if repo == &RepoId::from("repo2") {
                ShareDecision::DontShare
            } else {
                ShareDecision::Share
            }
        }))
        .run();
    let repo_handle_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage)).run();
    let repo_handle_3 = Repo::new(Some("repo3".to_string()), Box::new(SimpleStorage)).run();

    connect_repos(&repo_handle_1, &repo_handle_2);
    connect_repos(&repo_handle_1, &repo_handle_3);

    let doc_handle_1 = repo_handle_1.new_document();
    doc_handle_1.with_doc_mut(|doc| {
        let mut tx = doc.transaction();
        tx.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        tx.commit();
    });

    let doc_handle_2 = repo_handle_2.request_document(doc_handle_1.document_id());
    tokio::time::timeout(Duration::from_secs(1), doc_handle_2)
        .await
        .expect_err("doc_handle_2 should never resolve");

    let doc_handle_3 = repo_handle_3.request_document(doc_handle_1.document_id());
    tokio::time::timeout(Duration::from_secs(1), doc_handle_3)
        .await
        .expect("doc_handle_3 should resolve")
        .expect("doc_handle_3 should resolve to a document");
}
