use std::time::Duration;

use automerge_repo::{Repo, RepoError};
use futures::StreamExt;
use test_log::test;
use test_utils::storage_utils::SimpleStorage;

use crate::tincans::connect_repos;

#[test(tokio::test)]
async fn ephemeral_messages_sent_to_directly_connected_peers() {
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));

    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();

    connect_repos(&repo_handle_1, &repo_handle_2);

    let doc_1_repo_1 = repo_handle_1.new_document().await;

    let doc_1_repo_2 = tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_2.request_document(doc_1_repo_1.document_id()),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();

    let msg_on_repo_2 = tokio::spawn(async move { doc_1_repo_2.ephemera().await.next().await });

    doc_1_repo_1.broadcast_ephemeral(vec![1, 2, 3]).unwrap();

    let msg_on_repo_2 = tokio::time::timeout(Duration::from_millis(100), msg_on_repo_2)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(msg_on_repo_2.bytes(), &[1, 2, 3]);
}

#[test(tokio::test)]
async fn ephemeral_messages_are_forwarded_to_connected_peers() {
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));
    let repo_3 = Repo::new(Some("repo3".to_string()), Box::new(SimpleStorage));

    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();
    let repo_handle_3 = repo_3.run();

    connect_repos(&repo_handle_1, &repo_handle_2);
    connect_repos(&repo_handle_2, &repo_handle_3);

    let doc_1_repo_1 = repo_handle_1.new_document().await;

    let doc_1_repo_3 = tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_3.request_document(doc_1_repo_1.document_id()),
    )
    .await
    .expect("timed out getting handle on repo 3")
    .unwrap()
    .unwrap();

    let mut ephemera = doc_1_repo_3.ephemera().await;

    doc_1_repo_1.broadcast_ephemeral(vec![1, 2, 3]).unwrap();

    let msg_on_repo_3 =
        tokio::time::timeout(
            Duration::from_millis(100),
            async move { ephemera.next().await },
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(msg_on_repo_3.bytes(), &[1, 2, 3]);

    tokio::task::spawn_blocking(move || {
        repo_handle_1.stop()?;
        repo_handle_2.stop()?;
        Ok::<_, RepoError>(())
    })
    .await
    .unwrap()
    .unwrap();
}

#[test(tokio::test)]
async fn forwarded_messages_do_not_loop() {
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));
    let repo_3 = Repo::new(Some("repo3".to_string()), Box::new(SimpleStorage));

    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();
    let repo_handle_3 = repo_3.run();

    connect_repos(&repo_handle_1, &repo_handle_2);
    connect_repos(&repo_handle_2, &repo_handle_3);
    connect_repos(&repo_handle_1, &repo_handle_3);

    let doc_1_repo_1 = repo_handle_1.new_document().await;

    let doc_1_repo_3 = tokio::time::timeout(
        Duration::from_millis(100),
        repo_handle_3.request_document(doc_1_repo_1.document_id()),
    )
    .await
    .expect("timed out getting handle on repo 3")
    .unwrap()
    .unwrap();

    let mut ephemera = doc_1_repo_3.ephemera().await;
    let ephemera2 = doc_1_repo_3.ephemera().await;

    let msg_on_repo_3 = tokio::spawn(async move { ephemera.next().await });

    doc_1_repo_1.broadcast_ephemeral(vec![1, 2, 3]).unwrap();

    let _msg_on_repo_3 = tokio::time::timeout(Duration::from_millis(100), msg_on_repo_3)
        .await
        .expect("first message should not be none");

    // receive all messages for 100ms
    let messages = ephemera2.take_until(tokio::time::sleep(Duration::from_millis(100))).collect::<Vec<_>>().await;

    // there should be either 1 or 2 messages depending on which route the message took
    assert!(messages.len() == 1 || messages.len() == 2);


    tokio::task::spawn_blocking(move || {
        repo_handle_1.stop()?;
        repo_handle_2.stop()?;
        repo_handle_3.stop()?;
        Ok::<_, RepoError>(())
    })
    .await
    .unwrap()
    .unwrap();
}
