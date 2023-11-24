use std::time::Duration;

use automerge_repo::Repo;
use test_log::test;
use test_utils::storage_utils::SimpleStorage;
use tokio_stream::StreamExt;

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
        repo_handle_2
            .request_document(doc_1_repo_1.document_id())
    )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let msg_on_repo_2 = tokio::spawn(async move { doc_1_repo_2.ephemera().next().await });

    doc_1_repo_1
        .broadcast_ephemeral(vec![1, 2, 3])
        .await
        .unwrap();

    let msg_on_repo_2 = tokio::time::timeout(Duration::from_millis(100), msg_on_repo_2)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(msg_on_repo_2.as_ref(), &[1, 2, 3]);
}
