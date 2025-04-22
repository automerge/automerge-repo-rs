use std::time::Duration;

use automerge_repo::{ConnFinishedReason, Repo};
use test_log::test;
use test_utils::storage_utils::SimpleStorage;

use crate::{connect_repos, tincans::Connected};

#[test(tokio::test)]
async fn conn_complete_future_resolves_on_disconnect() {
    // Create two repos.
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));

    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();

    let Connected {
        left_complete,
        right_complete,
    } = connect_repos(&repo_handle_1, &repo_handle_2);

    // shutdown the right repo
    let right_shutdown = tokio::spawn(async move { repo_handle_2.stop() });
    let (left_result, right_result, right_shutdown) = tokio::time::timeout(
        Duration::from_millis(100),
        futures::future::join3(left_complete, right_complete, right_shutdown),
    )
    .await
    .expect("timed out");
    right_shutdown
        .expect("failed to shutdown right repo")
        .expect("failed to shutdown right repo");

    // Now, check that the right repo ConnComplete resolves with a disconnect error
    assert_eq!(left_result, ConnFinishedReason::TheyDisconnected);
    assert_eq!(right_result, ConnFinishedReason::RepoShutdown);
}

#[test(tokio::test)]
async fn conn_complete_future_resolves_on_duplicate_connection() {
    // Create two repos.
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));

    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();

    let Connected {
        left_complete,
        right_complete,
    } = connect_repos(&repo_handle_1, &repo_handle_2);

    connect_repos(&repo_handle_1, &repo_handle_2);

    // Now reconnect the left repo to the right repo, we should get disconnect errors for the previous two conn_complete futures
    let (left_result, right_result) = tokio::time::timeout(
        Duration::from_millis(100),
        futures::future::join(left_complete, right_complete),
    )
    .await
    .expect("timed out");

    // Now, check that we get errors on the connection futures
    // there are three possible results
    // * left gets the duplicate connection and terminates the connection before right
    // * right gets the duplicate connection and terminates the connection before left
    // * both left and right get the duplicate connection and terminate the connection simultaneously
    match (left_result, right_result) {
        (ConnFinishedReason::DuplicateConnection, ConnFinishedReason::TheyDisconnected) => {}
        (ConnFinishedReason::TheyDisconnected, ConnFinishedReason::DuplicateConnection) => {}
        (ConnFinishedReason::DuplicateConnection, ConnFinishedReason::DuplicateConnection) => {}
        (left, right) => panic!("Unexpected result, left: {:?}, right: {:?}", left, right),
    }
}
