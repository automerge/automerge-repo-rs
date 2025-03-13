use std::{collections::HashMap, time::Duration};

use automerge::transaction::Transactable as _;
use automerge_repo::{PeerConnectionInfo, Repo};
use futures::StreamExt as _;
use test_log::test;
use test_utils::storage_utils::SimpleStorage;

use crate::connect_repos;

#[test(tokio::test)]
async fn peer_conn_info_updated_on_sync() {
    // Create two repos.
    let repo_1 = Repo::new(Some("repo1".to_string()), Box::new(SimpleStorage));
    let repo_2 = Repo::new(Some("repo2".to_string()), Box::new(SimpleStorage));

    // Run the repos in the background.
    let repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();
    connect_repos(&repo_handle_1, &repo_handle_2);

    // check state on repo1 for repo2 is empty
    let mut states = repo_handle_1.peer_conn_info_changes(repo_handle_2.get_repo_id().clone());

    let first_state = states.next().await.expect("never received first state");
    assert_eq!(
        first_state,
        PeerConnectionInfo {
            last_sent: None,
            last_received: None,
            docs: HashMap::new(),
        }
    );

    // Now, create a doc
    let doc_on_1 = repo_handle_1.new_document();
    doc_on_1
        .with_doc_mut(|d| {
            d.transact::<_, _, automerge::AutomergeError>(|tx| {
                tx.put(automerge::ROOT, "foo", "bar")
            })
        })
        .unwrap();

    let next_state = tokio::time::timeout(Duration::from_millis(100), states.next())
        .await
        .expect("timed out waiting for next state")
        .expect("never received next doc state");
    let last_sent = next_state.last_sent;
    assert!(last_sent.is_some());
    assert!(next_state.last_received.is_none());
    let doc_state = next_state
        .docs
        .get(&doc_on_1.document_id())
        .expect("doc state should be in peer conn info state");
    assert_eq!(doc_state.last_sent, last_sent);

    // Wait a bit for sync
    tokio::time::sleep(Duration::from_millis(100)).await;

    let next_state = tokio::time::timeout(Duration::from_millis(100), states.next())
        .await
        .expect("timed out waiting for next state")
        .expect("never received next doc state");
    let last_received = next_state.last_received;
    assert!(last_received.is_some());
    let doc_state = next_state
        .docs
        .get(&doc_on_1.document_id())
        .expect("doc state should be in peer conn info state");
    assert_eq!(doc_state.last_received, last_received);

    let mut states_on_2 = repo_handle_2.peer_conn_info_changes(repo_handle_1.get_repo_id().clone());
    let next_state_on_2 = tokio::time::timeout(Duration::from_millis(100), states_on_2.next())
        .await
        .expect("timed out waiting for next state")
        .expect("never received next doc state");
    let last_received_on_2 = next_state_on_2.last_received;
    let last_sent_on_2 = next_state_on_2.last_sent;
    assert!(last_received_on_2.is_some());
    assert!(last_sent_on_2.is_some());
    let doc_state_on_2 = next_state_on_2
        .docs
        .get(&doc_on_1.document_id())
        .expect("doc state should be in peer conn info state");
    assert_eq!(doc_state_on_2.last_received, last_received_on_2);
    assert_eq!(doc_state_on_2.last_sent, last_sent_on_2);
}
