extern crate test_utils;

use automerge::transaction::Transactable;
use automerge::ReadDoc;
use automerge_repo::{NetworkEvent, NetworkMessage, Repo};
use std::collections::HashMap;
use test_utils::network_utils::Network;
use test_utils::storage_utils::SimpleStorage;
use tokio::sync::mpsc::channel;

#[test]
fn test_document_changed_over_sync() {
    // Create two repos.
    let repo_1 = Repo::new(None, Box::new(SimpleStorage));
    let repo_2 = Repo::new(None, Box::new(SimpleStorage));

    // Run the repos in the background.
    let mut repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();
    let repo_handle_2_clone = repo_handle_2.clone();
    let expected_repo_id = repo_handle_2.get_repo_id().clone();

    // Create a document for one repo.
    let mut document_handle_1 = repo_handle_1.new_document();

    // Add network adapters
    let mut peers = HashMap::new();
    let (sender, mut network_receiver) = channel(1);
    let network_1 = Network::new(sender.clone());
    let network_2 = Network::new(sender.clone());
    repo_handle_1.new_network_adapter(
        repo_handle_2.get_repo_id().clone(),
        Box::new(network_1.clone()),
    );
    repo_handle_2.new_network_adapter(
        repo_handle_1.get_repo_id().clone(),
        Box::new(network_2.clone()),
    );
    peers.insert(repo_handle_2.get_repo_id().clone(), network_1);
    peers.insert(repo_handle_1.get_repo_id().clone(), network_2);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Spawn a task that awaits the requested doc handle,
    // and then edits the document.
    let doc_id = document_handle_1.document_id();
    rt.spawn(async move {
        // Request the document.
        let mut doc_handle = repo_handle_2.request_document(doc_id).await.unwrap();
        doc_handle.with_doc_mut(|doc| {
            doc.put(
                automerge::ROOT,
                "repo_id",
                format!("{}", repo_handle_2.get_repo_id()),
            )
            .expect("Failed to change the document.");
            doc.commit();
        });
        repo_handle_2.stop().unwrap();
    });

    // Spawn a task that awaits the document change.
    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    let repo_id = repo_handle_1.get_repo_id().clone();
    rt.spawn(async move {
        // Edit the document.
        document_handle_1.with_doc_mut(|doc| {
            doc.put(automerge::ROOT, "repo_id", format!("{}", repo_id))
                .expect("Failed to change the document.");
            doc.commit();
        });
        loop {
            // Await changes until the edit comes through over sync.
            document_handle_1.changed().await.unwrap();
            let equals = document_handle_1.with_doc(|doc| {
                let val = doc
                    .get(automerge::ROOT, "repo_id")
                    .expect("Failed to read the document.")
                    .unwrap();
                val.0.to_str().clone().unwrap() == format!("{}", expected_repo_id)
            });
            if equals {
                done_sync_sender.send(()).await.unwrap();
            }
        }
    });

    rt.spawn(async move {
        // A router of network messages.
        loop {
            tokio::select! {
               msg = network_receiver.recv() => {
                   let (_from_repo_id, to_repo_id) = msg.unwrap();
                   let incoming = {
                       let peer = peers.get_mut(&to_repo_id).unwrap();
                       peer.take_outgoing()
                   };
                   match incoming {
                       NetworkMessage::Sync {
                           from_repo_id,
                           to_repo_id,
                           document_id,
                           message,
                       } => {
                           let peer = peers.get_mut(&from_repo_id).unwrap();
                           peer.receive_incoming(NetworkEvent::Sync {
                               from_repo_id,
                               to_repo_id,
                               document_id,
                               message,
                           });
                       }
                   }
               },
            }
        }
    });

    done_sync_receiver.blocking_recv().unwrap();

    // Stop the repo.
    repo_handle_1.stop().unwrap();
    repo_handle_2_clone.stop().unwrap();
}

#[test]
fn test_document_changed_locally() {
    // Create two repos.
    let repo_1 = Repo::new(None, Box::new(SimpleStorage));

    // Run the repo in the background.
    let mut repo_handle_1 = repo_1.run();
    let expected_repo_id = repo_handle_1.get_repo_id().clone();

    // Create a document for the repo.
    let mut document_handle_1 = repo_handle_1.new_document();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Spawn a task that awaits the document change.
    let (done_sender, mut done_receiver) = channel(1);
    let (start_wait_sender, mut start_wait_receiver) = channel(1);
    let expected = expected_repo_id.clone();
    let mut doc_handle = document_handle_1.clone();
    rt.spawn(async move {
        start_wait_sender.send(()).await.unwrap();
        // Await the local change.
        doc_handle.changed().await.unwrap();
        doc_handle.with_doc(|doc| {
            let val = doc
                .get(automerge::ROOT, "repo_id")
                .expect("Failed to read the document.")
                .unwrap();
            assert_eq!(val.0.to_str().clone().unwrap(), format!("{}", expected));
        });
        done_sender.send(()).await.unwrap();
    });

    start_wait_receiver.blocking_recv().unwrap();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        doc.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", expected_repo_id.clone()),
        )
        .expect("Failed to change the document.");
        doc.commit();
    });

    done_receiver.blocking_recv().unwrap();

    // Stop the repo.
    repo_handle_1.stop().unwrap();
}
