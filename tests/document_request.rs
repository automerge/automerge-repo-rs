mod common;

use automerge::transaction::Transactable;
use automerge_repo::{NetworkEvent, NetworkMessage, Repo, StorageAdapter};
use common::network_utils::Network;
use std::collections::HashMap;
use tokio::sync::mpsc::channel;

struct Storage;

impl StorageAdapter for Storage {}

#[test]
fn test_requesting_document_connected_peers() {
    // Create two repos.
    let repo_1 = Repo::new(None, Box::new(Storage));
    let repo_2 = Repo::new(None, Box::new(Storage));

    // Run the repos in the background.
    let mut repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();

    // Create a document for one repo.
    let mut document_handle_1 = repo_handle_1.new_document();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        doc.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        doc.commit();
    });

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

    // Request the document.
    let repo_handle_future = repo_handle_2.request_document(document_handle_1.document_id());

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Spawn a task that awaits the requested doc handle.
    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        let _doc_handle = repo_handle_future.await;
        done_sync_sender.send(()).await.unwrap();
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

    // Stop the repos.
    repo_handle_1.stop().unwrap();
    repo_handle_2.stop().unwrap();
}

#[test]
fn test_requesting_document_unconnected_peers() {
    // Create two repos.
    let repo_1 = Repo::new(None, Box::new(Storage));
    let repo_2 = Repo::new(None, Box::new(Storage));

    // Run the repos in the background.
    let mut repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();

    // Create a document for one repo.
    let mut document_handle_1 = repo_handle_1.new_document();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        doc.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        doc.commit();
    });

    // Note: requesting the document while peers aren't connected yet.

    // Request the document.
    let doc_handle_future = repo_handle_2.request_document(document_handle_1.document_id());

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

    // Spawn a task that awaits the requested doc handle.
    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    rt.spawn(async move {
        let _doc_handle = doc_handle_future.await;
        done_sync_sender.send(()).await.unwrap();
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

    // Stop the repos.
    repo_handle_1.stop().unwrap();
    repo_handle_2.stop().unwrap();
}

#[test]
fn test_request_with_repo_stop() {
    // Create two repos.
    let repo_1 = Repo::new(None, Box::new(Storage));
    let repo_2 = Repo::new(None, Box::new(Storage));

    // Run the repos in the background.
    let mut repo_handle_1 = repo_1.run();
    let repo_handle_2 = repo_2.run();

    // Create a document for one repo.
    let mut document_handle_1 = repo_handle_1.new_document();

    // Edit the document.
    document_handle_1.with_doc_mut(|doc| {
        doc.put(
            automerge::ROOT,
            "repo_id",
            format!("{}", repo_handle_1.get_repo_id()),
        )
        .expect("Failed to change the document.");
        doc.commit();
    });

    // Note: requesting the document while peers aren't connected yet.

    // Request the document.
    let doc_handle_future = repo_handle_2.request_document(document_handle_1.document_id());

    // Stop the repos.
    repo_handle_1.stop().unwrap();
    repo_handle_2.stop().unwrap();

    // Spawn a task that awaits the requested doc handle.
    let (done_sync_sender, mut done_sync_receiver) = channel(1);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.spawn(async move {
        // Since the repo is stopping, the future should error.
        if doc_handle_future.await.is_err() {
            done_sync_sender.send(()).await.unwrap();
        }
    });

    done_sync_receiver.blocking_recv().unwrap();
}
