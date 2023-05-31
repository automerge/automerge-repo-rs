use automerge::sync::Message as SyncMessage;
use automerge::transaction::Transactable;
use automerge::ReadDoc;
use automerge_repo::{
    DocumentId, NetworkAdapter, NetworkError, RepoMessage, Repo, RepoId,
    StorageAdapter,
};
use clap::Parser;
use core::pin::Pin;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::task::{Context, Poll, Waker};
use futures::FutureExt;
use futures::SinkExt;
use futures::TryStreamExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{channel, Sender};
use tokio_serde::formats::*;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};


struct Storage;

impl StorageAdapter for Storage {}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    run_ip: String,
    #[arg(long)]
    other_ip: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let run_ip = args.run_ip;
    let other_ip = args.other_ip;

    // Create a repo, the id is the local ip.
    let repo = Repo::new(
        Some(run_ip.clone()),
        Box::new(Storage),
    );
    let mut repo_handle = repo.run();

    // Create a document.
    {
        let mut doc_handle = repo_handle.new_document();
        let our_id = repo_handle.get_repo_id();
        doc_handle.with_doc_mut(|doc| {
            doc.put(automerge::ROOT, "repo_id", format!("{}", our_id))
                .expect("Failed to change the document.");
            doc.commit();
        });
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c().fuse() => {
            
            repo_handle.stop();
            println!("Stopped");
        }
    }
}
