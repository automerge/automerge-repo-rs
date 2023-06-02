use automerge::transaction::Transactable;
use automerge_repo::{ConnDirection, Repo, StorageAdapter};
use clap::Parser;
use futures::FutureExt;
use tokio::net::{TcpListener, TcpStream};

struct Storage;

impl StorageAdapter for Storage {}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    run_ip: Option<String>,
    #[arg(long)]
    other_ip: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let run_ip = args.run_ip;
    let other_ip = args.other_ip;

    // Create a repo.
    let repo = Repo::new(None, Box::new(Storage));
    let mut repo_handle = repo.run();
    let repo_handle_clone = repo_handle.clone();

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

    if let Some(run_ip) = run_ip {
        // Start a server.
        let listener = TcpListener::bind(run_ip).await.unwrap();
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    repo_handle
                        .connect_tokio_io(addr, socket, ConnDirection::Incoming)
                        .await
                        .unwrap();
                }
                Err(e) => println!("couldn't get client: {:?}", e),
            }
        }
    } else {
        // Start a client.
        // Spawn a task connecting to the other peer.
        let other_ip = other_ip.unwrap();
        let stream = loop {
            // Try to connect to a peer
            let res = TcpStream::connect(other_ip.clone()).await;
            if res.is_err() {
                continue;
            }
            break res.unwrap();
        };
        repo_handle
            .connect_tokio_io(other_ip, stream, ConnDirection::Outgoing)
            .await
            .unwrap();
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c().fuse() => {
            repo_handle_clone.stop().unwrap();
            println!("Stopped");
        }
    }
}
