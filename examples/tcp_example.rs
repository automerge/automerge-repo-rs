use clap::Parser;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;

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

    // Spawn a listening task.
    Handle::current().spawn(async move {
        let listener = TcpListener::bind(run_ip).await.unwrap();
        loop {
            match listener.accept().await {
                Ok((_socket, addr)) => println!("new client: {:?}", addr),
                Err(e) => println!("couldn't get client: {:?}", e),
            }
        }
    });

    // Spawn a task connecting to the other peer.
    Handle::current().spawn(async move {
        let mut stream = loop {
            // Try to connect to a peer
            let res = TcpStream::connect(other_ip.clone()).await;
            if res.is_err() {
                continue;
            }
            break res.unwrap();
        };
    });
}
