use clap::Parser;
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

mod http_sync_channel;
mod peer_server;
mod relay_server;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
enum Command {
    RunRelay {
        #[arg(long)]
        ip: SocketAddr,
    },
    RunPeer {
        #[arg(long)]
        ip: SocketAddr,
        #[arg(long)]
        relay_ip: SocketAddr,
    },
}

#[tokio::main]
async fn main() {
    setup_logging();
    let args = Args::parse();
    match args.command {
        Command::RunRelay { ip } => relay_server::run_relay(&ip).await,
        Command::RunPeer { ip, relay_ip } => peer_server::run_peer(&ip, &relay_ip).await,
    }
}

fn setup_logging() {
    let filter = EnvFilter::builder()
        .from_env()
        .unwrap()
        //.add_directive("automerge_repo=trace".parse().unwrap())
        .add_directive("tower_http::trace=trace".parse().unwrap())
        .add_directive("http_server_with_relay=info".parse().unwrap());
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
