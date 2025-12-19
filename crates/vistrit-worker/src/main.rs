//! Vistrit Worker Node
//!
//! The worker node connects to a coordinator and executes tasks.
//! It maintains a heartbeat connection and reports task progress.

mod client;
mod executor;
mod heartbeat;

use clap::Parser;
use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// Vistrit Worker - Task execution node
#[derive(Parser, Debug)]
#[command(name = "vistrit-worker")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Coordinator address to connect to
    #[arg(short, long)]
    coordinator: SocketAddr,
    
    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
    
    /// Node name for identification
    #[arg(short, long)]
    name: Option<String>,
    
    /// Maximum concurrent tasks
    #[arg(long, default_value = "4")]
    max_tasks: u32,
    
    /// Heartbeat interval in seconds
    #[arg(long, default_value = "5")]
    heartbeat_interval: u64,
    
    /// Bind address for worker (for direct communication)
    #[arg(short, long, default_value = "0.0.0.0:0")]
    bind: SocketAddr,
}

fn print_banner() {
    println!(r#"
 ██╗   ██╗██╗███████╗████████╗██████╗ ██╗████████╗
 ██║   ██║██║██╔════╝╚══██╔══╝██╔══██╗██║╚══██╔══╝
 ██║   ██║██║███████╗   ██║   ██████╔╝██║   ██║   
 ╚██╗ ██╔╝██║╚════██║   ██║   ██╔══██╗██║   ██║   
  ╚████╔╝ ██║███████║   ██║   ██║  ██║██║   ██║   
   ╚═══╝  ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝   ╚═╝   
                                                   
    Distributed Computing Platform - Worker
    "#);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    
    // Setup logging
    let log_level = if args.verbose { Level::DEBUG } else { Level::INFO };
    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(false)
        .with_thread_ids(true)
        .init();
    
    print_banner();
    
    let node_name = args.name.unwrap_or_else(|| {
        format!("worker-{}", &uuid::Uuid::new_v4().to_string()[..8])
    });
    
    info!("Starting Vistrit Worker");
    info!("  Node name: {}", node_name);
    info!("  Coordinator: {}", args.coordinator);
    info!("  Max concurrent tasks: {}", args.max_tasks);
    info!("  Heartbeat interval: {}s", args.heartbeat_interval);
    
    // Create and run worker
    let worker = client::WorkerClient::new(
        args.coordinator,
        args.bind,
        node_name,
        args.max_tasks,
        args.heartbeat_interval,
    );
    
    worker.run().await?;
    
    Ok(())
}
