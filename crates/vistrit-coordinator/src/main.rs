//! Vistrit Coordinator Node
//!
//! The coordinator is the central orchestration component of the Vistrit
//! distributed computing platform. It manages worker registration, task
//! scheduling, and result aggregation.

mod registry;
mod scheduler;
mod server;

use clap::Parser;
use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// Vistrit Coordinator - Central orchestration node
#[derive(Parser, Debug)]
#[command(name = "vistrit-coordinator")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Address to bind the coordinator server
    #[arg(short, long, default_value = "0.0.0.0:7878")]
    bind: SocketAddr,
    
    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
    
    /// Node name for identification
    #[arg(short, long, default_value = "coordinator-1")]
    name: String,
    
    /// Heartbeat timeout in seconds
    #[arg(long, default_value = "30")]
    heartbeat_timeout: u64,
    
    /// Task timeout in seconds (default)
    #[arg(long, default_value = "300")]
    task_timeout: u64,
}

fn print_banner() {
    println!(r#"
 ██╗   ██╗██╗███████╗████████╗██████╗ ██╗████████╗
 ██║   ██║██║██╔════╝╚══██╔══╝██╔══██╗██║╚══██╔══╝
 ██║   ██║██║███████╗   ██║   ██████╔╝██║   ██║   
 ╚██╗ ██╔╝██║╚════██║   ██║   ██╔══██╗██║   ██║   
  ╚████╔╝ ██║███████║   ██║   ██║  ██║██║   ██║   
   ╚═══╝  ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝   ╚═╝   
                                                   
    Distributed Computing Platform - Coordinator
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
    
    info!("Starting Vistrit Coordinator");
    info!("  Node name: {}", args.name);
    info!("  Bind address: {}", args.bind);
    info!("  Heartbeat timeout: {}s", args.heartbeat_timeout);
    info!("  Task timeout: {}s", args.task_timeout);
    
    // Create and run server
    let server = server::CoordinatorServer::new(
        args.bind,
        args.name,
        args.heartbeat_timeout,
        args.task_timeout,
    );
    
    server.run().await?;
    
    Ok(())
}
