//! Vistrit CLI
//!
//! Command-line interface for interacting with the Vistrit
//! distributed computing platform.

use clap::{Parser, Subcommand};
use colored::Colorize;
use std::net::SocketAddr;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use vistrit_client::VistritClient;
use vistrit_core::{Task, TaskPriority};

/// Vistrit CLI - Distributed Computing Platform
#[derive(Parser, Debug)]
#[command(name = "vistrit")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Coordinator address
    #[arg(short, long, default_value = "127.0.0.1:7878")]
    coordinator: SocketAddr,
    
    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
    
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Submit a task to the cluster
    Submit {
        /// Task name
        #[arg(short, long)]
        name: String,
        
        /// Command to execute
        #[arg(short, long)]
        command: Option<String>,
        
        /// Expression to compute
        #[arg(short, long)]
        expression: Option<String>,
        
        /// Priority (low, normal, high, critical)
        #[arg(short, long, default_value = "normal")]
        priority: String,
        
        /// Timeout in seconds
        #[arg(short, long, default_value = "300")]
        timeout: u64,
    },
    
    /// Query task status
    Status {
        /// Task ID
        task_id: String,
    },
    
    /// Get cluster status
    Cluster,
    
    /// List workers
    Workers,
    
    /// Cancel a task
    Cancel {
        /// Task ID
        task_id: String,
    },
}

fn print_banner() {
    println!("{}", r#"
 ██╗   ██╗██╗███████╗████████╗██████╗ ██╗████████╗
 ██║   ██║██║██╔════╝╚══██╔══╝██╔══██╗██║╚══██╔══╝
 ██║   ██║██║███████╗   ██║   ██████╔╝██║   ██║   
 ╚██╗ ██╔╝██║╚════██║   ██║   ██╔══██╗██║   ██║   
  ╚████╔╝ ██║███████║   ██║   ██║  ██║██║   ██║   
   ╚═══╝  ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝   ╚═╝   
"#.cyan());
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    
    // Setup logging
    if args.verbose {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .with_target(false)
            .init();
    }
    
    let client = VistritClient::new(args.coordinator);
    
    match args.command {
        Commands::Submit {
            name,
            command,
            expression,
            priority,
            timeout,
        } => {
            print_banner();
            println!("{}", "📤 Submitting task...".yellow());
            
            let task = if let Some(cmd) = command {
                Task::new_command(&name, cmd)
            } else if let Some(expr) = expression {
                Task::new_compute(&name, expr)
            } else {
                eprintln!("{}", "Error: Must specify --command or --expression".red());
                return Ok(());
            };
            
            let priority = match priority.to_lowercase().as_str() {
                "low" => TaskPriority::Low,
                "normal" => TaskPriority::Normal,
                "high" => TaskPriority::High,
                "critical" => TaskPriority::Critical,
                _ => TaskPriority::Normal,
            };
            
            let task = task
                .with_priority(priority)
                .with_timeout(timeout);
            
            match client.submit_task(task).await {
                Ok(task_id) => {
                    println!("{} Task submitted successfully!", "✅".green());
                    println!("   Task ID: {}", task_id.to_string().cyan());
                }
                Err(e) => {
                    eprintln!("{} Failed to submit task: {}", "❌".red(), e);
                }
            }
        }
        
        Commands::Status { task_id } => {
            print_banner();
            println!("{}", "🔍 Querying task status...".yellow());
            
            let task_id = vistrit_core::TaskId::parse(&task_id)
                .map_err(|_| anyhow::anyhow!("Invalid task ID"))?;
            
            match client.query_task(task_id).await {
                Ok((task, result)) => {
                    if let Some(t) = task {
                        println!("\n{}", "Task Information".bold());
                        println!("  Name:     {}", t.name);
                        println!("  State:    {}", format!("{}", t.state).cyan());
                        println!("  Priority: {}", t.priority);
                        println!("  Created:  {}", t.created_at);
                    } else {
                        println!("{}", "Task not found".red());
                    }
                    
                    if let Some(r) = result {
                        println!("\n{}", "Result".bold());
                        if r.success {
                            println!("  Status:   {}", "SUCCESS".green());
                        } else {
                            println!("  Status:   {}", "FAILED".red());
                            if let Some(err) = &r.error {
                                println!("  Error:    {}", err.red());
                            }
                        }
                        println!("  Duration: {}ms", r.duration_ms);
                        
                        if let Some(data) = &r.data {
                            if let Ok(text) = String::from_utf8(data.clone()) {
                                println!("  Output:   {}", text.trim());
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{} Failed to query task: {}", "❌".red(), e);
                }
            }
        }
        
        Commands::Cluster => {
            print_banner();
            println!("{}", "📊 Cluster Status".yellow().bold());
            
            match client.cluster_status().await {
                Ok(status) => {
                    println!();
                    println!("  {} Total Nodes:      {}", "🖥️".cyan(), status.total_nodes);
                    println!("  {} Online Workers:   {}", "✅".green(), status.online_workers);
                    println!("  {} Pending Tasks:    {}", "⏳".yellow(), status.pending_tasks);
                    println!("  {} Running Tasks:    {}", "🔄".blue(), status.running_tasks);
                    println!("  {} Completed Tasks:  {}", "✔️".green(), status.completed_tasks);
                    println!("  {} Failed Tasks:     {}", "❌".red(), status.failed_tasks);
                    println!();
                }
                Err(e) => {
                    eprintln!("{} Failed to get cluster status: {}", "❌".red(), e);
                }
            }
        }
        
        Commands::Workers => {
            print_banner();
            println!("{}", "👷 Worker List".yellow().bold());
            
            match client.list_workers().await {
                Ok(workers) => {
                    println!();
                    if workers.is_empty() {
                        println!("  No workers connected");
                    } else {
                        println!("  {:<12} {:<20} {:<10} {:<8} {}", 
                            "ID", "Address", "State", "Tasks", "Capacity");
                        println!("  {}", "-".repeat(60));
                        
                        for worker in workers {
                            let state_color = match worker.state {
                                vistrit_core::NodeState::Ready => "Ready".green(),
                                vistrit_core::NodeState::Busy => "Busy".yellow(),
                                vistrit_core::NodeState::Offline => "Offline".red(),
                                _ => worker.state.to_string().normal(),
                            };
                            
                            println!("  {:<12} {:<20} {:<10} {:<8} {}", 
                                worker.id.to_string(),
                                worker.address.to_string(),
                                state_color,
                                worker.running_tasks,
                                worker.capabilities.max_concurrent_tasks);
                        }
                    }
                    println!();
                }
                Err(e) => {
                    eprintln!("{} Failed to list workers: {}", "❌".red(), e);
                }
            }
        }
        
        Commands::Cancel { task_id } => {
            print_banner();
            println!("{}", "🛑 Cancelling task...".yellow());
            
            let task_id = vistrit_core::TaskId::parse(&task_id)
                .map_err(|_| anyhow::anyhow!("Invalid task ID"))?;
            
            match client.cancel_task(task_id).await {
                Ok(()) => {
                    println!("{} Task cancelled successfully!", "✅".green());
                }
                Err(e) => {
                    eprintln!("{} Failed to cancel task: {}", "❌".red(), e);
                }
            }
        }
    }
    
    Ok(())
}
