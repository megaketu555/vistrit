//! Coordinator network server.
//!
//! Handles incoming connections from workers and clients,
//! processes protocol messages, and coordinates task distribution.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};
use tracing::{debug, error, info, warn};

use vistrit_core::{NodeId, NodeInfo, NodeState, Task, Result, VistritError};
use vistrit_protocol::{
    VistritCodec, Message, PROTOCOL_VERSION,
    messages::*,
};

use crate::registry::WorkerRegistry;
use crate::scheduler::{Scheduler, SchedulingStrategy};

/// Channel for sending tasks to workers
type TaskSender = mpsc::Sender<Task>;

/// Map of worker ID to their task channel
type WorkerChannels = Arc<RwLock<HashMap<NodeId, TaskSender>>>;

/// Coordinator server configuration
pub struct CoordinatorConfig {
    pub bind_address: SocketAddr,
    pub node_name: String,
    pub heartbeat_timeout: Duration,
    pub task_timeout: Duration,
    pub scheduling_strategy: SchedulingStrategy,
}

/// Coordinator server
pub struct CoordinatorServer {
    config: CoordinatorConfig,
    node_info: NodeInfo,
    registry: Arc<WorkerRegistry>,
    scheduler: Arc<Scheduler>,
    worker_channels: WorkerChannels,
    shutdown_tx: broadcast::Sender<()>,
}

impl CoordinatorServer {
    /// Create a new coordinator server
    pub fn new(
        bind_address: SocketAddr,
        node_name: String,
        heartbeat_timeout_secs: u64,
        task_timeout_secs: u64,
    ) -> Self {
        let config = CoordinatorConfig {
            bind_address,
            node_name: node_name.clone(),
            heartbeat_timeout: Duration::from_secs(heartbeat_timeout_secs),
            task_timeout: Duration::from_secs(task_timeout_secs),
            scheduling_strategy: SchedulingStrategy::LeastLoaded,
        };
        
        let node_info = NodeInfo::new_coordinator(bind_address)
            .with_name(node_name);
        
        let registry = Arc::new(WorkerRegistry::new(config.heartbeat_timeout));
        let scheduler = Arc::new(Scheduler::new(config.scheduling_strategy));
        let worker_channels = Arc::new(RwLock::new(HashMap::new()));
        
        let (shutdown_tx, _) = broadcast::channel(1);
        
        Self {
            config,
            node_info,
            registry,
            scheduler,
            worker_channels,
            shutdown_tx,
        }
    }
    
    /// Run the coordinator server
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.config.bind_address).await?;
        info!("Coordinator listening on {}", self.config.bind_address);
        
        // Spawn background tasks
        let registry = Arc::clone(&self.registry);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        
        // Heartbeat check task
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        registry.check_stale_workers();
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });
        
        // Task dispatcher - schedules tasks and sends to workers
        let registry = Arc::clone(&self.registry);
        let scheduler = Arc::clone(&self.scheduler);
        let worker_channels = Arc::clone(&self.worker_channels);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let assignments = scheduler.schedule(&registry);
                        for (task, worker_id) in assignments {
                            // Send task to the worker
                            let channels = worker_channels.read().await;
                            if let Some(tx) = channels.get(&worker_id) {
                                if let Err(e) = tx.send(task.clone()).await {
                                    warn!(
                                        task_id = %task.id,
                                        worker_id = %worker_id,
                                        error = %e,
                                        "Failed to dispatch task to worker"
                                    );
                                } else {
                                    info!(
                                        task_id = %task.id,
                                        worker_id = %worker_id,
                                        "Task dispatched to worker"
                                    );
                                }
                            } else {
                                warn!(
                                    task_id = %task.id,
                                    worker_id = %worker_id,
                                    "Worker channel not found"
                                );
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });
        
        // Accept connections
        let node_info = Arc::new(self.node_info);
        let registry = Arc::clone(&self.registry);
        let scheduler = Arc::clone(&self.scheduler);
        let worker_channels = Arc::clone(&self.worker_channels);
        
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            info!(peer = %peer_addr, "New connection");
            
            let node_info = Arc::clone(&node_info);
            let registry = Arc::clone(&registry);
            let scheduler = Arc::clone(&scheduler);
            let worker_channels = Arc::clone(&worker_channels);
            
            tokio::spawn(async move {
                if let Err(e) = handle_connection(
                    stream,
                    peer_addr,
                    node_info,
                    registry,
                    scheduler,
                    worker_channels,
                ).await {
                    error!(peer = %peer_addr, error = %e, "Connection error");
                }
            });
        }
    }
}

/// Handle a single connection
async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    coordinator_info: Arc<NodeInfo>,
    registry: Arc<WorkerRegistry>,
    scheduler: Arc<Scheduler>,
    worker_channels: WorkerChannels,
) -> Result<()> {
    let mut framed = Framed::new(stream, VistritCodec::new());
    let mut node_id: Option<NodeId> = None;
    
    // Create channel for receiving tasks to send to this worker
    let (task_tx, mut task_rx) = mpsc::channel::<Task>(32);
    
    loop {
        tokio::select! {
            // Handle incoming messages
            result = framed.next() => {
                match result {
                    Some(Ok(message)) => {
                        debug!(peer = %peer_addr, msg_type = ?message.message_type(), "Received message");
                        
                        let response = process_message(
                            message,
                            &coordinator_info,
                            &registry,
                            &scheduler,
                            &mut node_id,
                            &task_tx,
                            &worker_channels,
                        ).await;
                        
                        if let Some(resp) = response {
                            if let Err(e) = framed.send(resp).await {
                                error!(peer = %peer_addr, error = %e, "Failed to send response");
                                break;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        error!(peer = %peer_addr, error = %e, "Protocol error");
                        
                        // Send error response
                        let error_msg = Message::Error(ErrorMessage::new(
                            ErrorCode::Protocol,
                            e.to_string(),
                        ));
                        let _ = framed.send(error_msg).await;
                        break;
                    }
                    None => {
                        // Connection closed
                        info!(peer = %peer_addr, "Connection closed");
                        break;
                    }
                }
            }
            
            // Send tasks to this worker
            Some(task) = task_rx.recv() => {
                info!(
                    task_id = %task.id,
                    task_name = %task.name,
                    "Sending TaskAssign to worker"
                );
                
                let assign_msg = Message::TaskAssign(TaskAssignMessage { task });
                if let Err(e) = framed.send(assign_msg).await {
                    error!(peer = %peer_addr, error = %e, "Failed to send task assignment");
                    break;
                }
            }
        }
    }
    
    // Clean up on disconnect
    if let Some(id) = node_id {
        // Remove worker channel
        worker_channels.write().await.remove(&id);
        registry.unregister(&id);
        info!(node_id = %id, "Node disconnected and cleaned up");
    }
    
    Ok(())
}

/// Process a single message and return optional response
async fn process_message(
    message: Message,
    coordinator_info: &NodeInfo,
    registry: &WorkerRegistry,
    scheduler: &Scheduler,
    node_id: &mut Option<NodeId>,
    task_tx: &TaskSender,
    worker_channels: &WorkerChannels,
) -> Option<Message> {
    match message {
        // ─────────────────────────────────────────────────────────────────────
        // Connection Messages
        // ─────────────────────────────────────────────────────────────────────
        
        Message::Handshake(msg) => {
            info!(
                node_type = ?msg.node_info.node_type,
                protocol_version = msg.protocol_version,
                "Handshake received"
            );
            
            // Version check
            if msg.protocol_version != PROTOCOL_VERSION {
                return Some(Message::HandshakeAck(HandshakeAckMessage {
                    accepted: false,
                    node_id: msg.node_info.id,
                    coordinator_info: None,
                    reason: Some(format!(
                        "Protocol version mismatch: expected {}, got {}",
                        PROTOCOL_VERSION, msg.protocol_version
                    )),
                }));
            }
            
            // Register worker
            let id = registry.register(msg.node_info);
            *node_id = Some(id);
            
            // Register channel for this worker
            worker_channels.write().await.insert(id, task_tx.clone());
            
            // Update state to ready
            registry.update_state(&id, NodeState::Ready);
            
            info!(node_id = %id, "Worker registered and ready");
            
            Some(Message::HandshakeAck(HandshakeAckMessage {
                accepted: true,
                node_id: id,
                coordinator_info: Some(coordinator_info.clone()),
                reason: None,
            }))
        }
        
        Message::Heartbeat(msg) => {
            if registry.heartbeat(&msg.node_id) {
                registry.update_state(&msg.node_id, msg.state);
                registry.update_running_tasks(&msg.node_id, msg.running_tasks);
                
                Some(Message::HeartbeatAck(HeartbeatAckMessage {
                    coordinator_id: coordinator_info.id,
                    echo_timestamp: msg.timestamp,
                    coordinator_timestamp: chrono::Utc::now().timestamp(),
                }))
            } else {
                Some(Message::Error(ErrorMessage::new(
                    ErrorCode::WorkerNotFound,
                    "Unknown worker",
                ).with_entity(msg.node_id.to_string())))
            }
        }
        
        Message::Disconnect(msg) => {
            info!(node_id = %msg.node_id, reason = %msg.reason, graceful = msg.graceful, "Disconnect");
            worker_channels.write().await.remove(&msg.node_id);
            registry.unregister(&msg.node_id);
            None // No response needed
        }
        
        // ─────────────────────────────────────────────────────────────────────
        // Task Messages
        // ─────────────────────────────────────────────────────────────────────
        
        Message::TaskSubmit(msg) => {
            let task_id = scheduler.submit(msg.task);
            info!(task_id = %task_id, "Task submitted to scheduler");
            
            Some(Message::TaskSubmitAck(TaskSubmitAckMessage {
                request_id: msg.request_id,
                accepted: true,
                task_id: Some(task_id),
                reason: None,
            }))
        }
        
        Message::TaskAssignAck(msg) => {
            if msg.accepted {
                scheduler.mark_running(&msg.task_id, msg.worker_id);
                info!(task_id = %msg.task_id, worker_id = %msg.worker_id, "Task now running");
            } else {
                warn!(
                    task_id = %msg.task_id,
                    worker_id = %msg.worker_id,
                    reason = ?msg.reason,
                    "Task assignment rejected - will reschedule"
                );
                // Task will be rescheduled automatically
            }
            None
        }
        
        Message::TaskStatus(msg) => {
            if let Some(progress) = msg.progress {
                scheduler.update_progress(&msg.task_id, progress);
            }
            None
        }
        
        Message::TaskResult(msg) => {
            info!(
                task_id = %msg.result.task_id,
                success = msg.result.success,
                duration_ms = msg.result.duration_ms,
                "Task result received"
            );
            scheduler.complete(msg.result);
            None
        }
        
        Message::TaskCancel(msg) => {
            let cancelled = scheduler.cancel(&msg.task_id);
            if !cancelled {
                return Some(Message::Error(ErrorMessage::new(
                    ErrorCode::TaskNotFound,
                    "Task not found or already completed",
                ).with_entity(msg.task_id.to_string())));
            }
            None
        }
        
        Message::TaskQuery(msg) => {
            let task = scheduler.get_task(&msg.task_id);
            let result = scheduler.get_result(&msg.task_id);
            
            Some(Message::TaskQueryResponse(TaskQueryResponseMessage {
                task_id: msg.task_id,
                found: task.is_some(),
                task,
                result,
            }))
        }
        
        // ─────────────────────────────────────────────────────────────────────
        // Cluster Messages
        // ─────────────────────────────────────────────────────────────────────
        
        Message::ClusterStatusRequest(msg) => {
            Some(Message::ClusterStatusResponse(ClusterStatusResponseMessage {
                request_id: msg.request_id,
                leader_id: Some(coordinator_info.id),
                total_nodes: registry.worker_count() as u32 + 1, // +1 for coordinator
                online_workers: registry.online_worker_count() as u32,
                pending_tasks: scheduler.pending_count() as u32,
                running_tasks: scheduler.running_count() as u32,
                completed_tasks: scheduler.total_completed.load(std::sync::atomic::Ordering::Relaxed),
                failed_tasks: scheduler.total_failed.load(std::sync::atomic::Ordering::Relaxed),
                uptime_seconds: 0, // TODO: track uptime
            }))
        }
        
        Message::WorkerListRequest(msg) => {
            Some(Message::WorkerListResponse(WorkerListResponseMessage {
                request_id: msg.request_id,
                workers: registry.get_all(),
            }))
        }
        
        // ─────────────────────────────────────────────────────────────────────
        // Unknown/Unhandled Messages
        // ─────────────────────────────────────────────────────────────────────
        
        _ => {
            warn!(msg_type = ?message.message_type(), "Unhandled message type");
            Some(Message::Error(ErrorMessage::new(
                ErrorCode::InvalidRequest,
                "Unhandled message type",
            )))
        }
    }
}
