//! Worker client for connecting to the coordinator.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};
use tracing::{debug, error, info, warn};

use vistrit_core::{NodeId, NodeInfo, NodeState, Result, VistritError, Task, TaskResult};
use vistrit_protocol::{
    VistritCodec, Message, PROTOCOL_VERSION,
    messages::*,
};

use crate::executor::TaskExecutor;
use crate::heartbeat::HeartbeatService;

/// Worker client
pub struct WorkerClient {
    coordinator_addr: SocketAddr,
    bind_addr: SocketAddr,
    node_name: String,
    max_concurrent_tasks: u32,
    heartbeat_interval: Duration,
}

impl WorkerClient {
    /// Create a new worker client
    pub fn new(
        coordinator_addr: SocketAddr,
        bind_addr: SocketAddr,
        node_name: String,
        max_concurrent_tasks: u32,
        heartbeat_interval_secs: u64,
    ) -> Self {
        Self {
            coordinator_addr,
            bind_addr,
            node_name,
            max_concurrent_tasks,
            heartbeat_interval: Duration::from_secs(heartbeat_interval_secs),
        }
    }
    
    /// Run the worker
    pub async fn run(self) -> Result<()> {
        loop {
            match self.connect_and_run().await {
                Ok(()) => {
                    info!("Worker shutdown gracefully");
                    break;
                }
                Err(e) => {
                    error!(error = %e, "Connection error, reconnecting in 5 seconds...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
        
        Ok(())
    }
    
    /// Connect to coordinator and run
    async fn connect_and_run(&self) -> Result<()> {
        info!(coordinator = %self.coordinator_addr, "Connecting to coordinator");
        
        let stream = TcpStream::connect(self.coordinator_addr).await?;
        let local_addr = stream.local_addr()?;
        let mut framed = Framed::new(stream, VistritCodec::new());
        
        // Create node info
        let mut node_info = NodeInfo::new_worker(local_addr)
            .with_name(&self.node_name);
        node_info.capabilities.max_concurrent_tasks = self.max_concurrent_tasks;
        
        // Send handshake
        info!("Sending handshake");
        let handshake = Message::Handshake(HandshakeMessage {
            node_info: node_info.clone(),
            protocol_version: PROTOCOL_VERSION,
        });
        framed.send(handshake).await.map_err(|e| {
            VistritError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;
        
        // Wait for handshake ack
        let response = framed.next().await
            .ok_or_else(|| VistritError::ConnectionClosed)?
            .map_err(|e| VistritError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
        
        let (node_id, coordinator_info) = match response {
            Message::HandshakeAck(ack) => {
                if !ack.accepted {
                    return Err(VistritError::RegistrationFailed(
                        ack.reason.unwrap_or_default()
                    ));
                }
                info!(node_id = %ack.node_id, "Registered with coordinator");
                (ack.node_id, ack.coordinator_info)
            }
            _ => {
                return Err(VistritError::RegistrationFailed(
                    "Unexpected response to handshake".to_string()
                ));
            }
        };
        
        // Shared state
        let running_tasks = Arc::new(AtomicU32::new(0));
        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        let (result_tx, mut result_rx) = mpsc::channel::<TaskResult>(32);
        
        // Start heartbeat service
        let heartbeat = HeartbeatService::new(
            node_id,
            self.heartbeat_interval,
            Arc::clone(&running_tasks),
        );
        
        // Start task executor
        let executor = TaskExecutor::new(
            node_id,
            self.max_concurrent_tasks,
            Arc::clone(&running_tasks),
            result_tx,
        );
        
        // Split the framed connection
        let (mut sink, mut stream) = framed.split();
        
        // Heartbeat sender task
        let heartbeat_interval = self.heartbeat_interval;
        let running_tasks_clone = Arc::clone(&running_tasks);
        let node_id_clone = node_id;
        let mut shutdown_rx = shutdown_tx.subscribe();
        
        let heartbeat_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let msg = Message::Heartbeat(HeartbeatMessage {
                            node_id: node_id_clone,
                            timestamp: chrono::Utc::now().timestamp(),
                            running_tasks: running_tasks_clone.load(Ordering::Relaxed),
                            state: NodeState::Ready,
                            cpu_usage: None,
                            memory_usage: None,
                        });
                        // We can't send here directly, need to use a channel
                        // This is simplified - in production we'd use proper channels
                        debug!("Heartbeat tick");
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });
        
        // Message handling loop
        let executor = Arc::new(executor);
        let mut heartbeat_interval = tokio::time::interval(self.heartbeat_interval);
        
        loop {
            tokio::select! {
                // Send heartbeat
                _ = heartbeat_interval.tick() => {
                    let msg = Message::Heartbeat(HeartbeatMessage {
                        node_id,
                        timestamp: chrono::Utc::now().timestamp(),
                        running_tasks: running_tasks.load(Ordering::Relaxed),
                        state: NodeState::Ready,
                        cpu_usage: None,
                        memory_usage: None,
                    });
                    
                    if let Err(e) = sink.send(msg).await {
                        error!(error = %e, "Failed to send heartbeat");
                        break;
                    }
                    debug!("Heartbeat sent");
                }
                
                // Send task results
                Some(result) = result_rx.recv() => {
                    let msg = Message::TaskResult(TaskResultMessage { result });
                    if let Err(e) = sink.send(msg).await {
                        error!(error = %e, "Failed to send task result");
                        break;
                    }
                }
                
                // Receive messages
                Some(result) = stream.next() => {
                    match result {
                        Ok(message) => {
                            debug!(msg_type = ?message.message_type(), "Received message");
                            
                            match message {
                                Message::HeartbeatAck(ack) => {
                                    let latency = chrono::Utc::now().timestamp() - ack.echo_timestamp;
                                    debug!(latency_ms = latency * 1000, "Heartbeat ack received");
                                }
                                
                                Message::TaskAssign(msg) => {
                                    info!(task_id = %msg.task.id, task_name = %msg.task.name, "Task assigned");
                                    
                                    let can_accept = running_tasks.load(Ordering::Relaxed) 
                                        < self.max_concurrent_tasks;
                                    
                                    if can_accept {
                                        // Accept task
                                        let ack = Message::TaskAssignAck(TaskAssignAckMessage {
                                            task_id: msg.task.id,
                                            accepted: true,
                                            worker_id: node_id,
                                            reason: None,
                                        });
                                        if let Err(e) = sink.send(ack).await {
                                            error!(error = %e, "Failed to send task ack");
                                        }
                                        
                                        // Execute task
                                        executor.execute(msg.task).await;
                                    } else {
                                        // Reject task - at capacity
                                        let ack = Message::TaskAssignAck(TaskAssignAckMessage {
                                            task_id: msg.task.id,
                                            accepted: false,
                                            worker_id: node_id,
                                            reason: Some("Worker at capacity".to_string()),
                                        });
                                        if let Err(e) = sink.send(ack).await {
                                            error!(error = %e, "Failed to send task rejection");
                                        }
                                    }
                                }
                                
                                Message::TaskCancel(msg) => {
                                    info!(task_id = %msg.task_id, "Task cancellation requested");
                                    executor.cancel(&msg.task_id);
                                }
                                
                                Message::Error(err) => {
                                    warn!(code = ?err.code, message = %err.message, "Error from coordinator");
                                }
                                
                                Message::Disconnect(msg) => {
                                    info!(reason = %msg.reason, "Coordinator requested disconnect");
                                    break;
                                }
                                
                                _ => {
                                    debug!(msg_type = ?message.message_type(), "Ignoring message");
                                }
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "Protocol error");
                            break;
                        }
                    }
                }
            }
        }
        
        // Send disconnect message
        let disconnect = Message::Disconnect(DisconnectMessage {
            node_id,
            reason: "Worker shutting down".to_string(),
            graceful: true,
        });
        let _ = sink.send(disconnect).await;
        
        // Signal shutdown
        let _ = shutdown_tx.send(());
        
        Ok(())
    }
}
