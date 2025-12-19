//! Vistrit Client API

use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};
use tracing::{debug, info};
use uuid::Uuid;

use vistrit_core::{NodeId, NodeInfo, Task, TaskId, TaskResult, Result, VistritError};
use vistrit_protocol::{
    VistritCodec, Message, PROTOCOL_VERSION,
    messages::*,
};

/// Vistrit client for submitting tasks and querying the cluster
pub struct VistritClient {
    coordinator_addr: SocketAddr,
    timeout: Duration,
}

impl VistritClient {
    /// Create a new client
    pub fn new(coordinator_addr: SocketAddr) -> Self {
        Self {
            coordinator_addr,
            timeout: Duration::from_secs(30),
        }
    }
    
    /// Set request timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    
    /// Connect to coordinator and execute a request
    async fn request(&self, message: Message) -> Result<Message> {
        let stream = tokio::time::timeout(
            self.timeout,
            TcpStream::connect(self.coordinator_addr),
        )
        .await
        .map_err(|_| VistritError::Timeout(self.timeout.as_secs()))?
        .map_err(VistritError::Io)?;
        
        // Get local address before moving stream
        let local_addr = stream.local_addr().unwrap_or(self.coordinator_addr);
        let mut framed = Framed::new(stream, VistritCodec::new());
        
        // Create temporary node info for handshake
        let node_info = NodeInfo::new_worker(local_addr);
        
        // Handshake first
        let handshake = Message::Handshake(HandshakeMessage {
            node_info,
            protocol_version: PROTOCOL_VERSION,
        });
        
        framed.send(handshake).await.map_err(|e| {
            VistritError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;
        
        // Wait for handshake ack
        let response = framed.next().await
            .ok_or_else(|| VistritError::ConnectionClosed)?
            .map_err(|_| VistritError::ConnectionClosed)?;
        
        if !matches!(response, Message::HandshakeAck(ref ack) if ack.accepted) {
            return Err(VistritError::ConnectionFailed("Handshake rejected".to_string()));
        }
        
        // Send actual request
        framed.send(message).await.map_err(|e| {
            VistritError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;
        
        // Wait for response
        let response = tokio::time::timeout(
            self.timeout,
            framed.next(),
        )
        .await
        .map_err(|_| VistritError::Timeout(self.timeout.as_secs()))?
        .ok_or_else(|| VistritError::ConnectionClosed)?
        .map_err(|_| VistritError::ConnectionClosed)?;
        
        Ok(response)
    }
    
    /// Submit a task
    pub async fn submit_task(&self, task: Task) -> Result<TaskId> {
        let request_id = Uuid::new_v4();
        
        let message = Message::TaskSubmit(TaskSubmitMessage {
            task,
            request_id,
        });
        
        let response = self.request(message).await?;
        
        match response {
            Message::TaskSubmitAck(ack) => {
                if ack.accepted {
                    Ok(ack.task_id.unwrap())
                } else {
                    Err(VistritError::TaskExecutionFailed(
                        ack.reason.unwrap_or_default()
                    ))
                }
            }
            Message::Error(err) => {
                Err(VistritError::TaskExecutionFailed(err.message))
            }
            _ => Err(VistritError::Internal("Unexpected response".to_string())),
        }
    }
    
    /// Query task status
    pub async fn query_task(&self, task_id: TaskId) -> Result<(Option<Task>, Option<TaskResult>)> {
        let message = Message::TaskQuery(TaskQueryMessage { task_id });
        
        let response = self.request(message).await?;
        
        match response {
            Message::TaskQueryResponse(resp) => {
                Ok((resp.task, resp.result))
            }
            Message::Error(err) => {
                Err(VistritError::TaskNotFound(err.message))
            }
            _ => Err(VistritError::Internal("Unexpected response".to_string())),
        }
    }
    
    /// Get cluster status
    pub async fn cluster_status(&self) -> Result<ClusterStatusResponseMessage> {
        let request_id = Uuid::new_v4();
        let message = Message::ClusterStatusRequest(ClusterStatusRequestMessage { request_id });
        
        let response = self.request(message).await?;
        
        match response {
            Message::ClusterStatusResponse(status) => Ok(status),
            Message::Error(err) => {
                Err(VistritError::Internal(err.message))
            }
            _ => Err(VistritError::Internal("Unexpected response".to_string())),
        }
    }
    
    /// Get list of workers
    pub async fn list_workers(&self) -> Result<Vec<NodeInfo>> {
        let request_id = Uuid::new_v4();
        let message = Message::WorkerListRequest(WorkerListRequestMessage { request_id });
        
        let response = self.request(message).await?;
        
        match response {
            Message::WorkerListResponse(resp) => Ok(resp.workers),
            Message::Error(err) => {
                Err(VistritError::Internal(err.message))
            }
            _ => Err(VistritError::Internal("Unexpected response".to_string())),
        }
    }
    
    /// Cancel a task
    pub async fn cancel_task(&self, task_id: TaskId) -> Result<()> {
        let message = Message::TaskCancel(TaskCancelMessage {
            task_id,
            requested_by: NodeId::new(),
            reason: Some("Cancelled by client".to_string()),
        });
        
        let response = self.request(message).await?;
        
        match response {
            Message::Error(err) if err.code == ErrorCode::TaskNotFound => {
                Err(VistritError::TaskNotFound(task_id.to_string()))
            }
            Message::Error(err) => {
                Err(VistritError::Internal(err.message))
            }
            _ => Ok(()),
        }
    }
}
