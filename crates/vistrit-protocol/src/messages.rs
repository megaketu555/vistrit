//! Protocol message definitions.
//!
//! All messages exchanged in the Vistrit protocol are defined here.
//! Messages are serialized using bincode for efficiency.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

// Re-export core types for convenience
pub use vistrit_core::{
    NodeId, NodeInfo, NodeType, NodeCapabilities, NodeState,
    TaskId, Task, TaskState, TaskResult, TaskPriority,
};

/// Message type identifiers for the protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MessageType {
    // ─────────────────────────────────────────────────────────────────────────
    // Connection Messages (0x01-0x0F)
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Initial handshake
    Handshake = 0x01,
    /// Handshake acknowledgment
    HandshakeAck = 0x02,
    /// Heartbeat ping
    Heartbeat = 0x03,
    /// Heartbeat response
    HeartbeatAck = 0x04,
    /// Graceful disconnect
    Disconnect = 0x05,
    
    // ─────────────────────────────────────────────────────────────────────────
    // Task Messages (0x10-0x1F)
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Submit a new task
    TaskSubmit = 0x10,
    /// Task submission acknowledgment
    TaskSubmitAck = 0x11,
    /// Assign task to worker
    TaskAssign = 0x12,
    /// Task assignment acknowledgment
    TaskAssignAck = 0x13,
    /// Task status update
    TaskStatus = 0x14,
    /// Task result
    TaskResult = 0x15,
    /// Cancel a task
    TaskCancel = 0x16,
    /// Query task status
    TaskQuery = 0x17,
    /// Task query response
    TaskQueryResponse = 0x18,
    
    // ─────────────────────────────────────────────────────────────────────────
    // Cluster Messages (0x20-0x2F)
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Request cluster status
    ClusterStatusRequest = 0x20,
    /// Cluster status response
    ClusterStatusResponse = 0x21,
    /// Worker list request
    WorkerListRequest = 0x22,
    /// Worker list response
    WorkerListResponse = 0x23,
    
    // ─────────────────────────────────────────────────────────────────────────
    // Leader Election Messages (0x30-0x3F)
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Election start
    ElectionStart = 0x30,
    /// Election vote
    ElectionVote = 0x31,
    /// Leader announcement
    LeaderAnnounce = 0x32,
    
    // ─────────────────────────────────────────────────────────────────────────
    // Error Messages (0xE0-0xEF)
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Error response
    Error = 0xE0,
}

impl MessageType {
    /// Convert from u8
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x01 => Some(MessageType::Handshake),
            0x02 => Some(MessageType::HandshakeAck),
            0x03 => Some(MessageType::Heartbeat),
            0x04 => Some(MessageType::HeartbeatAck),
            0x05 => Some(MessageType::Disconnect),
            0x10 => Some(MessageType::TaskSubmit),
            0x11 => Some(MessageType::TaskSubmitAck),
            0x12 => Some(MessageType::TaskAssign),
            0x13 => Some(MessageType::TaskAssignAck),
            0x14 => Some(MessageType::TaskStatus),
            0x15 => Some(MessageType::TaskResult),
            0x16 => Some(MessageType::TaskCancel),
            0x17 => Some(MessageType::TaskQuery),
            0x18 => Some(MessageType::TaskQueryResponse),
            0x20 => Some(MessageType::ClusterStatusRequest),
            0x21 => Some(MessageType::ClusterStatusResponse),
            0x22 => Some(MessageType::WorkerListRequest),
            0x23 => Some(MessageType::WorkerListResponse),
            0x30 => Some(MessageType::ElectionStart),
            0x31 => Some(MessageType::ElectionVote),
            0x32 => Some(MessageType::LeaderAnnounce),
            0xE0 => Some(MessageType::Error),
            _ => None,
        }
    }
    
    /// Convert to u8
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

/// All protocol messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    // ─────────────────────────────────────────────────────────────────────────
    // Connection Messages
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Initial handshake from node
    Handshake(HandshakeMessage),
    
    /// Handshake acknowledgment
    HandshakeAck(HandshakeAckMessage),
    
    /// Heartbeat ping
    Heartbeat(HeartbeatMessage),
    
    /// Heartbeat acknowledgment
    HeartbeatAck(HeartbeatAckMessage),
    
    /// Graceful disconnect notification
    Disconnect(DisconnectMessage),
    
    // ─────────────────────────────────────────────────────────────────────────
    // Task Messages
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Submit a task
    TaskSubmit(TaskSubmitMessage),
    
    /// Task submission acknowledgment
    TaskSubmitAck(TaskSubmitAckMessage),
    
    /// Assign task to worker
    TaskAssign(TaskAssignMessage),
    
    /// Task assignment acknowledgment
    TaskAssignAck(TaskAssignAckMessage),
    
    /// Task status update
    TaskStatus(TaskStatusMessage),
    
    /// Task result
    TaskResult(TaskResultMessage),
    
    /// Cancel a task
    TaskCancel(TaskCancelMessage),
    
    /// Query task status
    TaskQuery(TaskQueryMessage),
    
    /// Task query response
    TaskQueryResponse(TaskQueryResponseMessage),
    
    // ─────────────────────────────────────────────────────────────────────────
    // Cluster Messages
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Request cluster status
    ClusterStatusRequest(ClusterStatusRequestMessage),
    
    /// Cluster status response
    ClusterStatusResponse(ClusterStatusResponseMessage),
    
    /// Request worker list
    WorkerListRequest(WorkerListRequestMessage),
    
    /// Worker list response
    WorkerListResponse(WorkerListResponseMessage),
    
    // ─────────────────────────────────────────────────────────────────────────
    // Leader Election
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Start election
    ElectionStart(ElectionStartMessage),
    
    /// Election vote
    ElectionVote(ElectionVoteMessage),
    
    /// Leader announcement
    LeaderAnnounce(LeaderAnnounceMessage),
    
    // ─────────────────────────────────────────────────────────────────────────
    // Error
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Error response
    Error(ErrorMessage),
}

impl Message {
    /// Get the message type
    pub fn message_type(&self) -> MessageType {
        match self {
            Message::Handshake(_) => MessageType::Handshake,
            Message::HandshakeAck(_) => MessageType::HandshakeAck,
            Message::Heartbeat(_) => MessageType::Heartbeat,
            Message::HeartbeatAck(_) => MessageType::HeartbeatAck,
            Message::Disconnect(_) => MessageType::Disconnect,
            Message::TaskSubmit(_) => MessageType::TaskSubmit,
            Message::TaskSubmitAck(_) => MessageType::TaskSubmitAck,
            Message::TaskAssign(_) => MessageType::TaskAssign,
            Message::TaskAssignAck(_) => MessageType::TaskAssignAck,
            Message::TaskStatus(_) => MessageType::TaskStatus,
            Message::TaskResult(_) => MessageType::TaskResult,
            Message::TaskCancel(_) => MessageType::TaskCancel,
            Message::TaskQuery(_) => MessageType::TaskQuery,
            Message::TaskQueryResponse(_) => MessageType::TaskQueryResponse,
            Message::ClusterStatusRequest(_) => MessageType::ClusterStatusRequest,
            Message::ClusterStatusResponse(_) => MessageType::ClusterStatusResponse,
            Message::WorkerListRequest(_) => MessageType::WorkerListRequest,
            Message::WorkerListResponse(_) => MessageType::WorkerListResponse,
            Message::ElectionStart(_) => MessageType::ElectionStart,
            Message::ElectionVote(_) => MessageType::ElectionVote,
            Message::LeaderAnnounce(_) => MessageType::LeaderAnnounce,
            Message::Error(_) => MessageType::Error,
        }
    }
    
    /// Check if this is an acknowledgment message
    pub fn is_ack(&self) -> bool {
        matches!(
            self,
            Message::HandshakeAck(_) 
            | Message::HeartbeatAck(_) 
            | Message::TaskSubmitAck(_)
            | Message::TaskAssignAck(_)
        )
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Connection Messages
// ═══════════════════════════════════════════════════════════════════════════

/// Handshake message sent when a node connects
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeMessage {
    /// Node information
    pub node_info: NodeInfo,
    /// Protocol version the node supports
    pub protocol_version: u8,
}

/// Handshake acknowledgment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeAckMessage {
    /// Whether handshake was accepted
    pub accepted: bool,
    /// Assigned node ID (if different from proposed)
    pub node_id: NodeId,
    /// Coordinator's node info
    pub coordinator_info: Option<NodeInfo>,
    /// Rejection reason (if not accepted)
    pub reason: Option<String>,
}

/// Heartbeat message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    /// Node sending the heartbeat
    pub node_id: NodeId,
    /// Current timestamp
    pub timestamp: i64,
    /// Number of running tasks
    pub running_tasks: u32,
    /// Current node state
    pub state: NodeState,
    /// CPU usage percentage (0-100)
    pub cpu_usage: Option<u8>,
    /// Memory usage percentage (0-100)
    pub memory_usage: Option<u8>,
}

/// Heartbeat acknowledgment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatAckMessage {
    /// Coordinator node ID
    pub coordinator_id: NodeId,
    /// Timestamp from the original heartbeat
    pub echo_timestamp: i64,
    /// Coordinator's timestamp
    pub coordinator_timestamp: i64,
}

/// Disconnect notification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisconnectMessage {
    /// Node disconnecting
    pub node_id: NodeId,
    /// Reason for disconnect
    pub reason: String,
    /// Whether this is a graceful shutdown
    pub graceful: bool,
}

// ═══════════════════════════════════════════════════════════════════════════
// Task Messages
// ═══════════════════════════════════════════════════════════════════════════

/// Submit a new task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSubmitMessage {
    /// The task to submit
    pub task: Task,
    /// Request ID for tracking
    pub request_id: Uuid,
}

/// Task submission acknowledgment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSubmitAckMessage {
    /// Request ID from submission
    pub request_id: Uuid,
    /// Whether task was accepted
    pub accepted: bool,
    /// Assigned task ID
    pub task_id: Option<TaskId>,
    /// Rejection reason
    pub reason: Option<String>,
}

/// Assign a task to a worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAssignMessage {
    /// The task to execute
    pub task: Task,
}

/// Task assignment acknowledgment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAssignAckMessage {
    /// Task ID
    pub task_id: TaskId,
    /// Whether assignment was accepted
    pub accepted: bool,
    /// Worker node ID
    pub worker_id: NodeId,
    /// Rejection reason
    pub reason: Option<String>,
}

/// Task status update from worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusMessage {
    /// Task ID
    pub task_id: TaskId,
    /// Worker executing the task
    pub worker_id: NodeId,
    /// Current state
    pub state: TaskState,
    /// Progress percentage (0-100)
    pub progress: Option<u8>,
    /// Status message
    pub message: Option<String>,
}

/// Task result from worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResultMessage {
    /// The task result
    pub result: TaskResult,
}

/// Cancel a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCancelMessage {
    /// Task to cancel
    pub task_id: TaskId,
    /// Who requested cancellation
    pub requested_by: NodeId,
    /// Reason for cancellation
    pub reason: Option<String>,
}

/// Query task status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskQueryMessage {
    /// Task to query
    pub task_id: TaskId,
}

/// Task query response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskQueryResponseMessage {
    /// Task ID
    pub task_id: TaskId,
    /// Whether task was found
    pub found: bool,
    /// Task info (if found)
    pub task: Option<Task>,
    /// Result (if completed)
    pub result: Option<TaskResult>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Cluster Messages
// ═══════════════════════════════════════════════════════════════════════════

/// Request cluster status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatusRequestMessage {
    /// Request ID
    pub request_id: Uuid,
}

/// Cluster status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatusResponseMessage {
    /// Request ID
    pub request_id: Uuid,
    /// Current leader
    pub leader_id: Option<NodeId>,
    /// Total nodes in cluster
    pub total_nodes: u32,
    /// Online workers
    pub online_workers: u32,
    /// Pending tasks
    pub pending_tasks: u32,
    /// Running tasks
    pub running_tasks: u32,
    /// Completed tasks (total)
    pub completed_tasks: u64,
    /// Failed tasks (total)
    pub failed_tasks: u64,
    /// Cluster uptime in seconds
    pub uptime_seconds: u64,
}

/// Request worker list
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerListRequestMessage {
    /// Request ID
    pub request_id: Uuid,
}

/// Worker list response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerListResponseMessage {
    /// Request ID
    pub request_id: Uuid,
    /// List of workers
    pub workers: Vec<NodeInfo>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Leader Election Messages
// ═══════════════════════════════════════════════════════════════════════════

/// Start an election
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectionStartMessage {
    /// Election term/round
    pub term: u64,
    /// Candidate node ID
    pub candidate_id: NodeId,
    /// Candidate's priority (higher = more preferred)
    pub priority: u32,
}

/// Election vote
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectionVoteMessage {
    /// Election term
    pub term: u64,
    /// Voter's node ID
    pub voter_id: NodeId,
    /// Candidate voted for
    pub vote_for: NodeId,
}

/// Leader announcement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderAnnounceMessage {
    /// Election term
    pub term: u64,
    /// New leader's node ID
    pub leader_id: NodeId,
    /// Leader's address
    pub leader_address: SocketAddr,
}

// ═══════════════════════════════════════════════════════════════════════════
// Error Message
// ═══════════════════════════════════════════════════════════════════════════

/// Error code categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u16)]
pub enum ErrorCode {
    /// Unknown error
    Unknown = 0,
    /// Protocol error
    Protocol = 100,
    /// Authentication failed
    AuthFailed = 101,
    /// Permission denied
    PermissionDenied = 102,
    /// Resource not found
    NotFound = 200,
    /// Task not found
    TaskNotFound = 201,
    /// Worker not found
    WorkerNotFound = 202,
    /// Invalid request
    InvalidRequest = 300,
    /// Internal server error
    Internal = 500,
    /// Service unavailable
    Unavailable = 503,
}

/// Error response message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMessage {
    /// Error code
    pub code: ErrorCode,
    /// Human-readable message
    pub message: String,
    /// Related entity ID (task, node, etc.)
    pub entity_id: Option<String>,
    /// Additional details
    pub details: Option<String>,
}

impl ErrorMessage {
    /// Create a new error message
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            entity_id: None,
            details: None,
        }
    }
    
    /// Add entity ID
    pub fn with_entity(mut self, id: impl Into<String>) -> Self {
        self.entity_id = Some(id.into());
        self
    }
    
    /// Add details
    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    
    fn test_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7878)
    }
    
    #[test]
    fn test_message_type_conversion() {
        assert_eq!(MessageType::from_u8(0x01), Some(MessageType::Handshake));
        assert_eq!(MessageType::from_u8(0x10), Some(MessageType::TaskSubmit));
        assert_eq!(MessageType::from_u8(0xFF), None);
        
        assert_eq!(MessageType::Handshake.as_u8(), 0x01);
    }
    
    #[test]
    fn test_handshake_message() {
        let node_info = NodeInfo::new_worker(test_addr());
        let msg = HandshakeMessage {
            node_info,
            protocol_version: 1,
        };
        
        let message = Message::Handshake(msg);
        assert_eq!(message.message_type(), MessageType::Handshake);
        assert!(!message.is_ack());
    }
    
    #[test]
    fn test_error_message() {
        let err = ErrorMessage::new(ErrorCode::TaskNotFound, "Task not found")
            .with_entity("task-123")
            .with_details("The task may have been deleted");
        
        assert_eq!(err.code, ErrorCode::TaskNotFound);
        assert_eq!(err.entity_id, Some("task-123".to_string()));
    }
}
