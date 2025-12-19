//! Node identity and type definitions.
//!
//! Nodes in the Vistrit cluster are identified by unique UUIDs and have
//! associated metadata describing their capabilities and current state.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use uuid::Uuid;

/// Unique identifier for a node in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(Uuid);

impl NodeId {
    /// Generate a new random node ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
    
    /// Create a node ID from a UUID
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
    
    /// Get the underlying UUID
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
    
    /// Parse from a string
    pub fn parse(s: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Show shortened version: first 8 chars of UUID
        write!(f, "{}", &self.0.to_string()[..8])
    }
}

/// Type of node in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeType {
    /// Coordinator node - schedules tasks and manages workers
    Coordinator,
    /// Worker node - executes tasks
    Worker,
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::Coordinator => write!(f, "Coordinator"),
            NodeType::Worker => write!(f, "Worker"),
        }
    }
}

/// Node capabilities describing what the node can do
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    /// Number of concurrent tasks this node can handle
    pub max_concurrent_tasks: u32,
    
    /// Available memory in bytes
    pub available_memory_bytes: u64,
    
    /// Number of CPU cores available
    pub cpu_cores: u32,
    
    /// Custom tags for task routing (e.g., "gpu", "high-memory")
    pub tags: Vec<String>,
}

impl Default for NodeCapabilities {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 4,
            available_memory_bytes: 1024 * 1024 * 1024, // 1 GB
            cpu_cores: 4,
            tags: Vec::new(),
        }
    }
}

impl NodeCapabilities {
    /// Create capabilities from system info
    pub fn from_system() -> Self {
        // In a real implementation, we'd query actual system resources
        Self::default()
    }
    
    /// Add a tag to this node's capabilities
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }
    
    /// Set maximum concurrent tasks
    pub fn with_max_concurrent_tasks(mut self, max: u32) -> Self {
        self.max_concurrent_tasks = max;
        self
    }
}

/// Current state of a node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is starting up
    Starting,
    /// Node is ready to accept tasks
    Ready,
    /// Node is busy executing tasks
    Busy,
    /// Node is draining (finishing current tasks, not accepting new ones)
    Draining,
    /// Node is offline/disconnected
    Offline,
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeState::Starting => write!(f, "Starting"),
            NodeState::Ready => write!(f, "Ready"),
            NodeState::Busy => write!(f, "Busy"),
            NodeState::Draining => write!(f, "Draining"),
            NodeState::Offline => write!(f, "Offline"),
        }
    }
}

/// Comprehensive information about a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique node identifier
    pub id: NodeId,
    
    /// Type of node
    pub node_type: NodeType,
    
    /// Network address
    pub address: SocketAddr,
    
    /// Node capabilities
    pub capabilities: NodeCapabilities,
    
    /// Current state
    pub state: NodeState,
    
    /// Number of tasks currently running
    pub running_tasks: u32,
    
    /// Human-readable node name (optional)
    pub name: Option<String>,
    
    /// Timestamp when node started (Unix timestamp)
    pub started_at: i64,
    
    /// Last heartbeat timestamp (Unix timestamp)
    pub last_heartbeat: i64,
}

impl NodeInfo {
    /// Create a new NodeInfo for a worker
    pub fn new_worker(address: SocketAddr) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: NodeId::new(),
            node_type: NodeType::Worker,
            address,
            capabilities: NodeCapabilities::from_system(),
            state: NodeState::Starting,
            running_tasks: 0,
            name: None,
            started_at: now,
            last_heartbeat: now,
        }
    }
    
    /// Create a new NodeInfo for a coordinator
    pub fn new_coordinator(address: SocketAddr) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: NodeId::new(),
            node_type: NodeType::Coordinator,
            address,
            capabilities: NodeCapabilities::default(),
            state: NodeState::Starting,
            running_tasks: 0,
            name: None,
            started_at: now,
            last_heartbeat: now,
        }
    }
    
    /// Set a human-readable name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
    
    /// Check if node can accept more tasks
    pub fn can_accept_task(&self) -> bool {
        self.state == NodeState::Ready 
            && self.running_tasks < self.capabilities.max_concurrent_tasks
    }
    
    /// Get load percentage (running tasks / max concurrent tasks)
    pub fn load_percentage(&self) -> f64 {
        if self.capabilities.max_concurrent_tasks == 0 {
            return 100.0;
        }
        (self.running_tasks as f64 / self.capabilities.max_concurrent_tasks as f64) * 100.0
    }
    
    /// Update last heartbeat to current time
    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = chrono::Utc::now().timestamp();
    }
    
    /// Check if heartbeat is stale (older than given seconds)
    pub fn is_heartbeat_stale(&self, max_age_seconds: i64) -> bool {
        let now = chrono::Utc::now().timestamp();
        now - self.last_heartbeat > max_age_seconds
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
    fn test_node_id_generation() {
        let id1 = NodeId::new();
        let id2 = NodeId::new();
        assert_ne!(id1, id2);
    }
    
    #[test]
    fn test_node_id_display() {
        let id = NodeId::new();
        let display = format!("{}", id);
        assert_eq!(display.len(), 8);
    }
    
    #[test]
    fn test_worker_info() {
        let info = NodeInfo::new_worker(test_addr());
        assert_eq!(info.node_type, NodeType::Worker);
        assert_eq!(info.state, NodeState::Starting);
        assert!(info.can_accept_task() == false); // Still starting
    }
    
    #[test]
    fn test_can_accept_task() {
        let mut info = NodeInfo::new_worker(test_addr());
        info.state = NodeState::Ready;
        info.running_tasks = 0;
        assert!(info.can_accept_task());
        
        info.running_tasks = info.capabilities.max_concurrent_tasks;
        assert!(!info.can_accept_task());
    }
    
    #[test]
    fn test_load_percentage() {
        let mut info = NodeInfo::new_worker(test_addr());
        info.capabilities.max_concurrent_tasks = 4;
        
        info.running_tasks = 0;
        assert_eq!(info.load_percentage(), 0.0);
        
        info.running_tasks = 2;
        assert_eq!(info.load_percentage(), 50.0);
        
        info.running_tasks = 4;
        assert_eq!(info.load_percentage(), 100.0);
    }
}
