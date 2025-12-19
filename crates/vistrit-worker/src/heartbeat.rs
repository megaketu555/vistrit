//! Heartbeat service for maintaining connection to coordinator.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

use vistrit_core::NodeId;

/// Heartbeat service configuration
pub struct HeartbeatService {
    node_id: NodeId,
    interval: Duration,
    running_tasks: Arc<AtomicU32>,
}

impl HeartbeatService {
    /// Create a new heartbeat service
    pub fn new(
        node_id: NodeId,
        interval: Duration,
        running_tasks: Arc<AtomicU32>,
    ) -> Self {
        Self {
            node_id,
            interval,
            running_tasks,
        }
    }
    
    /// Get the node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
    
    /// Get the interval
    pub fn interval(&self) -> Duration {
        self.interval
    }
    
    /// Get current running task count
    pub fn running_tasks(&self) -> u32 {
        self.running_tasks.load(Ordering::Relaxed)
    }
}
