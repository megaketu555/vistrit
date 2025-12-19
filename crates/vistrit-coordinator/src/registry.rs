//! Worker registry for tracking connected workers.
//!
//! The registry maintains information about all workers in the cluster,
//! including their capabilities, current state, and health status.

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use vistrit_core::{NodeId, NodeInfo, NodeState};

/// Events that can occur in the registry
#[derive(Debug, Clone)]
pub enum RegistryEvent {
    /// Worker joined the cluster
    WorkerJoined(NodeId),
    /// Worker left the cluster
    WorkerLeft(NodeId),
    /// Worker state changed
    WorkerStateChanged { node_id: NodeId, old_state: NodeState, new_state: NodeState },
    /// Worker heartbeat timeout
    WorkerTimeout(NodeId),
}

/// Worker entry with metadata
#[derive(Debug, Clone)]
pub struct WorkerEntry {
    /// Worker information
    pub info: NodeInfo,
    /// When worker connected
    pub connected_at: Instant,
    /// Last heartbeat received
    pub last_heartbeat: Instant,
    /// Number of heartbeats received
    pub heartbeat_count: u64,
}

impl WorkerEntry {
    /// Create a new worker entry
    pub fn new(info: NodeInfo) -> Self {
        let now = Instant::now();
        Self {
            info,
            connected_at: now,
            last_heartbeat: now,
            heartbeat_count: 0,
        }
    }
    
    /// Update heartbeat timestamp
    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
        self.heartbeat_count += 1;
        self.info.update_heartbeat();
    }
    
    /// Check if heartbeat is stale
    pub fn is_stale(&self, timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() > timeout
    }
    
    /// Get connection duration
    pub fn connection_duration(&self) -> Duration {
        self.connected_at.elapsed()
    }
}

/// Worker registry
pub struct WorkerRegistry {
    /// Map of worker ID to worker entry
    workers: DashMap<NodeId, WorkerEntry>,
    /// Event broadcaster
    event_tx: broadcast::Sender<RegistryEvent>,
    /// Heartbeat timeout
    heartbeat_timeout: Duration,
}

impl WorkerRegistry {
    /// Create a new worker registry
    pub fn new(heartbeat_timeout: Duration) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            workers: DashMap::new(),
            event_tx,
            heartbeat_timeout,
        }
    }
    
    /// Subscribe to registry events
    pub fn subscribe(&self) -> broadcast::Receiver<RegistryEvent> {
        self.event_tx.subscribe()
    }
    
    /// Register a new worker
    pub fn register(&self, info: NodeInfo) -> NodeId {
        let node_id = info.id;
        let entry = WorkerEntry::new(info);
        
        self.workers.insert(node_id, entry);
        
        info!(node_id = %node_id, "Worker registered");
        let _ = self.event_tx.send(RegistryEvent::WorkerJoined(node_id));
        
        node_id
    }
    
    /// Unregister a worker
    pub fn unregister(&self, node_id: &NodeId) -> Option<WorkerEntry> {
        let removed = self.workers.remove(node_id);
        
        if removed.is_some() {
            info!(node_id = %node_id, "Worker unregistered");
            let _ = self.event_tx.send(RegistryEvent::WorkerLeft(*node_id));
        }
        
        removed.map(|(_, entry)| entry)
    }
    
    /// Update worker heartbeat
    pub fn heartbeat(&self, node_id: &NodeId) -> bool {
        if let Some(mut entry) = self.workers.get_mut(node_id) {
            entry.update_heartbeat();
            debug!(node_id = %node_id, count = entry.heartbeat_count, "Heartbeat received");
            true
        } else {
            warn!(node_id = %node_id, "Heartbeat from unknown worker");
            false
        }
    }
    
    /// Update worker state
    pub fn update_state(&self, node_id: &NodeId, new_state: NodeState) -> bool {
        if let Some(mut entry) = self.workers.get_mut(node_id) {
            let old_state = entry.info.state;
            if old_state != new_state {
                entry.info.state = new_state;
                debug!(node_id = %node_id, ?old_state, ?new_state, "Worker state changed");
                let _ = self.event_tx.send(RegistryEvent::WorkerStateChanged {
                    node_id: *node_id,
                    old_state,
                    new_state,
                });
            }
            true
        } else {
            false
        }
    }
    
    /// Update running task count for a worker
    pub fn update_running_tasks(&self, node_id: &NodeId, count: u32) {
        if let Some(mut entry) = self.workers.get_mut(node_id) {
            entry.info.running_tasks = count;
        }
    }
    
    /// Get worker info
    pub fn get(&self, node_id: &NodeId) -> Option<NodeInfo> {
        self.workers.get(node_id).map(|e| e.info.clone())
    }
    
    /// Get all workers
    pub fn get_all(&self) -> Vec<NodeInfo> {
        self.workers.iter().map(|e| e.info.clone()).collect()
    }
    
    /// Get workers that are ready to accept tasks
    pub fn get_ready_workers(&self) -> Vec<NodeInfo> {
        self.workers
            .iter()
            .filter(|e| e.info.can_accept_task())
            .map(|e| e.info.clone())
            .collect()
    }
    
    /// Get number of workers
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }
    
    /// Get number of online (non-stale) workers
    pub fn online_worker_count(&self) -> usize {
        self.workers
            .iter()
            .filter(|e| !e.is_stale(self.heartbeat_timeout))
            .count()
    }
    
    /// Check for stale workers and mark them as offline
    pub fn check_stale_workers(&self) -> Vec<NodeId> {
        let mut stale = Vec::new();
        
        for entry in self.workers.iter() {
            if entry.is_stale(self.heartbeat_timeout) {
                stale.push(entry.info.id);
            }
        }
        
        for node_id in &stale {
            if let Some(mut entry) = self.workers.get_mut(node_id) {
                if entry.info.state != NodeState::Offline {
                    entry.info.state = NodeState::Offline;
                    warn!(node_id = %node_id, "Worker marked offline due to heartbeat timeout");
                    let _ = self.event_tx.send(RegistryEvent::WorkerTimeout(*node_id));
                }
            }
        }
        
        stale
    }
    
    /// Remove all offline workers
    pub fn remove_offline_workers(&self) -> Vec<NodeId> {
        let offline: Vec<NodeId> = self.workers
            .iter()
            .filter(|e| e.info.state == NodeState::Offline)
            .map(|e| e.info.id)
            .collect();
        
        for node_id in &offline {
            self.unregister(node_id);
        }
        
        offline
    }
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    
    fn test_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7878)
    }
    
    #[test]
    fn test_register_worker() {
        let registry = WorkerRegistry::default();
        let info = NodeInfo::new_worker(test_addr());
        let node_id = info.id;
        
        registry.register(info);
        
        assert_eq!(registry.worker_count(), 1);
        assert!(registry.get(&node_id).is_some());
    }
    
    #[test]
    fn test_unregister_worker() {
        let registry = WorkerRegistry::default();
        let info = NodeInfo::new_worker(test_addr());
        let node_id = info.id;
        
        registry.register(info);
        assert_eq!(registry.worker_count(), 1);
        
        registry.unregister(&node_id);
        assert_eq!(registry.worker_count(), 0);
    }
    
    #[test]
    fn test_heartbeat() {
        let registry = WorkerRegistry::default();
        let info = NodeInfo::new_worker(test_addr());
        let node_id = info.id;
        
        registry.register(info);
        
        assert!(registry.heartbeat(&node_id));
        assert!(!registry.heartbeat(&NodeId::new())); // Unknown worker
    }
    
    #[test]
    fn test_ready_workers() {
        let registry = WorkerRegistry::default();
        
        let mut info1 = NodeInfo::new_worker(test_addr());
        info1.state = NodeState::Ready;
        
        let mut info2 = NodeInfo::new_worker(test_addr());
        info2.state = NodeState::Busy;
        
        registry.register(info1);
        registry.register(info2);
        
        let ready = registry.get_ready_workers();
        assert_eq!(ready.len(), 1);
    }
}
