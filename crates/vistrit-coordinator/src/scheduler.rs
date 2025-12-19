//! Task scheduler for distributing work to workers.
//!
//! The scheduler maintains a queue of pending tasks and assigns them
//! to available workers based on configurable strategies.

use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use vistrit_core::{NodeId, NodeInfo, Task, TaskId, TaskPriority, TaskResult, TaskState};

use crate::registry::WorkerRegistry;

/// Scheduling strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingStrategy {
    /// Round-robin assignment
    RoundRobin,
    /// Assign to worker with least load
    LeastLoaded,
    /// Random assignment
    Random,
}

impl Default for SchedulingStrategy {
    fn default() -> Self {
        SchedulingStrategy::LeastLoaded
    }
}

/// Task queue entry
#[derive(Debug, Clone)]
pub struct QueuedTask {
    pub task: Task,
    pub queued_at: std::time::Instant,
    pub attempts: u32,
}

impl QueuedTask {
    pub fn new(task: Task) -> Self {
        Self {
            task,
            queued_at: std::time::Instant::now(),
            attempts: 0,
        }
    }
}

/// Scheduler events
#[derive(Debug, Clone)]
pub enum SchedulerEvent {
    /// Task queued
    TaskQueued(TaskId),
    /// Task assigned to worker
    TaskAssigned { task_id: TaskId, worker_id: NodeId },
    /// Task completed
    TaskCompleted { task_id: TaskId, success: bool },
    /// Task failed and will be retried
    TaskRetry { task_id: TaskId, attempt: u32 },
}

/// Task scheduler
pub struct Scheduler {
    /// Pending tasks (priority queues)
    pending_critical: Mutex<VecDeque<QueuedTask>>,
    pending_high: Mutex<VecDeque<QueuedTask>>,
    pending_normal: Mutex<VecDeque<QueuedTask>>,
    pending_low: Mutex<VecDeque<QueuedTask>>,
    
    /// Running tasks: task_id -> (task, worker_id)
    running: DashMap<TaskId, (Task, NodeId)>,
    
    /// Completed tasks: task_id -> result
    completed: DashMap<TaskId, TaskResult>,
    
    /// All tasks by ID
    all_tasks: DashMap<TaskId, Task>,
    
    /// Scheduling strategy
    strategy: SchedulingStrategy,
    
    /// Round-robin index
    rr_index: AtomicU64,
    
    /// Event broadcaster
    event_tx: broadcast::Sender<SchedulerEvent>,
    
    /// Statistics
    pub total_submitted: AtomicU64,
    pub total_completed: AtomicU64,
    pub total_failed: AtomicU64,
}

impl Scheduler {
    /// Create a new scheduler
    pub fn new(strategy: SchedulingStrategy) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            pending_critical: Mutex::new(VecDeque::new()),
            pending_high: Mutex::new(VecDeque::new()),
            pending_normal: Mutex::new(VecDeque::new()),
            pending_low: Mutex::new(VecDeque::new()),
            running: DashMap::new(),
            completed: DashMap::new(),
            all_tasks: DashMap::new(),
            strategy,
            rr_index: AtomicU64::new(0),
            event_tx,
            total_submitted: AtomicU64::new(0),
            total_completed: AtomicU64::new(0),
            total_failed: AtomicU64::new(0),
        }
    }
    
    /// Subscribe to scheduler events
    pub fn subscribe(&self) -> broadcast::Receiver<SchedulerEvent> {
        self.event_tx.subscribe()
    }
    
    /// Submit a task
    pub fn submit(&self, task: Task) -> TaskId {
        let task_id = task.id;
        let priority = task.priority;
        
        self.all_tasks.insert(task_id, task.clone());
        
        let queued = QueuedTask::new(task);
        
        match priority {
            TaskPriority::Critical => self.pending_critical.lock().unwrap().push_back(queued),
            TaskPriority::High => self.pending_high.lock().unwrap().push_back(queued),
            TaskPriority::Normal => self.pending_normal.lock().unwrap().push_back(queued),
            TaskPriority::Low => self.pending_low.lock().unwrap().push_back(queued),
        }
        
        self.total_submitted.fetch_add(1, Ordering::Relaxed);
        info!(task_id = %task_id, ?priority, "Task submitted");
        let _ = self.event_tx.send(SchedulerEvent::TaskQueued(task_id));
        
        task_id
    }
    
    /// Get next task to schedule (respects priority)
    fn pop_next_task(&self) -> Option<QueuedTask> {
        // Try queues in priority order
        if let Some(task) = self.pending_critical.lock().unwrap().pop_front() {
            return Some(task);
        }
        if let Some(task) = self.pending_high.lock().unwrap().pop_front() {
            return Some(task);
        }
        if let Some(task) = self.pending_normal.lock().unwrap().pop_front() {
            return Some(task);
        }
        if let Some(task) = self.pending_low.lock().unwrap().pop_front() {
            return Some(task);
        }
        None
    }
    
    /// Select a worker based on strategy
    fn select_worker(&self, workers: &[NodeInfo]) -> Option<NodeId> {
        if workers.is_empty() {
            return None;
        }
        
        match self.strategy {
            SchedulingStrategy::RoundRobin => {
                let idx = self.rr_index.fetch_add(1, Ordering::Relaxed) as usize;
                Some(workers[idx % workers.len()].id)
            }
            SchedulingStrategy::LeastLoaded => {
                workers
                    .iter()
                    .min_by_key(|w| w.running_tasks)
                    .map(|w| w.id)
            }
            SchedulingStrategy::Random => {
                use std::time::SystemTime;
                let idx = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .subsec_nanos() as usize;
                Some(workers[idx % workers.len()].id)
            }
        }
    }
    
    /// Try to schedule pending tasks to available workers
    /// Returns list of (task, worker_id) pairs that should be assigned
    pub fn schedule(&self, registry: &WorkerRegistry) -> Vec<(Task, NodeId)> {
        let mut assignments = Vec::new();
        let ready_workers = registry.get_ready_workers();
        
        if ready_workers.is_empty() {
            debug!("No ready workers available");
            return assignments;
        }
        
        // Keep scheduling while we have tasks and workers
        while let Some(mut queued) = self.pop_next_task() {
            // Get current ready workers (may change as we assign)
            let available: Vec<_> = ready_workers
                .iter()
                .filter(|w| {
                    // Check if worker is not already at capacity
                    let assigned_to_this = assignments.iter()
                        .filter(|(_, wid)| wid == &w.id)
                        .count() as u32;
                    w.running_tasks + assigned_to_this < w.capabilities.max_concurrent_tasks
                })
                .cloned()
                .collect();
            
            if available.is_empty() {
                // No more workers available, put task back
                match queued.task.priority {
                    TaskPriority::Critical => self.pending_critical.lock().unwrap().push_front(queued),
                    TaskPriority::High => self.pending_high.lock().unwrap().push_front(queued),
                    TaskPriority::Normal => self.pending_normal.lock().unwrap().push_front(queued),
                    TaskPriority::Low => self.pending_low.lock().unwrap().push_front(queued),
                }
                break;
            }
            
            if let Some(worker_id) = self.select_worker(&available) {
                queued.attempts += 1;
                let task = queued.task.clone();
                let task_id = task.id;
                
                // Mark as running
                self.running.insert(task_id, (task.clone(), worker_id));
                
                // Update task state in all_tasks
                if let Some(mut t) = self.all_tasks.get_mut(&task_id) {
                    t.state = TaskState::Scheduled { worker_id };
                }
                
                info!(task_id = %task_id, worker_id = %worker_id, "Task assigned");
                let _ = self.event_tx.send(SchedulerEvent::TaskAssigned { task_id, worker_id });
                
                assignments.push((task, worker_id));
            }
        }
        
        assignments
    }
    
    /// Mark task as running on worker
    pub fn mark_running(&self, task_id: &TaskId, worker_id: NodeId) {
        if let Some(mut task) = self.all_tasks.get_mut(task_id) {
            task.state = TaskState::Running {
                worker_id,
                started_at: chrono::Utc::now().timestamp(),
                progress: None,
            };
        }
    }
    
    /// Update task progress
    pub fn update_progress(&self, task_id: &TaskId, progress: u8) {
        if let Some(mut task) = self.all_tasks.get_mut(task_id) {
            if let TaskState::Running { worker_id, started_at, .. } = task.state {
                task.state = TaskState::Running {
                    worker_id,
                    started_at,
                    progress: Some(progress),
                };
            }
        }
    }
    
    /// Complete a task
    pub fn complete(&self, result: TaskResult) {
        let task_id = result.task_id;
        let success = result.success;
        
        // Remove from running
        self.running.remove(&task_id);
        
        // Update task state
        if let Some(mut task) = self.all_tasks.get_mut(&task_id) {
            if success {
                task.state = TaskState::Completed {
                    worker_id: result.worker_id,
                    completed_at: chrono::Utc::now().timestamp(),
                    duration_ms: result.duration_ms,
                };
                self.total_completed.fetch_add(1, Ordering::Relaxed);
            } else {
                // Check if we should retry
                let current_retries = match &task.state {
                    TaskState::Failed { retries, .. } => *retries,
                    _ => 0,
                };
                
                if current_retries < task.max_retries {
                    // Requeue for retry
                    info!(task_id = %task_id, attempt = current_retries + 1, "Retrying failed task");
                    let _ = self.event_tx.send(SchedulerEvent::TaskRetry {
                        task_id,
                        attempt: current_retries + 1,
                    });
                    
                    task.state = TaskState::Pending;
                    let queued = QueuedTask {
                        task: task.clone(),
                        queued_at: std::time::Instant::now(),
                        attempts: current_retries + 1,
                    };
                    
                    match task.priority {
                        TaskPriority::Critical => self.pending_critical.lock().unwrap().push_back(queued),
                        TaskPriority::High => self.pending_high.lock().unwrap().push_back(queued),
                        TaskPriority::Normal => self.pending_normal.lock().unwrap().push_back(queued),
                        TaskPriority::Low => self.pending_low.lock().unwrap().push_back(queued),
                    }
                    return;
                }
                
                task.state = TaskState::Failed {
                    worker_id: Some(result.worker_id),
                    error: result.error.clone().unwrap_or_default(),
                    failed_at: chrono::Utc::now().timestamp(),
                    retries: current_retries,
                };
                self.total_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        // Store result
        self.completed.insert(task_id, result);
        
        info!(task_id = %task_id, success, "Task completed");
        let _ = self.event_tx.send(SchedulerEvent::TaskCompleted { task_id, success });
    }
    
    /// Get task by ID
    pub fn get_task(&self, task_id: &TaskId) -> Option<Task> {
        self.all_tasks.get(task_id).map(|t| t.clone())
    }
    
    /// Get task result
    pub fn get_result(&self, task_id: &TaskId) -> Option<TaskResult> {
        self.completed.get(task_id).map(|r| r.clone())
    }
    
    /// Get pending task count
    pub fn pending_count(&self) -> usize {
        self.pending_critical.lock().unwrap().len()
            + self.pending_high.lock().unwrap().len()
            + self.pending_normal.lock().unwrap().len()
            + self.pending_low.lock().unwrap().len()
    }
    
    /// Get running task count
    pub fn running_count(&self) -> usize {
        self.running.len()
    }
    
    /// Cancel a task
    pub fn cancel(&self, task_id: &TaskId) -> bool {
        // Remove from running if there
        if self.running.remove(task_id).is_some() {
            if let Some(mut task) = self.all_tasks.get_mut(task_id) {
                task.state = TaskState::Cancelled {
                    cancelled_at: chrono::Utc::now().timestamp(),
                };
            }
            info!(task_id = %task_id, "Task cancelled");
            return true;
        }
        
        // Try to remove from pending queues
        for queue in [
            &self.pending_critical,
            &self.pending_high,
            &self.pending_normal,
            &self.pending_low,
        ] {
            let mut q = queue.lock().unwrap();
            if let Some(pos) = q.iter().position(|t| t.task.id == *task_id) {
                q.remove(pos);
                if let Some(mut task) = self.all_tasks.get_mut(task_id) {
                    task.state = TaskState::Cancelled {
                        cancelled_at: chrono::Utc::now().timestamp(),
                    };
                }
                info!(task_id = %task_id, "Task cancelled");
                return true;
            }
        }
        
        false
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new(SchedulingStrategy::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vistrit_core::TaskPayload;
    
    fn create_test_task(name: &str, priority: TaskPriority) -> Task {
        Task::new_command(name, "echo")
            .with_priority(priority)
    }
    
    #[test]
    fn test_submit_task() {
        let scheduler = Scheduler::default();
        let task = create_test_task("test", TaskPriority::Normal);
        let task_id = task.id;
        
        scheduler.submit(task);
        
        assert_eq!(scheduler.pending_count(), 1);
        assert!(scheduler.get_task(&task_id).is_some());
    }
    
    #[test]
    fn test_priority_ordering() {
        let scheduler = Scheduler::default();
        
        let low = create_test_task("low", TaskPriority::Low);
        let normal = create_test_task("normal", TaskPriority::Normal);
        let high = create_test_task("high", TaskPriority::High);
        let critical = create_test_task("critical", TaskPriority::Critical);
        
        // Submit in reverse order
        scheduler.submit(low.clone());
        scheduler.submit(normal.clone());
        scheduler.submit(high.clone());
        scheduler.submit(critical.clone());
        
        // Pop should return in priority order
        let first = scheduler.pop_next_task().unwrap();
        assert_eq!(first.task.name, "critical");
        
        let second = scheduler.pop_next_task().unwrap();
        assert_eq!(second.task.name, "high");
        
        let third = scheduler.pop_next_task().unwrap();
        assert_eq!(third.task.name, "normal");
        
        let fourth = scheduler.pop_next_task().unwrap();
        assert_eq!(fourth.task.name, "low");
    }
}
