//! Task definitions and state management.
//!
//! Tasks are the unit of work in the Vistrit distributed computing platform.
//! They have a lifecycle from submission through completion or failure.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::node::NodeId;

/// Unique identifier for a task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(Uuid);

impl TaskId {
    /// Generate a new random task ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
    
    /// Create from a UUID
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
    
    /// Get the underlying UUID
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
    
    /// Parse from string
    pub fn parse(s: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0.to_string()[..8])
    }
}

/// Task priority level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TaskPriority {
    /// Lowest priority - run when resources are available
    Low = 0,
    /// Normal priority - default for most tasks
    Normal = 1,
    /// High priority - run before normal tasks
    High = 2,
    /// Critical priority - run immediately
    Critical = 3,
}

impl Default for TaskPriority {
    fn default() -> Self {
        TaskPriority::Normal
    }
}

impl std::fmt::Display for TaskPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskPriority::Low => write!(f, "Low"),
            TaskPriority::Normal => write!(f, "Normal"),
            TaskPriority::High => write!(f, "High"),
            TaskPriority::Critical => write!(f, "Critical"),
        }
    }
}

/// Current state of a task
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskState {
    /// Task is waiting to be scheduled
    Pending,
    /// Task has been assigned to a worker
    Scheduled { worker_id: NodeId },
    /// Task is currently running on a worker
    Running { 
        worker_id: NodeId,
        started_at: i64,
        progress: Option<u8>, // 0-100 percentage
    },
    /// Task completed successfully
    Completed {
        worker_id: NodeId,
        completed_at: i64,
        duration_ms: u64,
    },
    /// Task failed
    Failed {
        worker_id: Option<NodeId>,
        error: String,
        failed_at: i64,
        retries: u32,
    },
    /// Task was cancelled
    Cancelled { cancelled_at: i64 },
}

impl TaskState {
    /// Check if task is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(self, TaskState::Completed { .. } | TaskState::Failed { .. } | TaskState::Cancelled { .. })
    }
    
    /// Check if task is running
    pub fn is_running(&self) -> bool {
        matches!(self, TaskState::Running { .. })
    }
    
    /// Get the worker ID if task is assigned to one
    pub fn worker_id(&self) -> Option<NodeId> {
        match self {
            TaskState::Scheduled { worker_id } => Some(*worker_id),
            TaskState::Running { worker_id, .. } => Some(*worker_id),
            TaskState::Completed { worker_id, .. } => Some(*worker_id),
            TaskState::Failed { worker_id, .. } => *worker_id,
            _ => None,
        }
    }
    
    /// Get state name as string
    pub fn name(&self) -> &'static str {
        match self {
            TaskState::Pending => "Pending",
            TaskState::Scheduled { .. } => "Scheduled",
            TaskState::Running { .. } => "Running",
            TaskState::Completed { .. } => "Completed",
            TaskState::Failed { .. } => "Failed",
            TaskState::Cancelled { .. } => "Cancelled",
        }
    }
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskState::Pending => write!(f, "Pending"),
            TaskState::Scheduled { worker_id } => write!(f, "Scheduled ({})", worker_id),
            TaskState::Running { worker_id, progress, .. } => {
                if let Some(p) = progress {
                    write!(f, "Running on {} ({}%)", worker_id, p)
                } else {
                    write!(f, "Running on {}", worker_id)
                }
            }
            TaskState::Completed { duration_ms, .. } => {
                write!(f, "Completed ({}ms)", duration_ms)
            }
            TaskState::Failed { error, retries, .. } => {
                write!(f, "Failed: {} (retries: {})", error, retries)
            }
            TaskState::Cancelled { .. } => write!(f, "Cancelled"),
        }
    }
}

/// Task payload - the actual work to be done
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskPayload {
    /// Simple command to execute
    Command { command: String, args: Vec<String> },
    
    /// Raw data to process with a named function
    Data { 
        function: String, 
        data: Vec<u8>,
    },
    
    /// Compute task with expression
    Compute { expression: String },
    
    /// Map operation over data chunks
    Map { 
        function: String,
        input_chunks: Vec<Vec<u8>>,
    },
    
    /// Reduce operation to aggregate results
    Reduce {
        function: String,
        intermediate_results: Vec<Vec<u8>>,
    },
}

/// A task in the distributed computing platform
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Unique task identifier
    pub id: TaskId,
    
    /// Human-readable task name
    pub name: String,
    
    /// Task payload
    pub payload: TaskPayload,
    
    /// Current state
    pub state: TaskState,
    
    /// Priority level
    pub priority: TaskPriority,
    
    /// Maximum retries on failure
    pub max_retries: u32,
    
    /// Timeout in seconds (0 = no timeout)
    pub timeout_seconds: u64,
    
    /// Tags for routing (e.g., "gpu", "high-memory")
    pub tags: Vec<String>,
    
    /// When task was created (Unix timestamp)
    pub created_at: i64,
    
    /// ID of the client that submitted the task
    pub submitted_by: Option<NodeId>,
    
    /// Dependencies - task IDs that must complete before this task
    pub dependencies: Vec<TaskId>,
}

impl Task {
    /// Create a new task with a command payload
    pub fn new_command(name: impl Into<String>, command: impl Into<String>) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: TaskId::new(),
            name: name.into(),
            payload: TaskPayload::Command {
                command: command.into(),
                args: Vec::new(),
            },
            state: TaskState::Pending,
            priority: TaskPriority::Normal,
            max_retries: 3,
            timeout_seconds: 300, // 5 minutes default
            tags: Vec::new(),
            created_at: now,
            submitted_by: None,
            dependencies: Vec::new(),
        }
    }
    
    /// Create a new compute task
    pub fn new_compute(name: impl Into<String>, expression: impl Into<String>) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: TaskId::new(),
            name: name.into(),
            payload: TaskPayload::Compute {
                expression: expression.into(),
            },
            state: TaskState::Pending,
            priority: TaskPriority::Normal,
            max_retries: 3,
            timeout_seconds: 300,
            tags: Vec::new(),
            created_at: now,
            submitted_by: None,
            dependencies: Vec::new(),
        }
    }
    
    /// Create a new data processing task
    pub fn new_data(name: impl Into<String>, function: impl Into<String>, data: Vec<u8>) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: TaskId::new(),
            name: name.into(),
            payload: TaskPayload::Data {
                function: function.into(),
                data,
            },
            state: TaskState::Pending,
            priority: TaskPriority::Normal,
            max_retries: 3,
            timeout_seconds: 300,
            tags: Vec::new(),
            created_at: now,
            submitted_by: None,
            dependencies: Vec::new(),
        }
    }
    
    /// Set task priority
    pub fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = priority;
        self
    }
    
    /// Set max retries
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }
    
    /// Set timeout
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout_seconds = seconds;
        self
    }
    
    /// Add a tag
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }
    
    /// Add a dependency
    pub fn with_dependency(mut self, task_id: TaskId) -> Self {
        self.dependencies.push(task_id);
        self
    }
    
    /// Check if all dependencies are satisfied
    pub fn dependencies_satisfied(&self, completed_tasks: &[TaskId]) -> bool {
        self.dependencies.iter().all(|dep| completed_tasks.contains(dep))
    }
}

/// Result of a completed task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    /// Task that produced this result
    pub task_id: TaskId,
    
    /// Whether execution was successful
    pub success: bool,
    
    /// Result data (if successful)
    pub data: Option<Vec<u8>>,
    
    /// Error message (if failed)
    pub error: Option<String>,
    
    /// Exit code (for command tasks)
    pub exit_code: Option<i32>,
    
    /// Execution duration in milliseconds
    pub duration_ms: u64,
    
    /// Worker that executed the task
    pub worker_id: NodeId,
    
    /// Stdout output (for command tasks)
    pub stdout: Option<String>,
    
    /// Stderr output (for command tasks)
    pub stderr: Option<String>,
}

impl TaskResult {
    /// Create a successful result
    pub fn success(task_id: TaskId, worker_id: NodeId, data: Vec<u8>, duration_ms: u64) -> Self {
        Self {
            task_id,
            success: true,
            data: Some(data),
            error: None,
            exit_code: Some(0),
            duration_ms,
            worker_id,
            stdout: None,
            stderr: None,
        }
    }
    
    /// Create a failure result
    pub fn failure(task_id: TaskId, worker_id: NodeId, error: impl Into<String>, duration_ms: u64) -> Self {
        Self {
            task_id,
            success: false,
            data: None,
            error: Some(error.into()),
            exit_code: None,
            duration_ms,
            worker_id,
            stdout: None,
            stderr: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_task_id_generation() {
        let id1 = TaskId::new();
        let id2 = TaskId::new();
        assert_ne!(id1, id2);
    }
    
    #[test]
    fn test_task_creation() {
        let task = Task::new_command("test", "echo")
            .with_priority(TaskPriority::High)
            .with_timeout(60)
            .with_tag("test");
        
        assert_eq!(task.name, "test");
        assert_eq!(task.priority, TaskPriority::High);
        assert_eq!(task.timeout_seconds, 60);
        assert!(task.tags.contains(&"test".to_string()));
    }
    
    #[test]
    fn test_task_state_terminal() {
        assert!(!TaskState::Pending.is_terminal());
        assert!(!TaskState::Running { 
            worker_id: NodeId::new(),
            started_at: 0,
            progress: None,
        }.is_terminal());
        
        assert!(TaskState::Completed {
            worker_id: NodeId::new(),
            completed_at: 0,
            duration_ms: 100,
        }.is_terminal());
        
        assert!(TaskState::Failed {
            worker_id: None,
            error: "test".to_string(),
            failed_at: 0,
            retries: 0,
        }.is_terminal());
    }
    
    #[test]
    fn test_priority_ordering() {
        assert!(TaskPriority::Low < TaskPriority::Normal);
        assert!(TaskPriority::Normal < TaskPriority::High);
        assert!(TaskPriority::High < TaskPriority::Critical);
    }
    
    #[test]
    fn test_dependencies_satisfied() {
        let dep1 = TaskId::new();
        let dep2 = TaskId::new();
        
        let task = Task::new_command("test", "echo")
            .with_dependency(dep1)
            .with_dependency(dep2);
        
        assert!(!task.dependencies_satisfied(&[]));
        assert!(!task.dependencies_satisfied(&[dep1]));
        assert!(task.dependencies_satisfied(&[dep1, dep2]));
    }
}
