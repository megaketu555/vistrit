//! Task executor for running assigned tasks.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

use vistrit_core::{NodeId, Task, TaskId, TaskPayload, TaskResult};

/// Running task handle
struct RunningTask {
    task: Task,
    started_at: Instant,
    cancel_tx: tokio::sync::oneshot::Sender<()>,
}

/// Task executor
pub struct TaskExecutor {
    node_id: NodeId,
    max_concurrent: u32,
    running_count: Arc<AtomicU32>,
    result_tx: mpsc::Sender<TaskResult>,
    running_tasks: Arc<Mutex<HashMap<TaskId, tokio::task::JoinHandle<()>>>>,
}

impl TaskExecutor {
    /// Create a new task executor
    pub fn new(
        node_id: NodeId,
        max_concurrent: u32,
        running_count: Arc<AtomicU32>,
        result_tx: mpsc::Sender<TaskResult>,
    ) -> Self {
        Self {
            node_id,
            max_concurrent,
            running_count,
            result_tx,
            running_tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    /// Execute a task
    pub async fn execute(&self, task: Task) {
        let task_id = task.id;
        let task_name = task.name.clone();
        
        info!(task_id = %task_id, task_name = %task_name, "Starting task execution");
        
        // Increment running count
        self.running_count.fetch_add(1, Ordering::Relaxed);
        
        let node_id = self.node_id;
        let running_count = Arc::clone(&self.running_count);
        let result_tx = self.result_tx.clone();
        let running_tasks = Arc::clone(&self.running_tasks);
        
        // Spawn task execution
        let handle = tokio::spawn(async move {
            let started = Instant::now();
            
            // Execute based on payload type
            let (success, data, error) = match &task.payload {
                TaskPayload::Command { command, args } => {
                    execute_command(command, args).await
                }
                TaskPayload::Compute { expression } => {
                    execute_compute(expression).await
                }
                TaskPayload::Data { function, data } => {
                    execute_data(function, data).await
                }
                TaskPayload::Map { function, input_chunks } => {
                    execute_map(function, input_chunks).await
                }
                TaskPayload::Reduce { function, intermediate_results } => {
                    execute_reduce(function, intermediate_results).await
                }
            };
            
            let duration_ms = started.elapsed().as_millis() as u64;
            
            // Create result
            let result = if success {
                TaskResult::success(task_id, node_id, data.unwrap_or_default(), duration_ms)
            } else {
                TaskResult::failure(task_id, node_id, error.unwrap_or_default(), duration_ms)
            };
            
            info!(
                task_id = %task_id,
                success,
                duration_ms,
                "Task execution completed"
            );
            
            // Send result
            if let Err(e) = result_tx.send(result).await {
                error!(task_id = %task_id, error = %e, "Failed to send task result");
            }
            
            // Decrement running count
            running_count.fetch_sub(1, Ordering::Relaxed);
            
            // Remove from running tasks
            running_tasks.lock().await.remove(&task_id);
        });
        
        // Store handle for cancellation
        self.running_tasks.lock().await.insert(task_id, handle);
    }
    
    /// Cancel a running task
    pub fn cancel(&self, task_id: &TaskId) {
        let task_id = *task_id;
        let running_tasks = Arc::clone(&self.running_tasks);
        let running_count = Arc::clone(&self.running_count);
        
        tokio::spawn(async move {
            if let Some(handle) = running_tasks.lock().await.remove(&task_id) {
                handle.abort();
                running_count.fetch_sub(1, Ordering::Relaxed);
                info!(task_id = %task_id, "Task cancelled");
            }
        });
    }
    
    /// Get current running task count
    pub fn running_count(&self) -> u32 {
        self.running_count.load(Ordering::Relaxed)
    }
}

/// Execute a command task
async fn execute_command(
    command: &str,
    args: &[String],
) -> (bool, Option<Vec<u8>>, Option<String>) {
    info!(command = %command, args = ?args, "Executing command");
    
    #[cfg(target_family = "windows")]
    let result = tokio::process::Command::new("cmd")
        .arg("/C")
        .arg(command)
        .args(args)
        .output()
        .await;
    
    #[cfg(not(target_family = "windows"))]
    let result = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(format!("{} {}", command, args.join(" ")))
        .output()
        .await;
    
    match result {
        Ok(output) => {
            if output.status.success() {
                (true, Some(output.stdout), None)
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                (false, Some(output.stdout), Some(stderr))
            }
        }
        Err(e) => {
            (false, None, Some(format!("Command execution failed: {}", e)))
        }
    }
}

/// Execute a compute task (simple expression evaluation)
async fn execute_compute(
    expression: &str,
) -> (bool, Option<Vec<u8>>, Option<String>) {
    info!(expression = %expression, "Executing compute task");
    
    // Simple expression evaluator for demonstration
    // In a real system, you'd use a proper expression evaluator
    let result = evaluate_simple_expression(expression);
    
    match result {
        Ok(value) => {
            (true, Some(value.to_string().into_bytes()), None)
        }
        Err(e) => {
            (false, None, Some(e))
        }
    }
}

/// Simple expression evaluator (for demonstration)
fn evaluate_simple_expression(expr: &str) -> std::result::Result<f64, String> {
    // Very basic: handle simple arithmetic
    let expr = expr.trim();
    
    // Try to parse as a number
    if let Ok(n) = expr.parse::<f64>() {
        return Ok(n);
    }
    
    // Handle basic operations
    if let Some(pos) = expr.rfind('+') {
        let left = evaluate_simple_expression(&expr[..pos])?;
        let right = evaluate_simple_expression(&expr[pos+1..])?;
        return Ok(left + right);
    }
    
    if let Some(pos) = expr.rfind('-') {
        if pos > 0 {
            let left = evaluate_simple_expression(&expr[..pos])?;
            let right = evaluate_simple_expression(&expr[pos+1..])?;
            return Ok(left - right);
        }
    }
    
    if let Some(pos) = expr.rfind('*') {
        let left = evaluate_simple_expression(&expr[..pos])?;
        let right = evaluate_simple_expression(&expr[pos+1..])?;
        return Ok(left * right);
    }
    
    if let Some(pos) = expr.rfind('/') {
        let left = evaluate_simple_expression(&expr[..pos])?;
        let right = evaluate_simple_expression(&expr[pos+1..])?;
        if right == 0.0 {
            return Err("Division by zero".to_string());
        }
        return Ok(left / right);
    }
    
    Err(format!("Cannot evaluate expression: {}", expr))
}

/// Execute a data processing task
async fn execute_data(
    function: &str,
    data: &[u8],
) -> (bool, Option<Vec<u8>>, Option<String>) {
    info!(function = %function, data_len = data.len(), "Executing data task");
    
    // Built-in functions for demonstration
    match function {
        "uppercase" => {
            let text = String::from_utf8_lossy(data);
            (true, Some(text.to_uppercase().into_bytes()), None)
        }
        "lowercase" => {
            let text = String::from_utf8_lossy(data);
            (true, Some(text.to_lowercase().into_bytes()), None)
        }
        "reverse" => {
            let text = String::from_utf8_lossy(data);
            let reversed: String = text.chars().rev().collect();
            (true, Some(reversed.into_bytes()), None)
        }
        "word_count" => {
            let text = String::from_utf8_lossy(data);
            let count = text.split_whitespace().count();
            (true, Some(count.to_string().into_bytes()), None)
        }
        "line_count" => {
            let text = String::from_utf8_lossy(data);
            let count = text.lines().count();
            (true, Some(count.to_string().into_bytes()), None)
        }
        "sha256" => {
            // Simple hash for demonstration (not real SHA256)
            let hash = data.iter().fold(0u64, |acc, &b| acc.wrapping_add(b as u64).wrapping_mul(31));
            (true, Some(format!("{:016x}", hash).into_bytes()), None)
        }
        _ => {
            (false, None, Some(format!("Unknown function: {}", function)))
        }
    }
}

/// Execute a map task
async fn execute_map(
    function: &str,
    input_chunks: &[Vec<u8>],
) -> (bool, Option<Vec<u8>>, Option<String>) {
    info!(function = %function, chunks = input_chunks.len(), "Executing map task");
    
    let mut results = Vec::new();
    
    for chunk in input_chunks {
        let (success, data, error) = execute_data(function, chunk).await;
        if !success {
            return (false, None, error);
        }
        if let Some(d) = data {
            results.push(d);
        }
    }
    
    // Concatenate results with newlines
    let output: Vec<u8> = results.into_iter()
        .flat_map(|r| r.into_iter().chain(std::iter::once(b'\n')))
        .collect();
    
    (true, Some(output), None)
}

/// Execute a reduce task
async fn execute_reduce(
    function: &str,
    intermediate_results: &[Vec<u8>],
) -> (bool, Option<Vec<u8>>, Option<String>) {
    info!(function = %function, results = intermediate_results.len(), "Executing reduce task");
    
    match function {
        "sum" => {
            let mut total: f64 = 0.0;
            for result in intermediate_results {
                let text = String::from_utf8_lossy(result);
                for line in text.lines() {
                    if let Ok(n) = line.trim().parse::<f64>() {
                        total += n;
                    }
                }
            }
            (true, Some(total.to_string().into_bytes()), None)
        }
        "count" => {
            let mut total: usize = 0;
            for result in intermediate_results {
                let text = String::from_utf8_lossy(result);
                for line in text.lines() {
                    if let Ok(n) = line.trim().parse::<usize>() {
                        total += n;
                    }
                }
            }
            (true, Some(total.to_string().into_bytes()), None)
        }
        "concat" => {
            let mut output = Vec::new();
            for result in intermediate_results {
                output.extend_from_slice(result);
            }
            (true, Some(output), None)
        }
        "max" => {
            let mut max: f64 = f64::NEG_INFINITY;
            for result in intermediate_results {
                let text = String::from_utf8_lossy(result);
                for line in text.lines() {
                    if let Ok(n) = line.trim().parse::<f64>() {
                        if n > max {
                            max = n;
                        }
                    }
                }
            }
            (true, Some(max.to_string().into_bytes()), None)
        }
        "min" => {
            let mut min: f64 = f64::INFINITY;
            for result in intermediate_results {
                let text = String::from_utf8_lossy(result);
                for line in text.lines() {
                    if let Ok(n) = line.trim().parse::<f64>() {
                        if n < min {
                            min = n;
                        }
                    }
                }
            }
            (true, Some(min.to_string().into_bytes()), None)
        }
        _ => {
            (false, None, Some(format!("Unknown reduce function: {}", function)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_evaluate_expression() {
        assert_eq!(evaluate_simple_expression("42"), Ok(42.0));
        assert_eq!(evaluate_simple_expression("1+2"), Ok(3.0));
        assert_eq!(evaluate_simple_expression("10-3"), Ok(7.0));
        assert_eq!(evaluate_simple_expression("4*5"), Ok(20.0));
        assert_eq!(evaluate_simple_expression("20/4"), Ok(5.0));
    }
    
    #[tokio::test]
    async fn test_execute_data_uppercase() {
        let (success, data, _) = execute_data("uppercase", b"hello").await;
        assert!(success);
        assert_eq!(data, Some(b"HELLO".to_vec()));
    }
    
    #[tokio::test]
    async fn test_execute_data_word_count() {
        let (success, data, _) = execute_data("word_count", b"hello world test").await;
        assert!(success);
        assert_eq!(data, Some(b"3".to_vec()));
    }
}
