//! # Vistrit Core
//!
//! Core types and definitions for the Vistrit distributed computing platform.
//!
//! This crate provides shared types used across all Vistrit components:
//! - Node identity and types
//! - Task definitions and states
//! - Common error types

pub mod error;
pub mod node;
pub mod task;

pub use error::{VistritError, Result};
pub use node::{NodeId, NodeInfo, NodeType, NodeCapabilities, NodeState};
pub use task::{TaskId, Task, TaskState, TaskResult, TaskPriority, TaskPayload};
