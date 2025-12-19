//! Error types for Vistrit distributed computing platform.

use thiserror::Error;

/// Result type alias using VistritError
pub type Result<T> = std::result::Result<T, VistritError>;

/// Unified error type for Vistrit operations
#[derive(Error, Debug)]
pub enum VistritError {
    // ─────────────────────────────────────────────────────────────────────────
    // Protocol Errors
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Invalid magic bytes in protocol frame
    #[error("Invalid protocol magic bytes: expected 0x56495354, got {0:#010x}")]
    InvalidMagic(u32),
    
    /// Unsupported protocol version
    #[error("Unsupported protocol version: {0}")]
    UnsupportedVersion(u8),
    
    /// Unknown message type
    #[error("Unknown message type: {0:#04x}")]
    UnknownMessageType(u8),
    
    /// Frame checksum mismatch
    #[error("Checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch { expected: u32, actual: u32 },
    
    /// Frame too large
    #[error("Frame too large: {size} bytes exceeds maximum of {max} bytes")]
    FrameTooLarge { size: usize, max: usize },
    
    /// Incomplete frame data
    #[error("Incomplete frame: need {needed} more bytes")]
    IncompleteFrame { needed: usize },
    
    // ─────────────────────────────────────────────────────────────────────────
    // Serialization Errors
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Serialization failed
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    /// Deserialization failed
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    
    // ─────────────────────────────────────────────────────────────────────────
    // Network Errors
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Connection failed
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    
    /// Connection closed unexpectedly
    #[error("Connection closed")]
    ConnectionClosed,
    
    /// Connection timeout
    #[error("Connection timeout after {0} seconds")]
    Timeout(u64),
    
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    // ─────────────────────────────────────────────────────────────────────────
    // Task Errors
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Task not found
    #[error("Task not found: {0}")]
    TaskNotFound(String),
    
    /// Task execution failed
    #[error("Task execution failed: {0}")]
    TaskExecutionFailed(String),
    
    /// Task timeout
    #[error("Task timeout after {0} seconds")]
    TaskTimeout(u64),
    
    /// Invalid task state transition
    #[error("Invalid task state transition: {from} -> {to}")]
    InvalidTaskTransition { from: String, to: String },
    
    // ─────────────────────────────────────────────────────────────────────────
    // Node Errors
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Node not found
    #[error("Node not found: {0}")]
    NodeNotFound(String),
    
    /// Node registration failed
    #[error("Node registration failed: {0}")]
    RegistrationFailed(String),
    
    /// Heartbeat timeout - node considered dead
    #[error("Heartbeat timeout for node: {0}")]
    HeartbeatTimeout(String),
    
    // ─────────────────────────────────────────────────────────────────────────
    // Cluster Errors
    // ─────────────────────────────────────────────────────────────────────────
    
    /// No workers available
    #[error("No workers available to execute task")]
    NoWorkersAvailable,
    
    /// Leader election failed
    #[error("Leader election failed: {0}")]
    LeaderElectionFailed(String),
    
    /// Not the leader
    #[error("This node is not the leader")]
    NotLeader,
    
    // ─────────────────────────────────────────────────────────────────────────
    // Generic Errors
    // ─────────────────────────────────────────────────────────────────────────
    
    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
    
    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),
}

impl VistritError {
    /// Check if this error is recoverable (can retry)
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            VistritError::Timeout(_)
                | VistritError::ConnectionFailed(_)
                | VistritError::ConnectionClosed
                | VistritError::HeartbeatTimeout(_)
                | VistritError::NoWorkersAvailable
        )
    }
    
    /// Check if this is a protocol error
    pub fn is_protocol_error(&self) -> bool {
        matches!(
            self,
            VistritError::InvalidMagic(_)
                | VistritError::UnsupportedVersion(_)
                | VistritError::UnknownMessageType(_)
                | VistritError::ChecksumMismatch { .. }
                | VistritError::FrameTooLarge { .. }
                | VistritError::IncompleteFrame { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_error_display() {
        let err = VistritError::InvalidMagic(0x12345678);
        assert!(err.to_string().contains("0x12345678"));
        
        let err = VistritError::ChecksumMismatch {
            expected: 0xAABBCCDD,
            actual: 0x11223344,
        };
        assert!(err.to_string().contains("0xaabbccdd"));
    }
    
    #[test]
    fn test_recoverable_errors() {
        assert!(VistritError::Timeout(30).is_recoverable());
        assert!(VistritError::ConnectionClosed.is_recoverable());
        assert!(!VistritError::InvalidMagic(0).is_recoverable());
    }
    
    #[test]
    fn test_protocol_errors() {
        assert!(VistritError::InvalidMagic(0).is_protocol_error());
        assert!(VistritError::UnsupportedVersion(2).is_protocol_error());
        assert!(!VistritError::Timeout(30).is_protocol_error());
    }
}
