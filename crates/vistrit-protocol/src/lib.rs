//! # Vistrit Protocol
//!
//! Custom binary protocol for the Vistrit distributed computing platform.
//!
//! This crate provides:
//! - Message type definitions for all protocol messages
//! - Frame encoding/decoding with magic bytes, versioning, and CRC32 checksums
//! - Tokio codec for async networking

pub mod codec;
pub mod frame;
pub mod messages;

pub use codec::VistritCodec;
pub use frame::{Frame, FrameHeader, MAGIC_BYTES, PROTOCOL_VERSION, MAX_FRAME_SIZE};
pub use messages::{Message, MessageType};
