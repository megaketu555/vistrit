//! Protocol frame encoding and decoding.
//!
//! The VistritProtocol uses a binary frame format with:
//! - Magic bytes for protocol identification
//! - Version byte for compatibility
//! - Message type byte
//! - Length field for payload size
//! - Payload (bincode-serialized message)
//! - CRC32 checksum for integrity

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher;

use crate::messages::{Message, MessageType};
use vistrit_core::{VistritError, Result};

/// Magic bytes: "VIST" in ASCII (0x56 0x49 0x53 0x54)
pub const MAGIC_BYTES: u32 = 0x56495354;

/// Current protocol version
pub const PROTOCOL_VERSION: u8 = 1;

/// Maximum frame size (16 MB)
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Frame header size in bytes
/// Magic (4) + Version (1) + Type (1) + Length (4) = 10 bytes
pub const HEADER_SIZE: usize = 10;

/// Checksum size in bytes (CRC32)
pub const CHECKSUM_SIZE: usize = 4;

/// Frame header structure
#[derive(Debug, Clone, Copy)]
pub struct FrameHeader {
    /// Protocol magic bytes
    pub magic: u32,
    /// Protocol version
    pub version: u8,
    /// Message type
    pub message_type: u8,
    /// Payload length
    pub length: u32,
}

impl FrameHeader {
    /// Create a new frame header
    pub fn new(message_type: MessageType, payload_length: usize) -> Self {
        Self {
            magic: MAGIC_BYTES,
            version: PROTOCOL_VERSION,
            message_type: message_type.as_u8(),
            length: payload_length as u32,
        }
    }
    
    /// Encode header to bytes
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32(self.magic);
        buf.put_u8(self.version);
        buf.put_u8(self.message_type);
        buf.put_u32(self.length);
    }
    
    /// Decode header from bytes
    pub fn decode(buf: &mut BytesMut) -> Result<Self> {
        if buf.remaining() < HEADER_SIZE {
            return Err(VistritError::IncompleteFrame { 
                needed: HEADER_SIZE - buf.remaining() 
            });
        }
        
        let magic = buf.get_u32();
        if magic != MAGIC_BYTES {
            return Err(VistritError::InvalidMagic(magic));
        }
        
        let version = buf.get_u8();
        if version != PROTOCOL_VERSION {
            return Err(VistritError::UnsupportedVersion(version));
        }
        
        let message_type = buf.get_u8();
        let length = buf.get_u32();
        
        Ok(Self {
            magic,
            version,
            message_type,
            length,
        })
    }
    
    /// Peek at header without consuming bytes
    pub fn peek(buf: &BytesMut) -> Result<Self> {
        if buf.remaining() < HEADER_SIZE {
            return Err(VistritError::IncompleteFrame { 
                needed: HEADER_SIZE - buf.remaining() 
            });
        }
        
        let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != MAGIC_BYTES {
            return Err(VistritError::InvalidMagic(magic));
        }
        
        let version = buf[4];
        if version != PROTOCOL_VERSION {
            return Err(VistritError::UnsupportedVersion(version));
        }
        
        let message_type = buf[5];
        let length = u32::from_be_bytes([buf[6], buf[7], buf[8], buf[9]]);
        
        Ok(Self {
            magic,
            version,
            message_type,
            length,
        })
    }
    
    /// Get total frame size (header + payload + checksum)
    pub fn total_frame_size(&self) -> usize {
        HEADER_SIZE + self.length as usize + CHECKSUM_SIZE
    }
}

/// A complete protocol frame
#[derive(Debug, Clone)]
pub struct Frame {
    /// Frame header
    pub header: FrameHeader,
    /// Message payload
    pub payload: Bytes,
    /// CRC32 checksum
    pub checksum: u32,
}

impl Frame {
    /// Create a new frame from a message
    pub fn from_message(message: &Message) -> Result<Self> {
        let payload = bincode::serialize(message)
            .map_err(|e| VistritError::Serialization(e.to_string()))?;
        
        if payload.len() > MAX_FRAME_SIZE {
            return Err(VistritError::FrameTooLarge { 
                size: payload.len(), 
                max: MAX_FRAME_SIZE 
            });
        }
        
        let header = FrameHeader::new(message.message_type(), payload.len());
        let checksum = Self::compute_checksum(&header, &payload);
        
        Ok(Self {
            header,
            payload: Bytes::from(payload),
            checksum,
        })
    }
    
    /// Decode message from frame
    pub fn into_message(self) -> Result<Message> {
        bincode::deserialize(&self.payload)
            .map_err(|e| VistritError::Deserialization(e.to_string()))
    }
    
    /// Encode frame to bytes
    pub fn encode(&self, buf: &mut BytesMut) {
        self.header.encode(buf);
        buf.put_slice(&self.payload);
        buf.put_u32(self.checksum);
    }
    
    /// Compute CRC32 checksum over header (excluding magic) and payload
    pub fn compute_checksum(header: &FrameHeader, payload: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        
        // Include version, type, and length in checksum
        hasher.update(&[header.version]);
        hasher.update(&[header.message_type]);
        hasher.update(&header.length.to_be_bytes());
        hasher.update(payload);
        
        hasher.finalize()
    }
    
    /// Verify checksum
    pub fn verify_checksum(&self) -> bool {
        let expected = Self::compute_checksum(&self.header, &self.payload);
        self.checksum == expected
    }
    
    /// Decode a frame from bytes
    /// Returns Ok(Some(frame)) if complete frame available
    /// Returns Ok(None) if more data needed
    /// Returns Err if invalid data
    pub fn decode(buf: &mut BytesMut) -> Result<Option<Self>> {
        // Check if we have enough data for header
        if buf.len() < HEADER_SIZE {
            return Ok(None);
        }
        
        // Peek at header to determine full frame size
        let header = FrameHeader::peek(buf)?;
        let total_size = header.total_frame_size();
        
        // Check frame size limit
        if total_size > MAX_FRAME_SIZE + HEADER_SIZE + CHECKSUM_SIZE {
            return Err(VistritError::FrameTooLarge { 
                size: total_size, 
                max: MAX_FRAME_SIZE 
            });
        }
        
        // Check if we have the complete frame
        if buf.len() < total_size {
            return Ok(None);
        }
        
        // Now consume the data
        let mut frame_buf = buf.split_to(total_size);
        
        // Decode header (consumes 10 bytes)
        let header = FrameHeader::decode(&mut frame_buf)?;
        
        // Extract payload
        let payload_len = header.length as usize;
        let payload = frame_buf.split_to(payload_len).freeze();
        
        // Extract checksum
        let checksum = frame_buf.get_u32();
        
        let frame = Self {
            header,
            payload,
            checksum,
        };
        
        // Verify checksum
        if !frame.verify_checksum() {
            return Err(VistritError::ChecksumMismatch {
                expected: Self::compute_checksum(&frame.header, &frame.payload),
                actual: checksum,
            });
        }
        
        Ok(Some(frame))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{HandshakeMessage, NodeInfo};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    
    fn test_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7878)
    }
    
    #[test]
    fn test_magic_bytes() {
        // "VIST" in ASCII
        assert_eq!(MAGIC_BYTES, 0x56495354);
        let bytes = MAGIC_BYTES.to_be_bytes();
        assert_eq!(&bytes, b"VIST");
    }
    
    #[test]
    fn test_frame_header_encode_decode() {
        let header = FrameHeader::new(MessageType::Handshake, 100);
        
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        
        assert_eq!(buf.len(), HEADER_SIZE);
        
        let decoded = FrameHeader::decode(&mut buf).unwrap();
        assert_eq!(decoded.magic, MAGIC_BYTES);
        assert_eq!(decoded.version, PROTOCOL_VERSION);
        assert_eq!(decoded.message_type, MessageType::Handshake.as_u8());
        assert_eq!(decoded.length, 100);
    }
    
    #[test]
    fn test_frame_roundtrip() {
        let node_info = NodeInfo::new_worker(test_addr());
        let msg = Message::Handshake(HandshakeMessage {
            node_info,
            protocol_version: PROTOCOL_VERSION,
        });
        
        // Encode
        let frame = Frame::from_message(&msg).unwrap();
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        
        // Decode
        let decoded_frame = Frame::decode(&mut buf).unwrap().unwrap();
        assert!(decoded_frame.verify_checksum());
        
        let decoded_msg = decoded_frame.into_message().unwrap();
        assert_eq!(decoded_msg.message_type(), MessageType::Handshake);
    }
    
    #[test]
    fn test_invalid_magic() {
        let mut buf = BytesMut::new();
        buf.put_u32(0x12345678); // Wrong magic
        buf.put_u8(1);
        buf.put_u8(0x01);
        buf.put_u32(0);
        buf.put_u32(0);
        
        let result = Frame::decode(&mut buf);
        assert!(matches!(result, Err(VistritError::InvalidMagic(_))));
    }
    
    #[test]
    fn test_incomplete_frame() {
        let mut buf = BytesMut::new();
        buf.put_u32(MAGIC_BYTES);
        // Only partial header
        
        let result = Frame::decode(&mut buf);
        assert!(matches!(result, Ok(None)));
    }
    
    #[test]
    fn test_checksum_mismatch() {
        let node_info = NodeInfo::new_worker(test_addr());
        let msg = Message::Handshake(HandshakeMessage {
            node_info,
            protocol_version: PROTOCOL_VERSION,
        });
        
        let frame = Frame::from_message(&msg).unwrap();
        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        
        // Corrupt the checksum
        let len = buf.len();
        buf[len - 1] ^= 0xFF;
        
        let result = Frame::decode(&mut buf);
        assert!(matches!(result, Err(VistritError::ChecksumMismatch { .. })));
    }
}
