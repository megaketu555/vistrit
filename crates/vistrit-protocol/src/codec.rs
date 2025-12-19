//! Tokio codec for VistritProtocol.
//!
//! This module provides a codec implementation that can be used with
//! Tokio's `Framed` for async message sending and receiving.

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{trace, warn};

use crate::frame::Frame;
use crate::messages::Message;
use vistrit_core::{VistritError, Result};

/// Tokio codec for encoding and decoding Vistrit protocol messages
#[derive(Debug, Default)]
pub struct VistritCodec {
    /// Statistics: messages encoded
    pub messages_encoded: u64,
    /// Statistics: messages decoded
    pub messages_decoded: u64,
    /// Statistics: bytes written
    pub bytes_written: u64,
    /// Statistics: bytes read
    pub bytes_read: u64,
}

impl VistritCodec {
    /// Create a new codec instance
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.messages_encoded = 0;
        self.messages_decoded = 0;
        self.bytes_written = 0;
        self.bytes_read = 0;
    }
}

impl Decoder for VistritCodec {
    type Item = Message;
    type Error = VistritError;
    
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        // Try to decode a frame
        match Frame::decode(src) {
            Ok(Some(frame)) => {
                self.messages_decoded += 1;
                self.bytes_read += frame.payload.len() as u64;
                
                trace!(
                    message_type = ?frame.header.message_type,
                    payload_size = frame.payload.len(),
                    "Decoded frame"
                );
                
                // Convert frame to message
                let message = frame.into_message()?;
                Ok(Some(message))
            }
            Ok(None) => {
                // Need more data
                Ok(None)
            }
            Err(e) => {
                warn!(error = %e, "Frame decode error");
                Err(e)
            }
        }
    }
}

impl Encoder<Message> for VistritCodec {
    type Error = VistritError;
    
    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<()> {
        trace!(
            message_type = ?item.message_type(),
            "Encoding message"
        );
        
        // Create frame from message
        let frame = Frame::from_message(&item)?;
        
        // Reserve capacity
        let total_size = frame.header.total_frame_size();
        dst.reserve(total_size);
        
        // Encode frame
        frame.encode(dst);
        
        self.messages_encoded += 1;
        self.bytes_written += frame.payload.len() as u64;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{HandshakeMessage, HeartbeatMessage, NodeInfo, NodeState};
    use crate::frame::PROTOCOL_VERSION;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use vistrit_core::NodeId;
    
    fn test_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7878)
    }
    
    #[test]
    fn test_codec_roundtrip() {
        let mut codec = VistritCodec::new();
        let mut buf = BytesMut::new();
        
        // Create a message
        let node_info = NodeInfo::new_worker(test_addr());
        let msg = Message::Handshake(HandshakeMessage {
            node_info,
            protocol_version: PROTOCOL_VERSION,
        });
        
        // Encode
        codec.encode(msg.clone(), &mut buf).unwrap();
        assert_eq!(codec.messages_encoded, 1);
        
        // Decode
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(codec.messages_decoded, 1);
        
        assert_eq!(decoded.message_type(), msg.message_type());
    }
    
    #[test]
    fn test_codec_multiple_messages() {
        let mut codec = VistritCodec::new();
        let mut buf = BytesMut::new();
        
        // Encode multiple messages
        let node_info = NodeInfo::new_worker(test_addr());
        let msg1 = Message::Handshake(HandshakeMessage {
            node_info,
            protocol_version: PROTOCOL_VERSION,
        });
        
        let msg2 = Message::Heartbeat(HeartbeatMessage {
            node_id: NodeId::new(),
            timestamp: 12345,
            running_tasks: 2,
            state: NodeState::Ready,
            cpu_usage: Some(50),
            memory_usage: Some(30),
        });
        
        codec.encode(msg1, &mut buf).unwrap();
        codec.encode(msg2, &mut buf).unwrap();
        
        assert_eq!(codec.messages_encoded, 2);
        
        // Decode both
        let decoded1 = codec.decode(&mut buf).unwrap().unwrap();
        let decoded2 = codec.decode(&mut buf).unwrap().unwrap();
        
        assert_eq!(codec.messages_decoded, 2);
        assert!(matches!(decoded1, Message::Handshake(_)));
        assert!(matches!(decoded2, Message::Heartbeat(_)));
    }
    
    #[test]
    fn test_codec_partial_data() {
        let mut codec = VistritCodec::new();
        let mut buf = BytesMut::new();
        
        // Create a message
        let node_info = NodeInfo::new_worker(test_addr());
        let msg = Message::Handshake(HandshakeMessage {
            node_info,
            protocol_version: PROTOCOL_VERSION,
        });
        
        // Encode
        codec.encode(msg, &mut buf).unwrap();
        
        // Split the buffer to simulate partial data
        let full_len = buf.len();
        let partial = buf.split_to(full_len / 2);
        
        // Create new buffer with partial data
        let mut partial_buf = BytesMut::from(&partial[..]);
        
        // Should return None (need more data)
        let result = codec.decode(&mut partial_buf).unwrap();
        assert!(result.is_none());
    }
    
    #[test]
    fn test_codec_stats() {
        let mut codec = VistritCodec::new();
        let mut buf = BytesMut::new();
        
        let node_info = NodeInfo::new_worker(test_addr());
        let msg = Message::Handshake(HandshakeMessage {
            node_info,
            protocol_version: PROTOCOL_VERSION,
        });
        
        codec.encode(msg, &mut buf).unwrap();
        codec.decode(&mut buf).unwrap();
        
        assert!(codec.bytes_written > 0);
        assert!(codec.bytes_read > 0);
        
        codec.reset_stats();
        assert_eq!(codec.messages_encoded, 0);
        assert_eq!(codec.messages_decoded, 0);
    }
}
