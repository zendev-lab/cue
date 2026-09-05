use thiserror::Error;

use crate::Message;

pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

pub fn encode_message(message: &Message) -> Result<Vec<u8>, FrameError> {
    let body = serde_json::to_vec(message)?;
    if body.len() > MAX_MESSAGE_SIZE {
        return Err(FrameError::MessageTooLarge {
            actual: body.len(),
            maximum: MAX_MESSAGE_SIZE,
        });
    }
    let length = u32::try_from(body.len()).expect("maximum frame size fits u32");
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

pub fn decode_message(frame: &[u8]) -> Result<Message, FrameError> {
    let header: [u8; 4] = frame
        .get(..4)
        .ok_or(FrameError::MissingLengthPrefix)?
        .try_into()
        .expect("slice length checked");
    let declared = u32::from_be_bytes(header) as usize;
    if declared > MAX_MESSAGE_SIZE {
        return Err(FrameError::MessageTooLarge {
            actual: declared,
            maximum: MAX_MESSAGE_SIZE,
        });
    }
    let actual = frame.len() - 4;
    if actual != declared {
        return Err(FrameError::LengthMismatch { declared, actual });
    }
    Ok(serde_json::from_slice(&frame[4..])?)
}

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("message is missing its four-byte length prefix")]
    MissingLengthPrefix,
    #[error("message contains {actual} bytes; maximum is {maximum}")]
    MessageTooLarge { actual: usize, maximum: usize },
    #[error("message declares {declared} bytes but contains {actual}")]
    LengthMismatch { declared: usize, actual: usize },
    #[error("invalid message JSON: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use crate::{ClientId, Hello, Query, RequestId};

    use super::*;

    fn hello() -> Message {
        Message::Query {
            request_id: RequestId::new(1).unwrap(),
            query: Query::Hello(Hello {
                protocol_version: crate::PROTOCOL_VERSION,
                client_id: ClientId::new("test-client").unwrap(),
            }),
        }
    }

    #[test]
    fn exact_length_prefixed_json_roundtrips() {
        let message = hello();
        let frame = encode_message(&message).unwrap();
        assert_eq!(
            u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize,
            frame.len() - 4
        );
        assert_eq!(decode_message(&frame).unwrap(), message);
    }

    #[test]
    fn decoder_rejects_missing_truncated_and_trailing_bytes() {
        assert!(matches!(
            decode_message(&[0, 0, 0]),
            Err(FrameError::MissingLengthPrefix)
        ));
        let frame = encode_message(&hello()).unwrap();
        assert!(matches!(
            decode_message(&frame[..frame.len() - 1]),
            Err(FrameError::LengthMismatch { .. })
        ));
        let mut trailing = frame;
        trailing.push(0);
        assert!(matches!(
            decode_message(&trailing),
            Err(FrameError::LengthMismatch { .. })
        ));
    }
}
