use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Payload {
    pub key: Bytes,
    pub data: Bytes,
}
