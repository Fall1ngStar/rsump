use async_trait::async_trait;
use eyre::Result;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::types::Payload;

#[async_trait]
pub trait Consumer {
    async fn consume(&self, chan: Receiver<Payload>) -> Result<()>;
}

#[async_trait]
pub trait Producer {
    async fn produce(&self, chan: Sender<Payload>) -> Result<()>;
}

pub trait Wrapper: Producer + Consumer {}
