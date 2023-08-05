use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
};

use async_trait::async_trait;
use eyre::Result;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    traits::{Consumer, Producer, Wrapper},
    types::Payload,
};

pub struct FileWrapper {
    file_path: String,
}

impl FileWrapper {
    pub fn new(file_path: &str) -> Self {
        Self {
            file_path: file_path.to_owned(),
        }
    }
}

#[async_trait]
impl Producer for FileWrapper {
    async fn produce(&self, tx: Sender<Payload>) -> Result<()> {
        let file = File::open(&self.file_path)?;
        let mut reader = BufReader::new(file);
        while let Ok(payload) = rmp_serde::from_read::<_, Payload>(&mut reader) {
            tx.send(payload).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Consumer for FileWrapper {
    async fn consume(&self, mut rx: Receiver<Payload>) -> Result<()> {
        let file = File::create(&self.file_path)?;
        let mut writer = BufWriter::new(file);
        while let Some(payload) = rx.recv().await {
            let raw_data = rmp_serde::to_vec(&payload)?;
            writer.write_all(&raw_data)?;
        }
        Ok(())
    }
}

impl Wrapper for FileWrapper {}
