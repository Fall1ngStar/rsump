use async_trait::async_trait;
use bytes::Bytes;
use eyre::Result;
use fred::{
    prelude::{ClientLike, KeysInterface, RedisClient},
    types::{RedisConfig, Scanner},
};
use futures::StreamExt;
use kdam::{tqdm, BarExt};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, trace};

use crate::{
    traits::{Consumer, Producer, Wrapper},
    types::Payload,
};

pub struct RedisWrapper {
    client: RedisClient,
}

impl RedisWrapper {
    pub fn new(url: &str) -> Result<Self> {
        let config = RedisConfig::from_url(url)?;
        Ok(RedisWrapper {
            client: RedisClient::new(config, None, None),
        })
    }
}

#[async_trait]
impl Producer for RedisWrapper {
    async fn produce(&self, tx: Sender<Payload>) -> Result<()> {
        let _ = self.client.connect();
        let _ = self.client.wait_for_connect().await?;
        let mut stream = self.client.scan("*", Some(1000), None);
        let mut bar = tqdm!();
        while let Some(page) = stream.next().await {
            let mut page = page?;
            let keys = page.take_results().unwrap();
            let size = keys.len();
            let pipe = self.client.pipeline();
            for key in &keys {
                pipe.dump(key).await?;
            }
            let result: Vec<Bytes> = pipe.all().await?;
            for (dumped, key) in result.into_iter().zip(keys) {
                let payload = Payload {
                    key: key.into_bytes(),
                    data: dumped,
                };
                tx.send(payload).await?;
            }
            bar.update(size)?;
            page.next()?;
        }
        Ok(())
    }
}

#[async_trait]
impl Consumer for RedisWrapper {
    async fn consume(&self, mut rx: Receiver<Payload>) -> Result<()> {
        let _ = self.client.connect();
        let _ = self.client.wait_for_connect().await?;
        debug!("Started consuming");
        while let Some(payload) = rx.recv().await {
            trace!("Consuming {:?}", &payload);
            let _ = self
                .client
                .restore(
                    payload.key,
                    0,
                    fred::types::RedisValue::Bytes(payload.data),
                    true,
                    false,
                    None,
                    None,
                )
                .await?;
        }
        debug!("Finished consuming");
        Ok(())
    }
}

impl Wrapper for RedisWrapper {}
