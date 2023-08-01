use async_trait::async_trait;
use eyre::{eyre, Result};
use fred::{
    prelude::{ClientLike, KeysInterface, RedisClient, RedisError},
    types::{RedisConfig, ScanResult, Scanner},
};
use futures::{Stream, StreamExt, TryStreamExt};
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
        while let Some(page) = stream.next().await {
            let mut page = page?;
            for key in page.take_results().unwrap() {
                let dumped = self.client.dump(&key).await?;
                let payload = Payload {
                    key: key.into_bytes(),
                    data: dumped.into_bytes().unwrap(),
                };
                tx.send(payload).await?;
            }
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
