use async_trait::async_trait;
use bytes::Bytes;
use eyre::Result;
use fred::{
    prelude::{ClientLike, ClusterInterface, KeysInterface, RedisClient, ServerInterface},
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

    async fn get_cluster_size(&self) -> Result<usize> {
        let nodes = self
            .client
            .cached_cluster_state()
            .unwrap()
            .unique_primary_nodes()
            .into_iter()
            .map(|node| self.client.with_cluster_node(node));
        let mut dbsize = 0;
        for node in nodes {
            let size: usize = node.dbsize().await?;
            dbsize += size;
        }
        Ok(dbsize)
    }
}

#[async_trait]
impl Producer for RedisWrapper {
    async fn produce(&self, tx: Sender<Payload>) -> Result<()> {
        self.client.connect();
        let _ = self.client.wait_for_connect().await?;

        let mut bar = tqdm!(
            desc = "Read from redis",
            total = self.get_cluster_size().await?,
            position = 0,
            unit = "key"
        );

        let mut stream = self.client.scan_cluster("*", Some(1000), None);
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
        self.client.connect();
        let _ = self.client.wait_for_connect().await?;
        debug!("Started consuming");
        let mut pipe = self.client.pipeline();
        let mut counter = 0;
        let mut bar = tqdm!(desc = "Write to redis", position = 1, unit = "keys");
        while let Some(payload) = rx.recv().await {
            trace!("Consuming {:?}", &payload);
            let _ = pipe
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
            counter += 1;
            bar.update(1)?;
            if counter > 1000 {
                pipe.all().await?;
                pipe = self.client.pipeline()
            }
        }
        pipe.all().await?;
        debug!("Finished consuming");
        Ok(())
    }
}

impl Wrapper for RedisWrapper {}
