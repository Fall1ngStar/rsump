use clap::Parser;
use eyre::Result;
use fred::types::RedisConfig;
use rsump::{
    file::FileWrapper,
    redis::RedisWrapper,
    traits::{Consumer, Producer, Wrapper},
    types::Payload,
};
use tokio::sync::mpsc;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    from: String,

    #[arg(long)]
    to: String,
}

fn to_wrapper(input: &str) -> Result<Box<dyn Wrapper + Sync + Send>> {
    if let Ok(_) = RedisConfig::from_url(input) {
        return Ok(Box::new(RedisWrapper::new(input).unwrap()));
    }
    Ok(Box::new(FileWrapper::new(input)))
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let (tx, rx) = mpsc::channel::<Payload>(1000);
    let producer = to_wrapper(&args.from)?;
    let consumer = to_wrapper(&args.to)?;
    tokio::spawn(async move { producer.produce(tx.clone()).await });
    consumer.consume(rx).await?;
    Ok(())
}
