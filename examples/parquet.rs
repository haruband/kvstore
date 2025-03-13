use anyhow::Error;
use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use clap::{Args, Command};
use kvstore::KVStore;
use std::sync::Arc;

#[derive(Debug, Args)]
struct DSOption {
    #[arg(long, help = "Store path")]
    path: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let cmd = Command::new("Parquet");
    let cmd = DSOption::augment_args(cmd);
    let args = cmd.get_matches();

    env_logger::init();

    let path = args.get_one::<String>("path").unwrap();

    let store = KVStore::try_new(&path).await?;
    store.remove_many("/parquet").await?;

    let column = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
    let batch = RecordBatch::try_from_iter([("column", column as ArrayRef)]).unwrap();

    store.set_parquet("/parquet/sample0", vec![batch]).await?;

    let batches = store.get_parquet("/parquet/sample0").await?;
    println!("batches={:#?}", batches);

    Ok(())
}
