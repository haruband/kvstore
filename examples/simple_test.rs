use anyhow::Error;
use clap::{Args, Command};
use kvstore::KVStore;

#[derive(Debug, Args)]
struct DSOption {
    #[arg(long, help = "Store path")]
    path: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let cmd = Command::new("SimpleTest");
    let cmd = DSOption::augment_args(cmd);
    let args = cmd.get_matches();

    let path = args.get_one::<String>("path").unwrap();

    let store = KVStore::try_new(&path).await?;

    store.set("fruit", "apple".as_bytes()).await?;

    let value = store.get("fruit").await?;

    println!(
        "value={:?}",
        value.map(|value| String::from_utf8(value).unwrap())
    );

    Ok(())
}
