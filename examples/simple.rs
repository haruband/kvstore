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
    let cmd = Command::new("Simple");
    let cmd = DSOption::augment_args(cmd);
    let args = cmd.get_matches();

    let path = args.get_one::<String>("path").unwrap();

    let store = KVStore::try_new(&path).await?;

    store.set("simple/korea", "apple").await?;
    store.set("simple/japan", "mango").await?;
    store.set("simple/china", "melon").await?;

    let items = store.list(Some("simple")).await?;
    println!("items={:#?}", items);

    let items = store.get_many(Some("simple")).await?;
    println!(
        "items={:#?}",
        items
            .iter()
            .map(|item| String::from_utf8(item.clone()).unwrap())
            .collect::<Vec<_>>()
    );

    store.rename("simple/korea", "simple/russia").await?;

    let item = store.get("simple/russia").await?;
    println!(
        "item={:?}",
        item.map(|item| String::from_utf8(item).unwrap())
    );

    Ok(())
}
