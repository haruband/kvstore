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

    store.set("simple/asia/korea", "apple").await?;
    store.set("simple/asia/japan", "mango").await?;
    store.set("simple/europe/england", "melon").await?;
    store.set("simple/europe/france", "orange").await?;

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

    store
        .rename_recursive("simple/europe", "simple/asia")
        .await?;
    store
        .rename("simple/asia/france", "simple/asia/italy")
        .await?;

    let item = store.get("simple/asia/italy").await?;
    println!(
        "item={:?}",
        item.map(|item| String::from_utf8(item).unwrap())
    );

    Ok(())
}
