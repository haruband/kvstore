use anyhow::Error;
use clap::{Args, Command};
use kvstore::KVStoreBuilder;

#[derive(Debug, Args)]
struct KVOption {
    #[arg(long, help = "Store path")]
    path: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let cmd = Command::new("Simple");
    let cmd = KVOption::augment_args(cmd);
    let args = cmd.get_matches();

    env_logger::init();

    let path = args.get_one::<String>("path").unwrap();

    let store = KVStoreBuilder::new().build(&path).await?;
    store.remove_many("/simple").await?;

    store.set("/simple/asia/korea", "apple").await?;
    store.set("/simple/asia/japan", "mango").await?;
    store.set("/simple/europe/england", "melon").await?;
    store.set("/simple/europe/france", "orange").await?;

    let items = store.list(Some("/simple")).await?;
    println!("items={:#?}", items);

    store.rename_many("/simple/europe", "/europe").await?;
    store.rename("/europe/france", "/europe/italy").await?;

    let item = store.get("/europe/italy").await?;
    println!(
        "item={:?}",
        item.map(|item| String::from_utf8(item).unwrap())
    );
    let item = store.get("/europe/italy").await?;
    println!(
        "item={:?}",
        item.map(|item| String::from_utf8(item).unwrap())
    );

    store.set("/europe/italy", "kiwi").await?;
    let item = store.get("/europe/italy").await?;
    println!(
        "item={:?}",
        item.map(|item| String::from_utf8(item).unwrap())
    );

    store.remove_many("/").await?;

    Ok(())
}
