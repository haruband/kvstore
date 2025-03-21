use anyhow::Error;
use clap::{Args, Command};
use kvstore::KVStoreBuilder;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct JsonValue {
    pub name: String,
    pub score: usize,
}

#[derive(Debug, Args)]
struct KVOption {
    #[arg(long, help = "Store path")]
    path: String,

    #[arg(long, help = "Groups")]
    groups: Option<usize>,

    #[arg(long, help = "Items")]
    items: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let cmd = Command::new("Json");
    let cmd = KVOption::augment_args(cmd);
    let args = cmd.get_matches();

    env_logger::init();

    let path = args.get_one::<String>("path").unwrap();
    let groups = args.get_one::<usize>("groups").cloned().unwrap_or(3);
    let items = args.get_one::<usize>("items").cloned().unwrap_or(4);

    let store = KVStoreBuilder::new().with_caching().build(&path).await?;
    store.remove_many("/json").await?;

    for group in 0..groups {
        for item in 0..items {
            store
                .set_json(
                    &format!("/json/group{}/item{}", group, item),
                    JsonValue {
                        name: format!("item{}", item),
                        score: group * item,
                    },
                )
                .await?;
        }
    }

    let value = store.get_json::<JsonValue>("/json/group0/item0").await?;
    println!("value={:#?}", value);

    let value = store.get("/json/group0/item0").await?;
    println!(
        "value={:#?}",
        value.map(|value| String::from_utf8(value).unwrap())
    );

    Ok(())
}
