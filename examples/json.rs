use anyhow::Error;
use clap::{Args, Command};
use kvstore::KVStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct JsonValue {
    pub name: String,
    pub score: usize,
}

#[derive(Debug, Args)]
struct DSOption {
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
    let cmd = DSOption::augment_args(cmd);
    let args = cmd.get_matches();

    let path = args.get_one::<String>("path").unwrap();
    let groups = args.get_one::<usize>("groups").cloned().unwrap_or(3);
    let items = args.get_one::<usize>("items").cloned().unwrap_or(4);

    let store = KVStore::try_new(&path).await?;
    store.remove_recursive("json").await?;

    for group in 0..groups {
        for item in 0..items {
            store
                .set_json(
                    &format!("json/group{}/item{}", group, item),
                    JsonValue {
                        name: format!("item{}", item),
                        score: group * item,
                    },
                )
                .await?;
        }
    }

    let values = store
        .get_json_many::<JsonValue>(Some("json/group0"))
        .await?;
    println!("values={:#?}", values);

    Ok(())
}
