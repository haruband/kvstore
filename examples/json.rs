use anyhow::Error;
use clap::{Args, Command};
use kvstore::KVStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct JsonValue {
    pub name: String,
    pub score: usize,
}

impl From<Vec<u8>> for JsonValue {
    fn from(v: Vec<u8>) -> Self {
        serde_json::from_slice(&v).unwrap()
    }
}

impl Into<Vec<u8>> for JsonValue {
    fn into(self) -> Vec<u8> {
        serde_json::to_string(&self).unwrap().into()
    }
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

    for group in 0..groups {
        for item in 0..items {
            store
                .set(
                    &format!("json/group{}/item{}", group, item),
                    JsonValue {
                        name: format!("item{}", item),
                        score: group * item,
                    },
                )
                .await?;
        }
    }

    let items = store.get_many::<JsonValue>(Some("json")).await?;
    println!("items={:#?}", items);

    Ok(())
}
