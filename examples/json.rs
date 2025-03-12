use anyhow::Error;
use clap::{Args, Command};
use kvstore::KVStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct JsonValue {
    pub name: String,
    pub score: i64,
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
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let cmd = Command::new("Json");
    let cmd = DSOption::augment_args(cmd);
    let args = cmd.get_matches();

    let path = args.get_one::<String>("path").unwrap();

    let store = KVStore::try_new(&path).await?;

    store
        .set(
            "json/korea",
            JsonValue {
                name: "test0".into(),
                score: 90,
            },
        )
        .await?;
    store
        .set(
            "json/japan",
            JsonValue {
                name: "test1".into(),
                score: 70,
            },
        )
        .await?;
    store
        .set(
            "json/china",
            JsonValue {
                name: "test2".into(),
                score: 85,
            },
        )
        .await?;

    let items = store.get_many::<JsonValue>(Some("json")).await?;
    println!("items={:#?}", items);

    Ok(())
}
