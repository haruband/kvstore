use anyhow::{anyhow, Error};
use futures::StreamExt;
use futures::TryStreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ClientOptions, GetResultPayload, ObjectStore, PutPayload};
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use url::Url;

pub struct KVStore {
    store: Arc<dyn ObjectStore>,
    prefix: Vec<String>,
}

impl KVStore {
    pub async fn try_new(path: &str) -> Result<Self, Error> {
        let url = match Url::parse(&path) {
            Ok(url) => url,
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                std::fs::create_dir_all(&path)?;
                Url::from_directory_path(std::fs::canonicalize(&path)?)
                    .map_err(|err| anyhow!("invalid path: {:?}", err))?
            }
            Err(err) => return Err(err.into()),
        };

        let scheme = url.scheme();
        let store: Arc<dyn ObjectStore> = match scheme {
            "s3" => {
                let bucket = url.host_str().expect("could not get bucket name");
                let options = ClientOptions::new().with_timeout_disabled();
                let s3 = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_client_options(options)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .with_allow_http(true)
                    .build()?;
                Arc::new(s3)
            }
            "file" => Arc::new(LocalFileSystem::default()),
            _ => return Err(anyhow!("invalid object store")),
        };

        Ok(KVStore {
            store,
            prefix: url.path().split("/").map(|item| item.to_string()).collect(),
        })
    }

    pub async fn set(&self, key: &str, value: impl Into<Vec<u8>>) -> Result<(), Error> {
        self.store
            .put(
                &Path::from_iter(
                    self.prefix
                        .iter()
                        .map(|item| item.as_str())
                        .chain(key.split("/")),
                ),
                PutPayload::from(value.into()),
            )
            .await?;

        Ok(())
    }

    pub async fn get<T: From<Vec<u8>>>(&self, key: &str) -> Result<Option<T>, Error> {
        match self
            .store
            .get(&Path::from_iter(
                self.prefix
                    .iter()
                    .map(|item| item.as_str())
                    .chain(key.split("/")),
            ))
            .await
        {
            Ok(result) => match result.payload {
                GetResultPayload::Stream(_) => {
                    let value = result.bytes().await?;
                    Ok(Some(value.to_vec().into()))
                }
                GetResultPayload::File(mut file, _) => {
                    let mut value = Vec::new();
                    file.read_to_end(&mut value)?;
                    Ok(Some(value.into()))
                }
            },
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn get_many<T: From<Vec<u8>>>(
        &self,
        key: Option<&str>,
    ) -> Result<HashMap<String, T>, Error> {
        let parts: Vec<_> = if let Some(key) = key {
            self.prefix
                .iter()
                .map(|item| item.as_str())
                .chain(key.split("/"))
                .collect()
        } else {
            self.prefix.iter().map(|item| item.as_str()).collect()
        };

        let keys = self
            .store
            .list_with_delimiter(Some(&Path::from_iter(parts)))
            .await?
            .objects
            .iter()
            .map(|object| object.location.to_string())
            .collect::<Vec<_>>();

        let values = futures::stream::iter(keys.clone())
            .map(|key| async {
                let store = self.store.clone();

                tokio::task::spawn(async move {
                    match store.get(&Path::from(key)).await {
                        Ok(result) => match result.payload {
                            GetResultPayload::Stream(_) => {
                                let value = result.bytes().await?;
                                Ok::<Option<Vec<u8>>, Error>(Some(value.to_vec()))
                            }
                            GetResultPayload::File(mut file, _) => {
                                let mut value = Vec::new();
                                file.read_to_end(&mut value)?;
                                Ok(Some(value))
                            }
                        },
                        Err(object_store::Error::NotFound { .. }) => Ok(None),
                        Err(err) => Err(err.into()),
                    }
                })
                .await
                .unwrap()
            })
            .boxed()
            .buffer_unordered(num_cpus::get())
            .try_collect::<Vec<_>>()
            .await?;

        let mut items = HashMap::new();

        for (key, value) in keys.into_iter().zip(values.into_iter()) {
            match value {
                Some(value) => {
                    items.insert(key.into(), value.into());
                }
                None => {}
            }
        }

        Ok(items)
    }

    pub async fn list(&self, key: Option<&str>) -> Result<Vec<String>, Error> {
        let parts: Vec<_> = if let Some(key) = key {
            self.prefix
                .iter()
                .map(|item| item.as_str())
                .chain(key.split("/"))
                .collect()
        } else {
            self.prefix.iter().map(|item| item.as_str()).collect()
        };

        Ok(self
            .store
            .list_with_delimiter(Some(&Path::from_iter(parts)))
            .await?
            .objects
            .iter()
            .map(|object| object.location.to_string())
            .collect())
    }
}
