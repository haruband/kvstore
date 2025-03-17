use anyhow::{anyhow, Error};
use futures::StreamExt;
use futures::TryStreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ClientOptions, GetResultPayload, ObjectStore, PutPayload};
use std::io::Read;
use std::sync::Arc;
use url::Url;

pub struct KVStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,

    parallelism: usize,
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
            "memory" => Arc::new(InMemory::default()),
            _ => return Err(anyhow!("invalid object store")),
        };

        let prefix = url
            .path()
            .trim_start_matches(|c: char| c == '/')
            .trim_end_matches(|c: char| c == '/')
            .into();

        log::debug!("base={:?}", prefix);

        Ok(KVStore {
            store,
            prefix,
            parallelism: num_cpus::get(),
        })
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }
}

impl KVStore {
    pub async fn set(&self, key: &str, value: impl Into<Vec<u8>>) -> Result<(), Error> {
        let key = format!("{}{}", self.prefix, key);

        log::debug!("set={:?}", key);

        self.store
            .put(&Path::from(key), PutPayload::from(value.into()))
            .await?;

        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let key = format!("{}{}", self.prefix, key);

        log::debug!("get={:?}", key);

        match self.store.get(&Path::from(key)).await {
            Ok(result) => match result.payload {
                GetResultPayload::Stream(_) => {
                    let value = result.bytes().await?;
                    Ok(Some(value.to_vec()))
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
    }

    pub async fn get_many(&self, key: Option<&str>) -> Result<Vec<Vec<u8>>, Error> {
        let key = match key {
            Some(key) => format!("{}{}", self.prefix, key),
            None => self.prefix.clone(),
        };

        log::debug!("get_many={:?}", key);

        let items = self
            .store
            .list(Some(&Path::from(key)))
            .map(|object| async {
                match self.store.get(&object?.location).await {
                    Ok(result) => match result.payload {
                        GetResultPayload::Stream(_) => {
                            let value = result.bytes().await?;
                            Ok::<Vec<u8>, Error>(value.to_vec())
                        }
                        GetResultPayload::File(mut file, _) => {
                            let mut value = Vec::new();
                            file.read_to_end(&mut value)?;
                            Ok(value)
                        }
                    },
                    Err(err) => Err(err.into()),
                }
            })
            .boxed()
            .buffer_unordered(self.parallelism)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(items)
    }

    pub async fn list(&self, key: Option<&str>) -> Result<Vec<String>, Error> {
        let key = match key {
            Some(key) => format!("{}{}", self.prefix, key),
            None => self.prefix.clone(),
        };

        log::debug!("list={:?}", key);

        if self.prefix.is_empty() {
            Ok(self
                .store
                .list(Some(&Path::from(key)))
                .map(|object| async {
                    let path = object?.location.to_string();

                    Ok::<String, Error>(format!("/{}", path))
                })
                .boxed()
                .buffer_unordered(self.parallelism)
                .try_collect::<Vec<_>>()
                .await?)
        } else {
            Ok(self
                .store
                .list(Some(&Path::from(key)))
                .map(|object| async {
                    let path = object?.location.to_string();

                    Ok::<String, Error>(
                        path.strip_prefix(&self.prefix)
                            .map_or(path.clone(), |path| path.into()),
                    )
                })
                .boxed()
                .buffer_unordered(self.parallelism)
                .try_collect::<Vec<_>>()
                .await?)
        }
    }

    pub async fn rename_many(&self, from: &str, to: &str) -> Result<(), Error> {
        let from = format!("{}{}", self.prefix, from);
        let to = format!("{}{}", self.prefix, to);

        log::debug!("rename_many={:?}=={:?}", from, to);

        self.store
            .list(Some(&Path::from(from.clone())))
            .map(|object| async {
                let path0 = object?.location;
                let path1 = Path::from(path0.to_string().replace(&from, &to));

                self.store.rename(&path0, &path1).await?;

                Ok::<(), Error>(())
            })
            .boxed()
            .buffer_unordered(self.parallelism)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(())
    }

    pub async fn rename(&self, from: &str, to: &str) -> Result<(), Error> {
        let from = format!("{}{}", self.prefix, from);
        let to = format!("{}{}", self.prefix, to);

        log::debug!("rename={:?}=={:?}", from, to);

        self.store
            .rename(&Path::from(from), &Path::from(to))
            .await?;

        Ok(())
    }

    pub async fn remove_many(&self, key: &str) -> Result<(), Error> {
        let key = format!("{}{}", self.prefix, key);

        log::debug!("remove_many={:?}", key);

        self.store
            .list(Some(&Path::from(key)))
            .map(|object| async {
                self.store.delete(&object?.location).await?;

                Ok::<(), Error>(())
            })
            .boxed()
            .buffer_unordered(self.parallelism)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(())
    }

    pub async fn remove(&self, key: &str) -> Result<(), Error> {
        let key = format!("{}{}", self.prefix, key);

        log::debug!("remove={:?}", key);

        self.store.delete(&Path::from(key)).await?;

        Ok(())
    }
}

#[cfg(feature = "json")]
impl KVStore {
    pub async fn set_json<T: serde::Serialize>(&self, key: &str, value: T) -> Result<(), Error> {
        let key = format!("{}{}", self.prefix, key);

        log::debug!("set_json={:?}", key);

        self.store
            .put(
                &Path::from(key),
                PutPayload::from(serde_json::to_string(&value)?),
            )
            .await?;

        Ok(())
    }

    pub async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, Error> {
        let key = format!("{}{}", self.prefix, key);

        log::debug!("get_json={:?}", key);

        match self.store.get(&Path::from(key)).await {
            Ok(result) => match result.payload {
                GetResultPayload::Stream(_) => {
                    let value = result.bytes().await?;
                    Ok(Some(serde_json::from_slice(&value)?))
                }
                GetResultPayload::File(mut file, _) => {
                    let mut value = Vec::new();
                    file.read_to_end(&mut value)?;
                    Ok(Some(serde_json::from_slice(&value)?))
                }
            },
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn get_json_many<T: Send + 'static + serde::de::DeserializeOwned>(
        &self,
        key: Option<&str>,
    ) -> Result<Vec<T>, Error> {
        let key = match key {
            Some(key) => format!("{}{}", self.prefix, key),
            None => self.prefix.clone(),
        };

        log::debug!("get_json_many={:?}", key);

        let items = self
            .store
            .list(Some(&Path::from(key)))
            .map(|object| async {
                match self.store.get(&object?.location).await {
                    Ok(result) => match result.payload {
                        GetResultPayload::Stream(_) => {
                            let value = result.bytes().await?;
                            Ok::<T, Error>(serde_json::from_slice(&value)?)
                        }
                        GetResultPayload::File(mut file, _) => {
                            let mut value = Vec::new();
                            file.read_to_end(&mut value)?;
                            Ok(serde_json::from_slice(&value)?)
                        }
                    },
                    Err(err) => Err(err.into()),
                }
            })
            .boxed()
            .buffer_unordered(self.parallelism)
            .try_collect::<Vec<_>>()
            .await?;

        Ok(items)
    }
}

#[cfg(feature = "parquet")]
impl KVStore {
    pub async fn set_parquet(
        &self,
        key: &str,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Result<(), Error> {
        use parquet::arrow::AsyncArrowWriter;

        let key = format!("{}{}", self.prefix, key);

        log::debug!("set_parquet={:?}", key);

        if let Some(batch) = batches.first() {
            let mut buffer = Vec::new();
            let mut writer = AsyncArrowWriter::try_new(&mut buffer, batch.schema(), None)?;
            for batch in batches {
                writer.write(&batch).await?;
            }
            writer.close().await?;

            self.store
                .put(&Path::from(key), PutPayload::from(buffer))
                .await?;
        }

        Ok(())
    }

    pub async fn get_parquet(
        &self,
        key: &str,
    ) -> Result<Option<Vec<arrow::record_batch::RecordBatch>>, Error> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
        use parquet::arrow::ParquetRecordBatchStreamBuilder;

        let key = format!("{}{}", self.prefix, key);

        log::debug!("get_parquet={:?}", key);

        match self.store.get(&Path::from(key)).await {
            Ok(result) => match result.payload {
                GetResultPayload::Stream(_) => {
                    let stream = ParquetRecordBatchReader::try_new(result.bytes().await?, 1024)?;
                    let batches = stream.flatten().collect::<Vec<_>>();
                    Ok(Some(batches))
                }
                GetResultPayload::File(file, _) => {
                    let builder =
                        ParquetRecordBatchStreamBuilder::new(tokio::fs::File::from_std(file))
                            .await?;
                    let stream = builder.build()?;
                    let batches = stream.try_collect::<Vec<_>>().await?;
                    Ok(Some(batches))
                }
            },
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simple() {
        let store = KVStore::try_new("memory://").await.unwrap();

        store.set("/test/group0/key0", "value0").await.unwrap();
        store.set("/test/group0/key1", "value1").await.unwrap();
        store.set("/test/group0/key2", "value2").await.unwrap();

        let item = store.get("/test/group0/key0").await.unwrap();
        assert_eq!(item, Some("value0".as_bytes().to_vec()));

        let item = store.get("/test/group0/key1").await.unwrap();
        assert_eq!(item, Some("value1".as_bytes().to_vec()));

        let items = store.list(Some("/test")).await.unwrap();
        assert_eq!(
            items,
            vec![
                "/test/group0/key0",
                "/test/group0/key1",
                "/test/group0/key2"
            ]
        )
    }
}
