use anyhow::{anyhow, Error};
use arrow::datatypes::Schema;
use futures::{Future, StreamExt, TryStreamExt};
use moka::sync::Cache;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    ClientOptions, GetOptions, GetResult, GetResultPayload, ObjectStore, PutPayload,
};
use std::any::Any;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

#[derive(Debug, Clone)]
struct KVEntry {
    tag: String,
    value: Arc<dyn Any + Send + Sync>,
}

pub struct KVStoreBuilder {
    max_capacity: Option<u64>,
    time_to_live: Option<Duration>,
    parallelism: usize,
    enable_caching: bool,
}

impl Default for KVStoreBuilder {
    fn default() -> Self {
        Self {
            max_capacity: None,
            time_to_live: None,
            parallelism: num_cpus::get(),
            enable_caching: false,
        }
    }
}

impl KVStoreBuilder {
    pub fn new() -> Self {
        KVStoreBuilder::default()
    }

    pub fn with_max_capacity(mut self, max_capacity: u64) -> Self {
        self.max_capacity = Some(max_capacity);
        self
    }

    pub fn with_time_to_live(mut self, time_to_live: Duration) -> Self {
        self.time_to_live = Some(time_to_live);
        self
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }

    pub fn with_caching(mut self) -> Self {
        self.enable_caching = true;
        self
    }

    pub async fn build(self, path: &str) -> Result<KVStore, Error> {
        let url = match Url::parse(path) {
            Ok(url) => url,
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                std::fs::create_dir_all(path)?;
                Url::from_directory_path(std::fs::canonicalize(path)?)
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

        let mut builder = Cache::<String, KVEntry>::builder();
        builder = builder.max_capacity(self.max_capacity.unwrap_or(100));
        if let Some(time_to_live) = self.time_to_live {
            builder = builder.time_to_live(time_to_live);
        }
        let caches = builder.build();

        Ok(KVStore {
            store,
            prefix,
            caches,
            parallelism: self.parallelism,
            enable_caching: self.enable_caching,
        })
    }
}

pub struct KVStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,

    caches: Cache<String, KVEntry>,

    parallelism: usize,
    enable_caching: bool,
}

impl KVStore {
    async fn set_inner<T: Clone + std::marker::Send + std::marker::Sync + 'static, F, R>(
        &self,
        key: &str,
        value: T,
        encode: F,
    ) -> Result<(), Error>
    where
        F: Fn(T) -> R,
        R: Future<Output = Result<PutPayload, Error>>,
    {
        let key = format!("{}{}", self.prefix, key);

        log::debug!("set={:?}", key);

        let result = self
            .store
            .put(&Path::from(key.clone()), encode(value.clone()).await?)
            .await?;
        if let Some(tag) = result.e_tag {
            self.caches.insert(
                key,
                KVEntry {
                    tag: tag,
                    value: Arc::new(value),
                },
            );
        }

        Ok(())
    }

    async fn get_inner<T: Clone + std::marker::Send + std::marker::Sync + 'static, F, R>(
        &self,
        key: &str,
        decode: F,
    ) -> Result<Option<T>, Error>
    where
        F: Fn(GetResult) -> R,
        R: Future<Output = Result<T, Error>>,
    {
        let key = format!("{}{}", self.prefix, key);

        log::debug!("get={:?}", key);

        macro_rules! decode_and_update {
            ($result:expr) => {{
                let object = $result.meta.clone();
                let value = decode($result).await?;
                if let Some(tag) = object.e_tag {
                    self.caches.insert(
                        key,
                        KVEntry {
                            tag: tag,
                            value: Arc::new(value.clone()),
                        },
                    );
                }
                value
            }};
        }

        match self.caches.get(&key) {
            Some(entry) => {
                if self.enable_caching {
                    match entry.value.downcast_ref::<T>().cloned() {
                        value @ Some(_) => {
                            log::debug!("cached={:?}", key);

                            Ok(value)
                        }
                        None => match self.store.get(&Path::from(key.clone())).await {
                            Ok(result) => Ok(Some(decode_and_update!(result))),
                            Err(object_store::Error::NotFound { .. }) => Ok(None),
                            Err(err) => Err(err.into()),
                        },
                    }
                } else {
                    match self
                        .store
                        .get_opts(
                            &Path::from(key.clone()),
                            GetOptions {
                                if_none_match: Some(entry.tag.clone()),
                                ..GetOptions::default()
                            },
                        )
                        .await
                    {
                        Ok(result) => Ok(Some(decode_and_update!(result))),
                        Err(object_store::Error::NotModified { .. }) => {
                            match entry.value.downcast_ref::<T>().cloned() {
                                value @ Some(_) => {
                                    log::debug!("unmodified={:?}", key);

                                    Ok(value)
                                }
                                None => match self.store.get(&Path::from(key.clone())).await {
                                    Ok(result) => Ok(Some(decode_and_update!(result))),
                                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                                    Err(err) => Err(err.into()),
                                },
                            }
                        }
                        Err(object_store::Error::NotFound { .. }) => Ok(None),
                        Err(err) => Err(err.into()),
                    }
                }
            }
            None => match self.store.get(&Path::from(key.clone())).await {
                Ok(result) => Ok(Some(decode_and_update!(result))),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(err) => Err(err.into()),
            },
        }
    }

    pub async fn set(&self, key: &str, value: impl Into<Vec<u8>>) -> Result<(), Error> {
        self.set_inner(
            key,
            value.into(),
            async |value: Vec<u8>| -> Result<PutPayload, Error> { Ok(PutPayload::from(value)) },
        )
        .await
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.get_inner(key, async |result: GetResult| -> Result<Vec<u8>, Error> {
            let value = match result.payload {
                GetResultPayload::Stream(_) => {
                    let value = result.bytes().await?;
                    value.to_vec()
                }
                GetResultPayload::File(mut file, _) => {
                    let mut value = Vec::new();
                    file.read_to_end(&mut value)?;
                    value
                }
            };

            Ok(value)
        })
        .await
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
        if self.prefix.is_empty() {
            let from = from.trim_start_matches(|c: char| c == '/').to_string();
            let to = to.trim_start_matches(|c: char| c == '/').to_string();

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
        } else {
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
        }

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
    pub async fn set_json<
        T: serde::Serialize + Clone + std::marker::Send + std::marker::Sync + 'static,
    >(
        &self,
        key: &str,
        value: T,
    ) -> Result<(), Error> {
        self.set_inner(key, value, async |value: T| -> Result<PutPayload, Error> {
            Ok(PutPayload::from(serde_json::to_string(&value)?))
        })
        .await
    }

    pub async fn get_json<
        T: serde::de::DeserializeOwned + Clone + std::marker::Send + std::marker::Sync + 'static,
    >(
        &self,
        key: &str,
    ) -> Result<Option<T>, Error> {
        self.get_inner(key, async |result: GetResult| -> Result<T, Error> {
            let value = match result.payload {
                GetResultPayload::Stream(_) => {
                    let value = result.bytes().await?;
                    serde_json::from_slice(&value)?
                }
                GetResultPayload::File(mut file, _) => {
                    let mut value = Vec::new();
                    file.read_to_end(&mut value)?;
                    serde_json::from_slice(&value)?
                }
            };

            Ok(value)
        })
        .await
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

        self.set_inner(
            key,
            batches,
            async |batches: Vec<arrow::record_batch::RecordBatch>| -> Result<PutPayload, Error> {
                let mut buffer = Vec::new();
                let mut writer = AsyncArrowWriter::try_new(
                    &mut buffer,
                    batches
                        .first()
                        .map_or(Arc::new(Schema::empty()), |batch| batch.schema()),
                    None,
                )?;
                for batch in batches {
                    writer.write(&batch).await?;
                }
                writer.close().await?;

                Ok(PutPayload::from(buffer))
            },
        )
        .await
    }

    pub async fn get_parquet(
        &self,
        key: &str,
    ) -> Result<Option<Vec<arrow::record_batch::RecordBatch>>, Error> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
        use parquet::arrow::ParquetRecordBatchStreamBuilder;

        self.get_inner(
            key,
            async |result: GetResult| -> Result<Vec<arrow::record_batch::RecordBatch>, Error> {
                let batches = match result.payload {
                    GetResultPayload::Stream(_) => {
                        let stream =
                            ParquetRecordBatchReader::try_new(result.bytes().await?, 1024)?;
                        let batches = stream.flatten().collect::<Vec<_>>();
                        batches
                    }
                    GetResultPayload::File(file, _) => {
                        let builder =
                            ParquetRecordBatchStreamBuilder::new(tokio::fs::File::from_std(file))
                                .await?;
                        let stream = builder.build()?;
                        let batches = stream.try_collect::<Vec<_>>().await?;
                        batches
                    }
                };

                Ok(batches)
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simple() {
        let store = KVStoreBuilder::new().build("memory://").await.unwrap();

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
