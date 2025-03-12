use anyhow::{anyhow, Error};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use futures::TryStreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ClientOptions, GetResultPayload, ObjectStore, PutPayload};
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use url::Url;

pub struct KVStore {
    store: Arc<dyn ObjectStore>,
    url: Url,
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

        Ok(KVStore { store, url })
    }

    pub async fn set(&self, key: &str, value: impl Into<Vec<u8>>) -> Result<(), Error> {
        self.store
            .put(
                &Path::from(self.url.join(key)?.path()),
                PutPayload::from(value.into()),
            )
            .await?;

        Ok(())
    }

    pub async fn get<T: From<Vec<u8>>>(&self, key: &str) -> Result<Option<T>, Error> {
        match self
            .store
            .get(&Path::from(self.url.join(key)?.path()))
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

    pub async fn get_many<T: Send + 'static + From<Vec<u8>>>(
        &self,
        key: Option<&str>,
    ) -> Result<HashMap<String, T>, Error> {
        let url = match key {
            Some(key) => self.url.join(key)?,
            None => self.url.clone(),
        };

        let items = self
            .store
            .list(Some(&Path::from(url.path())))
            .map(|object| async {
                let object = object?;
                let store = self.store.clone();

                tokio::task::spawn(async move {
                    match store.get(&object.location).await {
                        Ok(result) => match result.payload {
                            GetResultPayload::Stream(_) => {
                                let value = result.bytes().await?;
                                Ok::<(String, T), Error>((
                                    object.location.to_string(),
                                    value.to_vec().into(),
                                ))
                            }
                            GetResultPayload::File(mut file, _) => {
                                let mut value = Vec::new();
                                file.read_to_end(&mut value)?;
                                Ok((object.location.to_string(), value.into()))
                            }
                        },
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

        Ok(items.into_iter().collect())
    }

    pub async fn list(&self, key: Option<&str>) -> Result<Vec<String>, Error> {
        let url = match key {
            Some(key) => self.url.join(key)?,
            None => self.url.clone(),
        };

        Ok(self
            .store
            .list(Some(&Path::from(url.path())))
            .map(|object| async { Ok::<String, Error>(object?.location.to_string()) })
            .boxed()
            .buffer_unordered(num_cpus::get())
            .try_collect::<Vec<_>>()
            .await?)
    }
}

#[cfg(feature = "parquet")]
impl KVStore {
    pub async fn set_parquet(&self, key: &str, batches: Vec<RecordBatch>) -> Result<(), Error> {
        let mut buffer = Vec::new();
        let mut writer =
            AsyncArrowWriter::try_new(&mut buffer, batches.first().unwrap().schema(), None)
                .unwrap();
        for batch in batches {
            writer.write(&batch).await.unwrap();
        }
        writer.close().await.unwrap();

        self.store
            .put(
                &Path::from(self.url.join(key)?.path()),
                PutPayload::from(buffer),
            )
            .await?;

        Ok(())
    }

    pub async fn get_parquet(&self, key: &str) -> Result<Option<Vec<RecordBatch>>, Error> {
        match self
            .store
            .get(&Path::from(self.url.join(key)?.path()))
            .await
        {
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
