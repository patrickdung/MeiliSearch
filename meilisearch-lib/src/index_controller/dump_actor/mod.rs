use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use log::{info, trace, warn};
use serde::{Deserialize, Serialize};
use tokio::fs::create_dir_all;

use loaders::v1::MetadataV1;
use loaders::v2::MetadataV2;

pub use actor::DumpActor;
pub use handle_impl::*;
pub use message::DumpMsg;

use super::index_resolver::index_store::IndexStore;
use super::index_resolver::uuid_store::UuidStore;
use super::index_resolver::IndexResolver;
use super::updates::UpdateSender;
use crate::compression::{from_tar_gz, to_tar_gz};
use crate::index_controller::dump_actor::error::DumpActorError;
use crate::index_controller::updates::UpdateMsg;
use crate::options::IndexerOpts;
use error::Result;

mod actor;
pub mod error;
mod handle_impl;
mod loaders;
mod message;

const META_FILE_NAME: &str = "metadata.json";

#[async_trait::async_trait]
pub trait DumpActorHandle {
    /// Start the creation of a dump
    /// Implementation: [handle_impl::DumpActorHandleImpl::create_dump]
    async fn create_dump(&self) -> Result<DumpInfo>;

    /// Return the status of an already created dump
    /// Implementation: [handle_impl::DumpActorHandleImpl::dump_info]
    async fn dump_info(&self, uid: String) -> Result<DumpInfo>;
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "dumpVersion")]
pub enum Metadata {
    V1(MetadataV1),
    V2(MetadataV2),
}

impl Metadata {
    pub fn new_v2(index_db_size: usize, update_db_size: usize) -> Self {
        let meta = MetadataV2::new(index_db_size, update_db_size);
        Self::V2(meta)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DumpStatus {
    Done,
    InProgress,
    Failed,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DumpInfo {
    pub uid: String,
    pub status: DumpStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    finished_at: Option<DateTime<Utc>>,
}

impl DumpInfo {
    pub fn new(uid: String, status: DumpStatus) -> Self {
        Self {
            uid,
            status,
            error: None,
            started_at: Utc::now(),
            finished_at: None,
        }
    }

    pub fn with_error(&mut self, error: String) {
        self.status = DumpStatus::Failed;
        self.finished_at = Some(Utc::now());
        self.error = Some(error);
    }

    pub fn done(&mut self) {
        self.finished_at = Some(Utc::now());
        self.status = DumpStatus::Done;
    }

    pub fn dump_already_in_progress(&self) -> bool {
        self.status == DumpStatus::InProgress
    }
}

pub fn load_dump(
    dst_path: impl AsRef<Path>,
    src_path: impl AsRef<Path>,
    index_db_size: usize,
    update_db_size: usize,
    indexer_opts: &IndexerOpts,
) -> anyhow::Result<()> {
    let tmp_src = tempfile::tempdir()?;
    let tmp_src_path = tmp_src.path();

    from_tar_gz(&src_path, tmp_src_path)?;

    let meta_path = tmp_src_path.join(META_FILE_NAME);
    let mut meta_file = File::open(&meta_path)?;
    let meta: Metadata = serde_json::from_reader(&mut meta_file)?;

    let tmp_dst = tempfile::tempdir()?;

    match meta {
        Metadata::V1(meta) => {
            meta.load_dump(&tmp_src_path, tmp_dst.path(), index_db_size, indexer_opts)?
        }
        Metadata::V2(meta) => meta.load_dump(
            &tmp_src_path,
            tmp_dst.path(),
            index_db_size,
            update_db_size,
            indexer_opts,
        )?,
    }
    // Persist and atomically rename the db
    let persisted_dump = tmp_dst.into_path();
    if dst_path.as_ref().exists() {
        warn!("Overwriting database at {}", dst_path.as_ref().display());
        std::fs::remove_dir_all(&dst_path)?;
    }

    std::fs::rename(&persisted_dump, &dst_path)?;

    Ok(())
}

struct DumpTask<U, I> {
    path: PathBuf,
    index_resolver: Arc<IndexResolver<U, I>>,
    update_sender: UpdateSender,
    uid: String,
    update_db_size: usize,
    index_db_size: usize,
}

impl<U, I> DumpTask<U, I>
where
    U: UuidStore + Sync + Send + 'static,
    I: IndexStore + Sync + Send + 'static,
{
    async fn run(self) -> Result<()> {
        trace!("Performing dump.");

        create_dir_all(&self.path).await?;

        let temp_dump_dir = tokio::task::spawn_blocking(tempfile::TempDir::new).await??;
        let temp_dump_path = temp_dump_dir.path().to_owned();

        let meta = Metadata::new_v2(self.index_db_size, self.update_db_size);
        let meta_path = temp_dump_path.join(META_FILE_NAME);
        let mut meta_file = File::create(&meta_path)?;
        serde_json::to_writer(&mut meta_file, &meta)?;

        let uuids = self.index_resolver.dump(temp_dump_path.clone()).await?;

        UpdateMsg::dump(&self.update_sender, uuids, temp_dump_path.clone()).await?;

        let dump_path = tokio::task::spawn_blocking(move || -> Result<PathBuf> {
            let temp_dump_file = tempfile::NamedTempFile::new()?;
            to_tar_gz(temp_dump_path, temp_dump_file.path())
                .map_err(|e| DumpActorError::Internal(e.into()))?;

            let dump_path = self.path.join(self.uid).with_extension("dump");
            temp_dump_file.persist(&dump_path)?;

            Ok(dump_path)
        })
        .await??;

        info!("Created dump in {:?}.", dump_path);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use futures::future::ok;
    use uuid::Uuid;

    use super::*;
    use crate::index::test::Mocker;
    use crate::index::Index;
    use crate::index_controller::index_resolver::index_store::MockIndexStore;
    use crate::index_controller::index_resolver::uuid_store::MockUuidStore;
    use crate::index_controller::updates::create_update_handler;
    use crate::index::error::Result as IndexResult;

    #[actix_rt::test]
    async fn test_dump_normal() {
        let tmp = tempfile::tempdir().unwrap();

        let uuids = std::iter::repeat_with(Uuid::new_v4)
            .take(4)
            .collect::<HashSet<_>>();
        let mut uuid_store = MockUuidStore::new();
        let uuids_cloned = uuids.clone();
        uuid_store
            .expect_dump()
            .once()
            .returning(move |_| Box::pin(ok(uuids_cloned.clone())));

        let mut index_store = MockIndexStore::new();
        index_store.expect_get().times(4).returning(move |uuid| {
            let mocker = Mocker::default();
            let uuids_clone = uuids.clone();
            mocker.when::<(), Uuid>("uuid").once().then(move |_| {
                assert!(uuids_clone.contains(&uuid));
                uuid
            });
            mocker.when::<&Path, IndexResult<()>>("dump").once().then(move |_| {
                Ok(())
            });
            Box::pin(ok(Some(Index::faux(mocker))))
        });

        let index_resolver = Arc::new(IndexResolver::new(uuid_store, index_store));


        let update_sender =
            create_update_handler(index_resolver.clone(), tmp.path(), 4096 * 100).unwrap();

        let task = DumpTask {
            path: tmp.path().to_owned(),
            index_resolver,
            update_sender,
            uid: String::from("test"),
            update_db_size: 4096 * 10,
            index_db_size: 4096 * 10,
        };

        task.run().await.unwrap();
    }
}
