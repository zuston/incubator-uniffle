// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::app::ReadingOptions::FILE_OFFSET_AND_LEN;
use crate::app::{
    PartitionedUId, PurgeDataContext, ReadingIndexViewContext, ReadingViewContext,
    ReleaseBufferContext, RequireBufferContext, WritingViewContext,
};
use crate::config::LocalfileStoreConfig;
use crate::error::WorkerError;
use crate::metric::TOTAL_LOCALFILE_USED;
use crate::store::ResponseDataIndex::Local;
use crate::store::{
    LocalDataIndex, PartitionedLocalData, Persistent, RequireBufferResponse, ResponseData,
    ResponseDataIndex, Store,
};

use anyhow::Result;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;

use log::{debug, error, warn};

use crate::runtime::manager::RuntimeManager;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::store::local::disk::{FileWriter, LocalDisk, LocalDiskConfig};

struct LockedObject {
    pub(crate) disk: Arc<LocalDisk>,
    pub(crate) next_offset: i64,
    pub(crate) data_file_writer: Option<FileWriter>,
    pub(crate) index_file_writer: Option<FileWriter>,
}

pub struct LocalFileStore {
    local_disks: Vec<Arc<LocalDisk>>,
    healthy_check_min_disks: i32,
    runtime_manager: RuntimeManager,
    partition_locks: DashMap<PartitionedUId, Arc<RwLock<LockedObject>>>,
}

impl Persistent for LocalFileStore {}

unsafe impl Send for LocalFileStore {}
unsafe impl Sync for LocalFileStore {}

impl LocalFileStore {
    // only for test cases
    pub fn new(local_disks: Vec<String>) -> Self {
        let mut local_disk_instances = vec![];
        let runtime_manager: RuntimeManager = Default::default();
        for path in local_disks {
            local_disk_instances.push(LocalDisk::new(
                path,
                LocalDiskConfig::default(),
                runtime_manager.clone(),
            ));
        }
        LocalFileStore {
            local_disks: local_disk_instances,
            healthy_check_min_disks: 1,
            runtime_manager,
            partition_locks: Default::default(),
        }
    }

    pub fn from(localfile_config: LocalfileStoreConfig, runtime_manager: RuntimeManager) -> Self {
        let mut local_disk_instances = vec![];
        for path in localfile_config.data_paths {
            let config = LocalDiskConfig {
                high_watermark: localfile_config.disk_high_watermark.unwrap_or(0.8),
                low_watermark: localfile_config.disk_low_watermark.unwrap_or(0.6),
                max_concurrency: localfile_config.disk_max_concurrency.unwrap_or(40),
            };

            local_disk_instances.push(LocalDisk::new(path, config, runtime_manager.clone()));
        }
        LocalFileStore {
            local_disks: local_disk_instances,
            healthy_check_min_disks: localfile_config.healthy_check_min_disks.unwrap_or(1),
            runtime_manager,
            partition_locks: Default::default(),
        }
    }

    fn gen_relative_path_for_app(app_id: &str) -> String {
        format!("{}", app_id)
    }

    fn gen_relative_path_for_shuffle(app_id: &str, shuffle_id: i32) -> String {
        format!("{}/{}", app_id, shuffle_id)
    }

    fn gen_relative_path_for_partition(uid: &PartitionedUId) -> (String, String) {
        (
            format!(
                "{}/{}/partition-{}.data",
                uid.app_id, uid.shuffle_id, uid.partition_id
            ),
            format!(
                "{}/{}/partition-{}.index",
                uid.app_id, uid.shuffle_id, uid.partition_id
            ),
        )
    }

    fn healthy_check(&self) -> Result<bool> {
        let mut available = 0;
        for local_disk in &self.local_disks {
            if local_disk.is_healthy()? && !local_disk.is_corrupted()? {
                available += 1;
            }
        }

        debug!(
            "disk: available={}, healthy_check_min={}",
            available, self.healthy_check_min_disks
        );
        Ok(available > self.healthy_check_min_disks)
    }

    fn select_disk(&self, uid: &PartitionedUId) -> Result<Arc<LocalDisk>, WorkerError> {
        let hash_value = PartitionedUId::get_hash(uid);

        let mut candidates = vec![];
        for local_disk in &self.local_disks {
            if !local_disk.is_corrupted().unwrap() && local_disk.is_healthy().unwrap() {
                candidates.push(local_disk);
            }
        }

        let len = candidates.len();
        if len == 0 {
            error!("There is no available local disk!");
            return Err(WorkerError::NO_AVAILABLE_LOCAL_DISK);
        }

        let index = (hash_value % len as u64) as usize;
        if let Some(&disk) = candidates.get(index) {
            Ok(disk.clone())
        } else {
            Err(WorkerError::INTERNAL_ERROR)
        }
    }
}

#[async_trait]
impl Store for LocalFileStore {
    fn start(self: Arc<Self>) {
        todo!()
    }

    async fn insert(&self, ctx: WritingViewContext) -> Result<(), WorkerError> {
        if ctx.data_blocks.len() <= 0 {
            return Ok(());
        }

        let uid = ctx.uid;
        let (data_file_path, index_file_path) =
            LocalFileStore::gen_relative_path_for_partition(&uid);

        let locked_obj = self
            .partition_locks
            .entry(uid.clone())
            .or_insert_with(|| {
                let disk = self.select_disk(&uid).unwrap();
                let parent_dir_path =
                    LocalFileStore::gen_relative_path_for_shuffle(&uid.app_id, uid.shuffle_id);
                disk.create_dir(parent_dir_path.as_str());

                let locked_obj = LockedObject {
                    disk,
                    next_offset: 0,
                    data_file_writer: None,
                    index_file_writer: None,
                };
                Arc::new(RwLock::new(locked_obj))
            })
            .clone();

        let mut locked_obj = locked_obj
            .write()
            .instrument_await(format!(
                "waiting localfile partition file lock. path: {}",
                &data_file_path
            ))
            .await;

        let local_disk = locked_obj.disk.clone();
        let mut next_offset = locked_obj.next_offset;

        if local_disk.is_corrupted()? {
            return Err(WorkerError::PARTIAL_DATA_LOST(
                local_disk.base_path.to_string(),
            ));
        }

        let mut index_bytes_holder = BytesMut::new();
        let mut data_bytes_holder = BytesMut::new();

        let mut total_size = 0;
        for block in ctx.data_blocks {
            let block_id = block.block_id;
            let length = block.length;
            let uncompress_len = block.uncompress_length;
            let task_attempt_id = block.task_attempt_id;
            let crc = block.crc;

            total_size += length;

            index_bytes_holder.put_i64(next_offset);
            index_bytes_holder.put_i32(length);
            index_bytes_holder.put_i32(uncompress_len);
            index_bytes_holder.put_i64(crc);
            index_bytes_holder.put_i64(block_id);
            index_bytes_holder.put_i64(task_attempt_id);

            let data = block.data;

            data_bytes_holder.extend_from_slice(&data);
            next_offset += length as i64;
        }


        if locked_obj.data_file_writer.is_none() {
            let writer = Some(local_disk.create_file_writer(&data_file_path).await?);
            locked_obj.data_file_writer = writer;
        }

        if locked_obj.index_file_writer.is_none() {
            let writer = Some(local_disk.create_file_writer(&index_file_path).await?);
            locked_obj.index_file_writer = writer;
        }

        locked_obj.data_file_writer.as_mut().unwrap().append(data_bytes_holder.freeze())
            .instrument_await(format!("localfile writing data. path: {}", &data_file_path))
            .await?;
        locked_obj.index_file_writer.as_mut().unwrap().append(index_bytes_holder.freeze())
            .instrument_await(format!("localfile writing index. path: {}", &index_file_path))
            .await?;

        TOTAL_LOCALFILE_USED.inc_by(total_size as u64);

        locked_obj.next_offset += total_size as i64;

        Ok(())
    }

    async fn get(&self, ctx: ReadingViewContext) -> Result<ResponseData, WorkerError> {
        let uid = ctx.uid;
        let (offset, len) = match ctx.reading_options {
            FILE_OFFSET_AND_LEN(offset, len) => (offset, len),
            _ => (0, 0),
        };

        if len == 0 {
            warn!("There is no data in localfile for [{:?}]", &uid);
            return Ok(ResponseData::Local(PartitionedLocalData {
                data: Default::default(),
            }));
        }

        let (data_file_path, _) = LocalFileStore::gen_relative_path_for_partition(&uid);

        let locked_object = self.partition_locks.get_mut(&uid);
        if locked_object.is_none() {
            warn!(
                "There is no cached data in localfile store for [{:?}]",
                &uid
            );
            return Ok(ResponseData::Local(PartitionedLocalData {
                data: Default::default(),
            }));
        }

        let locked_object = locked_object.unwrap().clone();
        let locked_obj = locked_object
            .write()
            .instrument_await(format!(
                "localfile partition file lock. path: {}",
                &data_file_path
            ))
            .await;
        let local_disk = locked_obj.disk.clone();

        if local_disk.is_corrupted()? {
            return Err(WorkerError::LOCAL_DISK_OWNED_BY_PARTITION_CORRUPTED(
                local_disk.base_path.to_string(),
            ));
        }

        let data = local_disk
            .read(data_file_path, offset, Some(len))
            .instrument_await("getting data from localfile")
            .await?;
        Ok(ResponseData::Local(PartitionedLocalData { data }))
    }

    async fn get_index(
        &self,
        ctx: ReadingIndexViewContext,
    ) -> Result<ResponseDataIndex, WorkerError> {
        let uid = ctx.partition_id;
        let (data_file_path, index_file_path) =
            LocalFileStore::gen_relative_path_for_partition(&uid);

        let locked_object = self.partition_locks.get(&uid);
        if locked_object.is_none() {
            warn!(
                "There is no cached data in localfile store for [{:?}]",
                &uid
            );
            return Ok(Local(LocalDataIndex {
                index_data: Default::default(),
                data_file_len: 0,
            }));
        }

        let locked_object = locked_object.unwrap().clone();
        let locked_obj = locked_object
            .write()
            .instrument_await(format!(
                "localfile partition file lock. path: {}",
                &data_file_path
            ))
            .await;
        let local_disk = locked_obj.disk.clone();
        if local_disk.is_corrupted()? {
            return Err(WorkerError::LOCAL_DISK_OWNED_BY_PARTITION_CORRUPTED(
                local_disk.base_path.to_string(),
            ));
        }

        let index_data_result = local_disk
            .read(index_file_path, 0, None)
            .instrument_await("reading index data from file")
            .await?;
        let len = local_disk
            .get_file_len(data_file_path)
            .instrument_await("getting file len from file")
            .await?;
        Ok(Local(LocalDataIndex {
            index_data: index_data_result,
            data_file_len: len,
        }))
    }

    async fn purge(&self, ctx: PurgeDataContext) -> Result<()> {
        let app_id = ctx.app_id;
        let shuffle_id_option = ctx.shuffle_id;

        let data_relative_dir_path = match shuffle_id_option {
            Some(shuffle_id) => LocalFileStore::gen_relative_path_for_shuffle(&app_id, shuffle_id),
            _ => LocalFileStore::gen_relative_path_for_app(&app_id),
        };

        for local_disk_ref in &self.local_disks {
            let disk = local_disk_ref.clone();
            disk.delete(data_relative_dir_path.to_string()).await?;
        }

        Ok(())
    }

    async fn is_healthy(&self) -> Result<bool> {
        self.healthy_check()
    }

    async fn require_buffer(
        &self,
        _ctx: RequireBufferContext,
    ) -> Result<RequireBufferResponse, WorkerError> {
        todo!()
    }

    async fn release_buffer(&self, _ctx: ReleaseBufferContext) -> Result<i64, WorkerError> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::app::{
        PartitionedUId, PurgeDataContext, ReadingIndexViewContext, ReadingOptions,
        ReadingViewContext, WritingViewContext,
    };
    use crate::store::localfile::LocalFileStore;

    use crate::store::{PartitionedDataBlock, ResponseData, ResponseDataIndex, Store};
    use bytes::{Buf, Bytes, BytesMut};
    use log::info;

    #[test]
    fn purge_test() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("init local file path: {}", &temp_path);
        let local_store = LocalFileStore::new(vec![temp_path.clone()]);

        let runtime = local_store.runtime_manager.clone();

        let app_id = "purge_test-app-id".to_string();
        let uid = PartitionedUId {
            app_id: app_id.clone(),
            shuffle_id: 0,
            partition_id: 0,
        };

        let data = b"hello world!hello china!";
        let size = data.len();
        let writing_ctx = WritingViewContext {
            uid: uid.clone(),
            data_blocks: vec![
                PartitionedDataBlock {
                    block_id: 0,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
                PartitionedDataBlock {
                    block_id: 1,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
            ],
        };

        let insert_result = runtime.wait(local_store.insert(writing_ctx));
        if insert_result.is_err() {
            println!("{:?}", insert_result.err());
            panic!()
        }
        assert_eq!(
            true,
            runtime.wait(tokio::fs::try_exists(format!(
                "{}/{}/{}/partition-{}.data",
                &temp_path, &app_id, "0", "0"
            )))?
        );

        // shuffle level purge
        runtime
            .wait(local_store.purge(PurgeDataContext::new(app_id.to_string(), Some(0))))
            .expect("");
        assert_eq!(
            false,
            runtime.wait(tokio::fs::try_exists(format!(
                "{}/{}/{}",
                &temp_path, &app_id, 0
            )))?
        );

        // app level purge
        runtime.wait(local_store.purge((&*app_id).into()))?;
        assert_eq!(
            false,
            runtime.wait(tokio::fs::try_exists(format!("{}/{}", &temp_path, &app_id)))?
        );

        Ok(())
    }

    #[test]
    fn local_store_test() {
        let temp_dir = tempdir::TempDir::new("test_local_store").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        info!("init local file path: {}", temp_path);
        let mut local_store = LocalFileStore::new(vec![temp_path]);

        let runtime = local_store.runtime_manager.clone();

        let uid = PartitionedUId {
            app_id: "100".to_string(),
            shuffle_id: 0,
            partition_id: 0,
        };

        let data = b"hello world!hello china!";
        let size = data.len();
        let writing_ctx = WritingViewContext {
            uid: uid.clone(),
            data_blocks: vec![
                PartitionedDataBlock {
                    block_id: 0,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
                PartitionedDataBlock {
                    block_id: 1,
                    length: size as i32,
                    uncompress_length: 200,
                    crc: 0,
                    data: Bytes::copy_from_slice(data),
                    task_attempt_id: 0,
                },
            ],
        };

        let insert_result = runtime.wait(local_store.insert(writing_ctx));
        if insert_result.is_err() {
            println!("{:?}", insert_result.err());
            panic!()
        }

        async fn get_and_check_partitial_data(
            local_store: &mut LocalFileStore,
            uid: PartitionedUId,
            size: i64,
            expected: &[u8],
        ) {
            let reading_ctx = ReadingViewContext {
                uid,
                reading_options: ReadingOptions::FILE_OFFSET_AND_LEN(0, size as i64),
                serialized_expected_task_ids_bitmap: Default::default(),
            };

            let read_result = local_store.get(reading_ctx).await;
            if read_result.is_err() {
                panic!()
            }

            match read_result.unwrap() {
                ResponseData::Local(partitioned_data) => {
                    assert_eq!(expected, partitioned_data.data.as_ref());
                }
                _ => panic!(),
            }
        }

        // case1: read the one partition block data
        runtime.wait(get_and_check_partitial_data(
            &mut local_store,
            uid.clone(),
            size as i64,
            data,
        ));

        // case2: read the complete block data
        let mut expected = BytesMut::with_capacity(size * 2);
        expected.extend_from_slice(data);
        expected.extend_from_slice(data);
        runtime.wait(get_and_check_partitial_data(
            &mut local_store,
            uid.clone(),
            size as i64 * 2,
            expected.freeze().as_ref(),
        ));

        // case3: get the index data
        let reading_index_view_ctx = ReadingIndexViewContext {
            partition_id: uid.clone(),
        };
        let result = runtime.wait(local_store.get_index(reading_index_view_ctx));
        if result.is_err() {
            panic!()
        }

        match result.unwrap() {
            ResponseDataIndex::Local(data) => {
                let mut index = data.index_data;
                let offset_1 = index.get_i64();
                assert_eq!(0, offset_1);
                let length_1 = index.get_i32();
                assert_eq!(size as i32, length_1);
                index.get_i32();
                index.get_i64();
                let block_id_1 = index.get_i64();
                assert_eq!(0, block_id_1);
                let task_id = index.get_i64();
                assert_eq!(0, task_id);

                let offset_2 = index.get_i64();
                assert_eq!(size as i64, offset_2);
                assert_eq!(size as i32, index.get_i32());
            }
        }

        temp_dir.close().unwrap();
    }
}
