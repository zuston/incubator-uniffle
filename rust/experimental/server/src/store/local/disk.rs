use crate::runtime::manager::RuntimeManager;
use anyhow::Result;
use await_tree::InstrumentAwait;
use bytes::{Bytes, BytesMut};
use log::{error, info, warn};
use std::io::SeekFrom;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Semaphore;

pub struct LocalDiskConfig {
    pub(crate) high_watermark: f32,
    pub(crate) low_watermark: f32,
    pub(crate) max_concurrency: i32,
}

impl LocalDiskConfig {
    pub fn create_mocked_config() -> Self {
        LocalDiskConfig {
            high_watermark: 1.0,
            low_watermark: 0.6,
            max_concurrency: 20,
        }
    }
}

impl Default for LocalDiskConfig {
    fn default() -> Self {
        LocalDiskConfig {
            high_watermark: 0.8,
            low_watermark: 0.6,
            max_concurrency: 40,
        }
    }
}

pub struct LocalDisk {
    pub(crate) base_path: String,
    concurrency_limiter: Semaphore,
    is_corrupted: AtomicBool,
    is_healthy: AtomicBool,
    config: LocalDiskConfig,
}

impl LocalDisk {
    pub fn new(
        path: String,
        config: LocalDiskConfig,
        runtime_manager: RuntimeManager,
    ) -> Arc<Self> {
        let instance = LocalDisk {
            base_path: path,
            concurrency_limiter: Semaphore::new(config.max_concurrency as usize),
            is_corrupted: AtomicBool::new(false),
            is_healthy: AtomicBool::new(true),
            config,
        };
        let instance = Arc::new(instance);

        let runtime = runtime_manager.default_runtime.clone();
        let cloned = instance.clone();
        runtime.spawn(async {
            info!(
                "Starting the disk healthy checking, base path: {}",
                &cloned.base_path
            );
            LocalDisk::loop_check_disk(cloned).await;
        });

        instance
    }

    async fn write_read_check(local_disk: Arc<LocalDisk>) -> Result<()> {
        let temp_path = format!("{}/{}", &local_disk.base_path, "corruption_check.file");
        let data = Bytes::copy_from_slice(b"file corruption check");
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&temp_path)
                .await?;
            file.write_all(&data).await?;
            file.flush().await?;
        }

        let mut read_data = Vec::new();
        {
            let mut file = tokio::fs::File::open(&temp_path).await?;
            file.read_to_end(&mut read_data).await?;

            tokio::fs::remove_file(&temp_path).await?;
        }

        if data != Bytes::copy_from_slice(&read_data) {
            local_disk.mark_corrupted();
            error!(
                "The local disk has been corrupted. path: {}",
                &local_disk.base_path
            );
        }

        Ok(())
    }

    async fn loop_check_disk(local_disk: Arc<LocalDisk>) {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            if local_disk.is_corrupted().unwrap() {
                return;
            }

            let check_succeed: Result<()> = LocalDisk::write_read_check(local_disk.clone()).await;
            if check_succeed.is_err() {
                local_disk.mark_corrupted();
                error!(
                    "Errors on checking local disk corruption. err: {:#?}",
                    check_succeed.err()
                );
            }

            // check the capacity
            let used_ratio = local_disk.get_disk_used_ratio();
            if used_ratio.is_err() {
                error!(
                    "Errors on getting the used ratio of the disk capacity. err: {:?}",
                    used_ratio.err()
                );
                continue;
            }

            let used_ratio = used_ratio.unwrap();
            if local_disk.is_healthy().unwrap()
                && used_ratio > local_disk.config.high_watermark as f64
            {
                warn!("Disk={} has been unhealthy.", &local_disk.base_path);
                local_disk.mark_unhealthy();
                continue;
            }

            if !local_disk.is_healthy().unwrap()
                && used_ratio < local_disk.config.low_watermark as f64
            {
                warn!("Disk={} has been healthy.", &local_disk.base_path);
                local_disk.mark_healthy();
                continue;
            }
        }
    }

    fn append_path(&self, path: String) -> String {
        format!("{}/{}", self.base_path.clone(), path)
    }

    pub fn create_dir(&self, dir_path: &str) {
        let absolute_path = self.append_path(dir_path.to_string());
        if !std::fs::metadata(&absolute_path).is_ok() {
            std::fs::create_dir_all(&absolute_path).expect("Errors on creating dirs.");
        }
    }

    pub async fn create_file_writer(&self, relative_file_path: &str) -> Result<FileWriter> {
        let absolute_path = self.append_path(relative_file_path.to_string());
        let output_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(absolute_path)
            .await?;
        Ok(FileWriter::new(output_file))
    }

    pub async fn write(&self, data: Bytes, relative_file_path: String) -> Result<()> {
        let _concurrency_guarder = self
            .concurrency_limiter
            .acquire()
            .instrument_await("meet the concurrency limiter")
            .await?;
        let absolute_path = self.append_path(relative_file_path.clone());
        let mut output_file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(absolute_path)
            .await?;
        output_file.write_all(data.as_ref()).await?;

        Ok(())
    }

    pub async fn get_file_len(&self, relative_file_path: String) -> Result<i64> {
        let file_path = self.append_path(relative_file_path);

        Ok(
            match tokio::fs::metadata(file_path)
                .instrument_await("getting metadata of path")
                .await
            {
                Ok(metadata) => metadata.len() as i64,
                _ => 0i64,
            },
        )
    }

    pub async fn read(
        &self,
        relative_file_path: String,
        offset: i64,
        length: Option<i64>,
    ) -> Result<Bytes> {
        let file_path = self.append_path(relative_file_path);

        let file = tokio::fs::File::open(&file_path)
            .instrument_await(format!("opening file. path: {}", &file_path))
            .await?;

        let read_len = match length {
            Some(len) => len,
            _ => file
                .metadata()
                .instrument_await(format!("getting file metadata. path: {}", &file_path))
                .await?
                .len()
                .try_into()
                .unwrap(),
        } as usize;

        let mut reader = tokio::io::BufReader::new(file);
        let mut buffer = vec![0; read_len];
        reader
            .seek(SeekFrom::Start(offset as u64))
            .instrument_await(format!(
                "seeking file [{}:{}] of path: {}",
                offset, read_len, &file_path
            ))
            .await?;
        reader
            .read_exact(buffer.as_mut())
            .instrument_await(format!(
                "reading data of len: {} from path: {}",
                read_len, &file_path
            ))
            .await?;

        let mut bytes_buffer = BytesMut::new();
        bytes_buffer.extend_from_slice(&*buffer);
        Ok(bytes_buffer.freeze())
    }

    pub async fn delete(&self, relative_file_path: String) -> Result<()> {
        let delete_path = self.append_path(relative_file_path);
        if !tokio::fs::try_exists(&delete_path).await? {
            info!("The path:{} does not exist, ignore purging.", &delete_path);
            return Ok(());
        }

        let metadata = tokio::fs::metadata(&delete_path).await?;
        if metadata.is_dir() {
            tokio::fs::remove_dir_all(delete_path).await?;
        } else {
            tokio::fs::remove_file(delete_path).await?;
        }
        Ok(())
    }

    fn mark_corrupted(&self) {
        self.is_corrupted.store(true, Ordering::SeqCst);
    }

    fn mark_unhealthy(&self) {
        self.is_healthy.store(false, Ordering::SeqCst);
    }

    fn mark_healthy(&self) {
        self.is_healthy.store(true, Ordering::SeqCst);
    }

    pub fn is_corrupted(&self) -> Result<bool> {
        Ok(self.is_corrupted.load(Ordering::SeqCst))
    }

    pub fn is_healthy(&self) -> Result<bool> {
        Ok(self.is_healthy.load(Ordering::SeqCst))
    }

    fn get_disk_used_ratio(&self) -> Result<f64> {
        // Get the total and available space in bytes
        let available_space = fs2::available_space(&self.base_path)?;
        let total_space = fs2::total_space(&self.base_path)?;
        Ok(1.0 - (available_space as f64 / total_space as f64))
    }
}

pub struct FileWriter {
    file_instance: File,
}

impl FileWriter {
    pub fn new(file: File) -> Self {
        Self {
            file_instance: file
        }
    }

    pub async fn append(&mut self, data: Bytes) -> Result<()> {
        self.file_instance.write(data.as_ref()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::manager::RuntimeManager;
    use crate::store::local::disk::{LocalDisk, LocalDiskConfig};
    use bytes::Bytes;
    use std::io::Read;
    use std::time::Duration;

    #[test]
    fn test_local_disk_delete_operation() {
        let temp_dir = tempdir::TempDir::new("test_local_disk_delete_operation-dir").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();

        println!("init the path: {}", &temp_path);

        let runtime: RuntimeManager = Default::default();
        let local_disk = LocalDisk::new(
            temp_path.clone(),
            LocalDiskConfig::default(),
            runtime.clone(),
        );

        let data = b"hello!";
        local_disk.create_dir("a");
        runtime
            .wait(local_disk.write(Bytes::copy_from_slice(data), "a/b".to_string()))
            .unwrap();

        assert_eq!(
            true,
            runtime
                .wait(tokio::fs::try_exists(format!(
                    "{}/{}",
                    &temp_path,
                    "a/b".to_string()
                )))
                .unwrap()
        );

        runtime
            .wait(local_disk.delete("a/".to_string()))
            .expect("TODO: panic message");
        assert_eq!(
            false,
            runtime
                .wait(tokio::fs::try_exists(format!(
                    "{}/{}",
                    &temp_path,
                    "a/b".to_string()
                )))
                .unwrap()
        );
    }

    #[test]
    fn local_disk_corruption_healthy_check() {
        let temp_dir = tempdir::TempDir::new("test_directory").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();

        let local_disk = LocalDisk::new(
            temp_path.clone(),
            LocalDiskConfig::create_mocked_config(),
            Default::default(),
        );

        awaitility::at_most(Duration::from_secs(10)).until(|| local_disk.is_healthy().unwrap());
        assert_eq!(false, local_disk.is_corrupted().unwrap());
    }

    #[test]
    fn local_disk_test() {
        let temp_dir = tempdir::TempDir::new("test_directory").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();

        let runtime: RuntimeManager = Default::default();
        let local_disk = LocalDisk::new(
            temp_path.clone(),
            LocalDiskConfig::default(),
            runtime.clone(),
        );

        let data = b"Hello, World!";

        let relative_path = "app-id/test_file.txt";

        local_disk.create_dir("app-id");

        let write_result =
            runtime.wait(local_disk.write(Bytes::copy_from_slice(data), relative_path.to_string()));
        assert!(write_result.is_ok());

        // test whether the content is written
        let file_path = format!("{}/{}", local_disk.base_path, relative_path);
        let mut file = std::fs::File::open(file_path).unwrap();
        let mut file_content = Vec::new();
        file.read_to_end(&mut file_content).unwrap();
        assert_eq!(file_content, data);

        // if the file has been created, append some content
        let write_result =
            runtime.wait(local_disk.write(Bytes::copy_from_slice(data), relative_path.to_string()));
        assert!(write_result.is_ok());

        let read_result = runtime.wait(local_disk.read(
            relative_path.to_string(),
            0,
            Some(data.len() as i64 * 2),
        ));
        assert!(read_result.is_ok());
        let read_data = read_result.unwrap();
        let expected = b"Hello, World!Hello, World!";
        assert_eq!(read_data.as_ref(), expected);

        temp_dir.close().unwrap();
    }
}
