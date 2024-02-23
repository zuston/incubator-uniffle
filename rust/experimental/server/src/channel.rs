use crate::app::{AppManager, PartitionedUId, WritingViewContext};
use crate::config::WritingChannelConfig;
use crate::error::WorkerError;
use crate::metric::{TOTAL_RECEIVED_DATA, TOTAL_WRITING_CHANNEL_OUT};
use crate::runtime::manager::RuntimeManager;
use crossbeam_channel::{unbounded, Receiver, Sender};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// The channel is to accept the writing data, and then using the
/// async thread to fill into memory buffer to reduce race condition
/// of lock to improve throughout.
#[derive(Clone)]
pub struct Channel {
    app_manager: Arc<AppManager>,
    runtime_manager: RuntimeManager,

    sender: Sender<WritingViewContext>,
    receiver: Receiver<WritingViewContext>,

    pdata_counter: Arc<DashMap<PartitionedUId, Arc<AtomicI64>>>,
}

impl Channel {
    pub fn new(
        config: WritingChannelConfig,
        app_manager: Arc<AppManager>,
        runtime_manager: RuntimeManager,
    ) -> Self {
        let (s, r) = unbounded();
        let rt = runtime_manager.clone();
        let chan = Self {
            app_manager,
            runtime_manager: rt,
            sender: s,
            receiver: r,
            pdata_counter: Default::default(),
        };

        let concurrency = config.consumer_max_concurrency;
        let chan_ref = chan.clone();
        let limiter = Arc::new(Semaphore::new(concurrency));
        let rt_ref = runtime_manager.clone();
        runtime_manager.default_runtime.spawn(async move {
            loop {
                let ctx = chan_ref.receiver.recv().unwrap();
                let limiter_guard = limiter.clone().acquire_owned().await.unwrap();

                let chan_ref_child = chan_ref.clone();
                rt_ref.write_runtime.spawn(async move {
                    let uid_ref = &ctx.clone().uid;
                    let app_id = ctx.uid.app_id.as_str();
                    let app = chan_ref_child.app_manager.get_app(app_id).unwrap();
                    let len = app.insert(ctx).await.unwrap();
                    TOTAL_WRITING_CHANNEL_OUT.inc_by(len as u64);
                    chan_ref_child.desc_partition_counter(uid_ref);
                    drop(limiter_guard);
                });
            }
        });

        chan
    }

    pub fn send(&self, ctx: WritingViewContext) -> anyhow::Result<i32, WorkerError> {
        self.incr_partition_counter(&ctx.uid);

        let len: i32 = ctx.data_blocks.iter().map(|block| block.length).sum();
        TOTAL_RECEIVED_DATA.inc_by(len as u64);

        self.sender
            .send(ctx)
            .map_err(|_| WorkerError::WRITING_TO_CHANNEL_FAIL)?;
        Ok(len)
    }

    fn incr_partition_counter(&self, uid: &PartitionedUId) {
        match self.pdata_counter.entry(uid.clone()) {
            Entry::Occupied(mut entry) => {
                let existing_value = entry.get_mut();
                existing_value.fetch_add(1i64, Ordering::SeqCst);
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(AtomicI64::new(1)));
            }
        }
    }

    fn desc_partition_counter(&self, uid: &PartitionedUId) {
        if let Some(existing_value) = self.pdata_counter.get(uid) {
            existing_value.value().fetch_add(-1, Ordering::SeqCst);
        }
    }

    fn is_partition_all_out(&self, uid: &PartitionedUId) -> anyhow::Result<bool> {
        if let Some(existing_value) = self.pdata_counter.get(uid) {
            let counter = existing_value.value().load(Ordering::SeqCst);
            if counter == 0 {
                drop(existing_value);
                self.pdata_counter.remove(uid);
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(true)
        }
    }

    pub async fn block_wait_partition_all_out(&self, uid: &PartitionedUId) -> anyhow::Result<bool> {
        loop {
            if let Ok(true) = self.is_partition_all_out(uid) {
                return Ok(true);
            } else {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[cfg(test)]
mod test {}
