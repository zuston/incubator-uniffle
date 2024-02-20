use std::sync::Arc;
use crossbeam_channel::{Receiver, Sender, unbounded};
use tokio::sync::Semaphore;
use crate::app::{AppManager, WritingViewContext};
use crate::error::WorkerError;
use crate::metric::{TOTAL_WRITING_CHANNEL_OUT, TOTAL_RECEIVED_DATA};
use crate::runtime::manager::RuntimeManager;

/// The channel is to accept the writing data, and then using the
/// async thread to fill into memory buffer to reduce race condition
/// of lock to improve throughout.
#[derive(Clone)]
pub struct Channel {
    app_manager: Arc<AppManager>,
    runtime_manager: RuntimeManager,

    sender: Sender<WritingViewContext>,
    receiver: Receiver<WritingViewContext>,
}

impl Channel {
    pub fn new(app_manager: Arc<AppManager>, runtime_manager: RuntimeManager) -> Self {
        let (s, r) = unbounded();
        let rt = runtime_manager.clone();
        let chan = Self {
            app_manager,
            runtime_manager: rt,
            sender: s,
            receiver: r,
        };

        let chan_ref = chan.clone();
        let limiter = Arc::new(Semaphore::new(100usize));
        let rt_ref = runtime_manager.clone();
        runtime_manager.default_runtime.spawn(async move {
            loop {
                let ctx = chan_ref.receiver.recv().unwrap();
                let _limiter_guard = limiter
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap();

                let chan_ref_child = chan_ref.clone();
                rt_ref.write_runtime.spawn(async move {
                    let app_id = ctx.uid.app_id.as_str();
                    let app = chan_ref_child.app_manager.get_app(app_id).unwrap();
                    let len = app.insert(ctx).await.unwrap();
                    TOTAL_WRITING_CHANNEL_OUT.inc_by(len as u64);
                });
            }
        });

        chan
    }

    pub fn send(&self, ctx: WritingViewContext) -> anyhow::Result<i32, WorkerError> {
        let len: i32 = ctx.data_blocks.iter().map(|block| block.length).sum();
        TOTAL_RECEIVED_DATA.inc_by(len as u64);

        self.sender.send(ctx).map_err(|_| WorkerError::WRITING_TO_CHANNEL_FAIL)?;
        Ok(len)
    }
}