use crate::config::WalConfig;
use crate::model::IngestBatch;
use anyhow::Context;
use chrono::Local;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::sync::{Arc, Mutex};

/// WAL 追加写器：按行写 JSON，先满足可恢复与可审计。
#[derive(Clone)]
pub struct WalWriter {
    inner: Arc<Mutex<File>>,
}

impl WalWriter {
    /// 打开当前小时 WAL 文件，不存在时自动创建。
    pub fn open(cfg: &WalConfig) -> anyhow::Result<Self> {
        create_dir_all(&cfg.dir).with_context(|| format!("create wal dir {}", cfg.dir))?;
        let file_name = format!(
            "{}_{}.log",
            cfg.file_prefix,
            Local::now().format("%Y%m%d%H")
        );
        let path = std::path::Path::new(&cfg.dir).join(file_name);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open wal file {}", path.display()))?;
        Ok(Self {
            inner: Arc::new(Mutex::new(file)),
        })
    }

    /// 追加一批数据并同步到磁盘，保证崩溃后可回放。
    pub fn append_batch(&self, batch: &IngestBatch) -> anyhow::Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("wal lock poisoned"))?;
        let line = serde_json::to_string(batch).context("serialize wal batch")?;
        guard
            .write_all(line.as_bytes())
            .context("write wal payload failed")?;
        guard.write_all(b"\n").context("write wal newline failed")?;
        guard.flush().context("flush wal failed")?;
        Ok(())
    }

    /// 主动停机时执行文件级同步，尽量降低最后一批数据在掉电场景下的丢失风险。
    pub fn sync_all(&self) -> anyhow::Result<()> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("wal lock poisoned"))?;
        guard.sync_all().context("sync wal file failed")
    }
}
