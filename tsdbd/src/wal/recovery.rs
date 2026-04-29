use crate::model::IngestBatch;
use anyhow::Context;
use serde::Serialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// 从 WAL 文件回放批次，损坏行会被跳过并继续恢复。
pub fn replay_batches(path: &Path) -> anyhow::Result<Vec<IngestBatch>> {
    let f = File::open(path).with_context(|| format!("open wal {}", path.display()))?;
    let reader = BufReader::new(f);
    let mut out = Vec::new();
    for line in reader.lines() {
        let line = line.with_context(|| format!("read wal line {}", path.display()))?;
        match serde_json::from_str::<IngestBatch>(&line) {
            Ok(v) => out.push(v),
            Err(_) => continue,
        }
    }
    Ok(out)
}

/// 扫描目录内所有 `.log` 并回放，返回总批次数。
pub fn replay_wal_dir(dir: &Path, mut on_batch: impl FnMut(IngestBatch)) -> anyhow::Result<usize> {
    if !dir.exists() {
        return Ok(0);
    }
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("read wal dir {}", dir.display()))? {
        let entry = entry.with_context(|| format!("iterate wal dir {}", dir.display()))?;
        let path = entry.path();
        if path.extension().and_then(|x| x.to_str()) == Some("log") {
            files.push(path);
        }
    }
    files.sort();
    let mut count = 0usize;
    for path in files {
        for batch in replay_batches(&path)? {
            on_batch(batch);
            count += 1;
        }
    }
    Ok(count)
}

/// WAL 目录统计信息，用于运维接口展示。
#[derive(Debug, Clone, Serialize)]
pub struct WalDirStats {
    pub log_files: usize,
    pub total_bytes: u64,
}

/// 统计 WAL 目录下 `.log` 文件数量与总字节数。
pub fn wal_dir_stats(dir: &Path) -> anyhow::Result<WalDirStats> {
    if !dir.exists() {
        return Ok(WalDirStats {
            log_files: 0,
            total_bytes: 0,
        });
    }
    let mut log_files = 0usize;
    let mut total_bytes = 0u64;
    for entry in std::fs::read_dir(dir).with_context(|| format!("read wal dir {}", dir.display()))? {
        let entry = entry.with_context(|| format!("iterate wal dir {}", dir.display()))?;
        let path = entry.path();
        if path.extension().and_then(|x| x.to_str()) != Some("log") {
            continue;
        }
        let md = std::fs::metadata(&path).with_context(|| format!("stat wal file {}", path.display()))?;
        log_files += 1;
        total_bytes = total_bytes.saturating_add(md.len());
    }
    Ok(WalDirStats {
        log_files,
        total_bytes,
    })
}
