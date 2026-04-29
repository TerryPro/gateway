use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{BinaryBuilder, Int16Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::{
    sync::{mpsc, oneshot},
    time::{Duration, interval},
};
use tracing::{error, info, warn};

use common::archive::{
    ARCHIVE_SCHEMA_VERSION, ManifestEntry, append_manifest_entry, build_day_dir,
    format_day_key_hour_local, parquet_file_name, parquet_tmp_file_name, parse_parquet_file_name,
};

use crate::cli::{ArchiveConfig, ArchiveRotateMode};

const BATCH_WRITE_ROWS: usize = 512;

/// 遥测归档事件，由设备会话线程投递给归档工作线程。
#[derive(Debug, Clone)]
pub struct ArchiveEvent {
    pub device_id: String,
    pub ts_ms: u64,
    pub payload: Vec<u8>,
}

/// 归档后台任务句柄，用于优雅停机并等待封段完成。
pub struct ArchiveWorkerHandle {
    pub tx: mpsc::Sender<ArchiveEvent>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl ArchiveWorkerHandle {
    /// 发送停机信号并等待归档后台任务完成收尾。
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Err(e) = self.join_handle.await {
            error!(error = ?e, "archive worker join failed");
        }
    }
}

/// 单设备当前归档写入上下文。
struct DeviceWriter {
    device_id: String,
    day_key: String,
    hour: u32,
    part: u32,
    day_dir: std::path::PathBuf,
    file_name: String,
    tmp_path: std::path::PathBuf,
    final_path: std::path::PathBuf,
    writer: Option<ArrowWriter<File>>,
    pending: Vec<ArchiveEvent>,
    rows: u64,
    payload_bytes: u64,
    min_ts_ms: u64,
    max_ts_ms: u64,
}

/// 启动遥测归档后台任务，并返回事件发送端。
pub fn start_archive_worker(cfg: ArchiveConfig) -> Option<ArchiveWorkerHandle> {
    if !cfg.enabled {
        info!("archive disabled by config");
        return None;
    }
    let (tx, rx) = mpsc::channel(cfg.queue_capacity);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join_handle = tokio::spawn(async move {
        if let Err(e) = archive_worker_loop(cfg, rx, shutdown_rx).await {
            error!(error = ?e, "archive worker exited with error");
        }
    });
    Some(ArchiveWorkerHandle {
        tx,
        shutdown_tx: Some(shutdown_tx),
        join_handle,
    })
}

/// 归档主循环，串行消费事件，统一维护写入和滚动状态。
async fn archive_worker_loop(
    cfg: ArchiveConfig,
    mut rx: mpsc::Receiver<ArchiveEvent>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(&cfg.root_dir)
        .with_context(|| format!("create archive root failed: {}", cfg.root_dir))?;
    info!(archive_root = %cfg.root_dir, "archive worker started (parquet)");

    let mut writers: HashMap<String, DeviceWriter> = HashMap::new();
    let mut ticker = interval(Duration::from_millis(cfg.flush_interval_ms));

    loop {
        tokio::select! {
            maybe_evt = rx.recv() => {
                let Some(evt) = maybe_evt else {
                    break;
                };
                if let Err(e) = write_event(&cfg, &mut writers, evt) {
                    error!(error = ?e, "archive write event failed");
                }
            }
            _ = ticker.tick() => {
                if let Err(e) = flush_all(&mut writers) {
                    warn!(error = ?e, "archive flush failed");
                }
            }
            _ = &mut shutdown_rx => {
                break;
            }
        }
    }

    while let Ok(evt) = rx.try_recv() {
        if let Err(e) = write_event(&cfg, &mut writers, evt) {
            error!(error = ?e, "archive write event failed while draining");
        }
    }

    seal_all(&cfg, &mut writers)?;
    info!("archive worker stopped");
    Ok(())
}

/// 写入一条事件，必要时触发滚动并分段封存。
fn write_event(
    cfg: &ArchiveConfig,
    writers: &mut HashMap<String, DeviceWriter>,
    evt: ArchiveEvent,
) -> anyhow::Result<()> {
    let (day_key, hour) = format_day_key_hour_local(evt.ts_ms)?;
    let rotate_required = should_rotate(cfg, writers.get(&evt.device_id), &evt, &day_key, hour);
    if rotate_required {
        if let Some(mut old) = writers.remove(&evt.device_id) {
            seal_writer(cfg, &mut old)?;
        }
        let new_writer = open_new_writer(cfg, &evt.device_id, &day_key, hour)?;
        writers.insert(evt.device_id.clone(), new_writer);
    } else if !writers.contains_key(&evt.device_id) {
        let new_writer = open_new_writer(cfg, &evt.device_id, &day_key, hour)?;
        writers.insert(evt.device_id.clone(), new_writer);
    }

    let writer = writers
        .get_mut(&evt.device_id)
        .context("archive writer missing after open")?;
    writer.pending.push(evt);
    if writer.pending.len() >= BATCH_WRITE_ROWS {
        flush_pending(writer)?;
    }
    Ok(())
}

/// 按策略判断是否需要切换新分段。
fn should_rotate(
    cfg: &ArchiveConfig,
    current: Option<&DeviceWriter>,
    evt: &ArchiveEvent,
    day_key: &str,
    hour: u32,
) -> bool {
    let Some(current) = current else {
        return true;
    };
    let time_rotate = current.day_key != day_key || current.hour != hour;
    let predicted_payload_bytes = current
        .payload_bytes
        .saturating_add(current.pending.iter().map(|x| x.payload.len() as u64).sum())
        .saturating_add(evt.payload.len() as u64);
    let size_limit_bytes = cfg.rotate_size_mb.saturating_mul(1024 * 1024);
    let size_rotate = predicted_payload_bytes > size_limit_bytes;

    match cfg.rotate_mode {
        ArchiveRotateMode::Time => time_rotate,
        ArchiveRotateMode::Size => size_rotate,
        ArchiveRotateMode::Hybrid => time_rotate || size_rotate,
    }
}

/// 刷新所有设备的待写入批次并触发 parquet flush。
fn flush_all(writers: &mut HashMap<String, DeviceWriter>) -> anyhow::Result<()> {
    for writer in writers.values_mut() {
        flush_pending(writer)?;
        let parquet = writer
            .writer
            .as_mut()
            .context("parquet writer missing in flush_all")?;
        parquet.flush()?;
    }
    Ok(())
}

/// 封存所有当前活跃 writer，保证退出时落盘完整。
fn seal_all(cfg: &ArchiveConfig, writers: &mut HashMap<String, DeviceWriter>) -> anyhow::Result<()> {
    let keys: Vec<String> = writers.keys().cloned().collect();
    for key in keys {
        if let Some(mut writer) = writers.remove(&key) {
            seal_writer(cfg, &mut writer)?;
        }
    }
    Ok(())
}

/// 打开新分段 writer，文件先写到 `.tmp`，封存后再 rename。
fn open_new_writer(
    cfg: &ArchiveConfig,
    device_id: &str,
    day_key: &str,
    hour: u32,
) -> anyhow::Result<DeviceWriter> {
    let day_dir = build_day_dir(&cfg.root_dir, device_id, day_key);
    std::fs::create_dir_all(&day_dir)
        .with_context(|| format!("create day dir failed: {}", day_dir.display()))?;
    let part = detect_next_part(&day_dir, hour)?;
    let file_name = parquet_file_name(hour, part);
    let tmp_name = parquet_tmp_file_name(hour, part);
    let tmp_path = day_dir.join(&tmp_name);
    let final_path = day_dir.join(&file_name);

    let file = File::create(&tmp_path)
        .with_context(|| format!("create parquet tmp failed: {}", tmp_path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let writer = ArrowWriter::try_new(file, archive_schema(), Some(props))?;

    info!(
        device_id = %device_id,
        day = %day_key,
        hour = hour,
        part = part,
        path = %final_path.display(),
        "archive parquet segment opened"
    );

    Ok(DeviceWriter {
        device_id: device_id.to_string(),
        day_key: day_key.to_string(),
        hour,
        part,
        day_dir,
        file_name,
        tmp_path,
        final_path,
        writer: Some(writer),
        pending: Vec::with_capacity(BATCH_WRITE_ROWS),
        rows: 0,
        payload_bytes: 0,
        min_ts_ms: u64::MAX,
        max_ts_ms: 0,
    })
}

/// 扫描日目录并计算当前小时下一个 part 编号。
fn detect_next_part(day_dir: &std::path::Path, hour: u32) -> anyhow::Result<u32> {
    let mut max_part = 0_u32;
    let rd = std::fs::read_dir(day_dir)?;
    for entry in rd {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let Some((h, p)) = parse_parquet_file_name(&name) else {
            continue;
        };
        if h == hour && p > max_part {
            max_part = p;
        }
    }
    Ok(max_part.saturating_add(1).max(1))
}

/// 将待写入事件批次转换为 RecordBatch 并写入 parquet。
fn flush_pending(writer: &mut DeviceWriter) -> anyhow::Result<()> {
    if writer.pending.is_empty() {
        return Ok(());
    }
    let count = writer.pending.len();
    let mut schema_builder = Int16Builder::with_capacity(count);
    let mut device_builder = StringBuilder::with_capacity(count, count * 16);
    let mut ts_builder = Int64Builder::with_capacity(count);
    let mut payload_builder = BinaryBuilder::with_capacity(
        count,
        writer.pending.iter().map(|e| e.payload.len()).sum(),
    );

    for evt in writer.pending.drain(..) {
        schema_builder.append_value(ARCHIVE_SCHEMA_VERSION);
        device_builder.append_value(&evt.device_id);
        let ts_i64 = i64::try_from(evt.ts_ms).context("timestamp too large")?;
        ts_builder.append_value(ts_i64);
        payload_builder.append_value(&evt.payload);

        writer.rows = writer.rows.saturating_add(1);
        writer.payload_bytes = writer.payload_bytes.saturating_add(evt.payload.len() as u64);
        writer.min_ts_ms = writer.min_ts_ms.min(evt.ts_ms);
        writer.max_ts_ms = writer.max_ts_ms.max(evt.ts_ms);
    }

    let batch = RecordBatch::try_new(
        archive_schema(),
        vec![
            Arc::new(schema_builder.finish()),
            Arc::new(device_builder.finish()),
            Arc::new(ts_builder.finish()),
            Arc::new(payload_builder.finish()),
        ],
    )?;
    let parquet = writer
        .writer
        .as_mut()
        .context("parquet writer missing in flush_pending")?;
    parquet.write(&batch)?;
    Ok(())
}

/// 封存单个 writer：flush、close、rename，并写入 manifest。
fn seal_writer(cfg: &ArchiveConfig, writer: &mut DeviceWriter) -> anyhow::Result<()> {
    flush_pending(writer)?;
    let mut parquet = writer
        .writer
        .take()
        .context("parquet writer missing in seal_writer")?;
    parquet.flush()?;
    let _ = parquet.close()?;

    std::fs::rename(&writer.tmp_path, &writer.final_path).with_context(|| {
        format!(
            "rename parquet tmp failed: {} -> {}",
            writer.tmp_path.display(),
            writer.final_path.display()
        )
    })?;
    let file_size_bytes = std::fs::metadata(&writer.final_path)?.len();
    let created_at_ms = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();

    let entry = ManifestEntry {
        schema_version: ARCHIVE_SCHEMA_VERSION,
        device_id: writer.device_id.clone(),
        file_name: writer.file_name.clone(),
        day_key: writer.day_key.clone(),
        hour: writer.hour,
        part: writer.part,
        min_ts_ms: if writer.rows == 0 { 0 } else { writer.min_ts_ms },
        max_ts_ms: writer.max_ts_ms,
        rows: writer.rows,
        payload_bytes: writer.payload_bytes,
        file_size_bytes,
        sealed: true,
        created_at_ms,
    };
    append_manifest_entry(&writer.day_dir, &entry)?;
    if writer.rows == 0 {
        warn!(path = %writer.final_path.display(), "sealed empty parquet segment");
    }
    if cfg.rotate_size_mb > 0 {
        info!(
            path = %writer.final_path.display(),
            rows = writer.rows,
            payload_bytes = writer.payload_bytes,
            "archive parquet segment sealed"
        );
    }
    Ok(())
}

/// 返回归档 Parquet 固定 schema。
fn archive_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("schema_version", DataType::Int16, false),
        Field::new("device_id", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}
