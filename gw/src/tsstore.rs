use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use arrow::{
    array::{Float32Builder, ListBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use chrono::Utc;
use common::tsmeta::{day_hour_from_ts, is_valid_param_code};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::properties::WriterProperties,
};
use serde::Serialize;
use tokio::{
    sync::{RwLock, mpsc, oneshot},
    time::{Duration, interval},
};
use tracing::{error, info, warn};
use redb::ReadableTable;

use crate::cli::TsStoreConfig;

const FLUSH_BATCH_ROWS: usize = 512;
const MANIFEST_FILE_NAME: &str = "manifest.jsonl";
const TSINDEX_FILE_NAME: &str = "tsindex.redb";
const TSINDEX_HOURLY_SEGMENTS_TABLE: redb::TableDefinition<&str, &str> =
    redb::TableDefinition::new("hourly_segments");

/// 参数存储事件，由设备会话线程投递给 `tsstore` 工作线程。
#[derive(Debug, Clone)]
pub struct TsStoreEvent {
    pub device_id: String,
    pub ts_ms: u64,
    pub payload: Vec<u8>,
}

/// 热数据查询结果中的单条记录结构。
#[derive(Debug, Clone, Serialize)]
pub struct TsPacket {
    pub ts: u64,
    pub param_ids: Vec<String>,
    pub values: Vec<f32>,
}

/// 参数存储后台任务句柄，用于发送事件、查询热数据和优雅停机。
pub struct TsStoreWorkerHandle {
    pub tx: mpsc::Sender<TsStoreEvent>,
    #[allow(dead_code)]
    hot_store: Arc<RwLock<HashMap<String, VecDeque<TsPacket>>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl TsStoreWorkerHandle {
    /// 查询指定设备最近热数据窗口中的记录（按时间范围过滤）。
    #[allow(dead_code)]
    pub async fn query_hot(&self, device_id: &str, from_ts: u64, to_ts: u64) -> Vec<TsPacket> {
        let guard = self.hot_store.read().await;
        let Some(buf) = guard.get(device_id) else {
            return Vec::new();
        };
        buf.iter()
            .filter(|x| x.ts >= from_ts && x.ts <= to_ts)
            .cloned()
            .collect()
    }

    /// 发送停机信号并等待后台任务完成收尾。
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Err(e) = self.join_handle.await {
            error!(error = ?e, "tsstore worker join failed");
        }
    }
}

/// 分段清单条目，用于查询层快速定位可见文件范围。
#[derive(Debug, Clone, Serialize)]
struct SegmentManifestEntry {
    segment_file: String,
    min_ts: u64,
    max_ts: u64,
    rows: u64,
    param_ids: Vec<String>,
    created_at_ms: u64,
}

/// `redb` 中按小时聚合存储的分段索引条目。
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
struct IndexSegmentEntry {
    segment_file: String,
    min_ts: u64,
    max_ts: u64,
    rows: u64,
    param_ids: Vec<String>,
}

/// 单设备当前活跃分段写入上下文。
struct DeviceSegmentWriter {
    device_id: String,
    day_key: String,
    hour: u32,
    hour_dir: PathBuf,
    segment_seq: u32,
    segment_start_ts: u64,
    segment_file: String,
    writer: Option<ArrowWriter<File>>,
    pending: Vec<TsPacket>,
    rows: u64,
    min_ts: u64,
    max_ts: u64,
    param_ids: BTreeSet<String>,
}

/// 启动参数存储后台任务，并返回事件发送句柄。
pub fn start_tsstore_worker(cfg: TsStoreConfig) -> Option<TsStoreWorkerHandle> {
    if !cfg.enabled {
        info!("tsstore disabled by config");
        return None;
    }
    let (tx, rx) = mpsc::channel(cfg.queue_capacity);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let hot_store = Arc::new(RwLock::new(HashMap::<String, VecDeque<TsPacket>>::new()));
    let hot_store_for_task = hot_store.clone();
    let join_handle = tokio::spawn(async move {
        if let Err(e) = tsstore_worker_loop(cfg, rx, shutdown_rx, hot_store_for_task).await {
            error!(error = ?e, "tsstore worker exited with error");
        }
    });
    Some(TsStoreWorkerHandle {
        tx,
        hot_store,
        shutdown_tx: Some(shutdown_tx),
        join_handle,
    })
}

/// 参数存储主循环：串行消费事件，维护热数据与 Parquet 分段写入。
async fn tsstore_worker_loop(
    cfg: TsStoreConfig,
    mut rx: mpsc::Receiver<TsStoreEvent>,
    mut shutdown_rx: oneshot::Receiver<()>,
    hot_store: Arc<RwLock<HashMap<String, VecDeque<TsPacket>>>>,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(&cfg.root_dir)
        .with_context(|| format!("create tsstore root failed: {}", cfg.root_dir))?;
    info!(root = %cfg.root_dir, "tsstore worker started");

    let mut writers: HashMap<String, DeviceSegmentWriter> = HashMap::new();
    let mut ticker = interval(Duration::from_millis(cfg.flush_interval_ms));
    loop {
        tokio::select! {
            maybe_evt = rx.recv() => {
                let Some(evt) = maybe_evt else {
                    break;
                };
                if let Some(packet) = parse_payload_to_packet(evt.ts_ms, &evt.payload) {
                    push_hot_packet(&hot_store, &evt.device_id, packet.clone(), cfg.hot_max_packets_per_device).await;
                    if let Err(e) = write_packet(&cfg, &mut writers, &evt.device_id, packet) {
                        error!(error = ?e, device_id = %evt.device_id, "tsstore write packet failed");
                    }
                }
            }
            _ = ticker.tick() => {
                if let Err(e) = flush_all(&mut writers) {
                    warn!(error = ?e, "tsstore flush failed");
                }
            }
            _ = &mut shutdown_rx => {
                break;
            }
        }
    }

    while let Ok(evt) = rx.try_recv() {
        if let Some(packet) = parse_payload_to_packet(evt.ts_ms, &evt.payload)
            && let Err(e) = write_packet(&cfg, &mut writers, &evt.device_id, packet)
        {
            error!(error = ?e, device_id = %evt.device_id, "tsstore write packet failed while draining");
        }
    }

    seal_all(&mut writers)?;
    info!("tsstore worker stopped");
    Ok(())
}

/// 将输入负载解析为参数包（要求 JSON 对象，值均为数值）。
fn parse_payload_to_packet(ts_ms: u64, payload: &[u8]) -> Option<TsPacket> {
    let value = serde_json::from_slice::<serde_json::Value>(payload).ok()?;
    let obj = value.as_object()?;
    if obj.is_empty() {
        return None;
    }

    let mut param_ids = Vec::with_capacity(obj.len());
    let mut values = Vec::with_capacity(obj.len());
    for (k, v) in obj {
        if !is_valid_param_code(k) {
            continue;
        }
        let Some(n) = v.as_f64() else {
            continue;
        };
        if !n.is_finite() {
            continue;
        }
        param_ids.push(k.to_ascii_uppercase());
        values.push(n as f32);
    }
    if param_ids.is_empty() {
        return None;
    }
    Some(TsPacket {
        ts: ts_ms / 1000,
        param_ids,
        values,
    })
}

/// 将记录写入设备热缓存，并按容量上限做环形淘汰。
async fn push_hot_packet(
    hot_store: &Arc<RwLock<HashMap<String, VecDeque<TsPacket>>>>,
    device_id: &str,
    packet: TsPacket,
    max_packets: usize,
) {
    let mut guard = hot_store.write().await;
    let buf = guard.entry(device_id.to_string()).or_default();
    buf.push_back(packet);
    while buf.len() > max_packets {
        let _ = buf.pop_front();
    }
}

/// 写入一条参数包，必要时触发分段滚动。
fn write_packet(
    cfg: &TsStoreConfig,
    writers: &mut HashMap<String, DeviceSegmentWriter>,
    device_id: &str,
    packet: TsPacket,
) -> anyhow::Result<()> {
    let (day_key, hour) = day_hour_from_ts(packet.ts);
    let need_rotate = should_rotate(cfg, writers.get(device_id), &day_key, hour, packet.ts);
    if need_rotate {
        if let Some(mut old) = writers.remove(device_id) {
            seal_writer(&mut old)?;
        }
        let new_writer = open_new_writer(cfg, device_id, &day_key, hour, packet.ts)?;
        writers.insert(device_id.to_string(), new_writer);
    } else if !writers.contains_key(device_id) {
        let new_writer = open_new_writer(cfg, device_id, &day_key, hour, packet.ts)?;
        writers.insert(device_id.to_string(), new_writer);
    }

    let writer = writers
        .get_mut(device_id)
        .context("tsstore writer missing after open")?;
    writer.pending.push(packet);
    if writer.pending.len() >= FLUSH_BATCH_ROWS {
        flush_pending(writer)?;
    }
    Ok(())
}

/// 判断当前分段是否需要滚动到新文件。
fn should_rotate(
    cfg: &TsStoreConfig,
    current: Option<&DeviceSegmentWriter>,
    day_key: &str,
    hour: u32,
    ts: u64,
) -> bool {
    let Some(current) = current else {
        return true;
    };
    if current.day_key != day_key || current.hour != hour {
        return true;
    }
    if ts.saturating_sub(current.segment_start_ts) >= cfg.segment_interval_sec {
        return true;
    }
    let predicted_rows = current.rows.saturating_add(current.pending.len() as u64);
    predicted_rows >= cfg.segment_max_rows as u64
}

/// 刷新所有活跃分段的待写入批次并执行 Parquet flush。
fn flush_all(writers: &mut HashMap<String, DeviceSegmentWriter>) -> anyhow::Result<()> {
    for writer in writers.values_mut() {
        flush_pending(writer)?;
        let parquet = writer
            .writer
            .as_mut()
            .context("tsstore parquet writer missing in flush_all")?;
        parquet.flush()?;
    }
    Ok(())
}

/// 封存所有活跃分段，保证进程退出时落盘完整。
fn seal_all(writers: &mut HashMap<String, DeviceSegmentWriter>) -> anyhow::Result<()> {
    let keys: Vec<String> = writers.keys().cloned().collect();
    for key in keys {
        if let Some(mut writer) = writers.remove(&key) {
            seal_writer(&mut writer)?;
        }
    }
    Ok(())
}

/// 打开新分段 writer（直接写最终文件，查询层以 manifest 可见性为准）。
fn open_new_writer(
    cfg: &TsStoreConfig,
    device_id: &str,
    day_key: &str,
    hour: u32,
    ts: u64,
) -> anyhow::Result<DeviceSegmentWriter> {
    let hour_dir = build_hour_dir(&cfg.root_dir, device_id, day_key, hour);
    std::fs::create_dir_all(&hour_dir)
        .with_context(|| format!("create tsstore hour dir failed: {}", hour_dir.display()))?;
    let segment_seq = detect_next_segment_seq(&hour_dir)?;
    let segment_file = format!("seg_{:010}_{:04}.parquet", ts, segment_seq);
    let file_path = hour_dir.join(&segment_file);
    let file = File::create(&file_path)
        .with_context(|| format!("create tsstore parquet failed: {}", file_path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let writer = ArrowWriter::try_new(file, tsstore_schema(), Some(props))?;

    Ok(DeviceSegmentWriter {
        device_id: device_id.to_string(),
        day_key: day_key.to_string(),
        hour,
        hour_dir,
        segment_seq,
        segment_start_ts: ts,
        segment_file,
        writer: Some(writer),
        pending: Vec::with_capacity(FLUSH_BATCH_ROWS),
        rows: 0,
        min_ts: u64::MAX,
        max_ts: 0,
        param_ids: BTreeSet::new(),
    })
}

/// 扫描小时目录并计算下一个分段序号。
fn detect_next_segment_seq(hour_dir: &Path) -> anyhow::Result<u32> {
    let mut max_seq = 0_u32;
    let rd = std::fs::read_dir(hour_dir)?;
    for entry in rd {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with("seg_") || !name.ends_with(".parquet") {
            continue;
        }
        let stem = &name[..name.len().saturating_sub(".parquet".len())];
        let Some((_, seq_raw)) = stem.rsplit_once('_') else {
            continue;
        };
        let Ok(seq) = seq_raw.parse::<u32>() else {
            continue;
        };
        if seq > max_seq {
            max_seq = seq;
        }
    }
    Ok(max_seq.saturating_add(1).max(1))
}

/// 将待写入参数包批次转换为 RecordBatch 并写入当前分段。
fn flush_pending(writer: &mut DeviceSegmentWriter) -> anyhow::Result<()> {
    if writer.pending.is_empty() {
        return Ok(());
    }
    let mut draining = Vec::new();
    std::mem::swap(&mut draining, &mut writer.pending);
    let count = draining.len();
    let mut ts_builder = UInt64Builder::with_capacity(count);
    let mut param_ids_builder = ListBuilder::new(arrow::array::StringBuilder::new());
    let mut values_builder = ListBuilder::new(Float32Builder::new());
    let mut local_rows = 0_u64;
    let mut local_min_ts = u64::MAX;
    let mut local_max_ts = 0_u64;
    let mut local_param_ids = BTreeSet::new();

    for packet in &draining {
        ts_builder.append_value(packet.ts);
        for id in &packet.param_ids {
            param_ids_builder.values().append_value(id);
            local_param_ids.insert(id.clone());
        }
        param_ids_builder.append(true);

        for v in &packet.values {
            values_builder.values().append_value(*v);
        }
        values_builder.append(true);

        local_rows = local_rows.saturating_add(1);
        local_min_ts = local_min_ts.min(packet.ts);
        local_max_ts = local_max_ts.max(packet.ts);
    }

    let batch = RecordBatch::try_new(
        tsstore_schema(),
        vec![
            Arc::new(ts_builder.finish()),
            Arc::new(param_ids_builder.finish()),
            Arc::new(values_builder.finish()),
        ],
    )?;
    let parquet = writer
        .writer
        .as_mut()
        .context("tsstore parquet writer missing in flush_pending")?;
    if let Err(e) = parquet.write(&batch) {
        writer.pending = draining;
        return Err(e.into());
    }
    writer.rows = writer.rows.saturating_add(local_rows);
    if local_rows > 0 {
        writer.min_ts = writer.min_ts.min(local_min_ts);
        writer.max_ts = writer.max_ts.max(local_max_ts);
        for id in local_param_ids {
            writer.param_ids.insert(id);
        }
    }
    Ok(())
}

/// 封存当前分段并追加 manifest 条目，完成可见性提交。
fn seal_writer(writer: &mut DeviceSegmentWriter) -> anyhow::Result<()> {
    flush_pending(writer)?;
    let mut parquet = writer
        .writer
        .take()
        .context("tsstore parquet writer missing in seal_writer")?;
    parquet.flush()?;
    let _ = parquet.close()?;

    let entry = SegmentManifestEntry {
        segment_file: writer.segment_file.clone(),
        min_ts: if writer.rows == 0 { 0 } else { writer.min_ts },
        max_ts: writer.max_ts,
        rows: writer.rows,
        param_ids: writer.param_ids.iter().cloned().collect(),
        created_at_ms: now_ms(),
    };
    append_manifest(&writer.hour_dir, &entry)?;
    if let Err(e) = append_segment_index(
        &writer.hour_dir,
        &writer.device_id,
        &writer.day_key,
        writer.hour,
        &entry,
    ) {
        warn!(
            error = ?e,
            device_id = %writer.device_id,
            day = %writer.day_key,
            hour = writer.hour,
            "tsstore append redb index failed, manifest remains source of truth"
        );
    }
    info!(
        device_id = %writer.device_id,
        day = %writer.day_key,
        hour = writer.hour,
        segment_seq = writer.segment_seq,
        rows = writer.rows,
        "tsstore segment sealed"
    );
    Ok(())
}

/// 将封段信息写入 `redb` 小时索引。
fn append_segment_index(
    hour_dir: &Path,
    device_id: &str,
    day_key: &str,
    hour: u32,
    entry: &SegmentManifestEntry,
) -> anyhow::Result<()> {
    let db_path = tsindex_db_path(hour_dir)?;
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create tsindex dir failed: {}", parent.display()))?;
    }
    let db = if db_path.exists() {
        redb::Database::open(&db_path)
            .with_context(|| format!("open tsindex db failed: {}", db_path.display()))?
    } else {
        redb::Database::create(&db_path)
            .with_context(|| format!("create tsindex db failed: {}", db_path.display()))?
    };
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TSINDEX_HOURLY_SEGMENTS_TABLE)?;
        let key = hourly_index_key(device_id, day_key, hour);
        let mut items = if let Some(v) = table.get(key.as_str())? {
            let raw = v.value().to_string();
            serde_json::from_str::<Vec<IndexSegmentEntry>>(&raw).unwrap_or_default()
        } else {
            Vec::new()
        };
        let item = IndexSegmentEntry {
            segment_file: entry.segment_file.clone(),
            min_ts: entry.min_ts,
            max_ts: entry.max_ts,
            rows: entry.rows,
            param_ids: entry.param_ids.clone(),
        };
        if let Some(idx) = items.iter().position(|x| x.segment_file == item.segment_file) {
            items[idx] = item;
        } else {
            items.push(item);
        }
        let payload = serde_json::to_string(&items)?;
        table.insert(key.as_str(), payload.as_str())?;
    }
    write_txn.commit()?;
    Ok(())
}

/// 返回 `redb` 小时索引数据库路径：`root/_index/tsindex.redb`。
fn tsindex_db_path(hour_dir: &Path) -> anyhow::Result<PathBuf> {
    let root = hour_dir
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .context("resolve tsindex root from hour_dir failed")?;
    Ok(root.join("_index").join(TSINDEX_FILE_NAME))
}

/// 生成小时索引主键：`device|day|hour`。
fn hourly_index_key(device_id: &str, day_key: &str, hour: u32) -> String {
    format!("{device_id}|{day_key}|{hour:02}")
}

/// 向小时目录的 manifest 追加一条分段元数据。
fn append_manifest(hour_dir: &Path, entry: &SegmentManifestEntry) -> anyhow::Result<()> {
    let manifest_path = hour_dir.join(MANIFEST_FILE_NAME);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&manifest_path)
        .with_context(|| format!("open tsstore manifest failed: {}", manifest_path.display()))?;
    let line = serde_json::to_string(entry)?;
    file.write_all(line.as_bytes())?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

/// 构建参数存储目录路径：`root/device_id/YYYY-MM-DD/HH`。
fn build_hour_dir(root: &str, device_id: &str, day_key: &str, hour: u32) -> PathBuf {
    PathBuf::from(root)
        .join(device_id)
        .join(day_key)
        .join(format!("{:02}", hour))
}

/// 返回参数存储 Parquet 的固定 schema。
fn tsstore_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::UInt64, false),
        Field::new(
            "param_ids",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            false,
        ),
    ]))
}

/// 获取当前 UTC 毫秒时间戳。
fn now_ms() -> u64 {
    u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default()
}
