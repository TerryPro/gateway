//! 时序数据存储模块，提供 Parquet 文件的读写能力。
//!
//! 支持两种存储模式：
//! - `PacketWide`: 宽表模式，每行是一个时间戳 + 多个测点值
//! - `LongRow`: 长表模式，每行是一个时间戳 + 一个测点值

use anyhow::{Context, Result};
use arrow::array::{Array, Float32Array, Float32Builder, ListArray, ListBuilder, StringArray, StringBuilder, UInt64Array, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Local, TimeZone, Timelike};
use clap::ValueEnum;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// 存储模式枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageMode {
    /// 宽表模式：每行是一个时间戳 + 多个测点值（适合一包多测点场景）。
    #[default]
    PacketWide,
    /// 长表模式：每行是一个时间戳 + 一个测点值（适合稀疏数据）。
    LongRow,
}

/// 宽表模式 Schema：ts + param_ids(list) + values(list)。
pub fn build_packet_wide_schema() -> Arc<Schema> {
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

/// 长表模式 Schema：ts + param_id + value。
pub fn build_long_row_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::UInt64, false),
        Field::new("param_id", DataType::Utf8, false),
        Field::new("value", DataType::Float32, false),
    ]))
}

/// 宽表模式数据行。
#[derive(Debug, Clone)]
pub struct PacketWideRow {
    pub ts: u64,
    pub param_ids: Vec<String>,
    pub values: Vec<f32>,
}

/// 长表模式数据行。
#[derive(Debug, Clone)]
pub struct LongRow {
    pub ts: u64,
    pub param_id: String,
    pub value: f32,
}

/// Parquet 文件写入器配置。
#[derive(Debug, Clone)]
pub struct ParquetWriterConfig {
    pub row_group_rows: usize,
    pub compression: Compression,
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            row_group_rows: 50_000,
            compression: Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3).unwrap()),
        }
    }
}

/// 将宽表数据行写入 Parquet 文件。
pub fn write_packet_wide_parquet(
    path: &Path,
    rows: &[PacketWideRow],
    config: Option<ParquetWriterConfig>,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    let config = config.unwrap_or_default();
    let schema = build_packet_wide_schema();

    // 构建 ts 列
    let mut ts_builder = UInt64Builder::with_capacity(rows.len());
    for row in rows {
        ts_builder.append_value(row.ts);
    }
    let ts_array = ts_builder.finish();

    // 构建 param_ids 列（ListArray）
    let mut param_ids_builder = ListBuilder::new(StringBuilder::new());
    for row in rows {
        for param_id in &row.param_ids {
            param_ids_builder.values().append_value(param_id);
        }
        param_ids_builder.append(true);
    }
    let param_ids_array = param_ids_builder.finish();

    // 构建 values 列（ListArray）
    let mut values_builder = ListBuilder::new(Float32Builder::new());
    for row in rows {
        for value in &row.values {
            values_builder.values().append_value(*value);
        }
        values_builder.append(true);
    }
    let values_array = values_builder.finish();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ts_array),
            Arc::new(param_ids_array),
            Arc::new(values_array),
        ],
    )?;

    write_parquet_file(path, schema, &[batch], &config)
}

/// 将长表数据行写入 Parquet 文件。
pub fn write_long_row_parquet(
    path: &Path,
    rows: &[LongRow],
    config: Option<ParquetWriterConfig>,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    let config = config.unwrap_or_default();
    let schema = build_long_row_schema();

    // 构建 ts 列
    let mut ts_builder = UInt64Builder::with_capacity(rows.len());
    for row in rows {
        ts_builder.append_value(row.ts);
    }
    let ts_array = ts_builder.finish();

    // 构建 param_id 列
    let mut param_id_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 10);
    for row in rows {
        param_id_builder.append_value(&row.param_id);
    }
    let param_id_array = param_id_builder.finish();

    // 构建 value 列
    let mut value_builder = Float32Builder::with_capacity(rows.len());
    for row in rows {
        value_builder.append_value(row.value);
    }
    let value_array = value_builder.finish();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ts_array),
            Arc::new(param_id_array),
            Arc::new(value_array),
        ],
    )?;

    write_parquet_file(path, schema, &[batch], &config)
}

/// 底层 Parquet 文件写入函数。
fn write_parquet_file(
    path: &Path,
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    config: &ParquetWriterConfig,
) -> Result<u64> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create dir failed: {}", parent.display()))?;
    }

    let file = File::create(path)
        .with_context(|| format!("create parquet file failed: {}", path.display()))?;

    let props = WriterProperties::builder()
        .set_max_row_group_size(config.row_group_rows)
        .set_compression(config.compression)
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

    let mut total_rows = 0u64;
    for batch in batches {
        total_rows += batch.num_rows() as u64;
        writer.write(batch)?;
    }

    writer.close()?;
    Ok(total_rows)
}

/// 从 Parquet 文件读取所有行（宽表模式）。
pub fn read_packet_wide_parquet(path: &Path) -> Result<Vec<PacketWideRow>> {
    let file = File::open(path)
        .with_context(|| format!("open parquet file failed: {}", path.display()))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;

    let mut rows = Vec::new();

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;

        let ts_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("ts column is not UInt64")?;

        let param_ids_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("param_ids column is not ListArray")?;

        let values_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .context("values column is not ListArray")?;

        for i in 0..batch.num_rows() {
            let ts = ts_array.value(i);

            let param_ids_list = param_ids_array.value(i);
            let param_ids_values = param_ids_list
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let param_ids: Vec<String> = (0..param_ids_values.len())
                .map(|j| param_ids_values.value(j).to_string())
                .collect();

            let values_list = values_array.value(i);
            let values_values = values_list
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let values: Vec<f32> = (0..values_values.len())
                .map(|j| values_values.value(j))
                .collect();

            rows.push(PacketWideRow {
                ts,
                param_ids,
                values,
            });
        }
    }

    Ok(rows)
}

/// 从 Parquet 文件读取所有行（长表模式）。
pub fn read_long_row_parquet(path: &Path) -> Result<Vec<LongRow>> {
    let file = File::open(path)
        .with_context(|| format!("open parquet file failed: {}", path.display()))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;

    let mut rows = Vec::new();

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;

        let ts_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("ts column is not UInt64")?;

        let param_id_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("param_id column is not StringArray")?;

        let value_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float32Array>()
            .context("value column is not Float32")?;

        for i in 0..batch.num_rows() {
            rows.push(LongRow {
                ts: ts_array.value(i),
                param_id: param_id_array.value(i).to_string(),
                value: value_array.value(i),
            });
        }
    }

    Ok(rows)
}

/// 生成 Parquet 文件路径（小时级分段）。
pub fn build_parquet_path(
    root: &Path,
    device_id: &str,
    ts_sec: u64,
    segment_seq: u32,
) -> PathBuf {
    let dt = Local.timestamp_opt(ts_sec as i64, 0).unwrap();
    let day_key = dt.format("%Y-%m-%d").to_string();
    let hour = dt.hour();

    root.join(device_id)
        .join(&day_key)
        .join(format!("{:02}", hour))
        .join(format!("seg_{:06}.parquet", segment_seq))
}

/// 获取 Parquet 文件的行数统计。
pub fn get_parquet_row_count(path: &Path) -> Result<u64> {
    let file = File::open(path)
        .with_context(|| format!("open parquet file failed: {}", path.display()))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata();

    let mut total_rows = 0u64;
    for row_group in metadata.row_groups() {
        total_rows += row_group.num_rows() as u64;
    }

    Ok(total_rows)
}

/// 获取 Parquet 文件的时间范围。
pub fn get_parquet_time_range(path: &Path) -> Result<Option<(u64, u64)>> {
    let file = File::open(path)
        .with_context(|| format!("open parquet file failed: {}", path.display()))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata();

    let mut min_ts = u64::MAX;
    let mut max_ts = u64::MIN;

    for row_group in metadata.row_groups() {
        for i in 0..row_group.num_columns() {
            let column = row_group.column(i);
            if let Some(stats) = column.statistics() {
                match stats {
                    Statistics::Int64(s) => {
                        if let Some(min) = s.min_opt() {
                            min_ts = min_ts.min(*min as u64);
                        }
                        if let Some(max) = s.max_opt() {
                            max_ts = max_ts.max(*max as u64);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    if min_ts == u64::MAX {
        return Ok(None);
    }

    Ok(Some((min_ts, max_ts)))
}

// ============================================================================
// 存储配置与清单
// ============================================================================

/// 压缩参数枚举。
#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CompressionArg {
    #[serde(rename = "uncompressed")]
    Uncompressed,
    #[serde(rename = "snappy")]
    Snappy,
    #[serde(rename = "zstd")]
    Zstd,
    #[serde(rename = "gzip")]
    Gzip,
}

impl CompressionArg {
    pub fn to_compression(&self) -> Compression {
        match self {
            CompressionArg::Uncompressed => Compression::UNCOMPRESSED,
            CompressionArg::Snappy => Compression::SNAPPY,
            CompressionArg::Zstd => Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3).unwrap()),
            CompressionArg::Gzip => Compression::GZIP(parquet::basic::GzipLevel::try_new(6).unwrap()),
        }
    }
}

impl Default for CompressionArg {
    fn default() -> Self {
        CompressionArg::Zstd
    }
}

/// 根目录存储配置文件结构（`_meta/storage.toml`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMeta {
    pub version: u32,
    pub default_mode: StorageMode,
    pub compression: CompressionArg,
    pub segment_sec: u64,
    pub segment_max_rows: usize,
    #[serde(default = "default_row_group_rows")]
    pub row_group_rows: usize,
    pub created_at: String,
}

fn default_row_group_rows() -> usize {
    50_000
}

impl StorageMeta {
    /// 创建默认存储配置。
    pub fn new(mode: StorageMode, compression: CompressionArg, segment_sec: u64) -> Self {
        Self {
            version: 1,
            default_mode: mode,
            compression,
            segment_sec,
            segment_max_rows: 1_000_000,
            row_group_rows: 50_000,
            created_at: Local::now().to_rfc3339(),
        }
    }

    /// 写入存储配置文件。
    pub fn write_to(&self, root: &Path) -> Result<()> {
        let meta_dir = root.join("_meta");
        std::fs::create_dir_all(&meta_dir)
            .with_context(|| format!("create meta dir failed: {}", meta_dir.display()))?;

        let path = meta_dir.join("storage.toml");
        let content = toml::to_string_pretty(self)
            .context("serialize storage meta failed")?;
        std::fs::write(&path, content)
            .with_context(|| format!("write storage meta failed: {}", path.display()))?;
        Ok(())
    }

    /// 从存储目录加载配置。
    pub fn load_from(root: &Path) -> Result<Self> {
        let path = root.join("_meta").join("storage.toml");
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("read storage meta failed: {}", path.display()))?;
        let meta: StorageMeta = toml::from_str(&content)
            .with_context(|| format!("parse storage meta failed: {}", path.display()))?;
        Ok(meta)
    }
}

/// 分段清单条目（manifest.jsonl 每行）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentManifestEntry {
    pub segment_file: String,
    pub min_ts: u64,
    pub max_ts: u64,
    pub rows: u64,
    #[serde(default)]
    pub points: u64,
    pub created_at_ms: u64,
    #[serde(default = "default_storage_mode")]
    pub mode: StorageMode,
}

fn default_storage_mode() -> StorageMode {
    StorageMode::PacketWide
}

impl SegmentManifestEntry {
    /// 创建新的清单条目。
    pub fn new(segment_file: &str, min_ts: u64, max_ts: u64, rows: u64, points: u64, mode: StorageMode) -> Self {
        Self {
            segment_file: segment_file.to_string(),
            min_ts,
            max_ts,
            rows,
            points,
            created_at_ms: Local::now().timestamp_millis() as u64,
            mode,
        }
    }

    /// 追加到 manifest 文件。
    pub fn append_to(&self, hour_dir: &Path) -> Result<()> {
        let path = hour_dir.join("manifest.jsonl");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open manifest failed: {}", path.display()))?;

        let line = serde_json::to_string(self)
            .context("serialize manifest entry failed")?;
        writeln!(file, "{}", line)
            .with_context(|| format!("write manifest failed: {}", path.display()))?;
        Ok(())
    }

    /// 从 manifest 文件读取所有条目。
    pub fn read_from(path: &Path) -> Result<Vec<Self>> {
        let file = File::open(path)
            .with_context(|| format!("open manifest failed: {}", path.display()))?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for (idx, line) in reader.lines().enumerate() {
            let line = line.with_context(|| format!("read manifest line {} failed", idx + 1))?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let entry: SegmentManifestEntry = serde_json::from_str(trimmed)
                .with_context(|| format!("parse manifest line {} failed", idx + 1))?;
            entries.push(entry);
        }

        Ok(entries)
    }
}

// ============================================================================
// 分段写入器
// ============================================================================

/// 分段写入器配置。
#[derive(Debug, Clone)]
pub struct SegmentWriterConfig {
    pub segment_sec: u64,
    pub segment_max_rows: usize,
    pub row_group_rows: usize,
    pub compression: Compression,
}

impl Default for SegmentWriterConfig {
    fn default() -> Self {
        Self {
            segment_sec: 1800,
            segment_max_rows: 1_000_000,
            row_group_rows: 50_000,
            compression: Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3).unwrap()),
        }
    }
}

/// 宽表模式分段写入器。
pub struct PacketWideSegmentWriter {
    segment_file: String,
    writer: Option<ArrowWriter<File>>,
    pending: Vec<PacketWideRow>,
    rows: u64,
    min_ts: u64,
    max_ts: u64,
    config: SegmentWriterConfig,
}

impl PacketWideSegmentWriter {
    /// 创建新的分段写入器。
    pub fn new(hour_dir: &Path, segment_seq: u32, _segment_start_ts: u64, config: SegmentWriterConfig) -> Result<Self> {
        let segment_file = format!("seg_{:06}.parquet", segment_seq);
        let path = hour_dir.join(&segment_file);

        let schema = build_packet_wide_schema();
        let file = File::create(&path)
            .with_context(|| format!("create segment file failed: {}", path.display()))?;

        let props = WriterProperties::builder()
            .set_max_row_group_size(config.row_group_rows)
            .set_compression(config.compression)
            .build();

        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        Ok(Self {
            segment_file,
            writer: Some(writer),
            pending: Vec::with_capacity(1024),
            rows: 0,
            min_ts: u64::MAX,
            max_ts: 0,
            config,
        })
    }

    /// 追加一行数据。
    pub fn append(&mut self, row: PacketWideRow) -> Result<bool> {
        self.min_ts = self.min_ts.min(row.ts);
        self.max_ts = self.max_ts.max(row.ts);
        self.pending.push(row);
        self.rows += 1;

        // 检查是否需要分段
        let need_flush = self.rows >= self.config.segment_max_rows as u64;
        Ok(need_flush)
    }

    /// 刷新到磁盘并关闭。
    pub fn flush_and_close(mut self, hour_dir: &Path) -> Result<SegmentManifestEntry> {
        if let Some(mut writer) = self.writer.take() {
            if !self.pending.is_empty() {
                write_packet_wide_parquet_inner(
                    &mut writer,
                    &self.pending,
                    &self.config,
                )?;
            }
            writer.flush()?;
            writer.close()?;
        }

        let entry = SegmentManifestEntry::new(
            &self.segment_file,
            if self.rows == 0 { 0 } else { self.min_ts },
            self.max_ts,
            self.rows,
            0, // points 在宽表模式下需要单独计算
            StorageMode::PacketWide,
        );
        entry.append_to(hour_dir)?;

        Ok(entry)
    }

    /// 获取当前行数。
    pub fn rows(&self) -> u64 {
        self.rows
    }
}

/// 长表模式分段写入器。
pub struct LongRowSegmentWriter {
    segment_file: String,
    writer: Option<ArrowWriter<File>>,
    pending: Vec<LongRow>,
    rows: u64,
    min_ts: u64,
    max_ts: u64,
    config: SegmentWriterConfig,
}

impl LongRowSegmentWriter {
    /// 创建新的分段写入器。
    pub fn new(hour_dir: &Path, segment_seq: u32, _segment_start_ts: u64, config: SegmentWriterConfig) -> Result<Self> {
        let segment_file = format!("seg_{:06}.parquet", segment_seq);
        let path = hour_dir.join(&segment_file);

        let schema = build_long_row_schema();
        let file = File::create(&path)
            .with_context(|| format!("create segment file failed: {}", path.display()))?;

        let props = WriterProperties::builder()
            .set_max_row_group_size(config.row_group_rows)
            .set_compression(config.compression)
            .build();

        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        Ok(Self {
            segment_file,
            writer: Some(writer),
            pending: Vec::with_capacity(1024),
            rows: 0,
            min_ts: u64::MAX,
            max_ts: 0,
            config,
        })
    }

    /// 追加一行数据。
    pub fn append(&mut self, row: LongRow) -> Result<bool> {
        self.min_ts = self.min_ts.min(row.ts);
        self.max_ts = self.max_ts.max(row.ts);
        self.pending.push(row);
        self.rows += 1;

        // 检查是否需要分段
        let need_flush = self.rows >= self.config.segment_max_rows as u64;
        Ok(need_flush)
    }

    /// 刷新到磁盘并关闭。
    pub fn flush_and_close(mut self, hour_dir: &Path) -> Result<SegmentManifestEntry> {
        if let Some(mut writer) = self.writer.take() {
            if !self.pending.is_empty() {
                write_long_row_parquet_inner(
                    &mut writer,
                    &self.pending,
                    &self.config,
                )?;
            }
            writer.flush()?;
            writer.close()?;
        }

        let entry = SegmentManifestEntry::new(
            &self.segment_file,
            if self.rows == 0 { 0 } else { self.min_ts },
            self.max_ts,
            self.rows,
            self.rows, // 长表模式下 points = rows
            StorageMode::LongRow,
        );
        entry.append_to(hour_dir)?;

        Ok(entry)
    }

    /// 获取当前行数。
    pub fn rows(&self) -> u64 {
        self.rows
    }
}

/// 内部函数：宽表数据写入。
fn write_packet_wide_parquet_inner(
    writer: &mut ArrowWriter<File>,
    rows: &[PacketWideRow],
    _config: &SegmentWriterConfig,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    // 构建 ts 列
    let mut ts_builder = UInt64Builder::with_capacity(rows.len());
    for row in rows {
        ts_builder.append_value(row.ts);
    }
    let ts_array = ts_builder.finish();

    // 构建 param_ids 列（ListArray）
    let mut param_ids_builder = ListBuilder::new(StringBuilder::new());
    for row in rows {
        for param_id in &row.param_ids {
            param_ids_builder.values().append_value(param_id);
        }
        param_ids_builder.append(true);
    }
    let param_ids_array = param_ids_builder.finish();

    // 构建 values 列（ListArray）
    let mut values_builder = ListBuilder::new(Float32Builder::new());
    for row in rows {
        for value in &row.values {
            values_builder.values().append_value(*value);
        }
        values_builder.append(true);
    }
    let values_array = values_builder.finish();

    let batch = RecordBatch::try_new(
        build_packet_wide_schema(),
        vec![
            Arc::new(ts_array),
            Arc::new(param_ids_array),
            Arc::new(values_array),
        ],
    )?;

    writer.write(&batch)?;
    Ok(())
}

/// 内部函数：长表数据写入。
fn write_long_row_parquet_inner(
    writer: &mut ArrowWriter<File>,
    rows: &[LongRow],
    _config: &SegmentWriterConfig,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    // 构建 ts 列
    let mut ts_builder = UInt64Builder::with_capacity(rows.len());
    for row in rows {
        ts_builder.append_value(row.ts);
    }
    let ts_array = ts_builder.finish();

    // 构建 param_id 列
    let mut param_id_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 10);
    for row in rows {
        param_id_builder.append_value(&row.param_id);
    }
    let param_id_array = param_id_builder.finish();

    // 构建 value 列
    let mut value_builder = Float32Builder::with_capacity(rows.len());
    for row in rows {
        value_builder.append_value(row.value);
    }
    let value_array = value_builder.finish();

    let batch = RecordBatch::try_new(
        build_long_row_schema(),
        vec![
            Arc::new(ts_array),
            Arc::new(param_id_array),
            Arc::new(value_array),
        ],
    )?;

    writer.write(&batch)?;
    Ok(())
}
