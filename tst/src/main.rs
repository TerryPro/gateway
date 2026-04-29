use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{
    Array, Float32Builder, ListArray, ListBuilder, StringArray, StringBuilder, UInt64Array,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, Duration, Local, NaiveDate, TimeZone, Timelike, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use common::tsmeta::is_valid_param_code;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redb::{ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

/// `tst` 命令行入口参数。
#[derive(Debug, Parser)]
#[command(name = "tst", version, about = "时序数据工具集（生成、导入、查询）")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// `tst` 子命令集合。
#[derive(Debug, Subcommand)]
enum Command {
    Gen(GenArgs),
    Import(ImportArgs),
    Stats(StatsArgs),
    Verify(VerifyArgs),
    Reindex(ReindexArgs),
    Export(ExportArgs),
}

/// 生成命令参数。
#[derive(Debug, Args, Clone)]
struct GenArgs {
    /// 设备代号，对应输出字段 `id`。
    #[arg(long, default_value = "dev001")]
    id: String,
    /// 起始时间（本地时间）格式：YYYYMMDDHH，例如：2026042400。
    #[arg(long)]
    start: String,
    /// 时间范围，格式：<数字><单位>，例如：1h、30m、1d。
    #[arg(long)]
    range: String,
    /// 发包频率间隔（毫秒）。
    #[arg(long, default_value_t = 500)]
    interval_ms: i64,
    /// 每包测点数量。
    #[arg(long, default_value_t = 2000)]
    points_per_packet: usize,
    /// 测点最小编号（包含），例如 1 -> P00001。
    #[arg(long, default_value_t = 1)]
    point_min: u32,
    /// 测点最大编号（包含），例如 10000 -> P10000。
    #[arg(long, default_value_t = 10000)]
    point_max: u32,
    /// 随机种子，不传时使用固定值保证可复现。
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// 输出 JSONL 文件路径。
    #[arg(long, default_value = "out/dev001_24h.jsonl")]
    out: PathBuf,
}

/// 导入命令参数。
#[derive(Debug, Args, Clone)]
struct ImportArgs {
    /// 输入 JSONL 文件路径（由 gen 生成）。
    #[arg(long)]
    input: PathBuf,
    /// 输出根目录（将创建 tsdata-like 目录结构）。
    #[arg(long, default_value = "tsdata_ingest")]
    root: PathBuf,
    /// 存储模式。
    #[arg(long, value_enum, default_value_t = StorageMode::PacketWide)]
    mode: StorageMode,
    /// 分段时间窗口（秒），超过后滚动新 parquet。
    #[arg(long, default_value_t = 1800)]
    segment_sec: u64,
    /// 分段最大行数，超过后滚动新 parquet。
    #[arg(long, default_value_t = 500_000)]
    segment_max_rows: usize,
    /// Parquet row group 最大行数（用于后续 row-group 级查询剪枝）。
    #[arg(long, default_value_t = 50_000)]
    row_group_rows: usize,
    /// 批量落盘行数，达到后写入 Arrow batch（主要作用于 packet-wide 模式）。
    #[arg(long, default_value_t = 512)]
    batch_rows: usize,
    /// Parquet 压缩方式。
    #[arg(long, value_enum, default_value_t = CompressionArg::Zstd)]
    compression: CompressionArg,
    /// 可选设备过滤，仅导入目标设备。
    #[arg(long)]
    device_id: Option<String>,
}

/// 统计命令参数。
#[derive(Debug, Args, Clone)]
struct StatsArgs {
    /// 存储根目录。
    #[arg(long, default_value = "tsdata_ingest")]
    root: PathBuf,
    /// 可选设备过滤，仅统计目标设备。
    #[arg(long)]
    device_id: Option<String>,
}

/// 校验命令参数。
#[derive(Debug, Args, Clone)]
struct VerifyArgs {
    /// 输入 JSONL 文件路径（作为基准）。
    #[arg(long)]
    input: PathBuf,
    /// 存储根目录。
    #[arg(long, default_value = "tsdata_ingest")]
    root: PathBuf,
    /// 可选设备过滤，仅校验目标设备。
    #[arg(long)]
    device_id: Option<String>,
}

/// 重建索引命令参数。
#[derive(Debug, Args, Clone)]
struct ReindexArgs {
    /// 存储根目录。
    #[arg(long, default_value = "tsdata_ingest")]
    root: PathBuf,
    /// 可选设备过滤，仅重建目标设备索引。
    #[arg(long)]
    device_id: Option<String>,
    /// 是否在重建前备份旧索引文件。
    #[arg(long, default_value_t = false)]
    backup: bool,
}

/// 导出命令参数。
#[derive(Debug, Args, Clone)]
struct ExportArgs {
    /// 输入 Parquet 文件路径。
    #[arg(long)]
    input: PathBuf,
    /// 输出 JSONL 文件路径。
    #[arg(long, default_value = "out/export.jsonl")]
    out: PathBuf,
    /// 存储模式（需要与输入文件模式匹配）。
    #[arg(long, value_enum, default_value_t = StorageMode::LongRow)]
    mode: StorageMode,
    /// 可选设备 ID（用于输出）。
    #[arg(long, default_value = "dev001")]
    device_id: String,
}

/// 支持的存储模式。
#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StorageMode {
    /// 行结构为 `ts + param_ids(list) + values(list)`，兼容 tsd 当前查询逻辑。
    PacketWide,
    /// 行结构为 `ts + param_id + value`，更利于参数级查询。
    LongRow,
}

/// Parquet 压缩选项。
#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CompressionArg {
    Zstd,
    Snappy,
    Uncompressed,
}

/// 输入 JSONL 每行的结构定义。
#[derive(Debug, Deserialize)]
struct InputPacket {
    id: String,
    t: u64,
    #[allow(dead_code)]
    s: u64,
    p: HashMap<String, serde_json::Value>,
}

/// 导入过程统计信息。
#[derive(Debug, Default)]
struct ImportStats {
    input_lines: u64,
    accepted_packets: u64,
    accepted_points: u64,
    skipped_packets: u64,
}

/// `stats` 汇总结果。
#[derive(Debug, Default)]
struct StorageStats {
    devices: usize,
    manifests: u64,
    segments: u64,
    rows: u64,
    points: u64,
    min_ts: Option<u64>,
    max_ts: Option<u64>,
}

/// 输入 JSONL 汇总结果（用于 verify 对比）。
#[derive(Debug, Default)]
struct SourceStats {
    packets: u64,
    points: u64,
}

/// 小时级 manifest 信息（用于重建索引）。
#[derive(Debug, Clone)]
struct HourManifestInfo {
    device_id: String,
    day_key: String,
    hour: u32,
    manifest: PathBuf,
}

/// 每个 Parquet 段写入完成后落盘的清单条目。
#[derive(Debug, Serialize, Deserialize)]
struct SegmentManifestEntry {
    segment_file: String,
    min_ts: u64,
    max_ts: u64,
    rows: u64,
    #[serde(default)]
    points: u64,
    created_at_ms: u64,
    #[serde(default = "default_storage_mode")]
    mode: StorageMode,
}

/// 根目录级存储模式声明文件结构（`_meta/storage.toml`）。
#[derive(Debug, Serialize, Deserialize)]
struct StorageMeta {
    version: u32,
    default_mode: StorageMode,
    compression: CompressionArg,
    segment_sec: u64,
    segment_max_rows: usize,
    #[serde(default = "default_row_group_rows")]
    row_group_rows: usize,
    created_at: String,
}

/// `redb` 索引文件名。
const TSINDEX_FILE_NAME: &str = "tsindex.redb";
/// `.pidx` 二进制格式魔数（Parameter InDeX）。
const PIDX_MAGIC: [u8; 4] = *b"PIDX";
/// `.pidx` 二进制格式版本号。
const PIDX_BINARY_VERSION: u8 = 2;
/// `redb` 小时索引表定义，与 `tsd/gw` 保持一致。
const TSINDEX_HOURLY_SEGMENTS_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("hourly_segments");

/// 写入 `redb` 的小时段索引条目结构。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexSegmentEntry {
    segment_file: String,
    min_ts: u64,
    max_ts: u64,
    rows: u64,
}

/// 参数在 row-group 内的连续行区间索引条目（`end_row` 为开区间）。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentParamIndexEntry {
    rg_id: u32,
    param_id: String,
    start_row: u32,
    end_row: u32,
    min_ts: u64,
    max_ts: u64,
}

/// 单条宽表数据行（一包一行）。
#[derive(Debug, Clone)]
struct PacketWideRow {
    ts: u64,
    param_ids: Vec<String>,
    values: Vec<f32>,
}

/// 单条长表数据行（一点一行）。
#[derive(Debug, Clone)]
struct LongRow {
    ts: u64,
    param_id: String,
    value: f32,
}

/// 小时目录维度的 writer key。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HourKey {
    device_id: String,
    day_key: String,
    hour: u32,
}

/// 宽表模式的小时写入状态。
struct PacketHourWriter {
    hour_dir: PathBuf,
    next_seq: u32,
    active: Option<PacketSegmentWriter>,
}

/// 宽表模式的单分段写入状态。
struct PacketSegmentWriter {
    segment_start_ts: u64,
    segment_file: String,
    writer: Option<ArrowWriter<File>>,
    pending: Vec<PacketWideRow>,
    rows: u64,
    points: u64,
    min_ts: u64,
    max_ts: u64,
}

/// 长表模式的小时写入状态。
struct LongHourWriter {
    hour_dir: PathBuf,
    next_seq: u32,
    active: Option<LongSegmentWriter>,
}

/// 长表模式的单分段写入状态。
struct LongSegmentWriter {
    segment_start_ts: u64,
    segment_file: String,
    writer: Option<ArrowWriter<File>>,
    pending: Vec<LongRow>,
    rows: u64,
    points: u64,
    min_ts: u64,
    max_ts: u64,
}

/// 程序入口，负责解析命令并分发执行。
fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Gen(args) => run_gen(args),
        Command::Import(args) => run_import(args),
        Command::Stats(args) => run_stats(args),
        Command::Verify(args) => run_verify(args),
        Command::Reindex(args) => run_reindex(args),
        Command::Export(args) => run_export(args),
    }
}

/// 执行 JSONL 导入流程并按目标模式生成 parquet + manifest。
fn run_import(args: ImportArgs) -> Result<()> {
    validate_args(&args)?;
    std::fs::create_dir_all(&args.root)
        .with_context(|| format!("create root dir failed: {}", args.root.display()))?;
    write_storage_meta(&args)?;

    let file = File::open(&args.input)
        .with_context(|| format!("open input jsonl failed: {}", args.input.display()))?;
    let reader = BufReader::new(file);
    let mut stats = ImportStats::default();
    let mut packet_writers: HashMap<HourKey, PacketHourWriter> = HashMap::new();
    let mut long_writers: HashMap<HourKey, LongHourWriter> = HashMap::new();

    for (idx, line) in reader.lines().enumerate() {
        stats.input_lines = stats.input_lines.saturating_add(1);
        let line = line.with_context(|| format!("read line {} failed", idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let packet: InputPacket = serde_json::from_str(&line)
            .with_context(|| format!("parse jsonl line {} failed", idx + 1))?;
        if let Some(filter) = &args.device_id
            && packet.id != *filter
        {
            continue;
        }
        let ts_sec = packet.t / 1000;
        let points = extract_points(packet.p);
        if points.is_empty() {
            stats.skipped_packets = stats.skipped_packets.saturating_add(1);
            continue;
        }
        let key = build_hour_key(&packet.id, ts_sec);
        stats.accepted_packets = stats.accepted_packets.saturating_add(1);
        stats.accepted_points = stats.accepted_points.saturating_add(points.len() as u64);
        match args.mode {
            StorageMode::PacketWide => {
                append_packet_wide(
                    &args,
                    &mut packet_writers,
                    key,
                    ts_sec,
                    points,
                )?;
            }
            StorageMode::LongRow => {
                append_long_row(&args, &mut long_writers, key, ts_sec, points)?;
            }
        }
    }

    seal_all_packet_writers(&args, &mut packet_writers)?;
    seal_all_long_writers(&args, &mut long_writers)?;

    println!("mode={:?}", args.mode);
    println!("root={}", args.root.display());
    println!("input_lines={}", stats.input_lines);
    println!("accepted_packets={}", stats.accepted_packets);
    println!("accepted_points={}", stats.accepted_points);
    println!("skipped_packets={}", stats.skipped_packets);
    Ok(())
}

/// 统计存储目录中的段/行/点规模，便于快速评估导入结果。
fn run_stats(args: StatsArgs) -> Result<()> {
    let meta = load_storage_meta(&args.root)?;
    let stats = collect_storage_stats(&args.root, args.device_id.as_deref())?;
    println!("root={}", args.root.display());
    if let Some(device_id) = &args.device_id {
        println!("device_filter={device_id}");
    }
    println!("default_mode={:?}", meta.default_mode);
    println!("compression={:?}", meta.compression);
    println!("devices={}", stats.devices);
    println!("manifests={}", stats.manifests);
    println!("segments={}", stats.segments);
    println!("rows={}", stats.rows);
    println!("points={}", stats.points);
    println!(
        "min_ts={}",
        stats.min_ts.map_or_else(|| "-".to_string(), |v| v.to_string())
    );
    println!(
        "max_ts={}",
        stats.max_ts.map_or_else(|| "-".to_string(), |v| v.to_string())
    );
    Ok(())
}

/// 对比输入 JSONL 与已导入存储的包数/点数，确认导入完整性。
fn run_verify(args: VerifyArgs) -> Result<()> {
    let meta = load_storage_meta(&args.root)?;
    let source = collect_source_stats(&args.input, args.device_id.as_deref())?;
    let storage = collect_storage_stats(&args.root, args.device_id.as_deref())?;

    let expected_rows = match meta.default_mode {
        StorageMode::PacketWide => source.packets,
        StorageMode::LongRow => source.points,
    };
    let expected_points = source.points;

    println!("verify_mode={:?}", meta.default_mode);
    println!("source_packets={}", source.packets);
    println!("source_points={}", source.points);
    println!("storage_rows={}", storage.rows);
    println!("storage_points={}", storage.points);
    println!("expected_rows={}", expected_rows);
    println!("expected_points={}", expected_points);

    let rows_ok = storage.rows == expected_rows;
    let points_ok = storage.points == expected_points;
    println!("rows_match={rows_ok}");
    println!("points_match={points_ok}");
    if !rows_ok || !points_ok {
        bail!("verify failed: rows_match={rows_ok}, points_match={points_ok}");
    }
    println!("verify_result=ok");
    Ok(())
}

/// 根据 manifest 重建 `redb` 小时索引，供查询工具快速剪枝。
fn run_reindex(args: ReindexArgs) -> Result<()> {
    let index_dir = args.root.join("_index");
    std::fs::create_dir_all(&index_dir)
        .with_context(|| format!("create index dir failed: {}", index_dir.display()))?;
    let db_path = index_dir.join(TSINDEX_FILE_NAME);

    if args.backup && db_path.exists() {
        let backup = index_dir.join(format!("{}.bak.{}", TSINDEX_FILE_NAME, now_ms()));
        std::fs::copy(&db_path, &backup).with_context(|| {
            format!(
                "backup index failed: {} -> {}",
                db_path.display(),
                backup.display()
            )
        })?;
        println!("backup={}", backup.display());
    }

    let db = if db_path.exists() {
        redb::Database::open(&db_path)
            .with_context(|| format!("open redb failed: {}", db_path.display()))?
    } else {
        redb::Database::create(&db_path)
            .with_context(|| format!("create redb failed: {}", db_path.display()))?
    };

    if let Some(device_id) = &args.device_id {
        clear_device_index_keys(&db, device_id)?;
    } else {
        clear_all_index_keys(&db)?;
    }

    let mut hour_count = 0_u64;
    let mut segment_count = 0_u64;
    let mut pidx_files = 0_u64;
    let mut pidx_entries = 0_u64;
    for hour in collect_hour_manifest_infos(&args.root, args.device_id.as_deref())? {
        let entries = read_manifest_entries(&hour.manifest)?;
        let mut index_items = Vec::new();
        for item in entries {
            index_items.push(IndexSegmentEntry {
                segment_file: item.segment_file,
                min_ts: item.min_ts,
                max_ts: item.max_ts,
                rows: item.rows,
            });
        }
        index_items.sort_by(|a, b| a.segment_file.cmp(&b.segment_file));
        index_items.dedup_by(|a, b| a.segment_file == b.segment_file);
        if index_items.is_empty() {
            continue;
        }
        let key = hourly_index_key(&hour.device_id, &hour.day_key, hour.hour);
        let payload = serde_json::to_string(&index_items)?;
        let write_txn = db.begin_write()?;
        {
            let mut table = write_txn.open_table(TSINDEX_HOURLY_SEGMENTS_TABLE)?;
            table.insert(key.as_str(), payload.as_str())?;
        }
        write_txn.commit()?;

        let hour_dir = hour
            .manifest
            .parent()
            .with_context(|| format!("invalid manifest path: {}", hour.manifest.display()))?;
        for item in &index_items {
            let seg_path = hour_dir.join(&item.segment_file);
            if !seg_path.exists() {
                continue;
            }
            match rebuild_segment_param_index(&seg_path) {
                Ok(cnt) if cnt > 0 => {
                    pidx_files = pidx_files.saturating_add(1);
                    pidx_entries = pidx_entries.saturating_add(cnt as u64);
                }
                Ok(_) => {}
                Err(e) => eprintln!("warn: rebuild pidx failed for {}: {}", seg_path.display(), e),
            }
        }
        hour_count = hour_count.saturating_add(1);
        segment_count = segment_count.saturating_add(index_items.len() as u64);
    }

    println!("reindex_root={}", args.root.display());
    if let Some(device_id) = args.device_id {
        println!("reindex_device={device_id}");
    }
    println!("reindex_db={}", db_path.display());
    println!("reindex_hours={hour_count}");
    println!("reindex_segments={segment_count}");
    println!("reindex_pidx_files={pidx_files}");
    println!("reindex_pidx_entries={pidx_entries}");
    Ok(())
}

/// 生成段级参数倒排索引 sidecar：记录参数在每个 row-group 的行区间与时间范围。
fn rebuild_segment_param_index(segment_path: &Path) -> Result<usize> {
    let file = File::open(segment_path)
        .with_context(|| format!("open segment failed: {}", segment_path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata().clone();
    let rg_count = metadata.num_row_groups();
    if rg_count == 0 {
        return Ok(0);
    }

    let mut entries = Vec::<SegmentParamIndexEntry>::new();
    for rg_id in 0..rg_count {
        let file = File::open(segment_path)
            .with_context(|| format!("open segment failed: {}", segment_path.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_row_groups(vec![rg_id])
            .build()?;

        let mut row_cursor = 0usize;
        let mut current_param: Option<String> = None;
        let mut current_start = 0usize;
        let mut current_end = 0usize;
        let mut current_min_ts = u64::MAX;
        let mut current_max_ts = 0u64;

        for batch in reader {
            let batch = batch?;
            let ts_arr = match batch
                .column_by_name("ts")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            {
                Some(v) => v,
                None => return Ok(0),
            };
            let param_arr = match batch
                .column_by_name("param_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            {
                Some(v) => v,
                None => return Ok(0),
            };
            if batch.num_rows() == 0 {
                continue;
            }
            for i in 0..batch.num_rows() {
                if param_arr.is_null(i) {
                    continue;
                }
                let param = param_arr.value(i);
                let ts = ts_arr.value(i);
                let global_row = row_cursor + i;

                match &current_param {
                    Some(cur) if cur == param => {
                        current_end = global_row + 1;
                        current_min_ts = current_min_ts.min(ts);
                        current_max_ts = current_max_ts.max(ts);
                    }
                    Some(_) => {
                        entries.push(SegmentParamIndexEntry {
                            rg_id: rg_id as u32,
                            param_id: current_param.take().unwrap_or_default(),
                            start_row: current_start as u32,
                            end_row: current_end as u32,
                            min_ts: current_min_ts,
                            max_ts: current_max_ts,
                        });
                        current_param = Some(param.to_string());
                        current_start = global_row;
                        current_end = global_row + 1;
                        current_min_ts = ts;
                        current_max_ts = ts;
                    }
                    None => {
                        current_param = Some(param.to_string());
                        current_start = global_row;
                        current_end = global_row + 1;
                        current_min_ts = ts;
                        current_max_ts = ts;
                    }
                }
            }
            row_cursor += batch.num_rows();
        }

        if let Some(param_id) = current_param.take() {
            entries.push(SegmentParamIndexEntry {
                rg_id: rg_id as u32,
                param_id,
                start_row: current_start as u32,
                end_row: current_end as u32,
                min_ts: current_min_ts,
                max_ts: current_max_ts,
            });
        }
    }

    if entries.is_empty() {
        return Ok(0);
    }
    entries.sort_by(|a, b| {
        a.rg_id
            .cmp(&b.rg_id)
            .then(a.param_id.cmp(&b.param_id))
            .then(a.start_row.cmp(&b.start_row))
    });
    let sidecar_path = segment_param_index_path(segment_path);
    let tmp_path = PathBuf::from(format!("{}.tmp", sidecar_path.display()));
    let payload = encode_segment_param_index_binary(&entries)?;
    std::fs::write(&tmp_path, payload)
        .with_context(|| format!("write pidx failed: {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, &sidecar_path).with_context(|| {
        format!(
            "replace pidx failed: {} -> {}",
            tmp_path.display(),
            sidecar_path.display()
        )
    })?;
    Ok(entries.len())
}

/// 将段级参数倒排索引编码为紧凑二进制格式，降低 `.pidx` 体积与解析开销。
fn encode_segment_param_index_binary(entries: &[SegmentParamIndexEntry]) -> Result<Vec<u8>> {
    let mut dict = Vec::<String>::new();
    let mut dict_map = HashMap::<String, u32>::new();
    let mut mapped = Vec::<(u32, &SegmentParamIndexEntry)>::with_capacity(entries.len());
    for entry in entries {
        let pidx = if let Some(&idx) = dict_map.get(&entry.param_id) {
            idx
        } else {
            let idx = u32::try_from(dict.len()).context("too many distinct param_id in pidx")?;
            let key = entry.param_id.clone();
            dict.push(key.clone());
            dict_map.insert(key, idx);
            idx
        };
        mapped.push((pidx, entry));
    }

    let mut out = Vec::<u8>::with_capacity(
        4 + 1 + 4 + dict.iter().map(|s| 1 + s.len()).sum::<usize>() + 4 + mapped.len() * 28,
    );
    out.extend_from_slice(&PIDX_MAGIC);
    out.push(PIDX_BINARY_VERSION);
    write_u32_le(
        &mut out,
        u32::try_from(dict.len()).context("param_id dictionary overflow")?,
    );
    for param in &dict {
        let bytes = param.as_bytes();
        let len = u8::try_from(bytes.len()).context("param_id length overflow")?;
        out.push(len);
        out.extend_from_slice(bytes);
    }
    write_u32_le(
        &mut out,
        u32::try_from(mapped.len()).context("pidx entry count overflow")?,
    );
    for (param_idx, entry) in mapped {
        write_u32_le(&mut out, entry.rg_id);
        write_u32_le(&mut out, param_idx);
        write_u32_le(&mut out, entry.start_row);
        write_u32_le(&mut out, entry.end_row);
        write_u64_le(&mut out, entry.min_ts);
        write_u64_le(&mut out, entry.max_ts);
    }
    Ok(out)
}

/// 向缓冲区写入小端序 `u32`。
fn write_u32_le(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// 向缓冲区写入小端序 `u64`。
fn write_u64_le(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// 返回段文件对应的 sidecar 参数索引路径：`seg_xxx.parquet.pidx`。
fn segment_param_index_path(segment_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.pidx", segment_path.display()))
}

/// 校验导入参数，确保关键阈值合理。
fn validate_args(args: &ImportArgs) -> Result<()> {
    if args.segment_sec == 0 {
        bail!("--segment-sec must be > 0");
    }
    if args.segment_max_rows == 0 {
        bail!("--segment-max-rows must be > 0");
    }
    if args.row_group_rows == 0 {
        bail!("--row-group-rows must be > 0");
    }
    if args.batch_rows == 0 {
        bail!("--batch-rows must be > 0");
    }
    Ok(())
}

/// 返回默认存储模式（用于兼容旧清单缺失 mode 字段）。
fn default_storage_mode() -> StorageMode {
    StorageMode::PacketWide
}

/// 旧版本 storage.toml 缺失 `row_group_rows` 时使用的默认值。
fn default_row_group_rows() -> usize {
    50_000
}

/// 加载根目录 `_meta/storage.toml`，用于识别默认模式与参数。
fn load_storage_meta(root: &Path) -> Result<StorageMeta> {
    let path = root.join("_meta").join("storage.toml");
    let content =
        std::fs::read_to_string(&path).with_context(|| format!("read {} failed", path.display()))?;
    let meta: StorageMeta =
        toml::from_str(&content).with_context(|| format!("parse {} failed", path.display()))?;
    Ok(meta)
}

/// 扫描存储目录并聚合 manifest 指标。
fn collect_storage_stats(root: &Path, device_filter: Option<&str>) -> Result<StorageStats> {
    let mut stats = StorageStats::default();
    let mut device_set = HashSet::new();
    for manifest in collect_manifest_files(root, device_filter)? {
        stats.manifests = stats.manifests.saturating_add(1);
        let lines = read_manifest_entries(&manifest)?;
        for item in lines {
            stats.segments = stats.segments.saturating_add(1);
            stats.rows = stats.rows.saturating_add(item.rows);
            let points = if item.points > 0 {
                item.points
            } else if matches!(item.mode, StorageMode::LongRow) {
                item.rows
            } else {
                let Some(parent) = manifest.parent() else {
                    bail!("manifest parent missing: {}", manifest.display());
                };
                let seg_path = parent.join(&item.segment_file);
                count_points_from_segment(&seg_path, item.mode)?
            };
            stats.points = stats.points.saturating_add(points);
            if item.rows > 0 {
                stats.min_ts = Some(stats.min_ts.map_or(item.min_ts, |v| v.min(item.min_ts)));
                stats.max_ts = Some(stats.max_ts.map_or(item.max_ts, |v| v.max(item.max_ts)));
            }
        }
        if let Some(device_id) = extract_device_id_from_manifest(root, &manifest) {
            device_set.insert(device_id);
        }
    }
    stats.devices = device_set.len();
    Ok(stats)
}

/// 统计源 JSONL 的有效包数与有效点数。
fn collect_source_stats(input: &Path, device_filter: Option<&str>) -> Result<SourceStats> {
    let file = File::open(input).with_context(|| format!("open {} failed", input.display()))?;
    let reader = BufReader::new(file);
    let mut out = SourceStats::default();
    for (idx, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read line {} failed", idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let packet: InputPacket =
            serde_json::from_str(&line).with_context(|| format!("parse line {} failed", idx + 1))?;
        if let Some(filter) = device_filter
            && packet.id != filter
        {
            continue;
        }
        let points = extract_points(packet.p);
        if points.is_empty() {
            continue;
        }
        out.packets = out.packets.saturating_add(1);
        out.points = out.points.saturating_add(points.len() as u64);
    }
    Ok(out)
}

/// 收集根目录下全部 manifest 文件路径。
fn collect_manifest_files(root: &Path, device_filter: Option<&str>) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let rd = std::fs::read_dir(root).with_context(|| format!("read root failed: {}", root.display()))?;
    for entry in rd {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let device = entry.file_name().to_string_lossy().to_string();
        if device.starts_with('_') {
            continue;
        }
        if let Some(filter) = device_filter
            && device != filter
        {
            continue;
        }
        let day_rd = std::fs::read_dir(entry.path())?;
        for day in day_rd {
            let day = day?;
            if !day.file_type()?.is_dir() {
                continue;
            }
            let hour_rd = std::fs::read_dir(day.path())?;
            for hour in hour_rd {
                let hour = hour?;
                if !hour.file_type()?.is_dir() {
                    continue;
                }
                let manifest = hour.path().join("manifest.jsonl");
                if manifest.exists() {
                    out.push(manifest);
                }
            }
        }
    }
    out.sort();
    Ok(out)
}

/// 收集小时维度 manifest 信息，用于 `reindex` 按小时写入索引。
fn collect_hour_manifest_infos(root: &Path, device_filter: Option<&str>) -> Result<Vec<HourManifestInfo>> {
    let mut out = Vec::new();
    let rd = std::fs::read_dir(root).with_context(|| format!("read root failed: {}", root.display()))?;
    for device_entry in rd {
        let device_entry = device_entry?;
        if !device_entry.file_type()?.is_dir() {
            continue;
        }
        let device_id = device_entry.file_name().to_string_lossy().to_string();
        if device_id.starts_with('_') {
            continue;
        }
        if let Some(filter) = device_filter
            && device_id != filter
        {
            continue;
        }
        let day_rd = std::fs::read_dir(device_entry.path())?;
        for day_entry in day_rd {
            let day_entry = day_entry?;
            if !day_entry.file_type()?.is_dir() {
                continue;
            }
            let day_key = day_entry.file_name().to_string_lossy().to_string();
            let hour_rd = std::fs::read_dir(day_entry.path())?;
            for hour_entry in hour_rd {
                let hour_entry = hour_entry?;
                if !hour_entry.file_type()?.is_dir() {
                    continue;
                }
                let hour_raw = hour_entry.file_name().to_string_lossy().to_string();
                let Ok(hour) = hour_raw.parse::<u32>() else {
                    continue;
                };
                if hour > 23 {
                    continue;
                }
                let manifest = hour_entry.path().join("manifest.jsonl");
                if !manifest.exists() {
                    continue;
                }
                out.push(HourManifestInfo {
                    device_id: device_id.clone(),
                    day_key: day_key.clone(),
                    hour,
                    manifest,
                });
            }
        }
    }
    out.sort_by(|a, b| {
        (&a.device_id, &a.day_key, a.hour, &a.manifest).cmp(&(
            &b.device_id,
            &b.day_key,
            b.hour,
            &b.manifest,
        ))
    });
    Ok(out)
}

/// 读取并解析 manifest 文件中的全部条目。
fn read_manifest_entries(path: &Path) -> Result<Vec<SegmentManifestEntry>> {
    let file = File::open(path).with_context(|| format!("open {} failed", path.display()))?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read {} line {} failed", path.display(), idx + 1))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let item: SegmentManifestEntry = serde_json::from_str(trimmed)
            .with_context(|| format!("parse {} line {} failed", path.display(), idx + 1))?;
        out.push(item);
    }
    Ok(out)
}

/// 从 manifest 文件路径反推设备 ID（用于统计设备数量）。
fn extract_device_id_from_manifest(root: &Path, manifest: &Path) -> Option<String> {
    let rel = manifest.strip_prefix(root).ok()?;
    let mut parts = rel.components();
    let first = parts.next()?;
    Some(first.as_os_str().to_string_lossy().to_string())
}

/// 生成小时索引主键：`device|day|hour`。
fn hourly_index_key(device_id: &str, day_key: &str, hour: u32) -> String {
    format!("{device_id}|{day_key}|{hour:02}")
}

/// 清空索引库中所有小时键（全量重建场景）。
fn clear_all_index_keys(db: &redb::Database) -> Result<()> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TSINDEX_HOURLY_SEGMENTS_TABLE)?;
        let mut keys = Vec::new();
        for item in table.iter()? {
            let (key, _) = item?;
            keys.push(key.value().to_string());
        }
        for key in keys {
            let _ = table.remove(key.as_str())?;
        }
    }
    write_txn.commit()?;
    Ok(())
}

/// 清空索引库中指定设备前缀键（局部重建场景）。
fn clear_device_index_keys(db: &redb::Database, device_id: &str) -> Result<()> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TSINDEX_HOURLY_SEGMENTS_TABLE)?;
        let mut keys = Vec::new();
        let prefix = format!("{device_id}|");
        for item in table.iter()? {
            let (key, _) = item?;
            let raw = key.value();
            if raw.starts_with(prefix.as_str()) {
                keys.push(raw.to_string());
            }
        }
        for key in keys {
            let _ = table.remove(key.as_str())?;
        }
    }
    write_txn.commit()?;
    Ok(())
}

/// 写入根目录模式声明文件，供后续查询工具自动识别。
fn write_storage_meta(args: &ImportArgs) -> Result<()> {
    let meta_dir = args.root.join("_meta");
    std::fs::create_dir_all(&meta_dir)
        .with_context(|| format!("create meta dir failed: {}", meta_dir.display()))?;
    let meta = StorageMeta {
        version: 1,
        default_mode: args.mode,
        compression: args.compression,
        segment_sec: args.segment_sec,
        segment_max_rows: args.segment_max_rows,
        row_group_rows: args.row_group_rows,
        created_at: chrono::Local::now().to_rfc3339(),
    };
    let content = toml::to_string_pretty(&meta).context("serialize storage meta failed")?;
    let path = meta_dir.join("storage.toml");
    std::fs::write(&path, content).with_context(|| format!("write {} failed", path.display()))?;
    Ok(())
}

/// 从包级点位对象中提取合法参数点并转为 `(param_id, value)` 列表。
fn extract_points(raw: HashMap<String, serde_json::Value>) -> Vec<(String, f32)> {
    let mut out = Vec::with_capacity(raw.len());
    let mut seen = HashSet::with_capacity(raw.len());
    for (param_id, value) in raw {
        let id = param_id.trim().to_ascii_uppercase();
        if !is_valid_param_code(&id) || !seen.insert(id.clone()) {
            continue;
        }
        let Some(v) = value.as_f64() else {
            continue;
        };
        if !v.is_finite() {
            continue;
        }
        out.push((id, v as f32));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// 根据秒级时间戳构建 `device/day/hour` 维度 key（本地时间）。
fn build_hour_key(device_id: &str, ts_sec: u64) -> HourKey {
    let dt = Local
        .timestamp_opt(ts_sec as i64, 0)
        .single()
        .unwrap_or_else(Local::now);
    HourKey {
        device_id: device_id.to_string(),
        day_key: format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day()),
        hour: dt.hour(),
    }
}

/// 追加一条宽表记录到目标小时 writer，并按阈值滚动分段。
fn append_packet_wide(
    args: &ImportArgs,
    writers: &mut HashMap<HourKey, PacketHourWriter>,
    key: HourKey,
    ts_sec: u64,
    points: Vec<(String, f32)>,
) -> Result<()> {
    let row = PacketWideRow {
        ts: ts_sec,
        param_ids: points.iter().map(|(id, _)| id.clone()).collect(),
        values: points.iter().map(|(_, v)| *v).collect(),
    };
    let writer = ensure_packet_hour_writer(args, writers, key, ts_sec)?;
    append_packet_row(args, writer, row)
}

/// 追加一条长表记录集合到目标小时 writer，并按阈值滚动分段。
fn append_long_row(
    args: &ImportArgs,
    writers: &mut HashMap<HourKey, LongHourWriter>,
    key: HourKey,
    ts_sec: u64,
    points: Vec<(String, f32)>,
) -> Result<()> {
    let writer = ensure_long_hour_writer(args, writers, key, ts_sec)?;
    for (param_id, value) in points {
        let row = LongRow {
            ts: ts_sec,
            param_id,
            value,
        };
        append_long_single_row(args, writer, row)?;
    }
    Ok(())
}

/// 获取或初始化宽表小时 writer，并保证 active 段可写。
fn ensure_packet_hour_writer<'a>(
    args: &ImportArgs,
    writers: &'a mut HashMap<HourKey, PacketHourWriter>,
    key: HourKey,
    ts_sec: u64,
) -> Result<&'a mut PacketHourWriter> {
    if !writers.contains_key(&key) {
        let hour_dir = args
            .root
            .join(&key.device_id)
            .join(&key.day_key)
            .join(format!("{:02}", key.hour));
        std::fs::create_dir_all(&hour_dir)
            .with_context(|| format!("create hour dir failed: {}", hour_dir.display()))?;
        writers.insert(
            key.clone(),
            PacketHourWriter {
                hour_dir,
                next_seq: 1,
                active: None,
            },
        );
    }
    let writer = writers.get_mut(&key).context("packet hour writer missing")?;
    ensure_packet_segment_open(args, writer, ts_sec)?;
    Ok(writer)
}

/// 获取或初始化长表小时 writer，并保证 active 段可写。
fn ensure_long_hour_writer<'a>(
    args: &ImportArgs,
    writers: &'a mut HashMap<HourKey, LongHourWriter>,
    key: HourKey,
    ts_sec: u64,
) -> Result<&'a mut LongHourWriter> {
    if !writers.contains_key(&key) {
        let hour_dir = args
            .root
            .join(&key.device_id)
            .join(&key.day_key)
            .join(format!("{:02}", key.hour));
        std::fs::create_dir_all(&hour_dir)
            .with_context(|| format!("create hour dir failed: {}", hour_dir.display()))?;
        writers.insert(
            key.clone(),
            LongHourWriter {
                hour_dir,
                next_seq: 1,
                active: None,
            },
        );
    }
    let writer = writers.get_mut(&key).context("long hour writer missing")?;
    ensure_long_segment_open(args, writer, ts_sec)?;
    Ok(writer)
}

/// 确保宽表 active 段存在并在需要时滚动新段。
fn ensure_packet_segment_open(args: &ImportArgs, writer: &mut PacketHourWriter, ts_sec: u64) -> Result<()> {
    let need_rotate = match &writer.active {
        None => true,
        Some(seg) => {
            (ts_sec.saturating_sub(seg.segment_start_ts) >= args.segment_sec)
                || (seg.rows as usize + seg.pending.len() >= args.segment_max_rows)
        }
    };
    if need_rotate {
        if let Some(mut old) = writer.active.take() {
            seal_packet_segment(args, &writer.hour_dir, &mut old)?;
        }
        writer.active = Some(open_packet_segment(
            &writer.hour_dir,
            writer.next_seq,
            ts_sec,
            args.compression,
            args.row_group_rows,
        )?);
        writer.next_seq = writer.next_seq.saturating_add(1);
    }
    Ok(())
}

/// 确保长表 active 段存在并在需要时滚动新段。
fn ensure_long_segment_open(args: &ImportArgs, writer: &mut LongHourWriter, ts_sec: u64) -> Result<()> {
    let need_rotate = match &writer.active {
        None => true,
        Some(seg) => {
            (ts_sec.saturating_sub(seg.segment_start_ts) >= args.segment_sec)
                || (seg.rows as usize + seg.pending.len() >= args.segment_max_rows)
        }
    };
    if need_rotate {
        if let Some(mut old) = writer.active.take() {
            seal_long_segment(args, &writer.hour_dir, &mut old)?;
        }
        writer.active = Some(open_long_segment(
            &writer.hour_dir,
            writer.next_seq,
            ts_sec,
            args.compression,
            args.segment_max_rows,
        )?);
        writer.next_seq = writer.next_seq.saturating_add(1);
    }
    Ok(())
}

/// 追加宽表行并在达到批量阈值时写入 parquet。
fn append_packet_row(args: &ImportArgs, writer: &mut PacketHourWriter, row: PacketWideRow) -> Result<()> {
    let active = writer.active.as_mut().context("packet segment not open")?;
    active.pending.push(row);
    if active.pending.len() >= args.batch_rows {
        flush_packet_pending(active)?;
    }
    Ok(())
}

/// 追加长表单行并在达到批量阈值时写入 parquet。
fn append_long_single_row(args: &ImportArgs, writer: &mut LongHourWriter, row: LongRow) -> Result<()> {
    let _ = args;
    let active = writer.active.as_mut().context("long segment not open")?;
    active.pending.push(row);
    Ok(())
}

/// 打开宽表分段并创建对应 Parquet writer。
fn open_packet_segment(
    hour_dir: &Path,
    seq: u32,
    ts_sec: u64,
    compression: CompressionArg,
    row_group_rows: usize,
) -> Result<PacketSegmentWriter> {
    let segment_file = format!("seg_{:010}_{:04}.parquet", ts_sec, seq);
    let file_path = hour_dir.join(&segment_file);
    let file = File::create(&file_path)
        .with_context(|| format!("create parquet failed: {}", file_path.display()))?;
    let writer = ArrowWriter::try_new(
        file,
        packet_wide_schema(),
        Some(parquet_properties(compression, row_group_rows)),
    )?;
    Ok(PacketSegmentWriter {
        segment_start_ts: ts_sec,
        segment_file,
        writer: Some(writer),
        pending: Vec::new(),
        rows: 0,
        points: 0,
        min_ts: u64::MAX,
        max_ts: 0,
    })
}

/// 打开长表分段并创建对应 Parquet writer。
fn open_long_segment(
    hour_dir: &Path,
    seq: u32,
    ts_sec: u64,
    compression: CompressionArg,
    row_group_max_rows: usize,
) -> Result<LongSegmentWriter> {
    let segment_file = format!("seg_{:010}_{:04}.parquet", ts_sec, seq);
    let file_path = hour_dir.join(&segment_file);
    let file = File::create(&file_path)
        .with_context(|| format!("create parquet failed: {}", file_path.display()))?;
    let writer = ArrowWriter::try_new(
        file,
        long_row_schema(),
        Some(parquet_properties(compression, row_group_max_rows)),
    )?;
    Ok(LongSegmentWriter {
        segment_start_ts: ts_sec,
        segment_file,
        writer: Some(writer),
        pending: Vec::new(),
        rows: 0,
        points: 0,
        min_ts: u64::MAX,
        max_ts: 0,
    })
}

/// 刷新宽表 pending 缓冲到 parquet。
fn flush_packet_pending(seg: &mut PacketSegmentWriter) -> Result<()> {
    if seg.pending.is_empty() {
        return Ok(());
    }
    let mut ts_builder = UInt64Builder::with_capacity(seg.pending.len());
    let mut ids_builder = ListBuilder::new(StringBuilder::new());
    let mut values_builder = ListBuilder::new(Float32Builder::new());

    for row in &seg.pending {
        ts_builder.append_value(row.ts);
        for id in &row.param_ids {
            ids_builder.values().append_value(id);
        }
        ids_builder.append(true);
        for value in &row.values {
            values_builder.values().append_value(*value);
        }
        values_builder.append(true);
        seg.rows = seg.rows.saturating_add(1);
        seg.points = seg.points.saturating_add(row.values.len() as u64);
        seg.min_ts = seg.min_ts.min(row.ts);
        seg.max_ts = seg.max_ts.max(row.ts);
    }

    let batch = RecordBatch::try_new(
        packet_wide_schema(),
        vec![
            Arc::new(ts_builder.finish()),
            Arc::new(ids_builder.finish()),
            Arc::new(values_builder.finish()),
        ],
    )?;
    let writer = seg.writer.as_mut().context("packet writer missing")?;
    writer.write(&batch)?;
    seg.pending.clear();
    Ok(())
}

/// 刷新长表 pending 缓冲到 parquet：达到目标行数后，遇到参数变化再切组。
fn flush_long_pending(args: &ImportArgs, seg: &mut LongSegmentWriter) -> Result<()> {
    if seg.pending.is_empty() {
        return Ok(());
    }
    let mut rows = std::mem::take(&mut seg.pending);
    rows.sort_by(|a, b| a.param_id.cmp(&b.param_id).then(a.ts.cmp(&b.ts)));

    let target_rows = args.row_group_rows.max(1);
    let mut start = 0usize;
    let mut idx = 1usize;
    while idx < rows.len() {
        let reached_target = idx - start >= target_rows;
        let param_changed = rows[idx - 1].param_id != rows[idx].param_id;
        if reached_target && param_changed {
            write_long_chunk(seg, &rows[start..idx])?;
            start = idx;
        }
        idx += 1;
    }
    write_long_chunk(seg, &rows[start..])?;
    Ok(())
}

/// 将一段 long-row 切片写入 parquet，并更新分段统计信息。
fn write_long_chunk(seg: &mut LongSegmentWriter, rows: &[LongRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let id_bytes = rows.iter().map(|r| r.param_id.len()).sum();
    let mut ts_builder = UInt64Builder::with_capacity(rows.len());
    let mut id_builder = StringBuilder::with_capacity(rows.len(), id_bytes);
    let mut value_builder = Float32Builder::with_capacity(rows.len());

    for row in rows {
        ts_builder.append_value(row.ts);
        id_builder.append_value(&row.param_id);
        value_builder.append_value(row.value);
        seg.rows = seg.rows.saturating_add(1);
        seg.points = seg.points.saturating_add(1);
        seg.min_ts = seg.min_ts.min(row.ts);
        seg.max_ts = seg.max_ts.max(row.ts);
    }

    let batch = RecordBatch::try_new(
        long_row_schema(),
        vec![
            Arc::new(ts_builder.finish()),
            Arc::new(id_builder.finish()),
            Arc::new(value_builder.finish()),
        ],
    )?;
    let writer = seg.writer.as_mut().context("long writer missing")?;
    writer.write(&batch)?;
    // 每个切片写完立即 flush，显式形成独立 row-group。
    writer.flush()?;
    Ok(())
}

/// 封存宽表分段并写入 manifest 条目。
fn seal_packet_segment(args: &ImportArgs, hour_dir: &Path, seg: &mut PacketSegmentWriter) -> Result<()> {
    flush_packet_pending(seg)?;
    let mut writer = seg.writer.take().context("packet writer missing in seal")?;
    writer.flush()?;
    let _ = writer.close()?;
    let entry = SegmentManifestEntry {
        segment_file: seg.segment_file.clone(),
        min_ts: if seg.rows == 0 { 0 } else { seg.min_ts },
        max_ts: seg.max_ts,
        rows: seg.rows,
        points: seg.points,
        created_at_ms: now_ms(),
        mode: args.mode,
    };
    append_manifest(hour_dir, &entry)
}

/// 封存长表分段并写入 manifest 条目。
fn seal_long_segment(args: &ImportArgs, hour_dir: &Path, seg: &mut LongSegmentWriter) -> Result<()> {
    flush_long_pending(args, seg)?;
    let mut writer = seg.writer.take().context("long writer missing in seal")?;
    writer.flush()?;
    let _ = writer.close()?;
    let entry = SegmentManifestEntry {
        segment_file: seg.segment_file.clone(),
        min_ts: if seg.rows == 0 { 0 } else { seg.min_ts },
        max_ts: seg.max_ts,
        rows: seg.rows,
        points: seg.points,
        created_at_ms: now_ms(),
        mode: args.mode,
    };
    append_manifest(hour_dir, &entry)
}

/// 关闭并封存全部宽表小时 writer。
fn seal_all_packet_writers(args: &ImportArgs, writers: &mut HashMap<HourKey, PacketHourWriter>) -> Result<()> {
    let keys: Vec<HourKey> = writers.keys().cloned().collect();
    for key in keys {
        if let Some(mut writer) = writers.remove(&key)
            && let Some(mut seg) = writer.active.take()
        {
            seal_packet_segment(args, &writer.hour_dir, &mut seg)?;
        }
    }
    Ok(())
}

/// 关闭并封存全部长表小时 writer。
fn seal_all_long_writers(args: &ImportArgs, writers: &mut HashMap<HourKey, LongHourWriter>) -> Result<()> {
    let keys: Vec<HourKey> = writers.keys().cloned().collect();
    for key in keys {
        if let Some(mut writer) = writers.remove(&key)
            && let Some(mut seg) = writer.active.take()
        {
            seal_long_segment(args, &writer.hour_dir, &mut seg)?;
        }
    }
    Ok(())
}

/// 追加一条 manifest JSONL 记录。
fn append_manifest(hour_dir: &Path, entry: &SegmentManifestEntry) -> Result<()> {
    let path = hour_dir.join("manifest.jsonl");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open manifest failed: {}", path.display()))?;
    let line = serde_json::to_string(entry).context("serialize manifest entry failed")?;
    file.write_all(line.as_bytes())?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

/// 从 parquet 文件回算点数，兼容历史 manifest 未写入 `points` 的场景。
fn count_points_from_segment(path: &Path, mode: StorageMode) -> Result<u64> {
    let file = File::open(path).with_context(|| format!("open segment failed: {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("open parquet reader failed: {}", path.display()))?;
    let mut reader = builder.with_batch_size(2048).build()?;
    let mut points = 0_u64;
    while let Some(batch) = reader.next() {
        let batch = batch?;
        match mode {
            StorageMode::LongRow => {
                points = points.saturating_add(batch.num_rows() as u64);
            }
            StorageMode::PacketWide => {
                let values = batch
                    .column_by_name("values")
                    .context("missing values column")?
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .context("values column type mismatch")?;
                for i in 0..values.len() {
                    if !values.is_valid(i) {
                        continue;
                    }
                    let arr = values.value(i);
                    points = points.saturating_add(arr.len() as u64);
                }
            }
        }
    }
    Ok(points)
}

/// 生成宽表模式的 Parquet schema。
fn packet_wide_schema() -> Arc<Schema> {
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

/// 生成长表模式的 Parquet schema。
fn long_row_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::UInt64, false),
        Field::new("param_id", DataType::Utf8, false),
        Field::new("value", DataType::Float32, false),
    ]))
}

/// 根据压缩参数构建 Parquet writer 属性。
fn parquet_properties(compression: CompressionArg, row_group_rows: usize) -> WriterProperties {
    let codec = match compression {
        CompressionArg::Zstd => Compression::ZSTD(Default::default()),
        CompressionArg::Snappy => Compression::SNAPPY,
        CompressionArg::Uncompressed => Compression::UNCOMPRESSED,
    };
    WriterProperties::builder()
        .set_compression(codec)
        .set_max_row_group_size(row_group_rows)
        .build()
}

/// 返回当前 UTC 毫秒时间戳。
fn now_ms() -> u64 {
    u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default()
}

/// 存放时间范围解析后的结果。
#[derive(Debug, Clone, Copy)]
struct TimeRange {
    start_ms: i64,
    end_ms: i64,
}

/// 执行数据生成命令。
fn run_gen(args: GenArgs) -> Result<()> {
    validate_gen_args(&args)?;
    let range = resolve_time_range(&args)?;
    let total_packets = packet_count(range, args.interval_ms);

    let parent = args
        .out
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    std::fs::create_dir_all(&parent)
        .with_context(|| format!("create output directory failed: {}", parent.display()))?;

    let file = File::create(&args.out)
        .with_context(|| format!("create output file failed: {}", args.out.display()))?;
    let mut writer = BufWriter::new(file);
    let mut rng = StdRng::seed_from_u64(args.seed);
    let mut points_pool: Vec<u32> = (args.point_min..=args.point_max).collect();

    for seq in 1..=total_packets {
        let ts = range.start_ms + (seq - 1) * args.interval_ms;
        let selected = select_unique_points(&mut points_pool, args.points_per_packet, &mut rng);
        write_packet_line(&mut writer, &args.id, ts, seq as u64, selected, &mut rng)?;
    }

    writer.flush().context("flush output file failed")?;
    println!(
        "done: packets={}, interval_ms={}, out={}",
        total_packets,
        args.interval_ms,
        args.out.display()
    );
    Ok(())
}

/// 校验生成命令参数是否合法。
fn validate_gen_args(cli: &GenArgs) -> Result<()> {
    if cli.interval_ms <= 0 {
        bail!("--interval-ms must be > 0");
    }
    if cli.point_min == 0 {
        bail!("--point-min must be >= 1");
    }
    if cli.point_max < cli.point_min {
        bail!("--point-max must be >= --point-min");
    }
    let space = (cli.point_max - cli.point_min + 1) as usize;
    if cli.points_per_packet == 0 {
        bail!("--points-per-packet must be > 0");
    }
    if cli.points_per_packet > space {
        bail!(
            "--points-per-packet ({}) cannot exceed point space ({})",
            cli.points_per_packet,
            space
        );
    }
    parse_start_hour_ms(&cli.start)?;
    let dur = parse_range_duration_ms(&cli.range)?;
    if dur <= 0 {
        bail!("--range must be > 0");
    }
    Ok(())
}

/// 根据命令行参数解析时间范围。
fn resolve_time_range(cli: &GenArgs) -> Result<TimeRange> {
    let start_ms = parse_start_hour_ms(&cli.start)?;
    let end_ms = start_ms + parse_range_duration_ms(&cli.range)?;
    if end_ms <= start_ms {
        bail!("end time must be greater than start time");
    }
    Ok(TimeRange { start_ms, end_ms })
}

/// 解析 `YYYYMMDDHH`（本地时间）到 Unix 毫秒时间戳。
fn parse_start_hour_ms(input: &str) -> Result<i64> {
    if input.len() != 10 || !input.bytes().all(|b| b.is_ascii_digit()) {
        bail!("invalid --start format: {input}, expect YYYYMMDDHH");
    }
    let year: i32 = input[0..4]
        .parse()
        .with_context(|| format!("invalid year in --start: {input}"))?;
    let month: u32 = input[4..6]
        .parse()
        .with_context(|| format!("invalid month in --start: {input}"))?;
    let day: u32 = input[6..8]
        .parse()
        .with_context(|| format!("invalid day in --start: {input}"))?;
    let hour: u32 = input[8..10]
        .parse()
        .with_context(|| format!("invalid hour in --start: {input}"))?;
    if hour > 23 {
        bail!("invalid hour in --start: {input}, hour must be 00..23");
    }
    let date = NaiveDate::from_ymd_opt(year, month, day)
        .with_context(|| format!("invalid date in --start: {input}"))?;
    let naive = date
        .and_hms_opt(hour, 0, 0)
        .with_context(|| format!("invalid datetime in --start: {input}"))?;
    let dt = Local
        .from_local_datetime(&naive)
        .single()
        .with_context(|| format!("ambiguous or invalid local datetime in --start: {input}"))?;
    Ok(dt.timestamp_millis())
}

/// 解析 `--range`，支持 `s/m/h/d` 单位并返回毫秒值。
fn parse_range_duration_ms(input: &str) -> Result<i64> {
    if input.len() < 2 {
        bail!("invalid --range: {input}, expect like 1h");
    }
    let unit = input
        .chars()
        .last()
        .with_context(|| format!("invalid --range: {input}"))?;
    let num_part = &input[..input.len() - 1];
    let value: i64 = num_part
        .parse()
        .with_context(|| format!("invalid --range value: {input}"))?;
    if value <= 0 {
        bail!("--range value must be > 0");
    }
    let ms = match unit {
        's' | 'S' => Duration::seconds(value).num_milliseconds(),
        'm' | 'M' => Duration::minutes(value).num_milliseconds(),
        'h' | 'H' => Duration::hours(value).num_milliseconds(),
        'd' | 'D' => Duration::days(value).num_milliseconds(),
        _ => bail!("invalid --range unit: {unit}, support s/m/h/d"),
    };
    Ok(ms)
}

/// 计算总包数，包含起始包且不超过结束时间。
fn packet_count(range: TimeRange, interval_ms: i64) -> i64 {
    ((range.end_ms - range.start_ms) / interval_ms).max(0)
}

/// 在点位池中执行部分洗牌并返回前 `count` 个点，实现无放回随机抽样。
fn select_unique_points<'a>(pool: &'a mut [u32], count: usize, rng: &mut StdRng) -> &'a [u32] {
    let n = pool.len();
    for i in 0..count {
        let j = rng.gen_range(i..n);
        pool.swap(i, j);
    }
    &pool[..count]
}

/// 写入一行 JSONL 包数据，结构为 `{"id","t","s","p"}`。
fn write_packet_line<W: Write>(
    writer: &mut W,
    device_id: &str,
    ts_ms: i64,
    seq: u64,
    point_ids: &[u32],
    rng: &mut StdRng,
) -> Result<()> {
    let mut line = String::with_capacity(80_000);
    write!(&mut line, "{{\"id\":\"{}\",\"t\":{},\"s\":{},\"p\":{{", device_id, ts_ms, seq)
        .context("build json line head failed")?;

    for (idx, pid) in point_ids.iter().enumerate() {
        if idx > 0 {
            line.push(',');
        }
        line.push('"');
        write!(&mut line, "P{:05}", pid).context("build param id failed")?;
        line.push_str("\":");
        write_value(&mut line, rng)?;
    }
    line.push_str("}}\n");
    writer
        .write_all(line.as_bytes())
        .context("write jsonl line failed")?;
    Ok(())
}

/// 按 `dev` 风格生成随机值：50% 整数、50% 三位小数浮点。
fn write_value(line: &mut String, rng: &mut StdRng) -> Result<()> {
    if rng.gen_bool(0.5) {
        let v: i32 = rng.gen_range(-100_000..=100_000);
        write!(line, "{v}").context("write int value failed")?;
    } else {
        let v: f64 = rng.gen_range(-100_000.0..=100_000.0);
        write!(line, "{v:.3}").context("write float value failed")?;
    }
    Ok(())
}

/// 执行导出命令：将 Parquet 文件导出为 JSONL 格式。
fn run_export(args: ExportArgs) -> Result<()> {
    let file = File::open(&args.input)
        .with_context(|| format!("open input parquet failed: {}", args.input.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| "create parquet reader builder failed")?;
    let mut reader = builder.build().with_context(|| "build parquet reader failed")?;

    let parent = args.out.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .with_context(|| format!("create output directory failed: {}", parent.display()))?;

    let out_file = File::create(&args.out)
        .with_context(|| format!("create output file failed: {}", args.out.display()))?;
    let mut writer = BufWriter::new(out_file);

    let mut total_rows = 0u64;
    let mut total_points = 0u64;

    match args.mode {
        StorageMode::LongRow => {
            while let Some(result) = reader.next() {
                let batch = result.with_context(|| "read parquet batch failed")?;
                let (rows, points) = export_long_row_batch(
                    &batch,
                    &mut writer,
                    &args.device_id,
                )?;
                total_rows += rows;
                total_points += points;
            }
        }
        StorageMode::PacketWide => {
            while let Some(result) = reader.next() {
                let batch = result.with_context(|| "read parquet batch failed")?;
                let (rows, points) = export_packet_wide_batch(
                    &batch,
                    &mut writer,
                    &args.device_id,
                )?;
                total_rows += rows;
                total_points += points;
            }
        }
    }

    writer.flush().context("flush output file failed")?;
    println!(
        "done: rows={}, points={}, out={}",
        total_rows,
        total_points,
        args.out.display()
    );
    Ok(())
}

/// 导出长表模式的 RecordBatch 到 JSONL。
fn export_long_row_batch<W: Write>(
    batch: &RecordBatch,
    writer: &mut W,
    device_id: &str,
) -> Result<(u64, u64)> {
    use arrow::array::{Float32Array, StringArray, UInt64Array};

    let ts_array = batch.column(0).as_any().downcast_ref::<UInt64Array>()
        .context("ts column is not UInt64")?;
    let param_array = batch.column(1).as_any().downcast_ref::<StringArray>()
        .context("param_id column is not String")?;
    let value_array = batch.column(2).as_any().downcast_ref::<Float32Array>()
        .context("value column is not Float32")?;

    let mut rows = 0u64;
    let mut points = 0u64;

    for i in 0..batch.num_rows() {
        let ts = ts_array.value(i);
        let param_id = param_array.value(i);
        let value = value_array.value(i);

        let mut line = String::with_capacity(128);
        write!(&mut line, "{{\"id\":\"{}\",\"t\":{},\"s\":{},\"p\":{{\"{}\":", 
               device_id, ts, rows + 1, param_id)
            .context("build json line head failed")?;
        
        // 判断是整数还是浮点数
        if value.fract() == 0.0 {
            write!(&mut line, "{:.0}", value).context("write int value failed")?;
        } else {
            write!(&mut line, "{:.3}", value).context("write float value failed")?;
        }
        
        line.push_str("}}\n");
        writer.write_all(line.as_bytes()).context("write jsonl line failed")?;
        
        rows += 1;
        points += 1;
    }

    Ok((rows, points))
}

/// 导出宽表模式的 RecordBatch 到 JSONL。
fn export_packet_wide_batch<W: Write>(
    batch: &RecordBatch,
    writer: &mut W,
    device_id: &str,
) -> Result<(u64, u64)> {
    use arrow::array::{Float32Array, ListArray, UInt64Array};

    let ts_array = batch.column(0).as_any().downcast_ref::<UInt64Array>()
        .context("ts column is not UInt64")?;
    let param_ids_array = batch.column(1).as_any().downcast_ref::<ListArray>()
        .context("param_ids column is not ListArray")?;
    let values_array = batch.column(2).as_any().downcast_ref::<ListArray>()
        .context("values column is not ListArray")?;

    let mut rows = 0u64;
    let mut points = 0u64;

    for i in 0..batch.num_rows() {
        let ts = ts_array.value(i);
        let param_ids_scalar = param_ids_array.value(i);
        let values_scalar = values_array.value(i);
        
        let param_ids = param_ids_scalar.as_any().downcast_ref::<StringArray>()
            .context("param_ids inner is not StringArray")?;
        let values = values_scalar.as_any().downcast_ref::<Float32Array>()
            .context("values inner is not Float32Array")?;

        let mut line = String::with_capacity(80_000);
        write!(&mut line, "{{\"id\":\"{}\",\"t\":{},\"s\":{},\"p\":{{", 
               device_id, ts, rows + 1)
            .context("build json line head failed")?;

        for j in 0..param_ids.len() {
            if j > 0 {
                line.push(',');
            }
            let param_id = param_ids.value(j);
            let value = values.value(j);
            
            line.push('"');
            line.push_str(param_id);
            line.push_str("\":");
            
            if value.fract() == 0.0 {
                write!(&mut line, "{:.0}", value).context("write int value failed")?;
            } else {
                write!(&mut line, "{:.3}", value).context("write float value failed")?;
            }
        }

        line.push_str("}}\n");
        writer.write_all(line.as_bytes()).context("write jsonl line failed")?;
        
        rows += 1;
        points += param_ids.len() as u64;
    }

    Ok((rows, points))
}
