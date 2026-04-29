use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, bail};
use arrow::array::{Array, Float32Array, ListArray, StringArray, UInt64Array};
use chrono::{Datelike, Local, NaiveDate, TimeZone};
use clap::{Args, Parser, Subcommand, ValueEnum};
use common::tsmeta::is_valid_param_code;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection};
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics as ParquetStatistics;
use redb::TableDefinition;
use serde::{Deserialize, Serialize};

/// `tsd` 命令行入口参数。
#[derive(Debug, Clone, Parser)]
#[command(name = "tsd", version, about = "tsdata 参数存储查询工具")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// `tsd` 子命令集合。
#[derive(Debug, Clone, Subcommand)]
enum Command {
    Query(QueryArgs),
    Stat(CommonArgs),
    Export(ExportArgs),
    Doctor(CommonArgs),
    Perf(PerfArgs),
    Reindex(ReindexArgs),
}

/// 公共参数，统一定义根目录、设备和时间范围。
#[derive(Debug, Clone, Args)]
struct CommonArgs {
    #[arg(short = 'r', long, default_value = "tsdata")]
    root: String,
    #[arg(short = 'd', long = "device-id", visible_alias = "device")]
    device_id: String,
    #[arg(short = 'f', long = "from", requires = "to_ts")]
    from_ts: Option<u64>,
    #[arg(short = 't', long = "to", requires = "from_ts")]
    to_ts: Option<u64>,
    #[arg(
        short = 'D',
        long = "day",
        value_parser = parse_day_key_arg,
        conflicts_with_all = ["from_ts", "to_ts", "today", "last"]
    )]
    day: Option<String>,
    #[arg(
        short = 'T',
        long = "today",
        default_value_t = false,
        conflicts_with_all = ["from_ts", "to_ts", "day", "last"]
    )]
    today: bool,
    #[arg(
        short = 'l',
        long = "last",
        value_parser = parse_last_window_arg,
        conflicts_with_all = ["from_ts", "to_ts", "day", "today"]
    )]
    last: Option<String>,
    #[arg(
        short = 'a',
        long = "all",
        default_value_t = false,
        conflicts_with_all = ["from_ts", "to_ts", "day", "today", "last"]
    )]
    all: bool,
    #[arg(long = "engine", value_enum, default_value_t = QueryEngine::Index)]
    engine: QueryEngine,
}

/// 查询引擎模式。
#[derive(Debug, Clone, Copy, ValueEnum)]
enum QueryEngine {
    /// 仅按时间范围筛选 manifest。
    Manifest,
    /// 通过 redb 小时索引加载候选段，再按时间范围筛选。
    Index,
}
const TSINDEX_FILE_NAME: &str = "tsindex.redb";
const TSINDEX_HOURLY_SEGMENTS_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("hourly_segments");
/// `.pidx` 二进制格式魔数（Parameter InDeX）。
const PIDX_MAGIC: [u8; 4] = *b"PIDX";
/// `.pidx` 二进制格式版本号。
const PIDX_BINARY_VERSION: u8 = 2;

/// 查询参数。
#[derive(Debug, Clone, Args)]
struct QueryArgs {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(short = 'n', long)]
    limit: Option<usize>,
    #[arg(short = 'p', long = "param")]
    params: Vec<String>,
    #[arg(long, default_value_t = false)]
    flat: bool,
    #[arg(long, default_value_t = false)]
    profile: bool,
}

/// 导出格式。
#[derive(Debug, Clone, Copy, ValueEnum)]
enum ExportFormat {
    Csv,
    Json,
}

/// 导出参数。
#[derive(Debug, Clone, Args)]
struct ExportArgs {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(short = 'n', long, default_value_t = 1000)]
    limit: usize,
    #[arg(short = 'p', long = "param")]
    params: Vec<String>,
    #[arg(short = 'o', long)]
    out: PathBuf,
    #[arg(short = 'F', long, value_enum, default_value_t = ExportFormat::Json)]
    format: ExportFormat,
    #[arg(long, default_value_t = false)]
    flat: bool,
}

/// 查询性能测试参数。
#[derive(Debug, Clone, Args)]
struct PerfArgs {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(short = 'p', long = "param")]
    params: Vec<String>,
    #[arg(short = 'n', long, default_value_t = 200)]
    limit: usize,
    #[arg(long, default_value_t = 20)]
    iterations: usize,
    #[arg(long, default_value_t = 3)]
    warmup: usize,
    #[arg(long, default_value_t = false)]
    compare_engine: bool,
}

/// 索引重建参数。
#[derive(Debug, Clone, Args)]
struct ReindexArgs {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long, default_value_t = false)]
    backup: bool,
}

/// 查询输出记录。
#[derive(Debug, Clone, Serialize)]
struct QueryRow {
    ts: u64,
    param_ids: Vec<String>,
    values: Vec<f32>,
}

/// 按参数展开的一行一点输出记录。
#[derive(Debug, Clone, Serialize)]
struct FlatRow {
    ts: u64,
    param_id: String,
    value: f32,
}

/// 单轮性能测试结果统计。
#[derive(Debug, Clone, Copy)]
struct PerfStats {
    matched: usize,
    min_ms: f64,
    avg_ms: f64,
    p95_ms: f64,
    max_ms: f64,
}

/// 单次 `query` 路径的性能分项统计（用于定位耗时热点）。
#[derive(Debug, Default, Clone)]
struct QueryProfileStats {
    collect_rows_ms: f64,
    output_ms: f64,
    candidate_collect_ms: f64,
    redb_index_ms: f64,
    manifest_scan_ms: f64,
    read_rows_ms: f64,
    parquet_open_ms: f64,
    parquet_reader_build_ms: f64,
    pidx_load_ms: f64,
    pidx_plan_ms: f64,
    row_selection_ms: f64,
    pidx_decode_ms: f64,
    fallback_decode_ms: f64,
    candidate_hour_dirs: usize,
    candidate_files: usize,
    files_scanned: usize,
    parquet_open_count: usize,
    pidx_load_count: usize,
    pidx_hit_count: usize,
    row_groups_candidate: usize,
    row_groups_read_via_pidx: usize,
    matched_rows: usize,
}

/// 统计输出结构。
#[derive(Debug, Clone)]
struct Stats {
    files: usize,
    rows: usize,
    points: usize,
    min_ts: Option<u64>,
    max_ts: Option<u64>,
}

/// `doctor` 检查结果汇总。
#[derive(Debug, Default)]
struct DoctorStats {
    hour_dirs: usize,
    manifest_files: usize,
    manifest_entries: usize,
    missing_manifest: usize,
    missing_segment_files: usize,
    bad_manifest_lines: usize,
    invalid_entries: usize,
    missing_redb_db: usize,
    missing_redb_hour_key: usize,
    bad_redb_payload: usize,
    manifest_not_indexed: usize,
    orphan_redb_entries: usize,
    redb_mismatch_entries: usize,
    redb_missing_segment_files: usize,
}

/// `tsstore` 分段清单条目。
#[derive(Debug, Clone, Deserialize)]
struct SegmentManifestEntry {
    segment_file: String,
    min_ts: u64,
    max_ts: u64,
    rows: u64,
    created_at_ms: u64,
}

/// `redb` 小时索引中的分段条目结构。
#[derive(Debug, Clone, Deserialize, Serialize)]
struct IndexSegmentEntry {
    segment_file: String,
    min_ts: u64,
    max_ts: u64,
    rows: u64,
}

/// 段级参数倒排索引 sidecar 结构（由 ingest reindex 生成）。
#[derive(Debug, Clone, Deserialize)]
struct SegmentParamIndexFile {
    version: u32,
    entries: Vec<SegmentParamIndexEntry>,
}

/// 参数在 row-group 内的连续行区间索引条目（`end_row` 为开区间）。
#[derive(Debug, Clone, Deserialize)]
struct SegmentParamIndexEntry {
    rg_id: u32,
    param_id: String,
    start_row: u32,
    end_row: u32,
    min_ts: u64,
    max_ts: u64,
}

/// 程序入口，负责解析参数并分发子命令。
fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Query(args) => run_query(args),
        Command::Stat(args) => run_stat(args),
        Command::Export(args) => run_export(args),
        Command::Doctor(args) => run_doctor(args),
        Command::Perf(args) => run_perf(args),
        Command::Reindex(args) => run_reindex(args),
    }
}

/// 执行查询命令并输出结果。
fn run_query(args: QueryArgs) -> anyhow::Result<()> {
    let mut profile = args.profile.then(QueryProfileStats::default);
    let collect_begin = Instant::now();
    let rows = collect_rows(&args.common, args.limit, &args.params, profile.as_mut())?;
    if let Some(stats) = profile.as_mut() {
        stats.collect_rows_ms = elapsed_ms(collect_begin);
        stats.matched_rows = rows.len();
    }
    let output_begin = Instant::now();
    if args.flat {
        let flat_rows = flatten_rows(&rows);
        println!("matched: {}", flat_rows.len());
        println!("ts,param_id,value");
        for row in flat_rows {
            println!("{},{},{:.6}", row.ts, row.param_id, row.value);
        }
    } else {
        println!("matched: {}", rows.len());
        println!("ts,param_ids,values");
        for row in rows {
            println!(
                "{},{},{}",
                row.ts,
                row.param_ids.join("|"),
                row.values
                    .iter()
                    .map(|x| format!("{x:.6}"))
                    .collect::<Vec<_>>()
                    .join("|")
            );
        }
    }
    if let Some(stats) = profile.as_mut() {
        stats.output_ms = elapsed_ms(output_begin);
        print_query_profile(stats);
    }
    Ok(())
}

/// 执行统计命令，输出文件和数据点总览。
fn run_stat(args: CommonArgs) -> anyhow::Result<()> {
    let (from_ts, to_ts) = resolve_time_range(&args)?;
    let empty_params = HashSet::new();
    let files = collect_candidate_files(
        &args.root,
        &args.device_id,
        from_ts,
        to_ts,
        &empty_params,
        args.engine,
        None,
    )?;
    let mut stats = Stats {
        files: files.len(),
        rows: 0,
        points: 0,
        min_ts: None,
        max_ts: None,
    };
    for file in files {
        let rows = match read_rows_from_file(&file, from_ts, to_ts, &empty_params, None) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("warn: skip unreadable file {}: {}", file.display(), e);
                continue;
            }
        };
        for row in rows {
            stats.rows += 1;
            stats.points += row.values.len();
            stats.min_ts = Some(stats.min_ts.map_or(row.ts, |v| v.min(row.ts)));
            stats.max_ts = Some(stats.max_ts.map_or(row.ts, |v| v.max(row.ts)));
        }
    }
    println!("root: {}", args.root);
    println!("device_id: {}", args.device_id);
    println!("files: {}", stats.files);
    println!("rows: {}", stats.rows);
    println!("points: {}", stats.points);
    println!("min_ts: {}", opt_u64(stats.min_ts));
    println!("max_ts: {}", opt_u64(stats.max_ts));
    Ok(())
}

/// 执行导出命令，将查询结果输出到 JSON/CSV 文件。
fn run_export(args: ExportArgs) -> anyhow::Result<()> {
    let rows = collect_rows(&args.common, Some(args.limit), &args.params, None)?;
    let mut file = File::create(&args.out)
        .with_context(|| format!("create output failed: {}", args.out.display()))?;
    let exported_count;
    if args.flat {
        let flat_rows = flatten_rows(&rows);
        exported_count = flat_rows.len();
        match args.format {
            ExportFormat::Csv => write_flat_csv(&mut file, &flat_rows)?,
            ExportFormat::Json => serde_json::to_writer_pretty(&mut file, &flat_rows)?,
        }
    } else {
        exported_count = rows.len();
        match args.format {
            ExportFormat::Csv => write_csv(&mut file, &rows)?,
            ExportFormat::Json => serde_json::to_writer_pretty(&mut file, &rows)?,
        }
    }
    file.flush()?;
    println!("exported {} rows to {}", exported_count, args.out.display());
    Ok(())
}

/// 执行一致性检查命令，扫描 manifest 与 parquet 文件匹配关系。
fn run_doctor(args: CommonArgs) -> anyhow::Result<()> {
    let (from_ts, to_ts) = resolve_time_range(&args)?;
    let hour_dirs = collect_hour_dirs(&args.root, &args.device_id, from_ts, to_ts);
    let mut stats = DoctorStats {
        hour_dirs: hour_dirs.len(),
        ..DoctorStats::default()
    };
    let mut issues = Vec::new();

    for hour_dir in hour_dirs {
        let (day_key, hour) = match parse_hour_dir(&hour_dir) {
            Ok(v) => v,
            Err(e) => {
                issues.push(format!("invalid hour dir: {} ({})", hour_dir.display(), e));
                continue;
            }
        };
        let manifest = hour_dir.join("manifest.jsonl");
        if !manifest.exists() {
            stats.missing_manifest += 1;
            issues.push(format!("missing manifest: {}", manifest.display()));
            continue;
        }
        stats.manifest_files += 1;

        let text = std::fs::read_to_string(&manifest)
            .with_context(|| format!("read manifest failed: {}", manifest.display()))?;
        let mut manifest_entries = Vec::<SegmentManifestEntry>::new();
        for (idx, raw_line) in text.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }
            let entry = match serde_json::from_str::<SegmentManifestEntry>(line) {
                Ok(v) => v,
                Err(e) => {
                    stats.bad_manifest_lines += 1;
                    issues.push(format!(
                        "bad manifest line: {}:{} err={}",
                        manifest.display(),
                        idx + 1,
                        e
                    ));
                    continue;
                }
            };
            stats.manifest_entries += 1;
            manifest_entries.push(entry.clone());
            if entry.rows > 0 && entry.min_ts > entry.max_ts {
                stats.invalid_entries += 1;
                issues.push(format!(
                    "invalid ts range: {}:{} min_ts={} max_ts={}",
                    manifest.display(),
                    idx + 1,
                    entry.min_ts,
                    entry.max_ts
                ));
            }
            let seg = hour_dir.join(&entry.segment_file);
            if !seg.exists() {
                stats.missing_segment_files += 1;
                issues.push(format!("missing segment file: {}", seg.display()));
            }
        }

        let redb_entries = match load_redb_hour_entries(&args.root, &args.device_id, &day_key, hour)
        {
            Ok(v) => v,
            Err(e) => {
                stats.bad_redb_payload += 1;
                issues.push(format!(
                    "read redb hour index failed: {} {} {:02} ({})",
                    args.device_id, day_key, hour, e
                ));
                continue;
            }
        };
        let Some(redb_entries) = redb_entries else {
            stats.missing_redb_db += 1;
            continue;
        };
        if redb_entries.is_empty() {
            stats.missing_redb_hour_key += 1;
        }

        let manifest_map: HashMap<String, &SegmentManifestEntry> = manifest_entries
            .iter()
            .map(|x| (x.segment_file.clone(), x))
            .collect();
        let redb_map: HashMap<String, &IndexSegmentEntry> = redb_entries
            .iter()
            .map(|x| (x.segment_file.clone(), x))
            .collect();

        for (segment_file, m) in &manifest_map {
            if !redb_map.contains_key(segment_file) {
                stats.manifest_not_indexed += 1;
                issues.push(format!(
                    "manifest segment not indexed: {}/{}/{}",
                    day_key, hour, segment_file
                ));
                continue;
            }
            let r = redb_map.get(segment_file).expect("checked contains_key");
            if m.min_ts != r.min_ts || m.max_ts != r.max_ts || m.rows != r.rows {
                stats.redb_mismatch_entries += 1;
                issues.push(format!(
                    "index mismatch: {}/{}/{}",
                    day_key, hour, segment_file
                ));
            }
        }

        for (segment_file, r) in &redb_map {
            if !manifest_map.contains_key(segment_file) {
                stats.orphan_redb_entries += 1;
                issues.push(format!(
                    "orphan redb entry: {}/{}/{}",
                    day_key, hour, segment_file
                ));
            }
            let seg = hour_dir.join(&r.segment_file);
            if !seg.exists() {
                stats.redb_missing_segment_files += 1;
                issues.push(format!("redb target segment missing: {}", seg.display()));
            }
        }
    }

    println!("root: {}", args.root);
    println!("device_id: {}", args.device_id);
    println!("hour_dirs: {}", stats.hour_dirs);
    println!("manifest_files: {}", stats.manifest_files);
    println!("manifest_entries: {}", stats.manifest_entries);
    println!("missing_manifest: {}", stats.missing_manifest);
    println!("bad_manifest_lines: {}", stats.bad_manifest_lines);
    println!("invalid_entries: {}", stats.invalid_entries);
    println!("missing_segment_files: {}", stats.missing_segment_files);
    println!("missing_redb_db: {}", stats.missing_redb_db);
    println!("missing_redb_hour_key: {}", stats.missing_redb_hour_key);
    println!("bad_redb_payload: {}", stats.bad_redb_payload);
    println!("manifest_not_indexed: {}", stats.manifest_not_indexed);
    println!("orphan_redb_entries: {}", stats.orphan_redb_entries);
    println!("redb_mismatch_entries: {}", stats.redb_mismatch_entries);
    println!("redb_missing_segment_files: {}", stats.redb_missing_segment_files);
    println!("issues: {}", issues.len());
    for issue in issues.iter().take(30) {
        println!("  - {}", issue);
    }
    if issues.len() > 30 {
        println!("  ... ({} more issues)", issues.len() - 30);
    }
    Ok(())
}

/// 执行索引重建：按 manifest 重建 `root/_index/tsindex.redb` 中目标时间范围的小时键。
fn run_reindex(args: ReindexArgs) -> anyhow::Result<()> {
    let (from_ts, to_ts) = resolve_time_range(&args.common)?;
    let hour_dirs = collect_hour_dirs(&args.common.root, &args.common.device_id, from_ts, to_ts);
    let mut indexed_rows = 0usize;
    let mut manifest_files = 0usize;
    let mut hour_payloads = Vec::<(String, String)>::new();

    for hour_dir in hour_dirs {
        let (day_key, hour) = match parse_hour_dir(&hour_dir) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let manifest = hour_dir.join("manifest.jsonl");
        if !manifest.exists() {
            continue;
        }
        manifest_files = manifest_files.saturating_add(1);
        let text = std::fs::read_to_string(&manifest)
            .with_context(|| format!("read manifest failed: {}", manifest.display()))?;
        let mut entries = Vec::<IndexSegmentEntry>::new();
        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(item) = serde_json::from_str::<SegmentManifestEntry>(line) else {
                continue;
            };
            if item.rows == 0 {
                continue;
            }
            entries.push(IndexSegmentEntry {
                segment_file: item.segment_file,
                min_ts: item.min_ts,
                max_ts: item.max_ts,
                rows: item.rows,
            });
        }
        entries.sort_by(|a, b| a.segment_file.cmp(&b.segment_file));
        entries.dedup_by(|a, b| a.segment_file == b.segment_file);
        indexed_rows = indexed_rows.saturating_add(entries.len());
        let key = hourly_index_key(&args.common.device_id, &day_key, hour);
        let payload = serde_json::to_string(&entries)?;
        hour_payloads.push((key, payload));
    }

    hour_payloads.sort_by(|a, b| a.0.cmp(&b.0));
    let db_dir = PathBuf::from(&args.common.root).join("_index");
    std::fs::create_dir_all(&db_dir)
        .with_context(|| format!("create index dir failed: {}", db_dir.display()))?;
    let db_path = db_dir.join(TSINDEX_FILE_NAME);
    let tmp_path = db_dir.join(format!("{TSINDEX_FILE_NAME}.rebuild"));
    if tmp_path.exists() {
        std::fs::remove_file(&tmp_path)
            .with_context(|| format!("remove stale temp db failed: {}", tmp_path.display()))?;
    }
    if args.backup && db_path.exists() {
        let backup = db_dir.join(format!("{TSINDEX_FILE_NAME}.bak.{}", now_ts()));
        std::fs::copy(&db_path, &backup).with_context(|| {
            format!(
                "backup index db failed: {} -> {}",
                db_path.display(),
                backup.display()
            )
        })?;
        println!("backup: {}", backup.display());
    }
    let db = redb::Database::create(&tmp_path)
        .with_context(|| format!("create rebuild db failed: {}", tmp_path.display()))?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TSINDEX_HOURLY_SEGMENTS_TABLE)?;
        for (key, payload) in &hour_payloads {
            table.insert(key.as_str(), payload.as_str())?;
        }
    }
    write_txn.commit()?;
    drop(db);
    if db_path.exists() {
        std::fs::remove_file(&db_path)
            .with_context(|| format!("remove old db failed: {}", db_path.display()))?;
    }
    std::fs::rename(&tmp_path, &db_path).with_context(|| {
        format!(
            "replace db failed: {} -> {}",
            tmp_path.display(),
            db_path.display()
        )
    })?;

    println!("reindex_root: {}", args.common.root);
    println!("reindex_device_id: {}", args.common.device_id);
    println!("reindex_from_ts: {}", from_ts);
    println!("reindex_to_ts: {}", to_ts);
    println!("reindex_manifest_files: {}", manifest_files);
    println!("reindex_hour_keys: {}", hour_payloads.len());
    println!("reindex_segment_rows: {}", indexed_rows);
    println!("reindex_db: {}", db_path.display());
    Ok(())
}

/// 执行查询性能测试，输出多轮耗时统计。
fn run_perf(args: PerfArgs) -> anyhow::Result<()> {
    if args.iterations == 0 {
        bail!("--iterations must be > 0");
    }
    let (from_ts, to_ts) = resolve_time_range(&args.common)?;
    let normalized_params = normalize_params(&args.params)?;

    println!("perf_root: {}", args.common.root);
    println!("perf_device_id: {}", args.common.device_id);
    println!("perf_params: {}", normalized_params.len());
    println!("perf_limit: {}", args.limit);
    println!("perf_from_ts: {}", from_ts);
    println!("perf_to_ts: {}", to_ts);
    println!("perf_warmup: {}", args.warmup);
    println!("perf_iterations: {}", args.iterations);

    if args.compare_engine {
        let mut index_common = args.common.clone();
        index_common.engine = QueryEngine::Index;
        let index_stats = bench_query_engine(
            &index_common,
            from_ts,
            to_ts,
            &normalized_params,
            args.limit,
            args.warmup,
            args.iterations,
        )?;
        print_perf_stats("index", index_stats);

        let mut manifest_common = args.common.clone();
        manifest_common.engine = QueryEngine::Manifest;
        let manifest_stats = bench_query_engine(
            &manifest_common,
            from_ts,
            to_ts,
            &normalized_params,
            args.limit,
            args.warmup,
            args.iterations,
        )?;
        print_perf_stats("manifest", manifest_stats);

        let speedup_avg = if index_stats.avg_ms > 0.0 {
            manifest_stats.avg_ms / index_stats.avg_ms
        } else {
            0.0
        };
        let speedup_p95 = if index_stats.p95_ms > 0.0 {
            manifest_stats.p95_ms / index_stats.p95_ms
        } else {
            0.0
        };
        println!("compare_speedup_avg_x: {:.3}", speedup_avg);
        println!("compare_speedup_p95_x: {:.3}", speedup_p95);
    } else {
        let stats = bench_query_engine(
            &args.common,
            from_ts,
            to_ts,
            &normalized_params,
            args.limit,
            args.warmup,
            args.iterations,
        )?;
        print_perf_stats(match args.common.engine {
            QueryEngine::Index => "index",
            QueryEngine::Manifest => "manifest",
        }, stats);
    }
    Ok(())
}

/// 执行指定引擎多轮查询并返回耗时统计。
fn bench_query_engine(
    common: &CommonArgs,
    from_ts: u64,
    to_ts: u64,
    normalized_params: &HashSet<String>,
    limit: usize,
    warmup: usize,
    iterations: usize,
) -> anyhow::Result<PerfStats> {
    for _ in 0..warmup {
        let _ = collect_rows_internal(common, from_ts, to_ts, normalized_params, Some(limit), None)?;
    }
    let mut costs_ms = Vec::with_capacity(iterations);
    let mut matched = 0usize;
    for _ in 0..iterations {
        let begin = Instant::now();
        let rows = collect_rows_internal(common, from_ts, to_ts, normalized_params, Some(limit), None)?;
        let elapsed = begin.elapsed();
        matched = rows.len();
        costs_ms.push(elapsed.as_secs_f64() * 1000.0);
    }
    costs_ms.sort_by(|a, b| a.total_cmp(b));
    let min_ms = *costs_ms.first().unwrap_or(&0.0);
    let max_ms = *costs_ms.last().unwrap_or(&0.0);
    let avg_ms = if costs_ms.is_empty() {
        0.0
    } else {
        costs_ms.iter().sum::<f64>() / costs_ms.len() as f64
    };
    let p95_idx = ((costs_ms.len() as f64) * 0.95).ceil() as usize;
    let p95_ms = if costs_ms.is_empty() {
        0.0
    } else {
        costs_ms[p95_idx.saturating_sub(1).min(costs_ms.len() - 1)]
    };
    Ok(PerfStats {
        matched,
        min_ms,
        avg_ms,
        p95_ms,
        max_ms,
    })
}

/// 输出单引擎性能统计结果。
fn print_perf_stats(engine_name: &str, stats: PerfStats) {
    println!("perf_engine: {}", engine_name);
    println!("perf_last_matched: {}", stats.matched);
    println!("perf_min_ms: {:.3}", stats.min_ms);
    println!("perf_avg_ms: {:.3}", stats.avg_ms);
    println!("perf_p95_ms: {:.3}", stats.p95_ms);
    println!("perf_max_ms: {:.3}", stats.max_ms);
}

/// 输出 `query --profile` 的分项耗时与关键计数。
fn print_query_profile(stats: &QueryProfileStats) {
    let total_query_ms = stats.collect_rows_ms + stats.output_ms;
    println!("profile_total_ms: {:.3}", total_query_ms);
    println!("profile_collect_rows_ms: {:.3}", stats.collect_rows_ms);
    println!("profile_output_ms: {:.3}", stats.output_ms);
    println!("profile_candidate_collect_ms: {:.3}", stats.candidate_collect_ms);
    println!("profile_redb_index_ms: {:.3}", stats.redb_index_ms);
    println!("profile_manifest_scan_ms: {:.3}", stats.manifest_scan_ms);
    println!("profile_read_rows_ms: {:.3}", stats.read_rows_ms);
    println!("profile_parquet_open_ms: {:.3}", stats.parquet_open_ms);
    println!(
        "profile_parquet_reader_build_ms: {:.3}",
        stats.parquet_reader_build_ms
    );
    println!("profile_pidx_load_ms: {:.3}", stats.pidx_load_ms);
    println!("profile_pidx_plan_ms: {:.3}", stats.pidx_plan_ms);
    println!("profile_row_selection_ms: {:.3}", stats.row_selection_ms);
    println!("profile_pidx_decode_ms: {:.3}", stats.pidx_decode_ms);
    println!("profile_fallback_decode_ms: {:.3}", stats.fallback_decode_ms);
    println!("profile_candidate_hour_dirs: {}", stats.candidate_hour_dirs);
    println!("profile_candidate_files: {}", stats.candidate_files);
    println!("profile_files_scanned: {}", stats.files_scanned);
    println!("profile_parquet_open_count: {}", stats.parquet_open_count);
    println!("profile_pidx_load_count: {}", stats.pidx_load_count);
    println!("profile_pidx_hit_count: {}", stats.pidx_hit_count);
    println!("profile_row_groups_candidate: {}", stats.row_groups_candidate);
    println!(
        "profile_row_groups_read_via_pidx: {}",
        stats.row_groups_read_via_pidx
    );
    println!("profile_matched_rows: {}", stats.matched_rows);
}

/// 将 `Instant` 起点转换为毫秒，便于统一累加分项统计。
fn elapsed_ms(begin: Instant) -> f64 {
    begin.elapsed().as_secs_f64() * 1000.0
}

/// 读取指定设备小时的 `redb` 索引条目；若索引库不存在返回 `None`。
fn load_redb_hour_entries(
    root: &str,
    device_id: &str,
    day_key: &str,
    hour: u32,
) -> anyhow::Result<Option<Vec<IndexSegmentEntry>>> {
    let db_path = PathBuf::from(root).join("_index").join(TSINDEX_FILE_NAME);
    if !db_path.exists() {
        return Ok(None);
    }
    let db = redb::Database::open(&db_path)
        .with_context(|| format!("open tsindex db failed: {}", db_path.display()))?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TSINDEX_HOURLY_SEGMENTS_TABLE)?;
    let key = hourly_index_key(device_id, day_key, hour);
    let Some(raw) = table.get(key.as_str())? else {
        return Ok(Some(Vec::new()));
    };
    let items = serde_json::from_str::<Vec<IndexSegmentEntry>>(raw.value()).with_context(|| {
        format!(
            "parse redb payload failed for key={} in {}",
            key,
            db_path.display()
        )
    })?;
    Ok(Some(items))
}

/// 汇总时间范围内的候选文件并读取行数据。
fn collect_rows(
    args: &CommonArgs,
    limit: Option<usize>,
    params: &[String],
    profile: Option<&mut QueryProfileStats>,
) -> anyhow::Result<Vec<QueryRow>> {
    let (from_ts, to_ts) = resolve_time_range(args)?;
    let normalized_params = normalize_params(params)?;
    collect_rows_internal(args, from_ts, to_ts, &normalized_params, limit, profile)
}

/// 按已解析时间范围与参数集合执行查询。
fn collect_rows_internal(
    args: &CommonArgs,
    from_ts: u64,
    to_ts: u64,
    normalized_params: &HashSet<String>,
    limit: Option<usize>,
    mut profile: Option<&mut QueryProfileStats>,
) -> anyhow::Result<Vec<QueryRow>> {
    let collect_begin = Instant::now();
    let files = collect_candidate_files(
        &args.root,
        &args.device_id,
        from_ts,
        to_ts,
        normalized_params,
        args.engine,
        profile.as_deref_mut(),
    )?;
    if let Some(stats) = profile.as_deref_mut() {
        stats.candidate_files = files.len();
    }
    let mut rows = Vec::new();

    for file in files {
        let mut one = match read_rows_from_file(&file, from_ts, to_ts, normalized_params, profile.as_deref_mut()) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("warn: skip unreadable file {}: {}", file.display(), e);
                continue;
            }
        };
        rows.append(&mut one);
        if let Some(limit) = limit
            && rows.len() >= limit
        {
            break;
        }
    }
    rows.sort_by_key(|x| x.ts);
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    if let Some(stats) = profile {
        stats.read_rows_ms = elapsed_ms(collect_begin);
    }
    Ok(rows)
}

/// 按时间范围收集小时目录列表。
fn collect_hour_dirs(root: &str, device_id: &str, from_ts: u64, to_ts: u64) -> Vec<PathBuf> {
    if from_ts > to_ts {
        return Vec::new();
    }
    let mut out = BTreeSet::new();
    let device_dir = PathBuf::from(root).join(device_id);
    let Ok(day_entries) = std::fs::read_dir(&device_dir) else {
        return Vec::new();
    };
    for day_entry in day_entries.flatten() {
        let day_path = day_entry.path();
        if !day_path.is_dir() {
            continue;
        }
        let Some(day_name) = day_path.file_name().and_then(|x| x.to_str()) else {
            continue;
        };
        let Ok(day) = NaiveDate::parse_from_str(day_name, "%Y-%m-%d") else {
            continue;
        };
        let Ok(hour_entries) = std::fs::read_dir(&day_path) else {
            continue;
        };
        for hour_entry in hour_entries.flatten() {
            let hour_path = hour_entry.path();
            if !hour_path.is_dir() {
                continue;
            }
            let Some(hour_name) = hour_path.file_name().and_then(|x| x.to_str()) else {
                continue;
            };
            let Ok(hour) = hour_name.parse::<u32>() else {
                continue;
            };
            if hour > 23 {
                continue;
            }
            let Some(dt) = Local
                .with_ymd_and_hms(day.year(), day.month(), day.day(), hour, 0, 0)
                .single()
            else {
                continue;
            };
            let hour_start = dt.timestamp();
            if hour_start < 0 {
                continue;
            }
            let hour_start = hour_start as u64;
            let hour_end = hour_start.saturating_add(3599);
            if hour_end < from_ts || hour_start > to_ts {
                continue;
            }
            out.insert(hour_path);
        }
    }
    out.into_iter().collect()
}

/// 按时间范围枚举 `tsdata` 小时目录中的 Parquet 文件。
fn collect_candidate_files(
    root: &str,
    device_id: &str,
    from_ts: u64,
    to_ts: u64,
    _params: &HashSet<String>,
    engine: QueryEngine,
    mut profile: Option<&mut QueryProfileStats>,
) -> anyhow::Result<Vec<PathBuf>> {
    if from_ts > to_ts {
        bail!("from_ts must be <= to_ts");
    }
    let begin = Instant::now();
    let mut set = BTreeSet::new();
    let hour_dirs = collect_hour_dirs(root, device_id, from_ts, to_ts);
    if let Some(stats) = profile.as_deref_mut() {
        stats.candidate_hour_dirs = hour_dirs.len();
    }
    for hour_dir in hour_dirs {
        let from_manifest = if matches!(engine, QueryEngine::Index) {
            let redb_begin = Instant::now();
            let redb_result = collect_from_redb_index(root, device_id, &hour_dir, from_ts, to_ts, _params);
            if let Some(stats) = profile.as_deref_mut() {
                stats.redb_index_ms += elapsed_ms(redb_begin);
            }
            match redb_result {
                Ok(v) if !v.is_empty() => v,
                Ok(_) => {
                    let manifest_begin = Instant::now();
                    let files = collect_from_manifest(&hour_dir, from_ts, to_ts)?;
                    if let Some(stats) = profile.as_deref_mut() {
                        stats.manifest_scan_ms += elapsed_ms(manifest_begin);
                    }
                    files
                }
                Err(e) => {
                    eprintln!(
                        "warn: redb index read failed, fallback manifest: {} ({})",
                        hour_dir.display(),
                        e
                    );
                    let manifest_begin = Instant::now();
                    let files = collect_from_manifest(&hour_dir, from_ts, to_ts)?;
                    if let Some(stats) = profile.as_deref_mut() {
                        stats.manifest_scan_ms += elapsed_ms(manifest_begin);
                    }
                    files
                }
            }
        } else {
            let manifest_begin = Instant::now();
            let files = collect_from_manifest(&hour_dir, from_ts, to_ts)?;
            if let Some(stats) = profile.as_deref_mut() {
                stats.manifest_scan_ms += elapsed_ms(manifest_begin);
            }
            files
        };
        for path in from_manifest {
            set.insert(path);
        }
    }
    if let Some(stats) = profile {
        stats.candidate_collect_ms = elapsed_ms(begin);
    }
    Ok(set.into_iter().collect())
}

/// 从 `redb` 小时索引读取候选分段，并按时间/参数过滤。
fn collect_from_redb_index(
    root: &str,
    device_id: &str,
    hour_dir: &Path,
    from_ts: u64,
    to_ts: u64,
    _params: &HashSet<String>,
) -> anyhow::Result<Vec<PathBuf>> {
    let (day_key, hour) = parse_hour_dir(hour_dir)?;
    let db_path = PathBuf::from(root).join("_index").join(TSINDEX_FILE_NAME);
    if !db_path.exists() {
        return Ok(Vec::new());
    }
    let db = redb::Database::open(&db_path)
        .with_context(|| format!("open tsindex db failed: {}", db_path.display()))?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TSINDEX_HOURLY_SEGMENTS_TABLE)?;
    let key = hourly_index_key(device_id, &day_key, hour);
    let Some(raw) = table.get(key.as_str())? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    let items = serde_json::from_str::<Vec<IndexSegmentEntry>>(raw.value()).unwrap_or_default();
    for item in items {
        if item.rows == 0 {
            continue;
        }
        if item.max_ts < from_ts || item.min_ts > to_ts {
            continue;
        }
        out.push(hour_dir.join(item.segment_file));
    }
    Ok(out)
}

/// 从小时目录路径解析 `(day_key, hour)`。
fn parse_hour_dir(hour_dir: &Path) -> anyhow::Result<(String, u32)> {
    let hour_raw = hour_dir
        .file_name()
        .and_then(|x| x.to_str())
        .context("invalid hour dir name")?;
    let day_key = hour_dir
        .parent()
        .and_then(Path::file_name)
        .and_then(|x| x.to_str())
        .context("invalid day dir name")?
        .to_string();
    let hour = hour_raw
        .parse::<u32>()
        .with_context(|| format!("invalid hour dir: {hour_raw}"))?;
    Ok((day_key, hour))
}

/// 生成小时索引主键：`device|day|hour`。
fn hourly_index_key(device_id: &str, day_key: &str, hour: u32) -> String {
    format!("{device_id}|{day_key}|{hour:02}")
}

/// 从小时目录的 `manifest.jsonl` 收集与时间范围重叠的已提交分段文件。
fn collect_from_manifest(
    hour_dir: &Path,
    from_ts: u64,
    to_ts: u64,
) -> anyhow::Result<Vec<PathBuf>> {
    let manifest = hour_dir.join("manifest.jsonl");
    if !manifest.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(&manifest)
        .with_context(|| format!("read manifest failed: {}", manifest.display()))?;
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry = match serde_json::from_str::<SegmentManifestEntry>(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let _ = entry.created_at_ms;
        if entry.rows == 0 {
            continue;
        }
        if entry.max_ts < from_ts || entry.min_ts > to_ts {
            continue;
        }
        out.push(hour_dir.join(entry.segment_file));
    }
    Ok(out)
}

/// 读取单个 Parquet 文件并过滤时间范围与参数条件。
fn read_rows_from_file(
    path: &Path,
    from_ts: u64,
    to_ts: u64,
    params: &HashSet<String>,
    mut profile: Option<&mut QueryProfileStats>,
) -> anyhow::Result<Vec<QueryRow>> {
    if let Some(stats) = profile.as_deref_mut() {
        stats.files_scanned += 1;
    }
    let open_begin = Instant::now();
    let file = File::open(path).with_context(|| format!("open parquet failed: {}", path.display()))?;
    if let Some(stats) = profile.as_deref_mut() {
        stats.parquet_open_ms += elapsed_ms(open_begin);
        stats.parquet_open_count += 1;
    }
    let builder_begin = Instant::now();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    if let Some(stats) = profile.as_deref_mut() {
        stats.parquet_reader_build_ms += elapsed_ms(builder_begin);
    }
    let row_groups = select_candidate_row_groups(builder.metadata().as_ref(), from_ts, to_ts, params);
    if let Some(stats) = profile.as_deref_mut() {
        stats.row_groups_candidate += row_groups.len();
    }
    if row_groups.is_empty() {
        return Ok(Vec::new());
    }
    if !params.is_empty() {
        let pidx_begin = Instant::now();
        let pidx_loaded = load_segment_param_index(path)?;
        if let Some(stats) = profile.as_deref_mut() {
            stats.pidx_load_ms += elapsed_ms(pidx_begin);
            stats.pidx_load_count += 1;
        }
        if let Some(pidx) = pidx_loaded
            && pidx.version >= 1
        {
            if let Some(stats) = profile.as_deref_mut() {
                stats.pidx_hit_count += 1;
            }
            let rows = read_rows_with_param_index(
                path,
                from_ts,
                to_ts,
                params,
                &row_groups,
                &pidx,
                profile.as_deref_mut(),
            )?;
            return Ok(rows);
        }
    }
    let reader_build_begin = Instant::now();
    let reader = builder.with_row_groups(row_groups).build()?;
    if let Some(stats) = profile.as_deref_mut() {
        stats.parquet_reader_build_ms += elapsed_ms(reader_build_begin);
    }
    let decode_begin = Instant::now();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let ts_arr = batch
            .column_by_name("ts")
            .context("missing column: ts")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("ts type mismatch")?;
        let has_packet_wide =
            batch.column_by_name("param_ids").is_some() && batch.column_by_name("values").is_some();
        let has_long_row =
            batch.column_by_name("param_id").is_some() && batch.column_by_name("value").is_some();

        if has_packet_wide {
            let param_list = batch
                .column_by_name("param_ids")
                .context("missing column: param_ids")?
                .as_any()
                .downcast_ref::<ListArray>()
                .context("param_ids type mismatch")?;
            let values_list = batch
                .column_by_name("values")
                .context("missing column: values")?
                .as_any()
                .downcast_ref::<ListArray>()
                .context("values type mismatch")?;

            for i in 0..batch.num_rows() {
                let ts = ts_arr.value(i);
                if ts < from_ts || ts > to_ts {
                    continue;
                }
                let param_values = param_list.value(i);
                let param_arr = param_values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("param_ids inner type mismatch")?;
                let value_values = values_list.value(i);
                let value_arr = value_values
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .context("values inner type mismatch")?;
                if param_arr.len() != value_arr.len() {
                    bail!("param_ids and values length mismatch in {}", path.display());
                }

                let mut row_param_ids = Vec::with_capacity(param_arr.len());
                let mut row_values = Vec::with_capacity(value_arr.len());
                for j in 0..param_arr.len() {
                    let id = param_arr.value(j);
                    let val = value_arr.value(j);
                    if params.is_empty() || params.contains(id) {
                        row_param_ids.push(id.to_string());
                        row_values.push(val);
                    }
                }
                if row_param_ids.is_empty() {
                    continue;
                }
                out.push(QueryRow {
                    ts,
                    param_ids: row_param_ids,
                    values: row_values,
                });
            }
            continue;
        }

        if has_long_row {
            let param_arr = batch
                .column_by_name("param_id")
                .context("missing column: param_id")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("param_id type mismatch")?;
            let value_col = batch
                .column_by_name("value")
                .context("missing column: value")?;
            let value_f32 = value_col.as_any().downcast_ref::<Float32Array>();

            // 当 long-row 批次内 param_id 已排序时，使用二分区间定位目标参数，避免整批逐行比对。
            if !params.is_empty() && is_non_decreasing_string_array(param_arr) {
                let mut targets = params.iter().map(String::as_str).collect::<Vec<_>>();
                targets.sort_unstable();
                for target in targets {
                    let start = lower_bound_string_array(param_arr, target);
                    let end = upper_bound_string_array(param_arr, target);
                    if start >= end {
                        continue;
                    }
                    for i in start..end {
                        let ts = ts_arr.value(i);
                        if ts < from_ts || ts > to_ts {
                            continue;
                        }
                        let value = if let Some(arr) = value_f32 {
                            if arr.is_null(i) {
                                continue;
                            }
                            arr.value(i)
                        } else if let Some(arr) =
                            value_col.as_any().downcast_ref::<arrow::array::Float64Array>()
                        {
                            if arr.is_null(i) {
                                continue;
                            }
                            arr.value(i) as f32
                        } else {
                            bail!("value type mismatch in {} (expect float32/float64)", path.display());
                        };
                        out.push(QueryRow {
                            ts,
                            param_ids: vec![target.to_string()],
                            values: vec![value],
                        });
                    }
                }
                continue;
            }

            for i in 0..batch.num_rows() {
                let ts = ts_arr.value(i);
                if ts < from_ts || ts > to_ts {
                    continue;
                }
                if param_arr.is_null(i) {
                    continue;
                }
                let id = param_arr.value(i);
                if !params.is_empty() && !params.contains(id) {
                    continue;
                }
                let value = if let Some(arr) = value_f32 {
                    if arr.is_null(i) {
                        continue;
                    }
                    arr.value(i)
                } else if let Some(arr) = value_col.as_any().downcast_ref::<arrow::array::Float64Array>()
                {
                    if arr.is_null(i) {
                        continue;
                    }
                    arr.value(i) as f32
                } else {
                    bail!("value type mismatch in {} (expect float32/float64)", path.display());
                };

                out.push(QueryRow {
                    ts,
                    param_ids: vec![id.to_string()],
                    values: vec![value],
                });
            }
            continue;
        }

        bail!(
            "unsupported parquet schema in {} (expect packet-wide or long-row columns)",
            path.display()
        );
    }
    if let Some(stats) = profile {
        stats.fallback_decode_ms += elapsed_ms(decode_begin);
    }
    Ok(out)
}

/// 使用段级参数倒排索引读取 long-row 数据，按 row-group 内行区间直接定位目标参数。
fn read_rows_with_param_index(
    path: &Path,
    from_ts: u64,
    to_ts: u64,
    params: &HashSet<String>,
    candidate_row_groups: &[usize],
    pidx: &SegmentParamIndexFile,
    mut profile: Option<&mut QueryProfileStats>,
) -> anyhow::Result<Vec<QueryRow>> {
    let plan_begin = Instant::now();
    let mut rg_plan: HashMap<usize, Vec<&SegmentParamIndexEntry>> = HashMap::new();
    let rg_set: HashSet<usize> = candidate_row_groups.iter().copied().collect();
    for entry in &pidx.entries {
        let rg_id = entry.rg_id as usize;
        if !rg_set.contains(&rg_id) {
            continue;
        }
        if !params.contains(&entry.param_id) {
            continue;
        }
        if entry.max_ts < from_ts || entry.min_ts > to_ts {
            continue;
        }
        rg_plan.entry(rg_id).or_default().push(entry);
    }
    if rg_plan.is_empty() {
        return Ok(Vec::new());
    }
    for ranges in rg_plan.values_mut() {
        ranges.sort_by_key(|x| x.start_row);
    }
    if let Some(stats) = profile.as_deref_mut() {
        stats.pidx_plan_ms += elapsed_ms(plan_begin);
    }

    let mut out = Vec::new();
    for &rg_id in candidate_row_groups {
        let Some(ranges) = rg_plan.get(&rg_id) else {
            continue;
        };
        if let Some(stats) = profile.as_deref_mut() {
            stats.row_groups_read_via_pidx += 1;
        }
        let open_begin = Instant::now();
        let file = File::open(path).with_context(|| format!("open parquet failed: {}", path.display()))?;
        if let Some(stats) = profile.as_deref_mut() {
            stats.parquet_open_ms += elapsed_ms(open_begin);
            stats.parquet_open_count += 1;
        }
        let builder_begin = Instant::now();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        if let Some(stats) = profile.as_deref_mut() {
            stats.parquet_reader_build_ms += elapsed_ms(builder_begin);
        }
        let rg_rows = builder
            .metadata()
            .row_group(rg_id)
            .num_rows()
            .try_into()
            .unwrap_or(0usize);
        if rg_rows == 0 {
            continue;
        }
        let selection_begin = Instant::now();
        let selection = build_row_selection_from_ranges(ranges, rg_rows);
        if let Some(stats) = profile.as_deref_mut() {
            stats.row_selection_ms += elapsed_ms(selection_begin);
        }
        let reader_build_begin = Instant::now();
        let reader = builder
            .with_row_groups(vec![rg_id])
            .with_row_selection(selection)
            .build()?;
        if let Some(stats) = profile.as_deref_mut() {
            stats.parquet_reader_build_ms += elapsed_ms(reader_build_begin);
        }
        let decode_begin = Instant::now();
        for batch in reader {
            let batch = batch?;
            let ts_arr = batch
                .column_by_name("ts")
                .context("missing column: ts")?
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("ts type mismatch")?;
            let param_arr = batch
                .column_by_name("param_id")
                .context("missing column: param_id")?
                .as_any()
                .downcast_ref::<StringArray>()
                .context("param_id type mismatch")?;
            let value_col = batch
                .column_by_name("value")
                .context("missing column: value")?;
            let value_f32 = value_col.as_any().downcast_ref::<Float32Array>();

            for i in 0..batch.num_rows() {
                let ts = ts_arr.value(i);
                if ts < from_ts || ts > to_ts {
                    continue;
                }
                if param_arr.is_null(i) {
                    continue;
                }
                let id = param_arr.value(i);
                if !params.contains(id) {
                    continue;
                }
                let value = if let Some(arr) = value_f32 {
                    if arr.is_null(i) {
                        continue;
                    }
                    arr.value(i)
                } else if let Some(arr) = value_col.as_any().downcast_ref::<arrow::array::Float64Array>() {
                    if arr.is_null(i) {
                        continue;
                    }
                    arr.value(i) as f32
                } else {
                    bail!("value type mismatch in {} (expect float32/float64)", path.display());
                };
                out.push(QueryRow {
                    ts,
                    param_ids: vec![id.to_string()],
                    values: vec![value],
                });
            }
        }
        if let Some(stats) = profile.as_deref_mut() {
            stats.pidx_decode_ms += elapsed_ms(decode_begin);
        }
    }
    Ok(out)
}

/// 将 `.pidx` 的局部行区间转换为 `RowSelection`，用于 Parquet 解码前跳读无关行。
fn build_row_selection_from_ranges(ranges: &[&SegmentParamIndexEntry], total_rows: usize) -> RowSelection {
    let mut merged = Vec::<(usize, usize)>::new();
    for r in ranges {
        let start = (r.start_row as usize).min(total_rows);
        let end = (r.end_row as usize).min(total_rows);
        if start >= end {
            continue;
        }
        match merged.last_mut() {
            Some((_, last_end)) if start <= *last_end => {
                *last_end = (*last_end).max(end);
            }
            _ => merged.push((start, end)),
        }
    }
    let iter = merged.into_iter().map(|(start, end)| start..end);
    RowSelection::from_consecutive_ranges(iter, total_rows)
}

/// 加载段文件 sidecar 参数索引；不存在时返回 `None`。
fn load_segment_param_index(path: &Path) -> anyhow::Result<Option<SegmentParamIndexFile>> {
    let pidx_path = segment_param_index_path(path);
    if !pidx_path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&pidx_path)
        .with_context(|| format!("read pidx failed: {}", pidx_path.display()))?;
    let pidx = if bytes.starts_with(&PIDX_MAGIC) {
        decode_segment_param_index_binary(&bytes)
            .with_context(|| format!("parse binary pidx failed: {}", pidx_path.display()))?
    } else {
        serde_json::from_slice::<SegmentParamIndexFile>(&bytes)
            .with_context(|| format!("parse json pidx failed: {}", pidx_path.display()))?
    };
    Ok(Some(pidx))
}

/// 解析二进制 `.pidx`，恢复为查询侧使用的索引结构。
fn decode_segment_param_index_binary(bytes: &[u8]) -> anyhow::Result<SegmentParamIndexFile> {
    if bytes.len() < 9 {
        bail!("binary pidx too short");
    }
    if bytes[0..4] != PIDX_MAGIC {
        bail!("invalid pidx magic");
    }
    let mut offset = 4usize;
    let version = read_u8(bytes, &mut offset)?;
    if version != PIDX_BINARY_VERSION {
        bail!("unsupported pidx binary version: {version}");
    }
    let dict_len = read_u32_le(bytes, &mut offset)? as usize;
    let mut dict = Vec::<String>::with_capacity(dict_len);
    for _ in 0..dict_len {
        let slen = read_u8(bytes, &mut offset)? as usize;
        let end = offset.saturating_add(slen);
        if end > bytes.len() {
            bail!("pidx dictionary out of range");
        }
        let s = std::str::from_utf8(&bytes[offset..end]).context("pidx param_id not utf8")?;
        dict.push(s.to_string());
        offset = end;
    }
    let entry_count = read_u32_le(bytes, &mut offset)? as usize;
    let mut entries = Vec::<SegmentParamIndexEntry>::with_capacity(entry_count);
    for _ in 0..entry_count {
        let rg_id = read_u32_le(bytes, &mut offset)?;
        let param_idx = read_u32_le(bytes, &mut offset)? as usize;
        let start_row = read_u32_le(bytes, &mut offset)?;
        let end_row = read_u32_le(bytes, &mut offset)?;
        let min_ts = read_u64_le(bytes, &mut offset)?;
        let max_ts = read_u64_le(bytes, &mut offset)?;
        let Some(param_id) = dict.get(param_idx) else {
            bail!("pidx param dictionary index out of range: {param_idx}");
        };
        entries.push(SegmentParamIndexEntry {
            rg_id,
            param_id: param_id.clone(),
            start_row,
            end_row,
            min_ts,
            max_ts,
        });
    }
    Ok(SegmentParamIndexFile {
        version: PIDX_BINARY_VERSION as u32,
        entries,
    })
}

/// 从缓冲区读取单字节并推进偏移。
fn read_u8(bytes: &[u8], offset: &mut usize) -> anyhow::Result<u8> {
    if *offset >= bytes.len() {
        bail!("pidx read out of range");
    }
    let v = bytes[*offset];
    *offset += 1;
    Ok(v)
}

/// 从缓冲区读取小端序 `u32` 并推进偏移。
fn read_u32_le(bytes: &[u8], offset: &mut usize) -> anyhow::Result<u32> {
    let end = offset.saturating_add(4);
    if end > bytes.len() {
        bail!("pidx read_u32 out of range");
    }
    let mut arr = [0_u8; 4];
    arr.copy_from_slice(&bytes[*offset..end]);
    *offset = end;
    Ok(u32::from_le_bytes(arr))
}

/// 从缓冲区读取小端序 `u64` 并推进偏移。
fn read_u64_le(bytes: &[u8], offset: &mut usize) -> anyhow::Result<u64> {
    let end = offset.saturating_add(8);
    if end > bytes.len() {
        bail!("pidx read_u64 out of range");
    }
    let mut arr = [0_u8; 8];
    arr.copy_from_slice(&bytes[*offset..end]);
    *offset = end;
    Ok(u64::from_le_bytes(arr))
}

/// 返回段文件对应的 sidecar 参数索引路径：`seg_xxx.parquet.pidx`。
fn segment_param_index_path(segment_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.pidx", segment_path.display()))
}

/// 基于 row-group 统计信息筛选候选分组，减少后续无关解码。
fn select_candidate_row_groups(
    metadata: &ParquetMetaData,
    from_ts: u64,
    to_ts: u64,
    params: &HashSet<String>,
) -> Vec<usize> {
    let mut out = Vec::new();
    for idx in 0..metadata.num_row_groups() {
        let rg = metadata.row_group(idx);
        if !row_group_time_maybe_hit(rg, from_ts, to_ts) {
            continue;
        }
        if !params.is_empty() && !row_group_param_maybe_hit(rg, params) {
            continue;
        }
        out.push(idx);
    }
    out
}

/// 判断 row-group 是否可能命中时间范围；若缺少可用统计则保守返回 true。
fn row_group_time_maybe_hit(rg: &RowGroupMetaData, from_ts: u64, to_ts: u64) -> bool {
    let Some(col) = find_column_by_name(rg, "ts") else {
        return true;
    };
    let Some(stats) = col.statistics() else {
        return true;
    };
    if !stats.min_is_exact() || !stats.max_is_exact() {
        return true;
    }
    let Some(from_i64) = i64::try_from(from_ts).ok() else {
        return true;
    };
    let Some(to_i64) = i64::try_from(to_ts).ok() else {
        return true;
    };
    match stats {
        ParquetStatistics::Int64(typed) => {
            let Some(min) = typed.min_opt().copied() else {
                return true;
            };
            let Some(max) = typed.max_opt().copied() else {
                return true;
            };
            !(max < from_i64 || min > to_i64)
        }
        _ => true,
    }
}

/// 判断 row-group 是否可能命中参数集合；若缺少可用统计则保守返回 true。
fn row_group_param_maybe_hit(rg: &RowGroupMetaData, params: &HashSet<String>) -> bool {
    let Some(col) = find_column_by_name(rg, "param_id") else {
        return true;
    };
    let Some(stats) = col.statistics() else {
        return true;
    };
    if !stats.min_is_exact() || !stats.max_is_exact() {
        return true;
    }
    match stats {
        ParquetStatistics::ByteArray(typed) => {
            let Some(min) = typed.min_opt().map(|v| v.data()) else {
                return true;
            };
            let Some(max) = typed.max_opt().map(|v| v.data()) else {
                return true;
            };
            params.iter().any(|p| {
                let key = p.as_bytes();
                key >= min && key <= max
            })
        }
        _ => true,
    }
}

/// 在 row-group 的列元信息中查找指定列名。
fn find_column_by_name<'a>(rg: &'a RowGroupMetaData, name: &str) -> Option<&'a ColumnChunkMetaData> {
    rg.columns()
        .iter()
        .find(|col| col.column_path().string().as_str() == name)
}

/// 判断 `StringArray` 是否按非降序排列。
fn is_non_decreasing_string_array(arr: &StringArray) -> bool {
    if arr.len() <= 1 {
        return true;
    }
    let mut prev = arr.value(0);
    for i in 1..arr.len() {
        let cur = arr.value(i);
        if cur < prev {
            return false;
        }
        prev = cur;
    }
    true
}

/// 在有序 `StringArray` 上查找 `target` 的左边界（第一个 >= target 的位置）。
fn lower_bound_string_array(arr: &StringArray, target: &str) -> usize {
    let mut l = 0usize;
    let mut r = arr.len();
    while l < r {
        let m = l + (r - l) / 2;
        if arr.value(m) < target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

/// 在有序 `StringArray` 上查找 `target` 的右边界（第一个 > target 的位置）。
fn upper_bound_string_array(arr: &StringArray, target: &str) -> usize {
    let mut l = 0usize;
    let mut r = arr.len();
    while l < r {
        let m = l + (r - l) / 2;
        if arr.value(m) <= target {
            l = m + 1;
        } else {
            r = m;
        }
    }
    l
}

/// 将公共参数解析为 `[from_ts, to_ts]` 秒级时间范围。
fn resolve_time_range(args: &CommonArgs) -> anyhow::Result<(u64, u64)> {
    if let (Some(from), Some(to)) = (args.from_ts, args.to_ts) {
        if from > to {
            bail!("--from must be <= --to");
        }
        return Ok((from, to));
    }
    if let Some(day) = &args.day {
        return day_ts_bounds(day);
    }
    if args.today {
        let now = Local::now();
        let day = format!("{:04}-{:02}-{:02}", now.year(), now.month(), now.day());
        return day_ts_bounds(&day);
    }
    if let Some(last) = &args.last {
        let window = parse_last_window_sec(last).context("invalid --last")?;
        let now = now_ts();
        return Ok((now.saturating_sub(window), now));
    }
    if args.all {
        return Ok((0, now_ts()));
    }
    bail!("must provide one time range mode: (--from and --to) | --day | --today | --last | --all");
}

/// 解析 `YYYY-MM-DD` 并返回当天本地时区秒级时间边界。
fn day_ts_bounds(day: &str) -> anyhow::Result<(u64, u64)> {
    let date = chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d")
        .with_context(|| format!("invalid day: {day}, expected YYYY-MM-DD"))?
        .and_hms_opt(0, 0, 0)
        .context("build day start failed")?;
    let start_dt = Local
        .from_local_datetime(&date)
        .single()
        .context("invalid local day start")?;
    let start = u64::try_from(start_dt.timestamp()).context("negative timestamp unsupported")?;
    Ok((start, start.saturating_add(86_399)))
}

/// 校验 `YYYY-MM-DD` 参数格式是否合法。
fn parse_day_key_arg(value: &str) -> Result<String, String> {
    let parsed = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d");
    if parsed.is_ok() {
        Ok(value.to_string())
    } else {
        Err(format!("invalid --day: {value} (expected YYYY-MM-DD)"))
    }
}

/// 校验 `--last` 窗口参数格式是否合法（如 `30m`、`6h`、`2d`）。
fn parse_last_window_arg(value: &str) -> Result<String, String> {
    if parse_last_window_sec(value).is_some() {
        Ok(value.to_string())
    } else {
        Err(format!(
            "invalid --last: {value} (expected <num>[s|m|h|d], e.g. 30m, 6h)"
        ))
    }
}

/// 将 `--last` 文本窗口解析为秒。
fn parse_last_window_sec(value: &str) -> Option<u64> {
    if value.len() < 2 {
        return None;
    }
    let (num, unit) = value.split_at(value.len() - 1);
    let n = num.parse::<u64>().ok()?;
    let unit_sec = match unit {
        "s" | "S" => 1_u64,
        "m" | "M" => 60_u64,
        "h" | "H" => 3600_u64,
        "d" | "D" => 86_400_u64,
        _ => return None,
    };
    n.checked_mul(unit_sec)
}

/// 解析参数编码，返回 `(prefix, number)`。
fn parse_param_code(code: &str) -> Option<(u8, u32)> {
    if !is_valid_param_code(code) {
        return None;
    }
    let bytes = code.as_bytes();
    let prefix = *bytes.first()?;
    let number = code.get(1..)?.parse::<u32>().ok()?;
    Some((prefix, number))
}

/// 规范化并校验参数过滤列表，支持单参数与范围参数（如 `P00001~P10000`）。
fn normalize_params(params: &[String]) -> anyhow::Result<HashSet<String>> {
    let mut out = HashSet::with_capacity(params.len());
    for raw in params {
        let normalized = raw.trim().to_ascii_uppercase();
        if normalized.is_empty() {
            continue;
        }
        if let Some((start_raw, end_raw)) = normalized.split_once('~') {
            let start = start_raw.trim();
            let end = end_raw.trim();
            let Some((sp, sv)) = parse_param_code(start) else {
                bail!(
                    "invalid --param range start: {} (expected A/Z/P + 5 digits)",
                    raw
                );
            };
            let Some((ep, ev)) = parse_param_code(end) else {
                bail!(
                    "invalid --param range end: {} (expected A/Z/P + 5 digits)",
                    raw
                );
            };
            if sp != ep {
                bail!("invalid --param range: {} (prefix must be same)", raw);
            }
            if sv > ev {
                bail!("invalid --param range: {} (start must be <= end)", raw);
            }
            for v in sv..=ev {
                out.insert(format!("{}{:05}", sp as char, v));
            }
            continue;
        }
        if !is_valid_param_code(&normalized) {
            bail!(
                "invalid --param: {} (expected A/Z/P + 5 digits or range like P00001~P10000)",
                raw
            );
        }
        out.insert(normalized);
    }
    Ok(out)
}

/// 返回当前 UTC 秒级时间戳。
fn now_ts() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    now.as_secs()
}

/// 导出 CSV 文件。
fn write_csv(file: &mut File, rows: &[QueryRow]) -> anyhow::Result<()> {
    file.write_all(b"ts,param_ids,values\n")?;
    for row in rows {
        let ids = row.param_ids.join("|");
        let vals = row
            .values
            .iter()
            .map(|x| format!("{x:.6}"))
            .collect::<Vec<_>>()
            .join("|");
        let line = format!("{},{},{}\n", row.ts, ids, vals);
        file.write_all(line.as_bytes())?;
    }
    Ok(())
}

/// 导出扁平化 CSV 文件（一行一个参数点）。
fn write_flat_csv(file: &mut File, rows: &[FlatRow]) -> anyhow::Result<()> {
    file.write_all(b"ts,param_id,value\n")?;
    for row in rows {
        let line = format!("{},{},{:.6}\n", row.ts, row.param_id, row.value);
        file.write_all(line.as_bytes())?;
    }
    Ok(())
}

/// 将包行查询结果扁平化为参数点结果（一参数一行）。
fn flatten_rows(rows: &[QueryRow]) -> Vec<FlatRow> {
    let mut out = Vec::new();
    for row in rows {
        let n = row.param_ids.len().min(row.values.len());
        for i in 0..n {
            out.push(FlatRow {
                ts: row.ts,
                param_id: row.param_ids[i].clone(),
                value: row.values[i],
            });
        }
    }
    out
}

/// 将可选时间戳渲染为文本。
fn opt_u64(v: Option<u64>) -> String {
    v.map(|x| x.to_string()).unwrap_or_else(|| "N/A".to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        collect_hour_dirs, flatten_rows, normalize_params, parse_day_key_arg, parse_last_window_sec, QueryRow,
    };

    /// 验证 `--last` 解析规则。
    #[test]
    fn parse_last_window_sec_should_work() {
        assert_eq!(parse_last_window_sec("30m"), Some(1800));
        assert_eq!(parse_last_window_sec("6h"), Some(21600));
        assert_eq!(parse_last_window_sec("2d"), Some(172800));
        assert_eq!(parse_last_window_sec("10x"), None);
    }

    /// 验证 `--day` 格式校验。
    #[test]
    fn parse_day_key_arg_should_work() {
        assert!(parse_day_key_arg("2026-05-20").is_ok());
        assert!(parse_day_key_arg("20260520").is_err());
    }

    /// 验证参数过滤编码校验。
    #[test]
    fn normalize_params_should_validate_code() {
        assert!(normalize_params(&["P00001".to_string(), "a00001".to_string()]).is_ok());
        assert!(normalize_params(&["C00001".to_string()]).is_err());
    }

    /// 验证参数范围写法可正确展开。
    #[test]
    fn normalize_params_should_expand_range() {
        let got = normalize_params(&["P00001~P00003".to_string()]).expect("range should be valid");
        assert_eq!(got.len(), 3);
        assert!(got.contains("P00001"));
        assert!(got.contains("P00002"));
        assert!(got.contains("P00003"));
    }

    /// 验证参数范围写法需满足同前缀与升序。
    #[test]
    fn normalize_params_should_reject_invalid_range() {
        assert!(normalize_params(&["P00003~P00001".to_string()]).is_err());
        assert!(normalize_params(&["P00001~A00003".to_string()]).is_err());
    }

    /// 验证小时目录收集在根目录不存在时返回空集合。
    #[test]
    fn collect_hour_dirs_should_return_empty_when_root_missing() {
        let dirs = collect_hour_dirs("non-exists-root", "dev001", 1_700_000_000, 1_700_000_600);
        assert!(dirs.is_empty());
    }

    /// 验证包行结果可按参数点正确展开。
    #[test]
    fn flatten_rows_should_expand_points() {
        let rows = vec![QueryRow {
            ts: 100,
            param_ids: vec!["P00001".to_string(), "P00002".to_string()],
            values: vec![1.25, 2.5],
        }];
        let flat = flatten_rows(&rows);
        assert_eq!(flat.len(), 2);
        assert_eq!(flat[0].ts, 100);
        assert_eq!(flat[0].param_id, "P00001");
        assert!((flat[0].value - 1.25).abs() < f32::EPSILON);
    }
}
