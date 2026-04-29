use std::{
    collections::BTreeSet,
    fs::File,
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use arrow::array::{Array, Float32Array, ListArray, StringArray, UInt64Array};
use chrono::{DateTime, Datelike, Timelike, Utc};
use common::tsmeta::is_valid_param_code;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use redb::TableDefinition;
use serde::{Deserialize, Serialize};

const TSINDEX_FILE_NAME: &str = "tsindex.redb";
const TSINDEX_HOURLY_SEGMENTS_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("hourly_segments");

/// 查询请求参数：按设备、参数和时间范围查询历史值。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TsQueryRequest {
    device_id: String,
    param_id: String,
    from_ts: u64,
    to_ts: u64,
    limit: Option<usize>,
    offset: Option<usize>,
    root: Option<String>,
}

/// 单条查询结果：包含秒级时间戳与参数值。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct TsPointRow {
    ts: u64,
    value: f32,
}

/// 查询响应：返回总量、分页结果和最终使用的数据根目录。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct TsQueryResponse {
    total: usize,
    rows: Vec<TsPointRow>,
    root: String,
}

/// 索引分段条目定义，结构与 `tsd` 使用的 redb payload 一致。
#[derive(Debug, Clone, Deserialize)]
struct IndexSegmentEntry {
    segment_file: String,
    min_ts: u64,
    max_ts: u64,
    rows: u64,
    #[serde(default)]
    param_ids: Vec<String>,
}

/// `manifest.jsonl` 中的分段条目定义。
#[derive(Debug, Clone, Deserialize)]
struct SegmentManifestEntry {
    segment_file: String,
    min_ts: u64,
    max_ts: u64,
    rows: u64,
    #[serde(default)]
    param_ids: Vec<String>,
}

/// Tauri 查询命令：直连 `tsdata`，返回指定参数在时间段内的值。
#[tauri::command]
fn query_param_history(req: TsQueryRequest) -> Result<TsQueryResponse, String> {
    run_query_param_history(req).map_err(|e| e.to_string())
}

/// 示例命令，保留用于基础连通性验证。
#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

/// 执行历史参数查询主流程，负责参数校验、文件筛选与分页。
fn run_query_param_history(req: TsQueryRequest) -> anyhow::Result<TsQueryResponse> {
    if req.device_id.trim().is_empty() {
        bail!("device_id 不能为空");
    }
    let param_id = req.param_id.trim().to_ascii_uppercase();
    if !is_valid_param_code(&param_id) {
        bail!("param_id 格式非法，期望 A/Z/P + 5位数字");
    }
    if req.from_ts > req.to_ts {
        bail!("from_ts 必须小于等于 to_ts");
    }
    let limit = req.limit.unwrap_or(200).clamp(1, 5_000);
    let offset = req.offset.unwrap_or(0);
    let root = resolve_tsdata_root(req.root.as_deref())?;
    let files = collect_candidate_files(&root, &req.device_id, req.from_ts, req.to_ts, &param_id)?;
    let mut rows = Vec::new();
    for file in files {
        let mut one = read_points_from_file(&file, req.from_ts, req.to_ts, &param_id)?;
        rows.append(&mut one);
    }
    rows.sort_by_key(|x| x.ts);
    let total = rows.len();
    let paged = rows.into_iter().skip(offset).take(limit).collect::<Vec<_>>();
    Ok(TsQueryResponse {
        total,
        rows: paged,
        root: root.display().to_string(),
    })
}

/// 解析并定位 `tsdata` 根目录，支持显式传参与常见相对路径自动探测。
fn resolve_tsdata_root(raw_root: Option<&str>) -> anyhow::Result<PathBuf> {
    if let Some(root) = raw_root {
        let path = PathBuf::from(root);
        if path.exists() {
            return Ok(path);
        }
        bail!("指定 root 不存在: {}", path.display());
    }
    let current = std::env::current_dir().context("读取当前目录失败")?;
    let candidates = [
        current.join("tsdata"),
        current.join("..").join("tsdata"),
        current.join("..").join("..").join("tsdata"),
    ];
    for path in candidates {
        if path.exists() {
            return Ok(path);
        }
    }
    bail!("未找到 tsdata 目录，请在查询参数中显式传入 root");
}

/// 收集时间范围内与参数匹配的候选 parquet 文件（优先 redb 索引，失败回退 manifest）。
fn collect_candidate_files(
    root: &Path,
    device_id: &str,
    from_ts: u64,
    to_ts: u64,
    param_id: &str,
) -> anyhow::Result<Vec<PathBuf>> {
    let mut set = BTreeSet::new();
    let mut ts = from_ts.saturating_sub(from_ts % 3600);
    while ts <= to_ts {
        let hour_dir = build_hour_dir(root, device_id, ts);
        if hour_dir.exists() {
            let from_index =
                collect_from_redb_index(root, device_id, &hour_dir, from_ts, to_ts, param_id);
            let from_index = match from_index {
                Ok(v) if !v.is_empty() => v,
                _ => collect_from_manifest(&hour_dir, from_ts, to_ts, param_id)?,
            };
            for file in from_index {
                set.insert(file);
            }
        }
        ts = ts.saturating_add(3600);
        if ts == u64::MAX {
            break;
        }
    }
    Ok(set.into_iter().collect())
}

/// 从 redb 小时索引读取分段文件，按时间范围和参数编号过滤。
fn collect_from_redb_index(
    root: &Path,
    device_id: &str,
    hour_dir: &Path,
    from_ts: u64,
    to_ts: u64,
    param_id: &str,
) -> anyhow::Result<Vec<PathBuf>> {
    let db_path = root.join("_index").join(TSINDEX_FILE_NAME);
    if !db_path.exists() {
        return Ok(Vec::new());
    }
    let (day_key, hour) = parse_hour_dir(hour_dir)?;
    let db = redb::Database::open(&db_path)
        .with_context(|| format!("打开索引库失败: {}", db_path.display()))?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TSINDEX_HOURLY_SEGMENTS_TABLE)?;
    let key = format!("{device_id}|{day_key}|{hour:02}");
    let Some(raw) = table.get(key.as_str())? else {
        return Ok(Vec::new());
    };
    let entries = serde_json::from_str::<Vec<IndexSegmentEntry>>(raw.value()).unwrap_or_default();
    let mut out = Vec::new();
    for entry in entries {
        if entry.rows == 0 {
            continue;
        }
        if entry.max_ts < from_ts || entry.min_ts > to_ts {
            continue;
        }
        if !entry.param_ids.is_empty() && !entry.param_ids.iter().any(|id| id == param_id) {
            continue;
        }
        out.push(hour_dir.join(entry.segment_file));
    }
    Ok(out)
}

/// 从 `manifest.jsonl` 收集候选分段文件，按时间和参数集合进行过滤。
fn collect_from_manifest(
    hour_dir: &Path,
    from_ts: u64,
    to_ts: u64,
    param_id: &str,
) -> anyhow::Result<Vec<PathBuf>> {
    let manifest = hour_dir.join("manifest.jsonl");
    if !manifest.exists() {
        return Ok(Vec::new());
    }
    let text = std::fs::read_to_string(&manifest)
        .with_context(|| format!("读取 manifest 失败: {}", manifest.display()))?;
    let mut out = Vec::new();
    for line in text.lines() {
        let row = line.trim();
        if row.is_empty() {
            continue;
        }
        let Ok(entry) = serde_json::from_str::<SegmentManifestEntry>(row) else {
            continue;
        };
        if entry.rows == 0 {
            continue;
        }
        if entry.max_ts < from_ts || entry.min_ts > to_ts {
            continue;
        }
        if !entry.param_ids.is_empty() && !entry.param_ids.iter().any(|id| id == param_id) {
            continue;
        }
        out.push(hour_dir.join(entry.segment_file));
    }
    Ok(out)
}

/// 读取单个 parquet 文件，提取指定参数在时间范围内的点值。
fn read_points_from_file(
    file: &Path,
    from_ts: u64,
    to_ts: u64,
    param_id: &str,
) -> anyhow::Result<Vec<TsPointRow>> {
    let reader = File::open(file).with_context(|| format!("打开 parquet 失败: {}", file.display()))?;
    let batch_reader = ParquetRecordBatchReaderBuilder::try_new(reader)?.build()?;
    let mut out = Vec::new();
    for batch in batch_reader {
        let batch = batch?;
        let ts_arr = batch
            .column_by_name("ts")
            .context("缺少列 ts")?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("列 ts 类型错误")?;
        let param_list = batch
            .column_by_name("param_ids")
            .context("缺少列 param_ids")?
            .as_any()
            .downcast_ref::<ListArray>()
            .context("列 param_ids 类型错误")?;
        let values_list = batch
            .column_by_name("values")
            .context("缺少列 values")?
            .as_any()
            .downcast_ref::<ListArray>()
            .context("列 values 类型错误")?;
        for i in 0..batch.num_rows() {
            let ts = ts_arr.value(i);
            if ts < from_ts || ts > to_ts {
                continue;
            }
            let param_values = param_list.value(i);
            let param_arr = param_values
                .as_any()
                .downcast_ref::<StringArray>()
                .context("param_ids 子类型错误")?;
            let value_values = values_list.value(i);
            let value_arr = value_values
                .as_any()
                .downcast_ref::<Float32Array>()
                .context("values 子类型错误")?;
            if param_arr.len() != value_arr.len() {
                continue;
            }
            for j in 0..param_arr.len() {
                if param_arr.value(j) == param_id {
                    out.push(TsPointRow {
                        ts,
                        value: value_arr.value(j),
                    });
                }
            }
        }
    }
    Ok(out)
}

/// 构建小时目录路径：`root/device_id/YYYY-MM-DD/HH`。
fn build_hour_dir(root: &Path, device_id: &str, ts: u64) -> PathBuf {
    let dt: DateTime<Utc> =
        DateTime::from_timestamp(ts as i64, 0).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    root.join(device_id)
        .join(format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day()))
        .join(format!("{:02}", dt.hour()))
}

/// 从小时目录中解析出 `day_key` 和小时值。
fn parse_hour_dir(hour_dir: &Path) -> anyhow::Result<(String, u32)> {
    let hour_raw = hour_dir
        .file_name()
        .and_then(|x| x.to_str())
        .context("小时目录名称非法")?;
    let day_key = hour_dir
        .parent()
        .and_then(Path::file_name)
        .and_then(|x| x.to_str())
        .context("日期目录名称非法")?
        .to_string();
    let hour = hour_raw
        .parse::<u32>()
        .with_context(|| format!("小时目录解析失败: {hour_raw}"))?;
    Ok((day_key, hour))
}

/// 启动 Tauri 应用并注册后端命令。
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![greet, query_param_history])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
