use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::{Datelike, Local, NaiveDate, TimeZone};
use clap::Args;
use redb::TableDefinition;
use serde::{Deserialize, Serialize};

const TSINDEX_FILE_NAME: &str = "tsindex.redb";
const TSINDEX_HOURLY_SEGMENTS_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("hourly_segments");

#[derive(Debug, Clone, Args)]
pub struct ReindexArgs {
    #[arg(short = 'r', long, default_value = "data/store")]
    pub root: String,

    #[arg(short = 'd', long = "device-id", visible_alias = "device")]
    pub device_id: String,

    #[arg(short = 'f', long = "from", requires = "to_ts")]
    pub from_ts: Option<u64>,

    #[arg(short = 't', long = "to", requires = "from_ts")]
    pub to_ts: Option<u64>,

    #[arg(
        short = 'D',
        long = "day",
        conflicts_with_all = ["from_ts", "to_ts", "today", "last"]
    )]
    pub day: Option<String>,

    #[arg(
        short = 'T',
        long = "today",
        default_value_t = false,
        conflicts_with_all = ["from_ts", "to_ts", "day", "last"]
    )]
    pub today: bool,

    #[arg(
        short = 'l',
        long = "last",
        conflicts_with_all = ["from_ts", "to_ts", "day", "today"]
    )]
    pub last: Option<String>,

    #[arg(
        short = 'a',
        long = "all",
        default_value_t = false,
        conflicts_with_all = ["from_ts", "to_ts", "day", "today", "last"]
    )]
    pub all: bool,

    #[arg(long, default_value_t = false)]
    pub backup: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct SegmentManifestEntry {
    segment_file: String,
    min_ts: u64,
    max_ts: u64,
    rows: u64,
    #[serde(default)]
    created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
struct IndexSegmentEntry {
    segment_file: String,
    min_ts: u64,
    max_ts: u64,
    rows: u64,
}

pub fn run(args: ReindexArgs) -> anyhow::Result<()> {
    let (from_ts, to_ts) = resolve_time_range(&args)?;
    let hour_dirs = collect_hour_dirs(&args.root, &args.device_id, from_ts, to_ts)?;
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
        let key = hourly_index_key(&args.device_id, &day_key, hour);
        let payload = serde_json::to_string(&entries)?;
        hour_payloads.push((key, payload));
    }

    hour_payloads.sort_by(|a, b| a.0.cmp(&b.0));
    let db_dir = PathBuf::from(&args.root).join("_index");
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

    println!("reindex_root: {}", args.root);
    println!("reindex_device_id: {}", args.device_id);
    println!("reindex_from_ts: {}", from_ts);
    println!("reindex_to_ts: {}", to_ts);
    println!("reindex_manifest_files: {}", manifest_files);
    println!("reindex_hour_keys: {}", hour_payloads.len());
    println!("reindex_segment_rows: {}", indexed_rows);
    println!("reindex_db: {}", db_path.display());
    Ok(())
}

fn resolve_time_range(args: &ReindexArgs) -> anyhow::Result<(u64, u64)> {
    if let (Some(from), Some(to)) = (args.from_ts, args.to_ts) {
        if from > to {
            anyhow::bail!("--from must be <= --to");
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
    anyhow::bail!(
        "must provide one time range mode: (--from and --to) | --day | --today | --last | --all"
    )
}

fn day_ts_bounds(day: &str) -> anyhow::Result<(u64, u64)> {
    let date = chrono::NaiveDate::parse_from_str(day, "%Y-%m-%d")
        .with_context(|| format!("invalid day: {day}, expected YYYY-MM-DD"))?
        .and_hms_opt(0, 0, 0)
        .context("build day start failed")?;
    let start_dt = Local
        .from_local_datetime(&date)
        .single()
        .context("invalid local day start")?;
    let start =
        u64::try_from(start_dt.timestamp()).context("negative timestamp unsupported")?;
    Ok((start, start.saturating_add(86_399)))
}

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

fn now_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn collect_hour_dirs(
    root: &str,
    device_id: &str,
    from_ts: u64,
    to_ts: u64,
) -> anyhow::Result<Vec<PathBuf>> {
    if from_ts > to_ts {
        return Ok(Vec::new());
    }
    let device_dir = PathBuf::from(root).join(device_id);
    let mut out = Vec::new();
    if !device_dir.exists() {
        return Ok(out);
    }
    for day_entry in std::fs::read_dir(&device_dir)?.flatten() {
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
        for hour_entry in std::fs::read_dir(&day_path)?.flatten() {
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
            out.push(hour_path);
        }
    }
    out.sort();
    Ok(out)
}

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

fn hourly_index_key(device_id: &str, day_key: &str, hour: u32) -> String {
    format!("{device_id}|{day_key}|{hour:02}")
}
