use std::path::PathBuf;
use anyhow::Context;
use chrono::{Datelike, Local, NaiveDate, TimeZone};
use clap::Args;
use std::sync::Arc;

use crate::query::datafusion_executor::QueryExecutor as DfExecutor;

#[derive(Debug, Clone, Args)]
pub struct StatArgs {
    #[arg(short = 'r', long, default_value = "data/store")]
    pub root: String,

    #[arg(short = 'd', long = "device-id", visible_alias = "device")]
    pub device_id: String,

    #[arg(short = 'f', long = "from", requires = "to_ts")]
    pub from_ts: Option<u64>,

    #[arg(short = 't', long = "to", requires = "from_ts")]
    pub to_ts: Option<u64>,

    #[arg(short = 'D', long = "day", conflicts_with_all = ["from_ts", "to_ts", "today", "last"])]
    pub day: Option<String>,

    #[arg(short = 'T', long = "today", default_value_t = false, conflicts_with_all = ["from_ts", "to_ts", "day", "last"])]
    pub today: bool,

    #[arg(short = 'l', long = "last", conflicts_with_all = ["from_ts", "to_ts", "day", "today"])]
    pub last: Option<String>,

    #[arg(short = 'a', long = "all", default_value_t = false, conflicts_with_all = ["from_ts", "to_ts", "day", "today", "last"])]
    pub all: bool,
}

#[derive(Debug, Default)]
struct Stats {
    files: usize,
    rows: u64,
    points: u64,
    min_ts: Option<u64>,
    max_ts: Option<u64>,
}

pub fn run(args: StatArgs) -> anyhow::Result<()> {
    let (from_ts, to_ts) = resolve_time_range(&args)?;
    let root = PathBuf::from(&args.root);
    let device_dir = root.join(&args.device_id);

    let hour_dirs = collect_hour_dirs(&device_dir, from_ts, to_ts)?;
    let mut stats = Stats {
        files: 0,
        rows: 0,
        points: 0,
        min_ts: None,
        max_ts: None,
    };

    let executor = DfExecutor::new(args.root.clone());

    for hour_dir in &hour_dirs {
        let parquet_files = collect_parquet_files(hour_dir)?;
        stats.files += parquet_files.len();

        for file in &parquet_files {
            match read_parquet_stats(&executor, file, from_ts, to_ts) {
                Ok(file_stats) => {
                    stats.rows += file_stats.rows;
                    stats.points += file_stats.points;
                    stats.min_ts = Some(stats.min_ts.map_or(file_stats.min_ts, |v| v.min(file_stats.min_ts)));
                    stats.max_ts = Some(stats.max_ts.map_or(file_stats.max_ts, |v| v.max(file_stats.max_ts)));
                }
                Err(e) => {
                    eprintln!("warn: skip unreadable file {}: {}", file.display(), e);
                }
            }
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

fn opt_u64(v: Option<u64>) -> String {
    v.map(|v| v.to_string()).unwrap_or_default()
}

fn resolve_time_range(args: &StatArgs) -> anyhow::Result<(u64, u64)> {
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
    anyhow::bail!("must provide one time range mode: (--from and --to) | --day | --today | --last | --all")
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
    let start = u64::try_from(start_dt.timestamp()).context("negative timestamp unsupported")?;
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

fn collect_hour_dirs(device_dir: &PathBuf, from_ts: u64, to_ts: u64) -> anyhow::Result<Vec<PathBuf>> {
    if from_ts > to_ts {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    if !device_dir.exists() {
        return Ok(out);
    }
    for day_entry in std::fs::read_dir(device_dir)?.flatten() {
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

fn collect_parquet_files(hour_dir: &PathBuf) -> anyhow::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !hour_dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(hour_dir)?.flatten() {
        let path = entry.path();
        if path.extension().and_then(|x| x.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
    Ok(out)
}

struct FileStats {
    rows: u64,
    points: u64,
    min_ts: u64,
    max_ts: u64,
}

fn read_parquet_stats(executor: &DfExecutor, path: &PathBuf, from_ts: u64, to_ts: u64) -> anyhow::Result<FileStats> {
    let points = executor.query(
        path.parent().unwrap().file_name().unwrap().to_str().unwrap(),
        from_ts,
        to_ts,
        &[],
    )?;

    let rows = points.len() as u64;
    let min_ts = points.iter().map(|p| p.ts).min().unwrap_or(0);
    let max_ts = points.iter().map(|p| p.ts).max().unwrap_or(0);

    Ok(FileStats {
        rows,
        points: rows,
        min_ts,
        max_ts,
    })
}
