use std::collections::HashMap;
use std::path::PathBuf;
use anyhow::Context;
use chrono::{Datelike, Local, NaiveDate, TimeZone};
use clap::Args;
use serde::Deserialize;

use crate::model::DataPoint;

#[derive(Debug, Clone, Args)]
pub struct DoctorArgs {
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
struct DoctorStats {
    hour_dirs: usize,
    manifest_files: usize,
    manifest_entries: usize,
    missing_manifest: usize,
    missing_segment_files: usize,
    bad_manifest_lines: usize,
    invalid_entries: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct SegmentManifestEntry {
    segment_file: String,
    min_ts: u64,
    max_ts: u64,
    rows: u64,
    created_at_ms: u64,
}

pub fn run(args: DoctorArgs) -> anyhow::Result<()> {
    let (from_ts, to_ts) = resolve_time_range(&args)?;
    let root = PathBuf::from(&args.root);
    let device_dir = root.join(&args.device_id);

    let hour_dirs = collect_hour_dirs(&device_dir, from_ts, to_ts)?;
    let mut stats = DoctorStats {
        hour_dirs: hour_dirs.len(),
        ..DoctorStats::default()
    };
    let mut issues = Vec::new();

    for hour_dir in &hour_dirs {
        let manifest = hour_dir.join("manifest.jsonl");
        if !manifest.exists() {
            stats.missing_manifest += 1;
            issues.push(format!("missing manifest: {}", manifest.display()));
            continue;
        }
        stats.manifest_files += 1;

        let text = std::fs::read_to_string(&manifest)
            .with_context(|| format!("read manifest failed: {}", manifest.display()))?;
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
    println!("issues: {}", issues.len());
    for issue in issues.iter().take(30) {
        println!("  - {}", issue);
    }
    if issues.len() > 30 {
        println!("  ... ({} more issues)", issues.len() - 30);
    }
    Ok(())
}

fn resolve_time_range(args: &DoctorArgs) -> anyhow::Result<(u64, u64)> {
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
