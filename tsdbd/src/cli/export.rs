use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use anyhow::Context;
use chrono::{Datelike, Local, NaiveDate, TimeZone};
use clap::{Args, ValueEnum};
use serde::Serialize;

use crate::model::DataPoint;
use crate::query::datafusion_executor::QueryExecutor as DfExecutor;

#[derive(Debug, Clone, ValueEnum)]
pub enum ExportFormat {
    Csv,
    Json,
}

#[derive(Debug, Clone, Args)]
pub struct ExportArgs {
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

    #[arg(short = 'n', long, default_value_t = 1000)]
    pub limit: usize,

    #[arg(short = 'p', long = "param")]
    pub params: Vec<String>,

    #[arg(short = 'o', long)]
    pub out: PathBuf,

    #[arg(short = 'F', long, value_enum, default_value_t = ExportFormat::Json)]
    pub format: ExportFormat,

    #[arg(long, default_value_t = false)]
    pub flat: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct FlatRow {
    ts: u64,
    param_id: String,
    value: f32,
}

pub fn run(args: ExportArgs) -> anyhow::Result<()> {
    let (from_ts, to_ts) = resolve_time_range(&args)?;

    let executor = DfExecutor::new(args.root.clone());
    let results = executor.query(&args.device_id, from_ts, to_ts, &args.params)?;

    let results: Vec<FlatRow> = results
        .into_iter()
        .take(args.limit)
        .map(|p| FlatRow {
            ts: p.ts,
            param_id: p.param_id,
            value: p.value,
        })
        .collect();

    let exported_count = results.len();
    let mut file = File::create(&args.out)
        .with_context(|| format!("create output failed: {}", args.out.display()))?;

    match args.format {
        ExportFormat::Csv => write_csv(&mut file, &results)?,
        ExportFormat::Json => serde_json::to_writer_pretty(&mut file, &results)?,
    }
    file.flush()?;
    println!("exported {} rows to {}", exported_count, args.out.display());
    Ok(())
}

fn resolve_time_range(args: &ExportArgs) -> anyhow::Result<(u64, u64)> {
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

fn write_csv(file: &mut File, rows: &[FlatRow]) -> anyhow::Result<()> {
    writeln!(file, "ts,param_id,value")?;
    for row in rows {
        writeln!(file, "{},{},{:.6}", row.ts, row.param_id, row.value)?;
    }
    Ok(())
}
