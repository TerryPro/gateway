use std::path::PathBuf;
use std::time::Instant;
use anyhow::Context;
use chrono::{Datelike, Local, NaiveDate, TimeZone};
use clap::Args;

use crate::query::datafusion_executor::QueryExecutor as DfExecutor;

#[derive(Debug, Clone, Args)]
pub struct PerfArgs {
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

    #[arg(short = 'p', long = "param")]
    pub params: Vec<String>,

    #[arg(short = 'n', long, default_value_t = 200)]
    pub limit: usize,

    #[arg(long, default_value_t = 20)]
    pub iterations: usize,

    #[arg(long, default_value_t = 3)]
    pub warmup: usize,
}

pub async fn run(args: PerfArgs) -> anyhow::Result<()> {
    if args.iterations == 0 {
        anyhow::bail!("--iterations must be > 0");
    }

    let (from_ts, to_ts) = resolve_time_range(&args)?;

    println!("perf_root: {}", args.root);
    println!("perf_device_id: {}", args.device_id);
    println!("perf_params: {}", args.params.len());
    println!("perf_limit: {}", args.limit);
    println!("perf_from_ts: {}", from_ts);
    println!("perf_to_ts: {}", to_ts);
    println!("perf_warmup: {}", args.warmup);
    println!("perf_iterations: {}", args.iterations);

    let executor = DfExecutor::new(args.root.clone());

    for _ in 0..args.warmup {
        let _ = executor.query_async(&args.device_id, from_ts, to_ts, &args.params).await;
    }

    let mut costs_ms = Vec::with_capacity(args.iterations);
    let mut matched = 0usize;
    for _ in 0..args.iterations {
        let begin = Instant::now();
        matched = executor.query_async(&args.device_id, from_ts, to_ts, &args.params).await?.len();
        let elapsed = begin.elapsed();
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

    println!("perf_last_matched: {}", matched);
    println!("perf_min_ms: {:.3}", min_ms);
    println!("perf_avg_ms: {:.3}", avg_ms);
    println!("perf_p95_ms: {:.3}", p95_ms);
    println!("perf_max_ms: {:.3}", max_ms);

    Ok(())
}

fn resolve_time_range(args: &PerfArgs) -> anyhow::Result<(u64, u64)> {
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
