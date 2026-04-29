use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, bail};
use arrow::array::{BinaryArray, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, Local};
use clap::{Args, Parser, Subcommand, ValueEnum};
use common::archive::{
    ARCHIVE_SCHEMA_VERSION, MANIFEST_FILE_NAME, ManifestEntry, build_day_dir, day_ts_bounds_ms,
    collect_manifest_candidates, parse_parquet_file_name,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;

/// `arv` 命令行入口参数。
#[derive(Debug, Clone, Parser)]
#[command(name = "arv", version, about = "归档查询分析工具")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// `arv` 子命令集合。
#[derive(Debug, Clone, Subcommand)]
enum Command {
    Query(QueryArgs),
    Stat(CommonArgs),
    Verify(CommonArgs),
    Export(ExportArgs),
    RebuildManifest(RebuildManifestArgs),
    Repl(ReplArgs),
}

/// 公共参数，统一定义根目录、设备和时间范围。
#[derive(Debug, Clone, Args)]
struct CommonArgs {
    #[arg(short = 'r', long, default_value = "data")]
    root: String,
    #[arg(short = 'd', long = "device-id", visible_alias = "device", visible_alias = "dev")]
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
}

/// 查询参数。
#[derive(Debug, Clone, Args)]
struct QueryArgs {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(short = 'n', long, default_value_t = 200)]
    limit: usize,
    #[arg(short = 'p', long)]
    show_payload: bool,
    #[arg(long = "payload-format", value_enum, default_value_t = PayloadFormat::Hex)]
    payload_format: PayloadFormat,
}

/// 导出格式。
#[derive(Debug, Clone, Copy, ValueEnum)]
enum ExportFormat {
    Csv,
    Json,
}

/// 载荷显示格式。
#[derive(Debug, Clone, Copy, ValueEnum)]
enum PayloadFormat {
    Hex,
    Ascii,
}

/// 导出参数。
#[derive(Debug, Clone, Args)]
struct ExportArgs {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(short = 'n', long, default_value_t = 200)]
    limit: usize,
    #[arg(short = 'o', long)]
    out: PathBuf,
    #[arg(short = 'F', long, value_enum, default_value_t = ExportFormat::Json)]
    format: ExportFormat,
}

/// 重建清单参数。
#[derive(Debug, Clone, Args)]
struct RebuildManifestArgs {
    #[arg(short = 'r', long, default_value = "data")]
    root: String,
    #[arg(short = 'd', long = "device-id", visible_alias = "device", visible_alias = "dev")]
    device_id: String,
    #[arg(short = 'D', long, value_parser = parse_day_key_arg)]
    day: String,
}

/// 交互式命令行参数。
#[derive(Debug, Clone, Args)]
struct ReplArgs {
    #[arg(short = 'r', long, default_value = "data")]
    root: String,
    #[arg(short = 'd', long = "device-id", visible_alias = "device", visible_alias = "dev")]
    device_id: Option<String>,
}

/// 查询输出记录。
#[derive(Debug, Clone, Serialize)]
struct QueryRow {
    ts_ms: u64,
    device_id: String,
    payload_len: usize,
    payload_hex: String,
}

/// 统计输出结构。
#[derive(Debug, Clone)]
struct Stats {
    files: usize,
    records: usize,
    min_ts_ms: Option<u64>,
    max_ts_ms: Option<u64>,
    payload_bytes: u64,
}

/// 校验输出结构。
#[derive(Debug, Clone)]
struct VerifyStats {
    files: usize,
    records: usize,
    failed: usize,
}

/// 程序入口，负责解析参数并分发子命令。
fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    validate_common_args_if_needed(&cli.command)?;
    match cli.command {
        Command::Query(args) => run_query(args),
        Command::Stat(args) => run_stat(args),
        Command::Verify(args) => run_verify(args),
        Command::Export(args) => run_export(args),
        Command::RebuildManifest(args) => run_rebuild_manifest(args),
        Command::Repl(args) => run_repl(args),
    }
}

/// 校验 `YYYYMMDD` 参数格式是否合法。
fn parse_day_key_arg(value: &str) -> Result<String, String> {
    if value.len() == 8 && value.bytes().all(|c| c.is_ascii_digit()) {
        return Ok(value.to_string());
    }
    Err(format!("invalid --day: {value} (expected YYYYMMDD)"))
}

/// 校验 `--last` 窗口参数格式是否合法（如 `30m`、`6h`、`2d`）。
fn parse_last_window_arg(value: &str) -> Result<String, String> {
    if parse_last_window_ms(value).is_some() {
        return Ok(value.to_string());
    }
    Err(format!(
        "invalid --last: {value} (expected <num>[s|m|h|d], e.g. 30m, 6h)"
    ))
}

/// 将 `--last` 文本窗口解析为毫秒。
fn parse_last_window_ms(value: &str) -> Option<u64> {
    if value.len() < 2 {
        return None;
    }
    let (num, unit) = value.split_at(value.len() - 1);
    let n = num.parse::<u64>().ok()?;
    let unit_ms = match unit {
        "s" | "S" => 1_000_u64,
        "m" | "M" => 60_000_u64,
        "h" | "H" => 3_600_000_u64,
        "d" | "D" => 86_400_000_u64,
        _ => return None,
    };
    n.checked_mul(unit_ms)
}

/// 返回当前 UTC 毫秒时间戳。
fn now_ts_ms() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    now.as_millis() as u64
}

/// 将通用时间参数解析为 `[from_ts, to_ts]` 毫秒范围。
fn resolve_time_range(args: &CommonArgs) -> anyhow::Result<(u64, u64)> {
    if let (Some(from), Some(to)) = (args.from_ts, args.to_ts) {
        if from > to {
            bail!("--from must be <= --to");
        }
        return Ok((from, to));
    }
    if let Some(day) = &args.day {
        return day_ts_bounds_ms(day);
    }
    if args.today {
        let now = Local::now();
        let day_key = format!("{:04}{:02}{:02}", now.year(), now.month(), now.day());
        return day_ts_bounds_ms(&day_key);
    }
    if let Some(last) = &args.last {
        let window_ms = parse_last_window_ms(last).context("invalid --last")?;
        let to = now_ts_ms();
        let from = to.saturating_sub(window_ms);
        return Ok((from, to));
    }
    bail!("must provide one time range mode: (--from and --to) | --day | --today | --last");
}

/// 校验命中 `CommonArgs` 的子命令参数一致性。
fn validate_common_args_if_needed(command: &Command) -> anyhow::Result<()> {
    let common = match command {
        Command::Query(v) => Some(&v.common),
        Command::Stat(v) => Some(v),
        Command::Verify(v) => Some(v),
        Command::Export(v) => Some(&v.common),
        Command::RebuildManifest(_) | Command::Repl(_) => None,
    };
    if let Some(c) = common {
        let _ = resolve_time_range(c)?;
    }
    Ok(())
}

/// REPL 时间上下文模式。
#[derive(Debug, Clone)]
enum ReplTimeMode {
    Today,
    Day(String),
    Last(String),
    Range(u64, u64),
}

/// REPL 会话上下文，保存默认查询参数。
#[derive(Debug, Clone)]
struct ReplContext {
    root: String,
    device_id: Option<String>,
    time_mode: ReplTimeMode,
    render_mode: ReplRenderMode,
    timing_on: bool,
}

/// REPL 执行结果动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplAction {
    Continue,
    Exit,
}

/// REPL 查询结果渲染模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplRenderMode {
    Table,
    Expanded,
}

/// 启动交互式查询模式，支持上下文设置与快速查询。
fn run_repl(args: ReplArgs) -> anyhow::Result<()> {
    let mut ctx = ReplContext {
        root: args.root,
        device_id: args.device_id,
        time_mode: ReplTimeMode::Today,
        render_mode: ReplRenderMode::Table,
        timing_on: false,
    };
    print_repl_banner();
    let stdin = io::stdin();
    loop {
        print!("arv> ");
        io::stdout().flush()?;
        let mut line = String::new();
        if stdin.read_line(&mut line)? == 0 {
            println!();
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match execute_repl_line(line, &mut ctx) {
            Ok(ReplAction::Continue) => {}
            Ok(ReplAction::Exit) => break,
            Err(e) => eprintln!("error: {e}"),
        }
    }
    Ok(())
}

/// 执行单条 REPL 命令行。
fn execute_repl_line(line: &str, ctx: &mut ReplContext) -> anyhow::Result<ReplAction> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(ReplAction::Continue);
    }
    let cmd = parts[0].to_ascii_lowercase();
    match cmd.as_str() {
        "help" | "?" => {
            print_repl_help();
            Ok(ReplAction::Continue)
        }
        "\\x" => {
            apply_repl_expanded_command(&parts[1..], ctx)?;
            print_repl_context(ctx);
            Ok(ReplAction::Continue)
        }
        "\\timing" => {
            apply_repl_timing_command(&parts[1..], ctx)?;
            print_repl_context(ctx);
            Ok(ReplAction::Continue)
        }
        "exit" | "quit" | "q" => Ok(ReplAction::Exit),
        "show" => {
            print_repl_context(ctx);
            Ok(ReplAction::Continue)
        }
        "use" => {
            let Some(device_id) = parts.get(1) else {
                bail!("usage: use <device_id>");
            };
            ctx.device_id = Some((*device_id).to_string());
            println!("device_id={device_id}");
            Ok(ReplAction::Continue)
        }
        "root" => {
            let Some(root) = parts.get(1) else {
                bail!("usage: root <path>");
            };
            ctx.root = (*root).to_string();
            println!("root={root}");
            Ok(ReplAction::Continue)
        }
        "time" => {
            apply_repl_time_command(&parts[1..], ctx)?;
            print_repl_context(ctx);
            Ok(ReplAction::Continue)
        }
        "stat" => {
            let common = build_common_args_from_ctx(ctx)?;
            run_stat(common)?;
            Ok(ReplAction::Continue)
        }
        "verify" => {
            let common = build_common_args_from_ctx(ctx)?;
            run_verify(common)?;
            Ok(ReplAction::Continue)
        }
        "query" => {
            let start = Instant::now();
            let q = parse_repl_query_args(&parts[1..], build_common_args_from_ctx(ctx)?)?;
            let rows = collect_rows(
                &q.common,
                Some(q.limit),
                q.show_payload,
                q.payload_format,
                Some(q.common.device_id.clone()),
            )?;
            render_repl_query_rows(&rows, q.show_payload, q.payload_format, ctx.render_mode);
            print_repl_query_footer(&q.common, rows.len(), start.elapsed(), ctx.timing_on)?;
            Ok(ReplAction::Continue)
        }
        "export" => {
            let e = parse_repl_export_args(&parts[1..], build_common_args_from_ctx(ctx)?)?;
            run_export(e)?;
            Ok(ReplAction::Continue)
        }
        "rebuild-manifest" => {
            let day = parts.get(1).context("usage: rebuild-manifest <YYYYMMDD>")?;
            let device_id = ctx
                .device_id
                .clone()
                .context("device_id not set, use: use <device_id>")?;
            let args = RebuildManifestArgs {
                root: ctx.root.clone(),
                device_id,
                day: parse_day_key_arg(day).map_err(anyhow::Error::msg)?,
            };
            run_rebuild_manifest(args)?;
            Ok(ReplAction::Continue)
        }
        _ => {
            bail!("unknown command: {cmd}. try `help`");
        }
    }
}

/// 处理 REPL `\x` 命令，用于切换扩展输出模式。
fn apply_repl_expanded_command(parts: &[&str], ctx: &mut ReplContext) -> anyhow::Result<()> {
    match parts.first().map(|x| x.to_ascii_lowercase()) {
        None => {
            ctx.render_mode = match ctx.render_mode {
                ReplRenderMode::Table => ReplRenderMode::Expanded,
                ReplRenderMode::Expanded => ReplRenderMode::Table,
            };
            Ok(())
        }
        Some(v) if v == "on" => {
            ctx.render_mode = ReplRenderMode::Expanded;
            Ok(())
        }
        Some(v) if v == "off" => {
            ctx.render_mode = ReplRenderMode::Table;
            Ok(())
        }
        _ => bail!(r"usage: \x [on|off]"),
    }
}

/// 处理 REPL `\timing` 命令，用于切换耗时显示。
fn apply_repl_timing_command(parts: &[&str], ctx: &mut ReplContext) -> anyhow::Result<()> {
    match parts.first().map(|x| x.to_ascii_lowercase()) {
        None => {
            ctx.timing_on = !ctx.timing_on;
            Ok(())
        }
        Some(v) if v == "on" => {
            ctx.timing_on = true;
            Ok(())
        }
        Some(v) if v == "off" => {
            ctx.timing_on = false;
            Ok(())
        }
        _ => bail!(r"usage: \timing [on|off]"),
    }
}

/// 渲染 REPL 查询结果，支持表格与扩展两种输出模式。
fn render_repl_query_rows(
    rows: &[QueryRow],
    show_payload: bool,
    payload_format: PayloadFormat,
    render_mode: ReplRenderMode,
) {
    match render_mode {
        ReplRenderMode::Table => render_repl_query_rows_table(rows, show_payload),
        ReplRenderMode::Expanded => render_repl_query_rows_expanded(rows, show_payload, payload_format),
    }
}

/// 以表格模式渲染查询结果。
fn render_repl_query_rows_table(rows: &[QueryRow], show_payload: bool) {
    const PAYLOAD_MAX_CHARS: usize = 64;
    let mut headers = vec!["ts_ms", "device_id", "payload_len"];
    if show_payload {
        headers.push("payload");
    }
    let mut widths = vec![headers[0].len(), headers[1].len(), headers[2].len()];
    if show_payload {
        widths.push(headers[3].len());
    }
    for row in rows {
        widths[0] = widths[0].max(row.ts_ms.to_string().len());
        widths[1] = widths[1].max(row.device_id.len());
        widths[2] = widths[2].max(row.payload_len.to_string().len());
        if show_payload {
            let payload = truncate_for_table(&row.payload_hex, PAYLOAD_MAX_CHARS);
            widths[3] = widths[3].max(payload.len());
        }
    }
    print_table_border(&widths);
    print_table_row(&headers, &widths);
    print_table_border(&widths);
    for row in rows {
        let ts = row.ts_ms.to_string();
        let len = row.payload_len.to_string();
        if show_payload {
            let payload = truncate_for_table(&row.payload_hex, PAYLOAD_MAX_CHARS);
            print_table_row(&[&ts, &row.device_id, &len, &payload], &widths);
        } else {
            print_table_row(&[&ts, &row.device_id, &len], &widths);
        }
    }
    print_table_border(&widths);
}

/// 以扩展模式渲染查询结果。
fn render_repl_query_rows_expanded(rows: &[QueryRow], show_payload: bool, payload_format: PayloadFormat) {
    for (idx, row) in rows.iter().enumerate() {
        println!("-[ RECORD {} ]------------------------------", idx + 1);
        println!("ts_ms       | {}", row.ts_ms);
        println!("device_id   | {}", row.device_id);
        println!("payload_len | {}", row.payload_len);
        if show_payload {
            let label = match payload_format {
                PayloadFormat::Hex => "payload_hex",
                PayloadFormat::Ascii => "payload_ascii",
            };
            println!("{label:<11} | {}", row.payload_hex);
        }
    }
}

/// 打印查询结果 footer，展示行数、范围与可选耗时。
fn print_repl_query_footer(
    common: &CommonArgs,
    rows: usize,
    elapsed: std::time::Duration,
    timing_on: bool,
) -> anyhow::Result<()> {
    let (from_ts, to_ts) = resolve_time_range(common)?;
    if timing_on {
        println!(
            "({rows} rows, elapsed: {:.2} ms, device: {}, range: {}..{})",
            elapsed.as_secs_f64() * 1000.0,
            common.device_id,
            from_ts,
            to_ts
        );
    } else {
        println!("({rows} rows)");
    }
    Ok(())
}

/// 打印表格边框。
fn print_table_border(widths: &[usize]) {
    print!("+");
    for w in widths {
        print!("{}+", "-".repeat(*w + 2));
    }
    println!();
}

/// 打印表格单行。
fn print_table_row(values: &[&str], widths: &[usize]) {
    print!("|");
    for (idx, value) in values.iter().enumerate() {
        print!(" {:<width$} |", value, width = widths[idx]);
    }
    println!();
}

/// 表格渲染时裁剪过长字段，避免横向撑爆终端。
fn truncate_for_table(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let head: String = input.chars().take(max_chars.saturating_sub(3)).collect();
    format!("{head}...")
}

/// 将 REPL 上下文构建为标准查询参数。
fn build_common_args_from_ctx(ctx: &ReplContext) -> anyhow::Result<CommonArgs> {
    let device_id = ctx
        .device_id
        .clone()
        .context("device_id not set, use: use <device_id>")?;
    let mut args = CommonArgs {
        root: ctx.root.clone(),
        device_id,
        from_ts: None,
        to_ts: None,
        day: None,
        today: false,
        last: None,
    };
    match &ctx.time_mode {
        ReplTimeMode::Today => args.today = true,
        ReplTimeMode::Day(day) => args.day = Some(day.clone()),
        ReplTimeMode::Last(last) => args.last = Some(last.clone()),
        ReplTimeMode::Range(from_ts, to_ts) => {
            args.from_ts = Some(*from_ts);
            args.to_ts = Some(*to_ts);
        }
    }
    let _ = resolve_time_range(&args)?;
    Ok(args)
}

/// 处理 `time` 子命令，更新 REPL 时间上下文。
fn apply_repl_time_command(parts: &[&str], ctx: &mut ReplContext) -> anyhow::Result<()> {
    let Some(mode) = parts.first() else {
        bail!("usage: time <today|day YYYYMMDD|last 30m|range <from_ts> <to_ts>>");
    };
    match mode.to_ascii_lowercase().as_str() {
        "today" => ctx.time_mode = ReplTimeMode::Today,
        "day" => {
            let Some(day) = parts.get(1) else {
                bail!("usage: time day <YYYYMMDD>");
            };
            ctx.time_mode = ReplTimeMode::Day(parse_day_key_arg(day).map_err(anyhow::Error::msg)?);
        }
        "last" => {
            let Some(last) = parts.get(1) else {
                bail!("usage: time last <num>[s|m|h|d]");
            };
            let normalized = parse_last_window_arg(last).map_err(anyhow::Error::msg)?;
            ctx.time_mode = ReplTimeMode::Last(normalized);
        }
        "range" => {
            let Some(from_raw) = parts.get(1) else {
                bail!("usage: time range <from_ts> <to_ts>");
            };
            let Some(to_raw) = parts.get(2) else {
                bail!("usage: time range <from_ts> <to_ts>");
            };
            let from_ts = from_raw
                .parse::<u64>()
                .with_context(|| format!("invalid from_ts: {from_raw}"))?;
            let to_ts = to_raw
                .parse::<u64>()
                .with_context(|| format!("invalid to_ts: {to_raw}"))?;
            if from_ts > to_ts {
                bail!("from_ts must be <= to_ts");
            }
            ctx.time_mode = ReplTimeMode::Range(from_ts, to_ts);
        }
        _ => bail!("unknown time mode: {mode}"),
    }
    Ok(())
}

/// 解析 REPL 的 `query` 参数。
fn parse_repl_query_args(parts: &[&str], common: CommonArgs) -> anyhow::Result<QueryArgs> {
    let mut limit = 200usize;
    let mut show_payload = false;
    let mut payload_format = PayloadFormat::Hex;
    let mut i = 0usize;
    while i < parts.len() {
        match parts[i] {
            "-n" | "--limit" => {
                i += 1;
                let Some(raw) = parts.get(i) else {
                    bail!("missing value for --limit");
                };
                limit = raw
                    .parse::<usize>()
                    .with_context(|| format!("invalid limit: {raw}"))?;
            }
            "-p" | "--show-payload" => show_payload = true,
            "--payload-format" => {
                i += 1;
                let Some(raw) = parts.get(i) else {
                    bail!("missing value for --payload-format");
                };
                payload_format = match raw.to_ascii_lowercase().as_str() {
                    "hex" => PayloadFormat::Hex,
                    "ascii" => PayloadFormat::Ascii,
                    _ => bail!("invalid payload format: {raw}"),
                };
            }
            other => bail!("unknown query arg: {other}"),
        }
        i += 1;
    }
    Ok(QueryArgs {
        common,
        limit,
        show_payload,
        payload_format,
    })
}

/// 解析 REPL 的 `export` 参数。
fn parse_repl_export_args(parts: &[&str], common: CommonArgs) -> anyhow::Result<ExportArgs> {
    let Some(format_raw) = parts.first() else {
        bail!("usage: export <json|csv> <out_path> [-n <limit>]");
    };
    let Some(out_raw) = parts.get(1) else {
        bail!("usage: export <json|csv> <out_path> [-n <limit>]");
    };
    let format = match format_raw.to_ascii_lowercase().as_str() {
        "json" => ExportFormat::Json,
        "csv" => ExportFormat::Csv,
        _ => bail!("invalid export format: {format_raw}"),
    };
    let mut limit = 200usize;
    let mut i = 2usize;
    while i < parts.len() {
        match parts[i] {
            "-n" | "--limit" => {
                i += 1;
                let Some(raw) = parts.get(i) else {
                    bail!("missing value for --limit");
                };
                limit = raw
                    .parse::<usize>()
                    .with_context(|| format!("invalid limit: {raw}"))?;
            }
            other => bail!("unknown export arg: {other}"),
        }
        i += 1;
    }
    Ok(ExportArgs {
        common,
        limit,
        out: PathBuf::from(out_raw),
        format,
    })
}

/// 打印 REPL 启动提示。
fn print_repl_banner() {
    println!("arv repl started. type `help` for commands.");
}

/// 打印 REPL 帮助信息。
fn print_repl_help() {
    println!(
        "commands:\n\
  help | ?\n\
  show\n\
  use <device_id>\n\
  root <path>\n\
  time today\n\
  time day <YYYYMMDD>\n\
  time last <num>[s|m|h|d]\n\
  time range <from_ts> <to_ts>\n\
  \\x [on|off]           # toggle expanded output\n\
  \\timing [on|off]      # toggle elapsed display\n\
  stat\n\
  verify\n\
  query [-n <limit>] [-p] [--payload-format hex|ascii]\n\
  export <json|csv> <out_path> [-n <limit>]\n\
  rebuild-manifest <YYYYMMDD>\n\
  exit | quit | q"
    );
}

/// 打印当前 REPL 上下文。
fn print_repl_context(ctx: &ReplContext) {
    let device_id = ctx.device_id.as_deref().unwrap_or("<unset>");
    let time = match &ctx.time_mode {
        ReplTimeMode::Today => "today".to_string(),
        ReplTimeMode::Day(day) => format!("day:{day}"),
        ReplTimeMode::Last(last) => format!("last:{last}"),
        ReplTimeMode::Range(from_ts, to_ts) => format!("range:{from_ts}-{to_ts}"),
    };
    let render = match ctx.render_mode {
        ReplRenderMode::Table => "table",
        ReplRenderMode::Expanded => "expanded",
    };
    let timing = if ctx.timing_on { "on" } else { "off" };
    println!(
        "root={} device_id={} time={} render={} timing={}",
        ctx.root, device_id, time, render, timing
    );
}

/// 执行查询命令，按设备+时间输出原始报文信息。
fn run_query(args: QueryArgs) -> anyhow::Result<()> {
    let rows = collect_rows(
        &args.common,
        Some(args.limit),
        true,
        args.payload_format,
        Some(args.common.device_id.clone()),
    )?;
    println!("matched: {}", rows.len());
    if args.show_payload {
        let payload_col = match args.payload_format {
            PayloadFormat::Hex => "payload_hex",
            PayloadFormat::Ascii => "payload_ascii",
        };
        println!("ts_ms,device_id,payload_len,{payload_col}");
    } else {
        println!("ts_ms,device_id,payload_len");
    }
    for row in rows {
        if args.show_payload {
            println!(
                "{},{},{},{}",
                row.ts_ms, row.device_id, row.payload_len, row.payload_hex
            );
        } else {
            println!("{},{},{}", row.ts_ms, row.device_id, row.payload_len);
        }
    }
    Ok(())
}

/// 执行统计命令，输出时间范围内归档总览。
fn run_stat(args: CommonArgs) -> anyhow::Result<()> {
    let (from_ts, to_ts) = resolve_time_range(&args)?;
    let candidates = collect_manifest_candidates(&args.root, &args.device_id, from_ts, to_ts)?;
    let stats = calc_stats(&args, from_ts, to_ts, &candidates)?;
    println!("root: {}", args.root);
    println!("device_id: {}", args.device_id);
    println!("files: {}", stats.files);
    println!("records: {}", stats.records);
    println!("min_ts_ms: {}", opt_u64(stats.min_ts_ms));
    println!("max_ts_ms: {}", opt_u64(stats.max_ts_ms));
    println!("payload_bytes: {}", stats.payload_bytes);
    Ok(())
}

/// 执行校验命令，验证 Parquet 文件可读且设备 ID 匹配。
fn run_verify(args: CommonArgs) -> anyhow::Result<()> {
    let (from_ts, to_ts) = resolve_time_range(&args)?;
    let candidates = collect_manifest_candidates(&args.root, &args.device_id, from_ts, to_ts)?;
    let mut stats = VerifyStats {
        files: candidates.len(),
        records: 0,
        failed: 0,
    };
    for (path, _) in candidates {
        match verify_single_file(&path, &args.device_id) {
            Ok(cnt) => stats.records += cnt,
            Err(e) => {
                stats.failed += 1;
                eprintln!("verify file failed: {} err={}", path.display(), e);
            }
        }
    }
    println!("root: {}", args.root);
    println!("device_id: {}", args.device_id);
    println!("files: {}", stats.files);
    println!("records: {}", stats.records);
    println!("failed_files: {}", stats.failed);
    Ok(())
}

/// 执行导出命令，将查询结果输出到 JSON/CSV 文件。
fn run_export(args: ExportArgs) -> anyhow::Result<()> {
    let rows = collect_rows(
        &args.common,
        Some(args.limit),
        true,
        PayloadFormat::Hex,
        Some(args.common.device_id.clone()),
    )?;
    let mut output = File::create(&args.out)
        .with_context(|| format!("create output failed: {}", args.out.display()))?;
    match args.format {
        ExportFormat::Csv => write_csv(&mut output, &rows)?,
        ExportFormat::Json => serde_json::to_writer_pretty(&mut output, &rows)?,
    }
    output.flush()?;
    println!("exported {} rows to {}", rows.len(), args.out.display());
    Ok(())
}

/// 重建指定设备与日期的 `manifest.jsonl`，用于历史数据修复。
fn run_rebuild_manifest(args: RebuildManifestArgs) -> anyhow::Result<()> {
    let day_dir = build_day_dir(&args.root, &args.device_id, &args.day);
    if !day_dir.exists() {
        bail!("day dir not found: {}", day_dir.display());
    }
    let mut files = collect_day_parquet_files(&day_dir)?;
    files.sort();

    let mut entries = Vec::<ManifestEntry>::new();
    for path in files {
        let file_name = path
            .file_name()
            .map(|x| x.to_string_lossy().to_string())
            .context("invalid parquet file name")?;
        let (hour, part) = parse_parquet_file_name(&file_name)
            .with_context(|| format!("invalid parquet file name: {file_name}"))?;
        let (rows, min_ts, max_ts, payload_bytes) = scan_file_meta(&path, &args.device_id)?;
        let file_size_bytes = std::fs::metadata(&path)?.len();
        entries.push(ManifestEntry {
            schema_version: ARCHIVE_SCHEMA_VERSION,
            device_id: args.device_id.clone(),
            file_name,
            day_key: args.day.clone(),
            hour,
            part,
            min_ts_ms: min_ts.unwrap_or(0),
            max_ts_ms: max_ts.unwrap_or(0),
            rows: rows as u64,
            payload_bytes,
            file_size_bytes,
            sealed: true,
            created_at_ms: now_ts_ms(),
        });
    }

    let manifest_path = day_dir.join(MANIFEST_FILE_NAME);
    let mut out = File::create(&manifest_path)
        .with_context(|| format!("create manifest failed: {}", manifest_path.display()))?;
    for entry in &entries {
        let line = serde_json::to_string(entry)?;
        out.write_all(line.as_bytes())?;
        out.write_all(b"\n")?;
    }
    out.flush()?;
    println!(
        "rebuilt manifest: {} entries={} path={}",
        args.day,
        entries.len(),
        manifest_path.display()
    );
    Ok(())
}

/// 从 Parquet 分段集合中读取记录并按条件过滤。
fn collect_rows(
    args: &CommonArgs,
    limit: Option<usize>,
    include_payload: bool,
    payload_format: PayloadFormat,
    expected_device: Option<String>,
) -> anyhow::Result<Vec<QueryRow>> {
    let (from_ts, to_ts) = resolve_time_range(args)?;
    let candidates = collect_manifest_candidates(&args.root, &args.device_id, from_ts, to_ts)?;
    let mut rows = Vec::<QueryRow>::new();
    let hard_limit = limit.unwrap_or(usize::MAX);
    for (path, _) in candidates {
        let mut one = read_file_rows(
            &path,
            from_ts,
            to_ts,
            include_payload,
            payload_format,
            expected_device.as_deref(),
        )?;
        rows.append(&mut one);
        if rows.len() >= hard_limit {
            break;
        }
    }
    rows.sort_by_key(|x| x.ts_ms);
    if rows.len() > hard_limit {
        rows.truncate(hard_limit);
    }
    Ok(rows)
}

/// 统计给定候选文件的记录数量、时间范围和 payload 总量。
fn calc_stats(
    args: &CommonArgs,
    from_ts: u64,
    to_ts: u64,
    candidates: &[(PathBuf, ManifestEntry)],
) -> anyhow::Result<Stats> {
    let mut records = 0usize;
    let mut min_ts = None::<u64>;
    let mut max_ts = None::<u64>;
    let mut payload_bytes = 0u64;
    for (path, _) in candidates {
        let rows = read_file_rows(
            path,
            from_ts,
            to_ts,
            false,
            PayloadFormat::Hex,
            Some(&args.device_id),
        )?;
        for row in rows {
            records += 1;
            payload_bytes = payload_bytes.saturating_add(row.payload_len as u64);
            min_ts = Some(min_ts.map_or(row.ts_ms, |v| v.min(row.ts_ms)));
            max_ts = Some(max_ts.map_or(row.ts_ms, |v| v.max(row.ts_ms)));
        }
    }
    Ok(Stats {
        files: candidates.len(),
        records,
        min_ts_ms: min_ts,
        max_ts_ms: max_ts,
        payload_bytes,
    })
}

/// 校验单个 Parquet 文件可读，且所有记录设备 ID 与目标一致。
fn verify_single_file(path: &PathBuf, device_id: &str) -> anyhow::Result<usize> {
    let rows = read_file_rows(path, 0, u64::MAX, false, PayloadFormat::Hex, Some(device_id))?;
    Ok(rows.len())
}

/// 读取单个 Parquet 文件并提取命中时间范围的记录。
fn read_file_rows(
    path: &PathBuf,
    from_ts: u64,
    to_ts: u64,
    include_payload: bool,
    payload_format: PayloadFormat,
    expected_device: Option<&str>,
) -> anyhow::Result<Vec<QueryRow>> {
    let file = File::open(path).with_context(|| format!("open parquet failed: {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut out = Vec::<QueryRow>::new();
    for batch in reader {
        let batch = batch?;
        append_rows_from_batch(
            &batch,
            from_ts,
            to_ts,
            include_payload,
            payload_format,
            expected_device,
            &mut out,
        )?;
    }
    Ok(out)
}

/// 扫描单个 Parquet 文件并汇总元信息。
fn scan_file_meta(
    path: &PathBuf,
    expected_device: &str,
) -> anyhow::Result<(usize, Option<u64>, Option<u64>, u64)> {
    let rows = read_file_rows(path, 0, u64::MAX, false, PayloadFormat::Hex, Some(expected_device))?;
    let mut min_ts = None::<u64>;
    let mut max_ts = None::<u64>;
    let mut payload_bytes = 0u64;
    for row in &rows {
        min_ts = Some(min_ts.map_or(row.ts_ms, |v| v.min(row.ts_ms)));
        max_ts = Some(max_ts.map_or(row.ts_ms, |v| v.max(row.ts_ms)));
        payload_bytes = payload_bytes.saturating_add(row.payload_len as u64);
    }
    Ok((rows.len(), min_ts, max_ts, payload_bytes))
}

/// 列出指定日目录下所有标准 Parquet 数据文件。
fn collect_day_parquet_files(day_dir: &std::path::Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let rd = std::fs::read_dir(day_dir)
        .with_context(|| format!("read day dir failed: {}", day_dir.display()))?;
    for entry in rd {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if parse_parquet_file_name(&name).is_some() {
            out.push(entry.path());
        }
    }
    Ok(out)
}

/// 从单个 RecordBatch 提取匹配记录。
fn append_rows_from_batch(
    batch: &RecordBatch,
    from_ts: u64,
    to_ts: u64,
    include_payload: bool,
    payload_format: PayloadFormat,
    expected_device: Option<&str>,
    out: &mut Vec<QueryRow>,
) -> anyhow::Result<()> {
    let device_arr = batch
        .column_by_name("device_id")
        .context("missing column: device_id")?
        .as_any()
        .downcast_ref::<StringArray>()
        .context("device_id type mismatch")?;
    let ts_arr = batch
        .column_by_name("timestamp_ms")
        .context("missing column: timestamp_ms")?
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("timestamp_ms type mismatch")?;
    let payload_arr = batch
        .column_by_name("payload")
        .context("missing column: payload")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .context("payload type mismatch")?;

    for i in 0..batch.num_rows() {
        let ts_i64 = ts_arr.value(i);
        if ts_i64 < 0 {
            continue;
        }
        let ts = ts_i64 as u64;
        if ts < from_ts || ts > to_ts {
            continue;
        }
        let device_id = device_arr.value(i).to_string();
        if let Some(expect) = expected_device && device_id != expect {
            bail!("device id mismatch in parquet row: expect={expect} actual={device_id}");
        }
        let payload = payload_arr.value(i);
        let payload_hex = if include_payload {
            match payload_format {
                PayloadFormat::Hex => encode_hex_lower(payload),
                PayloadFormat::Ascii => encode_ascii(payload),
            }
        } else {
            String::new()
        };
        out.push(QueryRow {
            ts_ms: ts,
            device_id,
            payload_len: payload.len(),
            payload_hex,
        });
    }
    Ok(())
}

/// 导出 CSV 文件。
fn write_csv(file: &mut File, rows: &[QueryRow]) -> anyhow::Result<()> {
    file.write_all(b"ts_ms,device_id,payload_len,payload_hex\n")?;
    for row in rows {
        let line = format!(
            "{},{},{},{}\n",
            row.ts_ms, row.device_id, row.payload_len, row.payload_hex
        );
        file.write_all(line.as_bytes())?;
    }
    Ok(())
}

/// 十六进制编码为小写文本。
fn encode_hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// 将字节流编码为可打印 ASCII 文本，不可打印字节使用 `.` 占位。
fn encode_ascii(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        if (0x20..=0x7e).contains(&b) {
            out.push(b as char);
        } else {
            out.push('.');
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
    use clap::Parser;

    use super::{
        Cli, Command, PayloadFormat, ReplContext, ReplRenderMode, ReplTimeMode,
        apply_repl_expanded_command, apply_repl_timing_command, build_common_args_from_ctx,
        encode_ascii, encode_hex_lower, execute_repl_line, parse_last_window_ms, resolve_time_range,
        truncate_for_table,
    };

    /// 验证十六进制编码结果应为小写。
    #[test]
    fn encode_hex_lower_should_work() {
        assert_eq!(encode_hex_lower(&[0, 15, 16, 255]), "000f10ff");
    }

    /// 验证 ASCII 编码会将不可打印字节替换为 `.`。
    #[test]
    fn encode_ascii_should_work() {
        assert_eq!(encode_ascii(b"ABC\x0A\xFF"), "ABC..");
    }

    /// 验证重建清单参数应可通过 `clap` 解析。
    #[test]
    fn parse_rebuild_manifest_args_should_work() {
        let cli = Cli::try_parse_from([
            "arv",
            "rebuild-manifest",
            "--root",
            "data",
            "-d",
            "dev001",
            "--day",
            "20260423",
        ])
        .expect("clap parse should succeed");
        match cli.command {
            Command::RebuildManifest(v) => {
                assert_eq!(v.root, "data");
                assert_eq!(v.device_id, "dev001");
                assert_eq!(v.day, "20260423");
            }
            _ => panic!("expected rebuild-manifest command"),
        }
    }

    /// 验证通用参数支持 `-d` 设备缩写。
    #[test]
    fn parse_common_args_should_support_short_device_flag() {
        let cli = Cli::try_parse_from([
            "arv", "stat", "-r", "data", "-d", "dev001", "-f", "1", "-t", "2",
        ])
        .expect("clap parse should succeed");
        match cli.command {
            Command::Stat(v) => {
                assert_eq!(v.device_id, "dev001");
                assert_eq!(v.from_ts, Some(1));
                assert_eq!(v.to_ts, Some(2));
            }
            _ => panic!("expected stat command"),
        }
    }

    /// 验证查询命令支持短参数组合。
    #[test]
    fn parse_query_args_should_support_short_flags() {
        let cli = Cli::try_parse_from([
            "arv", "query", "-r", "data", "-d", "dev001", "-f", "1", "-t", "2", "-n", "10", "-p",
        ])
        .expect("clap parse should succeed");
        match cli.command {
            Command::Query(v) => {
                assert_eq!(v.common.root, "data");
                assert_eq!(v.common.device_id, "dev001");
                assert_eq!(v.common.from_ts, Some(1));
                assert_eq!(v.common.to_ts, Some(2));
                assert_eq!(v.limit, 10);
                assert!(v.show_payload);
                assert!(matches!(v.payload_format, PayloadFormat::Hex));
            }
            _ => panic!("expected query command"),
        }
    }

    /// 验证查询命令支持 `--payload-format ascii`。
    #[test]
    fn parse_query_args_should_support_payload_format_ascii() {
        let cli = Cli::try_parse_from([
            "arv",
            "query",
            "-d",
            "dev001",
            "-f",
            "1",
            "-t",
            "2",
            "--payload-format",
            "ascii",
        ])
        .expect("clap parse should succeed");
        match cli.command {
            Command::Query(v) => {
                assert!(matches!(v.payload_format, PayloadFormat::Ascii));
            }
            _ => panic!("expected query command"),
        }
    }

    /// 验证 `--day` 可解析为全天毫秒范围。
    #[test]
    fn resolve_time_range_should_support_day() {
        let cli = Cli::try_parse_from(["arv", "stat", "-d", "dev001", "-D", "20260423"])
            .expect("clap parse should succeed");
        let Command::Stat(v) = cli.command else {
            panic!("expected stat command");
        };
        let (from_ts, to_ts) = resolve_time_range(&v).expect("resolve range should succeed");
        assert!(from_ts <= to_ts);
        assert!(to_ts - from_ts >= 86_399_000);
    }

    /// 验证 `--last` 窗口解析规则。
    #[test]
    fn parse_last_window_ms_should_work() {
        assert_eq!(parse_last_window_ms("30m"), Some(1_800_000));
        assert_eq!(parse_last_window_ms("6h"), Some(21_600_000));
        assert_eq!(parse_last_window_ms("2d"), Some(172_800_000));
        assert_eq!(parse_last_window_ms("10x"), None);
    }

    /// 验证 `--day` 非法格式应被拒绝。
    #[test]
    fn parse_rebuild_manifest_args_should_reject_invalid_day() {
        let err = Cli::try_parse_from([
            "arv",
            "rebuild-manifest",
            "-d",
            "dev001",
            "--day",
            "2026-04-23",
        ])
        .expect_err("clap parse should fail");
        assert!(err.to_string().contains("expected YYYYMMDD"));
    }

    /// 验证 REPL `time day` 命令可更新上下文。
    #[test]
    fn execute_repl_line_time_day_should_work() {
        let mut ctx = ReplContext {
            root: "data".to_string(),
            device_id: Some("dev001".to_string()),
            time_mode: ReplTimeMode::Today,
            render_mode: ReplRenderMode::Table,
            timing_on: false,
        };
        execute_repl_line("time day 20260423", &mut ctx).expect("repl execute should succeed");
        match ctx.time_mode {
            ReplTimeMode::Day(v) => assert_eq!(v, "20260423"),
            _ => panic!("expected day mode"),
        }
    }

    /// 验证 REPL `query` 参数可正确解析并执行。
    #[test]
    fn execute_repl_line_query_should_work() {
        let mut ctx = ReplContext {
            root: "data".to_string(),
            device_id: Some("dev001".to_string()),
            time_mode: ReplTimeMode::Range(1, 2),
            render_mode: ReplRenderMode::Table,
            timing_on: false,
        };
        let action = execute_repl_line(
            "query -n 10 -p --payload-format ascii",
            &mut ctx,
        )
        .expect("repl query should parse");
        assert!(matches!(action, super::ReplAction::Continue));
    }

    /// 验证 REPL 上下文可转换为导出参数依赖的通用参数。
    #[test]
    fn build_common_args_from_ctx_should_work() {
        let ctx = ReplContext {
            root: "data".to_string(),
            device_id: Some("dev001".to_string()),
            time_mode: ReplTimeMode::Last("30m".to_string()),
            render_mode: ReplRenderMode::Table,
            timing_on: false,
        };
        let common = build_common_args_from_ctx(&ctx).expect("build common should succeed");
        assert_eq!(common.device_id, "dev001");
        assert_eq!(common.last.as_deref(), Some("30m"));
    }

    /// 验证 REPL 扩展模式开关命令可正常切换。
    #[test]
    fn apply_repl_expanded_command_should_work() {
        let mut ctx = ReplContext {
            root: "data".to_string(),
            device_id: Some("dev001".to_string()),
            time_mode: ReplTimeMode::Today,
            render_mode: ReplRenderMode::Table,
            timing_on: false,
        };
        apply_repl_expanded_command(&["on"], &mut ctx).expect("expanded on should succeed");
        assert!(matches!(ctx.render_mode, ReplRenderMode::Expanded));
        apply_repl_expanded_command(&[], &mut ctx).expect("expanded toggle should succeed");
        assert!(matches!(ctx.render_mode, ReplRenderMode::Table));
    }

    /// 验证 REPL 耗时显示开关命令可正常切换。
    #[test]
    fn apply_repl_timing_command_should_work() {
        let mut ctx = ReplContext {
            root: "data".to_string(),
            device_id: Some("dev001".to_string()),
            time_mode: ReplTimeMode::Today,
            render_mode: ReplRenderMode::Table,
            timing_on: false,
        };
        apply_repl_timing_command(&["on"], &mut ctx).expect("timing on should succeed");
        assert!(ctx.timing_on);
        apply_repl_timing_command(&[], &mut ctx).expect("timing toggle should succeed");
        assert!(!ctx.timing_on);
    }

    /// 验证表格字段过长时应被裁剪并追加省略号。
    #[test]
    fn truncate_for_table_should_work() {
        assert_eq!(truncate_for_table("abcdef", 6), "abcdef");
        assert_eq!(truncate_for_table("abcdefghij", 6), "abc...");
    }
}
