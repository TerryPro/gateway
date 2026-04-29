mod api;
mod cli;
mod config;
mod flush;
mod ingress;
mod mem;
mod model;
mod query;
mod server;
mod wal;

use clap::{Parser, Subcommand};

/// tsdbd - EdgeTSDB service daemon
#[derive(Debug, Parser)]
#[command(name = "tsdbd", version, about = "EdgeTSDB service daemon")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Start the tsdbd server
    Server {
        /// 配置文件路径
        #[arg(short, long, default_value = "config.toml")]
        config: String,

        /// 测试数据文件路径（可选），启动时导入测试数据
        #[arg(short, long)]
        file: Option<String>,
    },

    /// Query time-series data
    Query {
        /// 存储根目录
        #[arg(long)]
        root: Option<String>,

        /// 设备 ID
        #[arg(long)]
        device: Option<String>,

        /// 起始时间戳
        #[arg(long)]
        from: Option<u64>,

        /// 结束时间戳
        #[arg(long)]
        to: Option<u64>,

        /// 参数列表（逗号分隔）
        #[arg(long, value_delimiter = ',')]
        params: Option<Vec<String>>,

        /// 结果数量限制
        #[arg(long, default_value = "100")]
        limit: usize,

        /// 输出格式：wide, long, json
        #[arg(long, value_enum, default_value = "wide")]
        format: cli::query::OutputFormat,

        /// 直接执行 SQL
        #[arg(long)]
        sql: Option<String>,

        /// SQL 模板（自动替换 {{table}}, {{from}}, {{to}}）
        #[arg(long)]
        sql_template: Option<String>,

        /// 显示查询统计
        #[arg(long)]
        profile: bool,

        /// 远程 API 地址（如 http://localhost:8080），指定后使用远程查询而非本地文件
        #[arg(long)]
        api: Option<String>,
    },

    /// Show data statistics (files, rows, points)
    Stat {
        #[arg(short = 'r', long, default_value = "data/store")]
        root: String,

        #[arg(short = 'd', long = "device-id", visible_alias = "device")]
        device_id: String,

        #[arg(short = 'f', long = "from", requires = "to_ts")]
        from_ts: Option<u64>,

        #[arg(short = 't', long = "to", requires = "from_ts")]
        to_ts: Option<u64>,

        #[arg(short = 'D', long = "day")]
        day: Option<String>,

        #[arg(short = 'T', long = "today", default_value_t = false)]
        today: bool,

        #[arg(short = 'l', long = "last")]
        last: Option<String>,

        #[arg(short = 'a', long = "all", default_value_t = false)]
        all: bool,
    },

    /// Export data to JSON or CSV
    Export {
        #[arg(short = 'r', long, default_value = "data/store")]
        root: String,

        #[arg(short = 'd', long = "device-id", visible_alias = "device")]
        device_id: String,

        #[arg(short = 'f', long = "from", requires = "to_ts")]
        from_ts: Option<u64>,

        #[arg(short = 't', long = "to", requires = "from_ts")]
        to_ts: Option<u64>,

        #[arg(short = 'D', long = "day")]
        day: Option<String>,

        #[arg(short = 'T', long = "today", default_value_t = false)]
        today: bool,

        #[arg(short = 'l', long = "last")]
        last: Option<String>,

        #[arg(short = 'a', long = "all", default_value_t = false)]
        all: bool,

        #[arg(short = 'n', long, default_value_t = 1000)]
        limit: usize,

        #[arg(short = 'p', long = "param")]
        params: Vec<String>,

        #[arg(short = 'o', long)]
        out: std::path::PathBuf,

        #[arg(short = 'F', long, value_enum, default_value_t = cli::export::ExportFormat::Json)]
        format: cli::export::ExportFormat,

        #[arg(long, default_value_t = false)]
        flat: bool,
    },

    /// Check data consistency (manifest vs files)
    Doctor {
        #[arg(short = 'r', long, default_value = "data/store")]
        root: String,

        #[arg(short = 'd', long = "device-id", visible_alias = "device")]
        device_id: String,

        #[arg(short = 'f', long = "from", requires = "to_ts")]
        from_ts: Option<u64>,

        #[arg(short = 't', long = "to", requires = "from_ts")]
        to_ts: Option<u64>,

        #[arg(short = 'D', long = "day")]
        day: Option<String>,

        #[arg(short = 'T', long = "today", default_value_t = false)]
        today: bool,

        #[arg(short = 'l', long = "last")]
        last: Option<String>,

        #[arg(short = 'a', long = "all", default_value_t = false)]
        all: bool,
    },

    /// Run performance benchmark
    Perf {
        #[arg(short = 'r', long, default_value = "data/store")]
        root: String,

        #[arg(short = 'd', long = "device-id", visible_alias = "device")]
        device_id: String,

        #[arg(short = 'f', long = "from", requires = "to_ts")]
        from_ts: Option<u64>,

        #[arg(short = 't', long = "to", requires = "from_ts")]
        to_ts: Option<u64>,

        #[arg(short = 'D', long = "day")]
        day: Option<String>,

        #[arg(short = 'T', long = "today", default_value_t = false)]
        today: bool,

        #[arg(short = 'l', long = "last")]
        last: Option<String>,

        #[arg(short = 'a', long = "all", default_value_t = false)]
        all: bool,

        #[arg(short = 'p', long = "param")]
        params: Vec<String>,

        #[arg(short = 'n', long, default_value_t = 200)]
        limit: usize,

        #[arg(long, default_value_t = 20)]
        iterations: usize,

        #[arg(long, default_value_t = 3)]
        warmup: usize,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { config, file } => {
            server::run(config, file).await
        }
        Commands::Query {
            root,
            device,
            from,
            to,
            params,
            limit,
            format,
            sql,
            sql_template,
            profile,
            api,
        } => {
            cli::query::run(
                root,
                device,
                from,
                to,
                params,
                limit,
                format,
                sql,
                sql_template,
                profile,
                api,
            )
            .await
        }
        Commands::Stat {
            root,
            device_id,
            from_ts,
            to_ts,
            day,
            today,
            last,
            all,
        } => {
            cli::stat::run(cli::stat::StatArgs {
                root,
                device_id,
                from_ts,
                to_ts,
                day,
                today,
                last,
                all,
            })
            .await
        }
        Commands::Export {
            root,
            device_id,
            from_ts,
            to_ts,
            day,
            today,
            last,
            all,
            limit,
            params,
            out,
            format,
            flat,
        } => {
            cli::export::run(cli::export::ExportArgs {
                root,
                device_id,
                from_ts,
                to_ts,
                day,
                today,
                last,
                all,
                limit,
                params,
                out,
                format,
                flat,
            })
        }
        Commands::Doctor {
            root,
            device_id,
            from_ts,
            to_ts,
            day,
            today,
            last,
            all,
        } => {
            cli::doctor::run(cli::doctor::DoctorArgs {
                root,
                device_id,
                from_ts,
                to_ts,
                day,
                today,
                last,
                all,
            })
        }
        Commands::Perf {
            root,
            device_id,
            from_ts,
            to_ts,
            day,
            today,
            last,
            all,
            params,
            limit,
            iterations,
            warmup,
        } => {
            cli::perf::run(cli::perf::PerfArgs {
                root,
                device_id,
                from_ts,
                to_ts,
                day,
                today,
                last,
                all,
                params,
                limit,
                iterations,
                warmup,
            })
            .await
        }
    }
}
