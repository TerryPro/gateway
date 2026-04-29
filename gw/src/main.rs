use std::sync::Arc;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::info;
use tracing_subscriber::EnvFilter;

mod archive;
mod cli;
mod control;
mod device;
mod hex;
mod mqtt;
mod state;

use cli::CliMode;
use control::run_control_listener;
use mqtt::start_mqtt_worker;
use state::{AppState, load_all_devices};

/// 初始化网关日志系统，优先使用 RUST_LOG，其次使用配置文件中的日志级别。
fn init_tracing(config_log_level: &str) {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(config_log_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

/// 程序入口，仅启动控制监听（设备连接由 CONNECT 命令触发）。
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = cli::parse_config().await?;
    let config = match mode {
        CliMode::Help => {
            cli::print_cli_help();
            return Ok(());
        }
        CliMode::Version => {
            cli::print_cli_version();
            return Ok(());
        }
        CliMode::Run(cfg) => cfg,
    };
    init_tracing(&config.log_level);
    let device_config_path = config.device_config_path.clone();
    let archive_worker = archive::start_archive_worker(config.archive.clone());
    let archive_tx = archive_worker.as_ref().map(|h| h.tx.clone());
    let (mqtt_tx, mqtt_rx) = mpsc::channel(config.mqtt.queue_capacity);

    let all_devices = load_all_devices(&device_config_path).await?;
    let state = Arc::new(AppState::new(all_devices, archive_tx, Some(mqtt_tx)));
    let mqtt_worker = start_mqtt_worker(config.mqtt.clone(), state.clone(), mqtt_rx);

    let control_listener = TcpListener::bind(&config.control_addr)
        .await
        .with_context(|| format!("bind control addr failed: {}", config.control_addr))?;

    info!("gateway started");
    info!(control_addr = %config.control_addr, "control listener ready");
    info!(device_config = %device_config_path, "device config loaded");
    info!("device mode: client (use CONNECT <sim_addr>)");
    info!("press Ctrl+C to shutdown");

    let result = tokio::select! {
        result = run_control_listener(control_listener, state) => result,
        _ = tokio::signal::ctrl_c() => {
            info!("gateway shutdown signal received");
            Ok(())
        }
    };

    if let Some(worker) = archive_worker {
        worker.shutdown().await;
    }
    if let Some(worker) = mqtt_worker {
        worker.shutdown().await;
    }
    result
}
