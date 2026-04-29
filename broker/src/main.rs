use std::{path::Path, sync::Arc};

use anyhow::Context;
use tokio::{net::TcpListener, sync::mpsc};
use tracing::info;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

mod archive;
mod cli;
mod command;
mod control;
mod device;
mod mqtt_bridge;
mod service;
mod state;

use cli::CliMode;
use control::run_control_listener;
use mqtt_bridge::start_mqtt_worker;
use archive::start_archive_worker;
use service::{load_rumqttd_config, start_broker};
use state::{AppState, load_all_devices};

/// 初始化日志系统：优先读取 RUST_LOG，其次使用配置中的日志级别，并写入日志文件。
fn init_tracing(config_log_level: &str, log_file_path: &str) -> anyhow::Result<WorkerGuard> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(config_log_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let log_path = Path::new(log_file_path);
    let file_name = log_path
        .file_name()
        .and_then(|v| v.to_str())
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("invalid log_file_path: missing file name"))?;
    let log_dir = log_path
        .parent()
        .filter(|v| !v.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(log_dir)
        .with_context(|| format!("create log directory failed: {}", log_dir.display()))?;

    let file_appender = tracing_appender::rolling::daily(log_dir, file_name);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(false)
        .with_writer(non_blocking)
        .try_init()
        .map_err(|e| anyhow::anyhow!("init tracing failed: {e}"))?;
    Ok(guard)
}

/// 程序入口：加载配置并启动内嵌 rumqttd broker。
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = cli::parse_config().await?;
    let cfg = match mode {
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

    let _log_guard = init_tracing(&cfg.log_level, &cfg.log_file_path)?;
    let archive_worker = start_archive_worker(cfg.archive.clone());
    let archive_tx = archive_worker.as_ref().map(|h| h.tx.clone());
    let (mqtt_tx, mqtt_rx) = mpsc::channel(cfg.mqtt.queue_capacity);
    let all_devices = load_all_devices(&cfg.device_config_path).await?;
    let state = Arc::new(AppState::new(
        all_devices,
        archive_tx,
        Some(mqtt_tx),
        cfg.mqtt.enable_device_telemetry,
        cfg.mqtt.enable_param_telemetry,
    ));
    let mqtt_worker = start_mqtt_worker(cfg.mqtt.clone(), state.clone(), mqtt_rx);
    let control_listener = TcpListener::bind(&cfg.control_addr)
        .await
        .with_context(|| format!("bind control addr failed: {}", cfg.control_addr))?;

    let rumqttd_cfg = load_rumqttd_config(&cfg.rumqttd_config_path)?;
    let broker = start_broker(rumqttd_cfg);

    info!("broker started");
    info!(log_file = %cfg.log_file_path, "file logging enabled");
    info!(control_addr = %cfg.control_addr, "control listener ready");
    info!(device_config = %cfg.device_config_path, "device config loaded");
    info!(rumqttd_config = %cfg.rumqttd_config_path, "rumqttd config loaded");
    info!("press Ctrl+C to shutdown");

    tokio::select! {
        result = run_control_listener(control_listener, state) => {
            result.with_context(|| "control listener stopped unexpectedly")?;
        }
        result = broker.wait() => {
            result.with_context(|| "broker stopped unexpectedly")?;
        }
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown signal received, process exiting");
        }
    }

    if let Some(worker) = mqtt_worker {
        worker.shutdown().await;
    }
    if let Some(worker) = archive_worker {
        worker.shutdown().await;
    }

    Ok(())
}
