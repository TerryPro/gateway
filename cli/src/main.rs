mod cli;
mod device_config;
mod local_commands;
mod mqtt_client;
mod repl;
mod resp_output;

use cli::CliMode;
use mqtt_client::MqttSession;

/// 程序入口，连接 MQTT Broker 后进入 REPL 交互模式。
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mode = cli::parse_config()?;
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

    let session = MqttSession::connect(
        &cfg.mqtt_host,
        cfg.mqtt_port,
        cfg.mqtt_client_id.clone(),
        &cfg.mqtt_topic_prefix,
        cfg.mqtt_qos,
    )
        .await
        .map_err(|e| anyhow::anyhow!("connect broker mqtt failed: {e}"))?;

    println!("connected mqtt: {}:{}", cfg.mqtt_host, cfg.mqtt_port);
    println!("client_id: {}", cfg.mqtt_client_id);
    println!("mode: REPL (type `HELP` for hints, `QUIT` to exit)");
    run_with_graceful_shutdown(&cfg, &session).await
}

/// 在 REPL 运行期间监听 Ctrl+C，收到信号后优雅退出。
async fn run_with_graceful_shutdown(
    cfg: &cli::Config,
    session: &MqttSession,
) -> anyhow::Result<()> {
    let run_result = tokio::select! {
        res = repl::run_repl_loop(cfg, session) => res,
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|e| anyhow::anyhow!("listen Ctrl+C failed: {e}"))?;
            println!("\nreceived Ctrl+C, graceful shutdown");
            Ok(())
        }
    };
    if let Err(e) = session.shutdown().await {
        eprintln!("(warn) mqtt session shutdown failed: {e}");
    }
    run_result
}
