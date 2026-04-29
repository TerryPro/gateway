use std::path::PathBuf;

use anyhow::Context;
use rumqttd::{Broker, Config};
use tokio::task::JoinHandle;
use tracing::info;

/// Broker 运行句柄，封装阻塞任务的 join handle。
pub struct BrokerServiceHandle {
    join_handle: JoinHandle<anyhow::Result<()>>,
}

impl BrokerServiceHandle {
    /// 等待 Broker 任务结束并返回运行结果。
    pub async fn wait(self) -> anyhow::Result<()> {
        self.join_handle
            .await
            .context("broker task join failed")?
    }
}

/// 从指定 TOML 文件加载 rumqttd 配置。
pub fn load_rumqttd_config(config_path: &str) -> anyhow::Result<Config> {
    let settings = config::Config::builder()
        .add_source(config::File::from(PathBuf::from(config_path)))
        .build()
        .with_context(|| format!("load rumqttd config file failed: {config_path}"))?;
    settings
        .try_deserialize::<Config>()
        .with_context(|| format!("deserialize rumqttd config failed: {config_path}"))
}

/// 启动内嵌 rumqttd broker 并返回运行句柄。
pub fn start_broker(config: Config) -> BrokerServiceHandle {
    let join_handle = tokio::task::spawn_blocking(move || {
        info!("starting embedded rumqttd broker");
        let mut broker = Broker::new(config);
        broker.start().context("rumqttd broker exited with error")
    });
    BrokerServiceHandle { join_handle }
}
