use anyhow::Context;
use serde::Deserialize;

/// Broker 进程配置。
#[derive(Debug, Clone)]
pub struct BrokerAppConfig {
    pub control_addr: String,
    pub device_config_path: String,
    pub log_level: String,
    pub log_file_path: String,
    pub rumqttd_config_path: String,
    pub archive: ArchiveConfig,
    pub mqtt: MqttBridgeConfig,
}

/// 遥测归档配置。
#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    pub enabled: bool,
    pub root_dir: String,
    pub rotate_mode: ArchiveRotateMode,
    pub rotate_size_mb: u64,
    pub queue_capacity: usize,
    pub flush_interval_ms: u64,
}

/// 归档切换模式。
#[derive(Debug, Clone, Copy)]
pub enum ArchiveRotateMode {
    Time,
    Size,
    Hybrid,
}

impl ArchiveRotateMode {
    /// 解析归档切换模式字符串。
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "time" => Ok(Self::Time),
            "size" => Ok(Self::Size),
            "hybrid" => Ok(Self::Hybrid),
            _ => anyhow::bail!(
                "invalid archive_rotate_mode: {value} (expected: time|size|hybrid)"
            ),
        }
    }
}

/// MQTT 桥接配置。
#[derive(Debug, Clone)]
pub struct MqttBridgeConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub topic_prefix: String,
    pub queue_capacity: usize,
    pub qos: u8,
    pub cli_offline_grace_secs: u64,
    pub enable_device_telemetry: bool,
    pub enable_param_telemetry: bool,
    pub trace_topics: bool,
    pub trace_payload_preview_bytes: usize,
}

/// Broker 启动模式：运行、显示帮助或显示版本。
pub enum CliMode {
    Run(BrokerAppConfig),
    Help,
    Version,
}

/// 配置文件结构。
#[derive(Debug, Deserialize)]
struct FileConfig {
    control_addr: Option<String>,
    device_config_path: Option<String>,
    log_level: Option<String>,
    log_file_path: Option<String>,
    rumqttd_config_path: Option<String>,
    archive_enabled: Option<bool>,
    archive_root: Option<String>,
    archive_rotate_mode: Option<String>,
    archive_rotate_size_mb: Option<u64>,
    archive_queue_capacity: Option<usize>,
    archive_flush_interval_ms: Option<u64>,
    mqtt_enabled: Option<bool>,
    mqtt_host: Option<String>,
    mqtt_port: Option<u16>,
    mqtt_client_id: Option<String>,
    mqtt_username: Option<String>,
    mqtt_password: Option<String>,
    mqtt_topic_prefix: Option<String>,
    mqtt_queue_capacity: Option<usize>,
    mqtt_qos: Option<u8>,
    mqtt_cli_offline_grace_secs: Option<u64>,
    mqtt_enable_device_telemetry: Option<bool>,
    mqtt_enable_param_telemetry: Option<bool>,
    mqtt_trace_topics: Option<bool>,
    mqtt_trace_payload_preview_bytes: Option<usize>,
}

/// 命令行覆盖项。
#[derive(Debug, Default)]
struct CliOverrides {
    config_path: String,
    config_explicit: bool,
    control_addr: Option<String>,
    device_config_path: Option<String>,
    log_level: Option<String>,
    log_file_path: Option<String>,
    rumqttd_config_path: Option<String>,
}

/// 解析命令行参数并返回运行模式或覆盖项。
fn parse_cli_args(raw_args: &[String]) -> anyhow::Result<CliModeOrOverrides> {
    let mut overrides = CliOverrides {
        config_path: "broker.toml".to_string(),
        config_explicit: false,
        control_addr: None,
        device_config_path: None,
        log_level: None,
        log_file_path: None,
        rumqttd_config_path: None,
    };

    let mut i = 0;
    while i < raw_args.len() {
        match raw_args[i].as_str() {
            "-h" | "--help" => return Ok(CliModeOrOverrides::Mode(CliMode::Help)),
            "-V" | "--version" => return Ok(CliModeOrOverrides::Mode(CliMode::Version)),
            "--config" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    anyhow::bail!("missing value for --config");
                };
                overrides.config_path = value.clone();
                overrides.config_explicit = true;
            }
            "--log-level" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    anyhow::bail!("missing value for --log-level");
                };
                overrides.log_level = Some(value.clone());
            }
            "--log-file" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    anyhow::bail!("missing value for --log-file");
                };
                overrides.log_file_path = Some(value.clone());
            }
            "--control-addr" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    anyhow::bail!("missing value for --control-addr");
                };
                overrides.control_addr = Some(value.clone());
            }
            "--device-config" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    anyhow::bail!("missing value for --device-config");
                };
                overrides.device_config_path = Some(value.clone());
            }
            "--rumqttd-config" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    anyhow::bail!("missing value for --rumqttd-config");
                };
                overrides.rumqttd_config_path = Some(value.clone());
            }
            unknown => anyhow::bail!("unknown argument: {unknown}"),
        }
        i += 1;
    }

    Ok(CliModeOrOverrides::Overrides(overrides))
}

/// 参数解析结果：直接模式或运行态覆盖项。
enum CliModeOrOverrides {
    Mode(CliMode),
    Overrides(CliOverrides),
}

/// 解析启动参数并合并配置文件。
pub async fn parse_config() -> anyhow::Result<CliMode> {
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    parse_config_from_args(&raw_args).await
}

/// 基于给定参数列表生成运行配置，便于后续做单测扩展。
async fn parse_config_from_args(raw_args: &[String]) -> anyhow::Result<CliMode> {
    let mut control_addr = "0.0.0.0:7003".to_string();
    let mut device_config_path = "dev.toml".to_string();
    let mut log_level = "info".to_string();
    let mut log_file_path = "logs/broker.log".to_string();
    let mut rumqttd_config_path = "rumqttd.toml".to_string();
    let mut archive = ArchiveConfig {
        enabled: true,
        root_dir: "data/raw".to_string(),
        rotate_mode: ArchiveRotateMode::Time,
        rotate_size_mb: 64,
        queue_capacity: 10_000,
        flush_interval_ms: 1_000,
    };
    let mut mqtt = MqttBridgeConfig {
        enabled: true,
        host: "127.0.0.1".to_string(),
        port: 18830,
        client_id: "broker-bridge".to_string(),
        username: None,
        password: None,
        topic_prefix: "gw".to_string(),
        queue_capacity: 10_000,
        qos: 1,
        cli_offline_grace_secs: 30,
        enable_device_telemetry: true,
        enable_param_telemetry: false,
        trace_topics: true,
        trace_payload_preview_bytes: 160,
    };
    let parsed = parse_cli_args(raw_args)?;

    let overrides = match parsed {
        CliModeOrOverrides::Mode(mode) => return Ok(mode),
        CliModeOrOverrides::Overrides(v) => v,
    };

    match tokio::fs::read_to_string(&overrides.config_path).await {
        Ok(text) => {
            let file_cfg = toml::from_str::<FileConfig>(&text)
                .with_context(|| format!("parse config failed: {}", overrides.config_path))?;
            if let Some(v) = file_cfg.control_addr {
                control_addr = v;
            }
            if let Some(v) = file_cfg.device_config_path {
                device_config_path = v;
            }
            if let Some(v) = file_cfg.log_level {
                log_level = v;
            }
            if let Some(v) = file_cfg.log_file_path {
                log_file_path = v;
            }
            if let Some(v) = file_cfg.rumqttd_config_path {
                rumqttd_config_path = v;
            }
            if let Some(v) = file_cfg.archive_enabled {
                archive.enabled = v;
            }
            if let Some(v) = file_cfg.archive_root {
                archive.root_dir = v;
            }
            if let Some(v) = file_cfg.archive_rotate_mode {
                archive.rotate_mode = ArchiveRotateMode::parse(&v)?;
            }
            if let Some(v) = file_cfg.archive_rotate_size_mb {
                archive.rotate_size_mb = v;
            }
            if let Some(v) = file_cfg.archive_queue_capacity {
                archive.queue_capacity = v;
            }
            if let Some(v) = file_cfg.archive_flush_interval_ms {
                archive.flush_interval_ms = v;
            }
            if let Some(v) = file_cfg.mqtt_enabled {
                mqtt.enabled = v;
            }
            if let Some(v) = file_cfg.mqtt_host {
                mqtt.host = v;
            }
            if let Some(v) = file_cfg.mqtt_port {
                mqtt.port = v;
            }
            if let Some(v) = file_cfg.mqtt_client_id {
                mqtt.client_id = v;
            }
            if let Some(v) = file_cfg.mqtt_username {
                mqtt.username = Some(v);
            }
            if let Some(v) = file_cfg.mqtt_password {
                mqtt.password = Some(v);
            }
            if let Some(v) = file_cfg.mqtt_topic_prefix {
                mqtt.topic_prefix = v;
            }
            if let Some(v) = file_cfg.mqtt_queue_capacity {
                mqtt.queue_capacity = v;
            }
            if let Some(v) = file_cfg.mqtt_qos {
                mqtt.qos = v;
            }
            if let Some(v) = file_cfg.mqtt_cli_offline_grace_secs {
                mqtt.cli_offline_grace_secs = v;
            }
            if let Some(v) = file_cfg.mqtt_enable_device_telemetry {
                mqtt.enable_device_telemetry = v;
            }
            if let Some(v) = file_cfg.mqtt_enable_param_telemetry {
                mqtt.enable_param_telemetry = v;
            }
            if let Some(v) = file_cfg.mqtt_trace_topics {
                mqtt.trace_topics = v;
            }
            if let Some(v) = file_cfg.mqtt_trace_payload_preview_bytes {
                mqtt.trace_payload_preview_bytes = v;
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !overrides.config_explicit => {}
        Err(e) => {
            return Err(e)
                .with_context(|| format!("read config failed: {}", overrides.config_path));
        }
    }

    if let Some(v) = overrides.control_addr {
        control_addr = v;
    }
    if let Some(v) = overrides.device_config_path {
        device_config_path = v;
    }
    if let Some(v) = overrides.log_level {
        log_level = v;
    }
    if let Some(v) = overrides.log_file_path {
        log_file_path = v;
    }
    if let Some(v) = overrides.rumqttd_config_path {
        rumqttd_config_path = v;
    }

    if control_addr.trim().is_empty() {
        anyhow::bail!("control_addr must not be empty");
    }
    if device_config_path.trim().is_empty() {
        anyhow::bail!("device_config_path must not be empty");
    }
    if log_level.trim().is_empty() {
        anyhow::bail!("log_level must not be empty");
    }
    if log_file_path.trim().is_empty() {
        anyhow::bail!("log_file_path must not be empty");
    }
    if rumqttd_config_path.trim().is_empty() {
        anyhow::bail!("rumqttd_config_path must not be empty");
    }
    if archive.root_dir.trim().is_empty() {
        anyhow::bail!("archive_root must not be empty");
    }
    if archive.rotate_size_mb == 0 {
        anyhow::bail!("archive_rotate_size_mb must be > 0");
    }
    if archive.queue_capacity == 0 {
        anyhow::bail!("archive_queue_capacity must be > 0");
    }
    if archive.flush_interval_ms == 0 {
        anyhow::bail!("archive_flush_interval_ms must be > 0");
    }

    if mqtt.host.trim().is_empty() {
        anyhow::bail!("mqtt_host must not be empty");
    }
    if mqtt.client_id.trim().is_empty() {
        anyhow::bail!("mqtt_client_id must not be empty");
    }
    if mqtt.topic_prefix.trim().is_empty() {
        anyhow::bail!("mqtt_topic_prefix must not be empty");
    }
    if mqtt.queue_capacity == 0 {
        anyhow::bail!("mqtt_queue_capacity must be > 0");
    }
    if mqtt.qos > 2 {
        anyhow::bail!("mqtt_qos must be 0|1|2");
    }
    if mqtt.cli_offline_grace_secs > 86_400 {
        anyhow::bail!("mqtt_cli_offline_grace_secs must be <= 86400");
    }
    if mqtt.trace_payload_preview_bytes > 65_536 {
        anyhow::bail!("mqtt_trace_payload_preview_bytes must be <= 65536");
    }

    Ok(CliMode::Run(BrokerAppConfig {
        control_addr,
        device_config_path,
        log_level,
        log_file_path,
        rumqttd_config_path,
        archive,
        mqtt,
    }))
}

/// 返回帮助文本。
pub fn cli_usage_text() -> &'static str {
    "usage: broker [OPTIONS]\n\
options:\n\
  --config <path>                   broker 配置文件路径，默认 broker.toml\n\
  --control-addr <host:port>        控制监听地址，默认 0.0.0.0:7003\n\
  --device-config <path>            设备配置文件路径，默认 dev.toml\n\
  --rumqttd-config <path>           rumqttd 配置文件路径，默认 rumqttd.toml\n\
  # archive_* 建议在 broker.toml 中配置\n\
  --log-level <level>               日志级别，默认 info（trace/debug/info/warn/error）\n\
  --log-file <path>                 日志文件路径，默认 logs/broker.log\n\
  -h, --help                        显示帮助\n\
  -V, --version                     显示版本"
}

/// 打印帮助文本。
pub fn print_cli_help() {
    println!("{}", cli_usage_text());
}

/// 打印版本信息。
pub fn print_cli_version() {
    println!("broker {}", env!("CARGO_PKG_VERSION"));
}
