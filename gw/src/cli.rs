use anyhow::Context;
use serde::Deserialize;

/// 网关配置。
#[derive(Debug, Clone)]
pub struct Config {
    pub control_addr: String,
    pub device_config_path: String,
    pub log_level: String,
    pub archive: ArchiveConfig,
    pub mqtt: MqttConfig,
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

/// MQTT 发布配置。
#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub topic_prefix: String,
    pub queue_capacity: usize,
    pub qos: u8,
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

/// 网关启动模式：运行、显示帮助或显示版本。
pub enum CliMode {
    Run(Config),
    Help,
    Version,
}

/// 网关配置文件结构。
#[derive(Debug, Deserialize)]
struct FileConfig {
    control_addr: Option<String>,
    device_config_path: Option<String>,
    log_level: Option<String>,
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
}

/// 命令行覆盖项，用于在读取配置文件后进行最终合并。
#[derive(Debug, Default)]
struct CliOverrides {
    config_path: String,
    config_explicit: bool,
    control_addr: Option<String>,
    device_config_path: Option<String>,
    log_level: Option<String>,
}

/// 解析命令行参数并返回运行模式或覆盖项。
fn parse_cli_args(raw_args: &[String]) -> anyhow::Result<CliModeOrOverrides> {
    let mut overrides = CliOverrides {
        config_path: "config.toml".to_string(),
        config_explicit: false,
        control_addr: None,
        device_config_path: None,
        log_level: None,
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
            "--log-level" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    anyhow::bail!("missing value for --log-level");
                };
                overrides.log_level = Some(value.clone());
            }
            unknown => {
                anyhow::bail!("unknown argument: {unknown}");
            }
        }
        i += 1;
    }

    Ok(CliModeOrOverrides::Overrides(overrides))
}

/// 参数解析结果：直接返回模式，或返回运行态覆盖项。
enum CliModeOrOverrides {
    Mode(CliMode),
    Overrides(CliOverrides),
}

/// 解析启动参数与配置文件，生成网关运行模式。
pub async fn parse_config() -> anyhow::Result<CliMode> {
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    parse_config_from_args(&raw_args).await
}

/// 基于指定参数列表解析配置，便于单元测试覆盖参数优先级与错误分支。
async fn parse_config_from_args(raw_args: &[String]) -> anyhow::Result<CliMode> {
    let mut control_addr = "0.0.0.0:7002".to_string();
    let mut device_config_path = "dev.toml".to_string();
    let mut log_level = "info".to_string();
    let mut archive = ArchiveConfig {
        enabled: true,
        root_dir: "data/raw".to_string(),
        rotate_mode: ArchiveRotateMode::Time,
        rotate_size_mb: 64,
        queue_capacity: 10_000,
        flush_interval_ms: 1_000,
    };
    let mut mqtt = MqttConfig {
        enabled: false,
        host: "127.0.0.1".to_string(),
        port: 1883,
        client_id: "gw-publisher".to_string(),
        username: None,
        password: None,
        topic_prefix: "gw".to_string(),
        queue_capacity: 10_000,
        qos: 1,
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

    Ok(CliMode::Run(Config {
        control_addr,
        device_config_path,
        log_level,
        archive,
        mqtt,
    }))
}

/// 返回网关启动参数帮助文本。
pub fn cli_usage_text() -> &'static str {
    "usage: gw [OPTIONS]\n\
options:\n\
  --config <path>                   网关配置文件路径，默认 config.toml\n\
  --control-addr <host:port>        控制监听地址，默认 0.0.0.0:7002\n\
  --device-config <path>            设备配置文件路径，默认 dev.toml\n\
  --log-level <level>               日志级别，默认 info（如 trace/debug/info/warn/error）\n\
  -h, --help                        显示帮助\n\
  -V, --version                     显示版本"
}

/// 打印网关帮助信息。
pub fn print_cli_help() {
    println!("{}", cli_usage_text());
}

/// 打印网关版本信息。
pub fn print_cli_version() {
    println!("gw {}", env!("CARGO_PKG_VERSION"));
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{CliMode, parse_config_from_args};

    /// 生成唯一的临时文件路径，避免并发测试相互覆盖。
    fn unique_temp_path(file_name: &str) -> PathBuf {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time must be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("gw_cli_{ts}_{file_name}"))
    }

    /// 验证命令行参数应覆盖配置文件中的同名字段。
    #[tokio::test]
    async fn cli_should_override_file_config() {
        let cfg_path = unique_temp_path("config.toml");
        tokio::fs::write(
            &cfg_path,
            "control_addr = \"127.0.0.1:7002\"\ndevice_config_path = \"from_file.toml\"\nlog_level = \"warn\"\n",
        )
        .await
        .expect("write temp config should succeed");
        let args = vec![
            "--config".to_string(),
            cfg_path.to_string_lossy().to_string(),
            "--device-config".to_string(),
            "from_cli.toml".to_string(),
            "--control-addr".to_string(),
            "0.0.0.0:9999".to_string(),
            "--log-level".to_string(),
            "trace".to_string(),
        ];

        let mode = parse_config_from_args(&args)
            .await
            .expect("parse config should succeed");
        match mode {
            CliMode::Run(cfg) => {
                assert_eq!(cfg.device_config_path, "from_cli.toml");
                assert_eq!(cfg.control_addr, "0.0.0.0:9999");
                assert_eq!(cfg.log_level, "trace");
            }
            _ => panic!("expected run mode"),
        }

        let _ = tokio::fs::remove_file(&cfg_path).await;
    }

    /// 验证未知参数应直接返回错误，避免静默忽略。
    #[tokio::test]
    async fn unknown_argument_should_fail() {
        let args = vec!["--bad-flag".to_string()];
        let err = match parse_config_from_args(&args).await {
            Ok(_) => panic!("unknown arg should fail"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("unknown argument"));
    }

    /// 验证参数缺失值时应直接返回错误。
    #[tokio::test]
    async fn missing_value_should_fail() {
        let args = vec!["--device-config".to_string()];
        let err = match parse_config_from_args(&args).await {
            Ok(_) => panic!("missing value should fail"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("missing value"));
    }

    /// 验证显式指定不存在的配置文件时应返回读取错误。
    #[tokio::test]
    async fn explicit_missing_config_should_fail() {
        let cfg_path = unique_temp_path("not_exists.toml");
        let args = vec!["--config".to_string(), cfg_path.to_string_lossy().to_string()];
        let err = match parse_config_from_args(&args).await {
            Ok(_) => panic!("explicit missing config should fail"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("read config failed"));
    }
}
