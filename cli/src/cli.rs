use std::time::{SystemTime, UNIX_EPOCH};

/// CLI 配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub mqtt_host: String,
    pub mqtt_port: u16,
    pub mqtt_client_id: String,
    pub mqtt_topic_prefix: String,
    pub mqtt_qos: u8,
    pub req_timeout_ms: u64,
    pub device_config_path: String,
}

/// CLI 启动模式：运行、显示帮助或显示版本。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliMode {
    Run(Config),
    Help,
    Version,
}

/// 解析 CLI 参数（处理运行/帮助/版本模式）。
pub fn parse_config() -> anyhow::Result<CliMode> {
    parse_args(std::env::args().skip(1))
}

/// 解析参数迭代器（便于测试与复用）。
fn parse_args<I>(args: I) -> anyhow::Result<CliMode>
where
    I: IntoIterator<Item = String>,
{
    let mut addr = "127.0.0.1:18830".to_string();
    let mut mqtt_client_id = generate_random_client_id();
    let mut mqtt_topic_prefix = "gw".to_string();
    let mut mqtt_qos = 1_u8;
    let mut req_timeout_ms = 8000_u64;
    let mut device_config_path = "dev.toml".to_string();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == "-h" || arg == "--help" {
            return Ok(CliMode::Help);
        } else if arg == "-V" || arg == "--version" {
            return Ok(CliMode::Version);
        } else if arg == "--addr" {
            let v = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("--addr requires value"))?;
            addr = v;
        } else if arg == "--device-config" {
            let v = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("--device-config requires value"))?;
            device_config_path = v;
        } else if arg == "--mqtt-client-id" {
            mqtt_client_id = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("--mqtt-client-id requires value"))?;
        } else if arg == "--mqtt-topic-prefix" {
            mqtt_topic_prefix = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("--mqtt-topic-prefix requires value"))?;
        } else if arg == "--mqtt-qos" {
            let raw = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("--mqtt-qos requires value"))?;
            mqtt_qos = raw
                .parse::<u8>()
                .map_err(|_| anyhow::anyhow!("invalid --mqtt-qos: {raw}"))?;
            if mqtt_qos > 2 {
                anyhow::bail!("invalid --mqtt-qos: {mqtt_qos} (expected 0/1/2)");
            }
        } else if arg == "--req-timeout-ms" {
            let raw = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("--req-timeout-ms requires value"))?;
            req_timeout_ms = raw
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!("invalid --req-timeout-ms: {raw}"))?;
        } else {
            anyhow::bail!("unknown arg: {arg}\n\n{}", cli_usage_text());
        }
    }
    let (mqtt_host, mqtt_port) = split_host_port(&addr)?;

    Ok(CliMode::Run(Config {
        mqtt_host,
        mqtt_port,
        mqtt_client_id,
        mqtt_topic_prefix,
        mqtt_qos,
        req_timeout_ms,
        device_config_path,
    }))
}

/// 解析 `host:port` 格式地址。
fn split_host_port(addr: &str) -> anyhow::Result<(String, u16)> {
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("invalid --addr: {addr}"))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| anyhow::anyhow!("invalid port in --addr: {addr}"))?;
    if host.trim().is_empty() {
        anyhow::bail!("invalid host in --addr: {addr}");
    }
    Ok((host.to_string(), port))
}

/// 返回 CLI 启动参数帮助文本。
pub fn cli_usage_text() -> &'static str {
    "usage: cli [OPTIONS]\n\
options:\n\
  --addr <host:port>                MQTT 地址，默认 127.0.0.1:18830\n\
  --device-config <path>            设备配置文件路径，默认 dev.toml\n\
  --mqtt-client-id <id>             MQTT 客户端 ID，默认随机生成（cli-<pid>-<time>）\n\
  --mqtt-topic-prefix <prefix>      MQTT 主题前缀，默认 gw\n\
  --mqtt-qos <0|1|2>                MQTT QoS，默认 1\n\
  --req-timeout-ms <ms>             请求等待超时，默认 8000\n\
  -h, --help                        显示帮助\n\
  -V, --version                     显示版本"
}

/// 生成默认 MQTT 客户端 ID（每次启动随机变化）。
fn generate_random_client_id() -> String {
    let now_nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(v) => v.as_nanos(),
        Err(_) => 0,
    };
    format!("cli-{}-{now_nanos:x}", std::process::id())
}

/// 打印 CLI 启动参数帮助信息。
pub fn print_cli_help() {
    println!("{}", cli_usage_text());
}

/// 打印 CLI 版本信息。
pub fn print_cli_version() {
    println!("cli {}", env!("CARGO_PKG_VERSION"));
}

#[cfg(test)]
mod tests {
    use super::{CliMode, parse_args};

    /// 验证帮助参数可切换到 Help 模式。
    #[test]
    fn parse_args_help_mode() {
        let mode = parse_args(vec!["--help".to_string()]).expect("parse --help should succeed");
        assert!(matches!(mode, CliMode::Help));
    }

    /// 验证地址和设备配置参数可正确解析。
    #[test]
    fn parse_args_run_mode_with_values() {
        let mode = parse_args(vec![
            "--addr".to_string(),
            "127.0.0.1:18830".to_string(),
            "--device-config".to_string(),
            "test-dev.toml".to_string(),
        ])
        .expect("parse args should succeed");
        match mode {
            CliMode::Run(cfg) => {
                assert_eq!(cfg.mqtt_host, "127.0.0.1");
                assert_eq!(cfg.mqtt_port, 18830);
                assert_eq!(cfg.device_config_path, "test-dev.toml");
                assert!(cfg.mqtt_client_id.starts_with("cli-"));
            }
            _ => panic!("expected CliMode::Run"),
        }
    }

    /// 验证未知参数会返回错误。
    #[test]
    fn parse_args_unknown_flag() {
        let err = parse_args(vec!["--bad-flag".to_string()]).expect_err("should be error");
        assert!(err.to_string().contains("unknown arg"));
    }
}
