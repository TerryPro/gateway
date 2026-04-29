use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// 服务总配置，覆盖 ingest/WAL/存储/查询接口。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub mqtt: MqttConfig,
    pub wal: WalConfig,
    pub mem: MemConfig,
    pub storage: StorageConfig,
    pub flush: FlushConfig,
    pub api: ApiConfig,
    pub ingest: IngestConfig,
}

/// MQTT 接入配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub topic: String,
    pub qos: u8,
}

/// WAL 配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    pub dir: String,
    pub file_prefix: String,
}

/// 最近热数据的内存窗口配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemConfig {
    pub window_sec: u64,
    /// 双缓冲行数阈值，默认 7200 行（约 1 小时，500ms 间隔）
    #[serde(default = "default_buffer_row_threshold")]
    pub buffer_row_threshold: usize,
    /// 双缓冲时间阈值（秒），默认 3600 秒（1 小时）
    #[serde(default = "default_buffer_flush_interval_sec")]
    pub buffer_flush_interval_sec: u64,
}

fn default_buffer_row_threshold() -> usize {
    7200
}

fn default_buffer_flush_interval_sec() -> u64 {
    3600
}

/// 落盘存储配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub root: String,
    pub segment_sec: u64,
}

/// Flush 调度配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushConfig {
    pub interval_ms: u64,
}

/// HTTP API 配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub listen: String,
}

/// ingest 通道配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestConfig {
    pub channel_capacity: usize,
}

impl AppConfig {
    /// 读取配置文件；若不存在则写入默认配置并返回默认值。
    pub fn load_or_create_default(path: &str) -> anyhow::Result<Self> {
        if Path::new(path).exists() {
            let s = std::fs::read_to_string(path).with_context(|| format!("read {}", path))?;
            if let Ok(cfg) = toml::from_str::<AppConfig>(&s) {
                return Ok(cfg);
            }
            let root =
                toml::from_str::<UnifiedRootConfig>(&s).with_context(|| format!("parse {}", path))?;
            let cfg = root
                .tsdbd
                .ok_or_else(|| anyhow::anyhow!("missing [tsdbd] section in {}", path))?;
            return Ok(cfg);
        }
        let cfg = Self::default();
        let text = toml::to_string_pretty(&UnifiedRootConfig {
            tsdbd: Some(cfg.clone()),
        })
        .context("serialize default config")?;
        std::fs::write(path, text).with_context(|| format!("write {}", path))?;
        Ok(cfg)
    }
}

/// 统一配置根结构：在 `config.toml` 下通过 `[tsdbd]` 子段承载服务配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UnifiedRootConfig {
    tsdbd: Option<AppConfig>,
}

impl Default for AppConfig {
    /// 提供可直接启动的默认配置模板。
    fn default() -> Self {
        Self {
            mqtt: MqttConfig {
                host: "127.0.0.1".to_string(),
                port: 1883,
                client_id: "tsdbd-dev".to_string(),
                topic: "gw/+/telemetry".to_string(),
                qos: 1,
            },
            wal: WalConfig {
                dir: "data/wal".to_string(),
                file_prefix: "wal".to_string(),
            },
            mem: MemConfig { 
                window_sec: 3600,
                buffer_row_threshold: 7200,
                buffer_flush_interval_sec: 3600,
            },
            storage: StorageConfig {
                root: "data/store".to_string(),
                segment_sec: 3600,
            },
            flush: FlushConfig { interval_ms: 10_000 },
            api: ApiConfig {
                listen: "127.0.0.1:8088".to_string(),
            },
            ingest: IngestConfig {
                channel_capacity: 4096,
            },
        }
    }
}
