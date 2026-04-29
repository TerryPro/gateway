use anyhow::Context;
use serde::Deserialize;
use std::{collections::HashMap, sync::OnceLock, time::UNIX_EPOCH};
use tokio::sync::RwLock;

/// 设备清单文件结构。
#[derive(Debug, Deserialize, Clone)]
struct DeviceListConfig {
    devices: Vec<DeviceEntry>,
}

/// 单设备配置项，描述设备 ID 与监听地址。
#[derive(Debug, Deserialize, Clone)]
struct DeviceEntry {
    id: String,
    ip: String,
    port: u16,
}

/// 设备配置缓存条目。
#[derive(Debug, Clone)]
struct CacheEntry {
    modified_ts_ms: u128,
    config: DeviceListConfig,
}

/// 返回全局设备配置缓存映射。
fn cache_map() -> &'static RwLock<HashMap<String, CacheEntry>> {
    static CACHE: OnceLock<RwLock<HashMap<String, CacheEntry>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// 读取配置文件修改时间戳（毫秒）。
async fn read_modified_ts_ms(config_path: &str) -> anyhow::Result<u128> {
    let metadata = tokio::fs::metadata(config_path)
        .await
        .with_context(|| format!("read config metadata failed: {config_path}"))?;
    let modified = metadata
        .modified()
        .with_context(|| format!("read config modify time failed: {config_path}"))?;
    let duration = modified
        .duration_since(UNIX_EPOCH)
        .with_context(|| format!("convert modify time failed: {config_path}"))?;
    Ok(duration.as_millis())
}

/// 加载设备配置（带基于 mtime 的缓存）。
async fn load_device_config(config_path: &str) -> anyhow::Result<DeviceListConfig> {
    let modified_ts_ms = read_modified_ts_ms(config_path).await?;
    {
        let cache = cache_map().read().await;
        if let Some(entry) = cache.get(config_path)
            && entry.modified_ts_ms == modified_ts_ms
        {
            return Ok(entry.config.clone());
        }
    }

    let text = tokio::fs::read_to_string(config_path)
        .await
        .with_context(|| format!("read device config failed: {config_path}"))?;
    let cfg = toml::from_str::<DeviceListConfig>(&text)
        .with_context(|| format!("parse toml failed: {config_path}"))?;
    {
        let mut cache = cache_map().write().await;
        cache.insert(
            config_path.to_string(),
            CacheEntry {
                modified_ts_ms,
                config: cfg.clone(),
            },
        );
    }
    Ok(cfg)
}

/// 从设备清单中查找指定设备的监听地址。
pub async fn find_device_addr(config_path: &str, device_id: &str) -> anyhow::Result<String> {
    let cfg = load_device_config(config_path).await?;

    let dev = cfg
        .devices
        .iter()
        .find(|d| d.id == device_id)
        .ok_or_else(|| anyhow::anyhow!("device id not found in config: {device_id}"))?;
    Ok(format!("{}:{}", dev.ip, dev.port))
}

/// 从设备清单中读取全部设备（设备 ID + 地址）。
pub async fn list_device_targets(config_path: &str) -> anyhow::Result<Vec<(String, String)>> {
    let cfg = load_device_config(config_path).await?;
    let mut out = Vec::with_capacity(cfg.devices.len());
    for dev in cfg.devices {
        out.push((dev.id, format!("{}:{}", dev.ip, dev.port)));
    }
    Ok(out)
}
