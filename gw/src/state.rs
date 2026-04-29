use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use common::device_proto::Frame;
use dashmap::DashMap;
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot, watch};

use crate::archive::ArchiveEvent;
use crate::mqtt::MqttPublishEvent;

/// 设备清单文件结构。
#[derive(Debug, Deserialize)]
struct DeviceListConfig {
    devices: Vec<DeviceEntry>,
}

/// 单设备配置项。
#[derive(Debug, Deserialize, Clone)]
struct DeviceEntry {
    id: String,
    ip: String,
    port: u16,
}

/// 设备信息（包含配置和状态）。
#[derive(Clone)]
pub struct DeviceInfo {
    pub ip: String,
    pub port: u16,
    pub last_seen_ts: Arc<AtomicU64>,
    pub online: bool,
}

/// 单个设备会话句柄。
#[derive(Clone)]
pub struct DeviceHandle {
    pub tx: mpsc::Sender<Frame>,
    pub cancel_tx: watch::Sender<bool>,
}

/// 全局网关状态。
pub struct AppState {
    pub all_devices: DashMap<String, DeviceInfo>,
    pub device_handles: DashMap<String, DeviceHandle>,
    pub device_to_sim: DashMap<String, String>,
    pub sim_connections: DashMap<String, String>,
    pub pending_sims: DashMap<String, ()>,
    pub pending: DashMap<u32, oneshot::Sender<Vec<u8>>>,
    pub request_seq: AtomicU32,
    pub archive_tx: Option<mpsc::Sender<ArchiveEvent>>,
    pub mqtt_tx: Option<mpsc::Sender<MqttPublishEvent>>,
}

impl AppState {
    /// 创建网关全局状态。
    pub fn new(
        all_devices: DashMap<String, DeviceInfo>,
        archive_tx: Option<mpsc::Sender<ArchiveEvent>>,
        mqtt_tx: Option<mpsc::Sender<MqttPublishEvent>>,
    ) -> Self {
        Self {
            all_devices,
            device_handles: DashMap::new(),
            device_to_sim: DashMap::new(),
            sim_connections: DashMap::new(),
            pending_sims: DashMap::new(),
            pending: DashMap::new(),
            request_seq: AtomicU32::new(1),
            archive_tx,
            mqtt_tx,
        }
    }
}

/// 从设备配置文件加载所有设备信息。
pub async fn load_all_devices(config_path: &str) -> anyhow::Result<DashMap<String, DeviceInfo>> {
    let text = tokio::fs::read_to_string(config_path)
        .await
        .with_context(|| format!("read device config failed: {config_path}"))?;
    let cfg = toml::from_str::<DeviceListConfig>(&text)
        .with_context(|| format!("parse device config failed: {config_path}"))?;

    let map = DashMap::new();
    for dev in cfg.devices {
        let info = DeviceInfo {
            ip: dev.ip,
            port: dev.port,
            last_seen_ts: Arc::new(AtomicU64::new(0)),
            online: false,
        };
        map.insert(dev.id, info);
    }
    Ok(map)
}

/// 获取当前 Unix 时间戳（秒）。
pub fn now_ts() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => 0,
    }
}
