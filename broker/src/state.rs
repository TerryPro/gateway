use std::{
    collections::BTreeSet,
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
use crate::mqtt_bridge::MqttPublishEvent;

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

/// 客户端参数化订阅项，表示某客户端在某设备上的参数集合。
#[derive(Debug, Clone)]
pub struct ClientParamSubscription {
    pub client_id: String,
    pub device_id: String,
    pub param_ids: BTreeSet<String>,
}

/// CLI 客户端活跃信息。
#[derive(Debug, Clone)]
pub struct CliSessionInfo {
    pub client_id: String,
    pub last_seen_ts: u64,
    pub last_cmd: String,
}

/// 参数当前值缓存项。
#[derive(Debug, Clone)]
pub struct ParamCurrentValue {
    pub device_id: String,
    pub param_id: String,
    pub value: f64,
    pub ts_ms: u64,
}

/// 全局 Broker 状态。
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
    pub param_subscriptions: DashMap<String, ClientParamSubscription>,
    pub param_current_values: DashMap<(String, String), ParamCurrentValue>,
    pub cli_sessions: DashMap<String, CliSessionInfo>,
    pub enable_device_telemetry: bool,
    pub enable_param_telemetry: bool,
}

impl AppState {
    /// 创建 Broker 全局状态。
    pub fn new(
        all_devices: DashMap<String, DeviceInfo>,
        archive_tx: Option<mpsc::Sender<ArchiveEvent>>,
        mqtt_tx: Option<mpsc::Sender<MqttPublishEvent>>,
        enable_device_telemetry: bool,
        enable_param_telemetry: bool,
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
            param_subscriptions: DashMap::new(),
            param_current_values: DashMap::new(),
            cli_sessions: DashMap::new(),
            enable_device_telemetry,
            enable_param_telemetry,
        }
    }

    /// 生成参数化订阅的唯一键（client_id + device_id）。
    fn subscription_key(client_id: &str, device_id: &str) -> String {
        format!("{client_id}::{device_id}")
    }

    /// 新增或覆盖参数化订阅，返回最终参数数量。
    pub fn upsert_param_subscription(
        &self,
        client_id: &str,
        device_id: &str,
        param_ids: impl IntoIterator<Item = String>,
    ) -> usize {
        let mut normalized = BTreeSet::new();
        for id in param_ids {
            let trimmed = id.trim().to_ascii_uppercase();
            if !trimmed.is_empty() {
                normalized.insert(trimmed);
            }
        }
        let key = Self::subscription_key(client_id, device_id);
        let item = ClientParamSubscription {
            client_id: client_id.to_string(),
            device_id: device_id.to_string(),
            param_ids: normalized.clone(),
        };
        self.param_subscriptions.insert(key, item);
        normalized.len()
    }

    /// 删除参数化订阅，返回是否存在并删除成功。
    pub fn remove_param_subscription(&self, client_id: &str, device_id: &str) -> bool {
        let key = Self::subscription_key(client_id, device_id);
        self.param_subscriptions.remove(&key).is_some()
    }

    /// 获取参数化订阅详情。
    pub fn get_param_subscription(
        &self,
        client_id: &str,
        device_id: &str,
    ) -> Option<ClientParamSubscription> {
        let key = Self::subscription_key(client_id, device_id);
        self.param_subscriptions.get(&key).map(|v| v.clone())
    }

    /// 按设备筛选全部参数化订阅项，供遥测分发复用。
    pub fn list_param_subscriptions_for_device(
        &self,
        device_id: &str,
    ) -> Vec<ClientParamSubscription> {
        self.param_subscriptions
            .iter()
            .filter(|entry| entry.device_id == device_id)
            .map(|entry| entry.clone())
            .collect()
    }

    /// 列出参数化订阅项，可按 client_id 过滤。
    pub fn list_param_subscriptions(
        &self,
        client_id: Option<&str>,
    ) -> Vec<ClientParamSubscription> {
        self.param_subscriptions
            .iter()
            .filter(|entry| {
                if let Some(filter_client_id) = client_id {
                    entry.client_id == filter_client_id
                } else {
                    true
                }
            })
            .map(|entry| entry.clone())
            .collect()
    }

    /// 更新单个参数当前值缓存（device_id + param_id）。
    pub fn upsert_param_current_value(&self, device_id: &str, param_id: &str, value: f64, ts_ms: u64) {
        let normalized_param_id = param_id.trim().to_ascii_uppercase();
        if normalized_param_id.is_empty() {
            return;
        }
        let key = (device_id.to_string(), normalized_param_id.clone());
        self.param_current_values.insert(
            key,
            ParamCurrentValue {
                device_id: device_id.to_string(),
                param_id: normalized_param_id,
                value,
                ts_ms,
            },
        );
    }

    /// 查询单个参数当前值缓存。
    pub fn get_param_current_value(&self, device_id: &str, param_id: &str) -> Option<ParamCurrentValue> {
        let normalized_param_id = param_id.trim().to_ascii_uppercase();
        let key = (device_id.to_string(), normalized_param_id);
        self.param_current_values.get(&key).map(|entry| entry.clone())
    }

    /// 查询设备下全部参数当前值缓存。
    pub fn list_param_current_values_for_device(&self, device_id: &str) -> Vec<ParamCurrentValue> {
        self.param_current_values
            .iter()
            .filter(|entry| entry.device_id == device_id)
            .map(|entry| entry.clone())
            .collect()
    }

    /// 刷新 CLI 会话活跃信息。
    pub fn touch_cli_session(&self, client_id: &str, cmd: &str) {
        let ts = now_ts();
        if let Some(mut entry) = self.cli_sessions.get_mut(client_id) {
            entry.last_seen_ts = ts;
            entry.last_cmd = cmd.to_ascii_uppercase();
            return;
        }
        self.cli_sessions.insert(
            client_id.to_string(),
            CliSessionInfo {
                client_id: client_id.to_string(),
                last_seen_ts: ts,
                last_cmd: cmd.to_ascii_uppercase(),
            },
        );
    }

    /// 列出当前已登记的 CLI 会话信息。
    pub fn list_cli_sessions(&self) -> Vec<CliSessionInfo> {
        self.cli_sessions.iter().map(|entry| entry.clone()).collect()
    }

    /// 清理指定 CLI 会话及其关联参数订阅，返回被删除的参数订阅数量。
    pub fn cleanup_cli_session(&self, client_id: &str) -> usize {
        self.cli_sessions.remove(client_id);
        let mut keys = Vec::new();
        for entry in self.param_subscriptions.iter() {
            if entry.client_id == client_id {
                keys.push(entry.key().clone());
            }
        }
        let removed = keys.len();
        for key in keys {
            self.param_subscriptions.remove(&key);
        }
        removed
    }

    /// 标记设备连接已建立：同步更新设备在线状态与连接映射。
    pub fn mark_device_connected(&self, device_id: &str, sim_addr: &str) {
        if let Some(mut info) = self.all_devices.get_mut(device_id) {
            info.online = true;
            info.last_seen_ts
                .store(now_ts(), std::sync::atomic::Ordering::Relaxed);
        }
        self.device_to_sim
            .insert(device_id.to_string(), sim_addr.to_string());
        self.sim_connections
            .insert(sim_addr.to_string(), device_id.to_string());
        self.pending_sims.remove(sim_addr);
    }

    /// 标记设备连接已断开：同步清理连接映射与在线状态。
    pub fn mark_device_disconnected(&self, device_id: &str) {
        if let Some((_, sim_addr)) = self.device_to_sim.remove(device_id) {
            self.sim_connections.remove(&sim_addr);
            self.pending_sims.remove(&sim_addr);
        }
        if let Some(mut info) = self.all_devices.get_mut(device_id) {
            info.online = false;
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
