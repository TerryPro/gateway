use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use common::device_proto::{DeviceCodec, Frame, MsgType, payload_as_text};
use common::tsmeta::is_valid_param_code;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
    time::timeout,
};
use tokio_util::codec::Framed;
use tracing::{error, info, warn};

use crate::archive::ArchiveEvent;
use crate::mqtt_bridge::MqttPublishEvent;
use crate::state::{AppState, DeviceHandle, now_ts};

const MAX_MQTT_PAYLOAD_BYTES: usize = 8 * 1024;
const MAX_CLIENT_MQTT_PAYLOAD_BYTES: usize = 512 * 1024;
const CLIENT_PUBLISH_WAIT_MS: u64 = 20;

/// 主动连接 sim 并完成设备会话建立。
pub async fn connect_one_sim(state: Arc<AppState>, sim_addr: &str) -> Result<(), String> {
    let socket = TcpStream::connect(sim_addr)
        .await
        .map_err(|e| format!("connect {sim_addr} failed: {e}"))?;
    let peer = socket
        .peer_addr()
        .map_err(|e| format!("get peer addr failed: {e}"))?;
    let target = sim_addr.to_string();
    let state_for_task = state.clone();
    tokio::spawn(async move {
        if let Err(e) =
            handle_device_stream(socket, peer, state_for_task.clone(), target.clone()).await
        {
            state_for_task.pending_sims.remove(&target);
            state_for_task.sim_connections.remove(&target);
            error!(sim_addr = %target, error = ?e, "device stream error");
        }
    });
    Ok(())
}

/// 处理单个已建立的设备连接生命周期（Broker 作为客户端）。
async fn handle_device_stream(
    socket: TcpStream,
    peer: SocketAddr,
    state: Arc<AppState>,
    sim_addr: String,
) -> anyhow::Result<()> {
    let framed = Framed::new(socket, DeviceCodec);
    let (sink, mut stream) = framed.split();

    let first = timeout(Duration::from_secs(5), stream.next())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "wait hello timeout"))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "peer closed before hello"))??;

    if first.msg_type != MsgType::Hello {
        anyhow::bail!("first frame must be HELLO");
    }
    let raw_device_id = payload_as_text(&first.payload)?.to_string();
    let device_id = raw_device_id.clone();

    if state.sim_connections.contains_key(&sim_addr) {
        state.pending_sims.remove(&sim_addr);
        anyhow::bail!("sim already connected: {sim_addr}");
    }
    if state.device_handles.contains_key(&device_id) {
        state.pending_sims.remove(&sim_addr);
        anyhow::bail!("device already connected: {device_id}");
    }

    let (tx, rx) = mpsc::channel::<Frame>(256);
    let (cancel_tx, cancel_rx) = watch::channel(false);

    let handle = DeviceHandle {
        tx: tx.clone(),
        cancel_tx: cancel_tx.clone(),
    };
    state.device_handles.insert(device_id.clone(), handle);

    state.mark_device_connected(&device_id, &sim_addr);

    tokio::spawn(device_writer_task(sink, rx, cancel_rx.clone(), device_id.clone()));

    info!(
        device_id = %device_id,
        peer = %peer,
        sim_addr = %sim_addr,
        "device connected (outbound)"
    );
    let mut reader_cancel = cancel_rx;
    loop {
        tokio::select! {
            changed = reader_cancel.changed() => {
                if changed.is_ok() && *reader_cancel.borrow() {
                    break;
                }
            }
            msg = stream.next() => {
                let Some(msg) = msg else {
                    break;
                };
                let msg = msg?;
                if let Some(info) = state.all_devices.get_mut(&raw_device_id) {
                    info.last_seen_ts
                        .store(now_ts(), std::sync::atomic::Ordering::Relaxed);
                }
                on_device_frame(&state, &device_id, msg).await;
            }
        }
    }

    state.device_handles.remove(&device_id);
    state.mark_device_disconnected(&device_id);

    info!(
        device_id = %device_id,
        peer = %peer,
        sim_addr = %sim_addr,
        "device disconnected"
    );
    Ok(())
}

/// 设备写循环，接收下发消息并写入设备连接。
async fn device_writer_task(
    mut sink: futures_util::stream::SplitSink<Framed<TcpStream, DeviceCodec>, Frame>,
    mut rx: mpsc::Receiver<Frame>,
    mut cancel_rx: watch::Receiver<bool>,
    device_id: String,
) {
    loop {
        tokio::select! {
            changed = cancel_rx.changed() => {
                if changed.is_ok() && *cancel_rx.borrow() {
                    break;
                }
            }
            frame = rx.recv() => {
                let Some(frame) = frame else {
                    break;
                };
                if let Err(e) = sink.send(frame).await {
                    warn!(device_id = %device_id, error = %e, "send to device failed");
                    break;
                }
            }
        }
    }
    let _ = sink.close().await;
}

/// 处理设备上行帧。
async fn on_device_frame(state: &Arc<AppState>, device_id: &str, frame: Frame) {
    match frame.msg_type {
        MsgType::Telemetry => {
            let payload = frame.payload;
            let ts_ms = now_ms();
            let mqtt_points = collect_mqtt_points(&payload);
            for point in &mqtt_points {
                state.upsert_param_current_value(device_id, &point.id, point.value, ts_ms);
            }
            if let Some(tx) = state.archive_tx.as_ref() {
                let evt = ArchiveEvent {
                    device_id: device_id.to_string(),
                    ts_ms,
                    payload: payload.clone(),
                };
                if let Err(e) = tx.try_send(evt) {
                    warn!(device_id = %device_id, error = %e, "archive queue full, telemetry dropped");
                }
            }
            let mqtt_payloads = if state.enable_device_telemetry {
                build_mqtt_payloads(device_id, ts_ms, &mqtt_points)
            } else {
                Vec::new()
            };
            let mqtt_param_payloads = if state.enable_param_telemetry {
                build_mqtt_param_payloads(device_id, ts_ms, &mqtt_points)
            } else {
                Vec::new()
            };
            let mqtt_client_payloads =
                build_mqtt_client_payloads(state, device_id, ts_ms, &mqtt_points);
            if let Some(tx) = state.mqtt_tx.as_ref() {
                // 优先发送按 client_id 聚合后的订阅流，保证 CLI 关键数据路径。
                for (client_id, mqtt_payload) in mqtt_client_payloads {
                    let evt = MqttPublishEvent {
                        device_id: format!("{client_id}/{device_id}"),
                        sub_topic: "payload".to_string(),
                        full_topic: None,
                        payload: mqtt_payload,
                    };
                    match timeout(Duration::from_millis(CLIENT_PUBLISH_WAIT_MS), tx.send(evt)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            warn!(
                                device_id = %device_id,
                                client_id = %client_id,
                                error = %e,
                                "mqtt channel closed, client telemetry dropped"
                            );
                            break;
                        }
                        Err(_) => {
                            warn!(
                                device_id = %device_id,
                                client_id = %client_id,
                                wait_ms = CLIENT_PUBLISH_WAIT_MS,
                                "mqtt client telemetry enqueue timeout, payload dropped"
                            );
                            break;
                        }
                    }
                }
                for mqtt_payload in mqtt_payloads {
                    let evt = MqttPublishEvent {
                        device_id: device_id.to_string(),
                        sub_topic: "telemetry".to_string(),
                        full_topic: None,
                        payload: mqtt_payload,
                    };
                    if let Err(e) = tx.try_send(evt) {
                        warn!(device_id = %device_id, error = %e, "mqtt queue full, telemetry dropped");
                        break;
                    }
                }
                for (param_id, mqtt_payload) in mqtt_param_payloads {
                    let evt = MqttPublishEvent {
                        device_id: device_id.to_string(),
                        sub_topic: param_id.clone(),
                        full_topic: None,
                        payload: mqtt_payload,
                    };
                    if let Err(e) = tx.try_send(evt) {
                        warn!(device_id = %device_id, param_id = %param_id, error = %e, "mqtt queue full, param telemetry dropped");
                        break;
                    }
                }
            }
        }
        MsgType::CommandReply => {
            if let Some((_, sender)) = state.pending.remove(&frame.request_id) {
                let _ = sender.send(frame.payload);
            } else {
                warn!(
                    device_id = %device_id,
                    request_id = frame.request_id,
                    "orphan reply"
                );
            }
        }
        MsgType::Heartbeat => {}
        MsgType::Hello => {}
        MsgType::Command => {}
        MsgType::Error => {
            warn!(device_id = %device_id, payload = ?frame.payload, "device error frame");
        }
    }
}

/// MQTT 输出点位结构，表示单个参数值。
#[derive(Debug, Clone, Serialize)]
struct MqttPoint {
    id: String,
    value: f64,
}

/// MQTT 输出包结构，包含设备、时间戳和点位数组。
#[derive(Debug, Clone, Serialize)]
struct MqttTelemetry {
    device_id: String,
    ts_ms: u64,
    points: Vec<MqttPoint>,
}

/// MQTT 参数级输出包结构，表示单个参数的独立消息。
#[derive(Debug, Clone, Serialize)]
struct MqttParamTelemetry {
    device_id: String,
    ts_ms: u64,
    param_id: String,
    value: f64,
}

/// MQTT 客户端聚合输出包结构，表示按 client_id + device_id 过滤后的参数集合。
#[derive(Debug, Clone, Serialize)]
struct MqttClientTelemetry {
    client_id: String,
    device_id: String,
    ts_ms: u64,
    points: Vec<MqttPoint>,
}

/// 将设备上行 payload 提取为点位列表，供 MQTT 打包与参数级发布复用。
fn collect_mqtt_points(payload: &[u8]) -> Vec<MqttPoint> {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return Vec::new();
    };
    let Some(obj) = value.as_object() else {
        return Vec::new();
    };
    let mut points = Vec::with_capacity(obj.len());
    for (k, v) in obj {
        if !is_valid_param_code(k) {
            continue;
        }
        let Some(n) = v.as_f64() else {
            continue;
        };
        if !n.is_finite() {
            continue;
        }
        points.push(MqttPoint {
            id: k.to_ascii_uppercase(),
            value: n,
        });
    }
    points
}

/// 将点位列表转换为 MQTT 设备级分包 JSON 列表（保持原主题输出）。
fn build_mqtt_payloads(device_id: &str, ts_ms: u64, points: &[MqttPoint]) -> Vec<Vec<u8>> {
    if points.is_empty() {
        return Vec::new();
    }
    split_mqtt_payloads(device_id, ts_ms, points.to_vec(), MAX_MQTT_PAYLOAD_BYTES)
}

/// 将点位列表转换为参数级 MQTT JSON 列表（新增参数主题输出）。
fn build_mqtt_param_payloads(
    device_id: &str,
    ts_ms: u64,
    points: &[MqttPoint],
) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::with_capacity(points.len());
    for point in points {
        if let Some(bytes) = encode_mqtt_param_packet(device_id, ts_ms, point) {
            out.push((point.id.clone(), bytes));
        }
    }
    out
}

/// 按 client_id + device_id 的参数订阅关系构建聚合下发包。
fn build_mqtt_client_payloads(
    state: &Arc<AppState>,
    device_id: &str,
    ts_ms: u64,
    points: &[MqttPoint],
) -> Vec<(String, Vec<u8>)> {
    if points.is_empty() {
        return Vec::new();
    }
    let mut value_map = HashMap::<&str, f64>::with_capacity(points.len());
    for point in points {
        value_map.insert(point.id.as_str(), point.value);
    }

    let subscriptions = state.list_param_subscriptions_for_device(device_id);
    let mut out = Vec::new();
    for item in subscriptions {
        let mut selected_points = Vec::with_capacity(item.param_ids.len());
        for param_id in &item.param_ids {
            if let Some(value) = value_map.get(param_id.as_str()) {
                selected_points.push(MqttPoint {
                    id: param_id.clone(),
                    value: *value,
                });
            }
        }
        if selected_points.is_empty() {
            continue;
        }
        let payloads = split_client_mqtt_payloads(
            &item.client_id,
            device_id,
            ts_ms,
            selected_points,
            MAX_CLIENT_MQTT_PAYLOAD_BYTES,
        );
        for payload in payloads {
            out.push((item.client_id.clone(), payload));
        }
    }
    out
}

/// 将点位集合按目标字节上限切分成多个 MQTT JSON 包。
fn split_mqtt_payloads(
    device_id: &str,
    ts_ms: u64,
    points: Vec<MqttPoint>,
    max_bytes: usize,
) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut current = Vec::<MqttPoint>::new();
    for point in points {
        let mut candidate = current.clone();
        candidate.push(point.clone());
        if let Some(bytes) = encode_mqtt_packet(device_id, ts_ms, candidate.clone())
            && bytes.len() <= max_bytes
        {
            current = candidate;
            continue;
        }
        if !current.is_empty() {
            if let Some(bytes) = encode_mqtt_packet(device_id, ts_ms, current.clone()) {
                out.push(bytes);
            }
            current.clear();
        }
        if let Some(bytes) = encode_mqtt_packet(device_id, ts_ms, vec![point.clone()])
            && bytes.len() <= max_bytes
        {
            current.push(point);
        } else {
            warn!(
                device_id = %device_id,
                max_bytes = max_bytes,
                "single telemetry point exceeds mqtt max payload, point dropped"
            );
        }
    }
    if !current.is_empty()
        && let Some(bytes) = encode_mqtt_packet(device_id, ts_ms, current)
    {
        out.push(bytes);
    }
    out
}

/// 将客户端聚合点位按目标字节上限切分成多个 MQTT JSON 包。
fn split_client_mqtt_payloads(
    client_id: &str,
    device_id: &str,
    ts_ms: u64,
    points: Vec<MqttPoint>,
    max_bytes: usize,
) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut current = Vec::<MqttPoint>::new();
    for point in points {
        let mut candidate = current.clone();
        candidate.push(point.clone());
        if let Some(bytes) = encode_mqtt_client_packet(client_id, device_id, ts_ms, candidate.clone())
            && bytes.len() <= max_bytes
        {
            current = candidate;
            continue;
        }
        if !current.is_empty() {
            if let Some(bytes) = encode_mqtt_client_packet(client_id, device_id, ts_ms, current.clone()) {
                out.push(bytes);
            }
            current.clear();
        }
        if let Some(bytes) = encode_mqtt_client_packet(client_id, device_id, ts_ms, vec![point.clone()])
            && bytes.len() <= max_bytes
        {
            current.push(point);
        } else {
            warn!(
                client_id = %client_id,
                device_id = %device_id,
                max_bytes = max_bytes,
                "single client telemetry point exceeds mqtt max payload, point dropped"
            );
        }
    }
    if !current.is_empty()
        && let Some(bytes) = encode_mqtt_client_packet(client_id, device_id, ts_ms, current)
    {
        out.push(bytes);
    }
    out
}

/// 编码单个 MQTT JSON 包。
fn encode_mqtt_packet(device_id: &str, ts_ms: u64, points: Vec<MqttPoint>) -> Option<Vec<u8>> {
    let packet = MqttTelemetry {
        device_id: device_id.to_string(),
        ts_ms,
        points,
    };
    serde_json::to_vec(&packet).ok()
}

/// 编码单个参数级 MQTT JSON 包。
fn encode_mqtt_param_packet(device_id: &str, ts_ms: u64, point: &MqttPoint) -> Option<Vec<u8>> {
    let packet = MqttParamTelemetry {
        device_id: device_id.to_string(),
        ts_ms,
        param_id: point.id.clone(),
        value: point.value,
    };
    serde_json::to_vec(&packet).ok()
}

/// 编码单个客户端聚合 MQTT JSON 包。
fn encode_mqtt_client_packet(
    client_id: &str,
    device_id: &str,
    ts_ms: u64,
    points: Vec<MqttPoint>,
) -> Option<Vec<u8>> {
    let packet = MqttClientTelemetry {
        client_id: client_id.to_string(),
        device_id: device_id.to_string(),
        ts_ms,
        points,
    };
    serde_json::to_vec(&packet).ok()
}

/// 获取当前 Unix 时间戳（毫秒）。
fn now_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as u64,
        Err(_) => 0,
    }
}
