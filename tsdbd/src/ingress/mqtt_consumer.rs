use crate::config::MqttConfig;
use crate::model::{DataPoint, IngestBatch};
use anyhow::Context;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// `gw` 设备级 telemetry 消息结构。
#[derive(Debug, Clone, Deserialize)]
struct GwTelemetryPacket {
    device_id: String,
    ts_ms: u64,
    points: Vec<GwPoint>,
}

/// `gw` 设备级 telemetry 点位结构。
#[derive(Debug, Clone, Deserialize)]
struct GwPoint {
    id: String,
    value: f64,
}

/// 启动 MQTT 消费循环：订阅主题并把 payload 转成 ingest 批次。
pub async fn run_mqtt_consumer(
    cfg: MqttConfig,
    tx: mpsc::Sender<IngestBatch>,
) -> anyhow::Result<()> {
    let mut opts = MqttOptions::new(cfg.client_id.clone(), cfg.host.clone(), cfg.port);
    opts.set_keep_alive(std::time::Duration::from_secs(30));
    let (client, mut eventloop) = AsyncClient::new(opts, 1024);

    client
        .subscribe(cfg.topic, qos_from_u8(cfg.qos))
        .await
        .context("mqtt subscribe failed")?;
    info!("mqtt subscribed");

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(p))) => {
                if let Some(batch) = parse_publish_to_batch(&p.payload) {
                    if tx.send(batch).await.is_err() {
                        return Ok(());
                    }
                }
            }
            Ok(_) => {}
            Err(e) => {
                warn!("mqtt poll error: {}", e);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }
}

/// 将配置中的数字 QoS 转为 rumqttc 枚举。
fn qos_from_u8(v: u8) -> QoS {
    match v {
        0 => QoS::AtMostOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    }
}

/// 解析 publish payload（JSON）为 ingest 批次。
fn parse_publish_to_batch(payload: &[u8]) -> Option<IngestBatch> {
    parse_gw_telemetry_packet(payload)
}

/// 解析 `gw` 当前 telemetry 格式并转换为 `IngestBatch`。
fn parse_gw_telemetry_packet(payload: &[u8]) -> Option<IngestBatch> {
    let packet = serde_json::from_slice::<GwTelemetryPacket>(payload).ok()?;
    let mut points = Vec::with_capacity(packet.points.len());
    for p in packet.points {
        if !p.value.is_finite() {
            continue;
        }
        points.push(DataPoint {
            ts: packet.ts_ms,
            param_id: p.id,
            value: p.value as f32,
        });
    }
    if points.is_empty() {
        return None;
    }
    Some(IngestBatch {
        device_id: packet.device_id,
        recv_ts: packet.ts_ms,
        points,
    })
}
