use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use rumqttc::{AsyncClient, Event, Incoming, LastWill, MqttOptions, QoS};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio::time::sleep;

/// MQTT 控制响应结构。
#[derive(Debug, Clone, Deserialize)]
struct MqttControlResponse {
    req_id: String,
    ok: bool,
    resp: Value,
}

/// MQTT 遥测消息结构。
#[derive(Debug, Clone)]
pub struct TelemetryMessage {
    pub topic: String,
    pub payload: Vec<u8>,
}

/// CLI 的 MQTT 会话，负责请求发布与响应分发。
#[derive(Clone)]
pub struct MqttSession {
    client: AsyncClient,
    command_topic: String,
    cli_status_topic: String,
    client_id: String,
    qos: u8,
    req_seq: Arc<AtomicU64>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<MqttControlResponse>>>>,
    telemetry_tx: broadcast::Sender<TelemetryMessage>,
}

impl MqttSession {
    /// 建立 MQTT 会话并启动后台事件循环任务。
    pub async fn connect(
        host: &str,
        port: u16,
        client_id: String,
        topic_prefix: &str,
        qos: u8,
    ) -> anyhow::Result<Self> {
        let cli_status_topic = format!("{topic_prefix}/cli/status/{client_id}");
        let mut options = MqttOptions::new(client_id.clone(), host, port);
        options.set_keep_alive(Duration::from_secs(20));
        // 支持较大的控制命令负载（例如大量参数区间展开后）。
        options.set_max_packet_size(1024 * 1024, 1024 * 1024);
        let offline_payload = serde_json::to_vec(&serde_json::json!({ "online": false }))?;
        options.set_last_will(LastWill::new(
            cli_status_topic.clone(),
            offline_payload,
            to_qos(qos),
            true,
        ));
        let (client, mut eventloop) = AsyncClient::new(options, 100);

        let response_topic = format!("{topic_prefix}/resp/{client_id}");
        let command_topic = format!("{topic_prefix}/cmd/{client_id}");
        client
            .subscribe(response_topic.clone(), to_qos(qos))
            .await
            .with_context(|| format!("subscribe response topic failed: {response_topic}"))?;

        let pending: Arc<Mutex<HashMap<String, oneshot::Sender<MqttControlResponse>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pending_for_loop = pending.clone();
        let (telemetry_tx, _) = broadcast::channel::<TelemetryMessage>(1024);
        let telemetry_tx_for_loop = telemetry_tx.clone();
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        if publish.topic == response_topic {
                            let Ok(message) =
                                serde_json::from_slice::<MqttControlResponse>(&publish.payload)
                            else {
                                continue;
                            };
                            if let Some(tx) = pending_for_loop.lock().await.remove(&message.req_id) {
                                let _ = tx.send(message);
                            }
                        } else {
                            let _ = telemetry_tx_for_loop.send(TelemetryMessage {
                                topic: publish.topic,
                                payload: publish.payload.to_vec(),
                            });
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("(warn) mqtt eventloop error: {e}");
                        sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        });

        publish_cli_status(&client, &cli_status_topic, qos, true).await?;

        Ok(Self {
            client,
            command_topic,
            cli_status_topic,
            client_id,
            qos,
            req_seq: Arc::new(AtomicU64::new(1)),
            pending,
            telemetry_tx,
        })
    }

    /// 发送控制请求并等待对应响应。
    pub async fn request(
        &self,
        cmd: &str,
        args: Value,
        timeout_ms: u64,
        qos: u8,
    ) -> anyhow::Result<Value> {
        let req_id = format!(
            "{}-{}",
            self.client_id,
            self.req_seq.fetch_add(1, Ordering::Relaxed)
        );
        let payload = serde_json::json!({
            "req_id": req_id,
            "client_id": self.client_id,
            "cmd": cmd,
            "args": args
        });
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(req_id.clone(), tx);

        if let Err(e) = self
            .client
            .publish(
                self.command_topic.clone(),
                to_qos(qos),
                false,
                serde_json::to_vec(&payload)?,
            )
            .await
        {
            self.pending.lock().await.remove(&req_id);
            return Err(e).context("publish command failed");
        }

        let response = match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(v)) => v,
            Ok(Err(_)) => {
                self.pending.lock().await.remove(&req_id);
                anyhow::bail!("response channel closed");
            }
            Err(_) => {
                self.pending.lock().await.remove(&req_id);
                anyhow::bail!("wait response timeout");
            }
        };
        if !response.ok {
            return Ok(response.resp);
        }
        Ok(response.resp)
    }

    /// 订阅指定 MQTT 主题。
    pub async fn subscribe_topic(&self, topic: &str, qos: u8) -> anyhow::Result<()> {
        self.client
            .subscribe(topic, to_qos(qos))
            .await
            .with_context(|| format!("subscribe topic failed: {topic}"))?;
        Ok(())
    }

    /// 取消订阅指定 MQTT 主题。
    pub async fn unsubscribe_topic(&self, topic: &str) -> anyhow::Result<()> {
        self.client
            .unsubscribe(topic)
            .await
            .with_context(|| format!("unsubscribe topic failed: {topic}"))?;
        Ok(())
    }

    /// 创建一个遥测消息订阅接收器。
    pub fn telemetry_receiver(&self) -> broadcast::Receiver<TelemetryMessage> {
        self.telemetry_tx.subscribe()
    }

    /// 主动标记 CLI 离线并断开 MQTT 连接。
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        publish_cli_status(&self.client, &self.cli_status_topic, self.qos, false).await?;
        self.client
            .disconnect()
            .await
            .with_context(|| "disconnect mqtt client failed")?;
        Ok(())
    }
}

/// 将整数 QoS 映射到 `rumqttc::QoS`。
fn to_qos(qos: u8) -> QoS {
    match qos {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

/// 发布 CLI 在线状态（保留消息），用于 broker 感知连接生命周期。
async fn publish_cli_status(
    client: &AsyncClient,
    topic: &str,
    qos: u8,
    online: bool,
) -> anyhow::Result<()> {
    let payload = serde_json::to_vec(&serde_json::json!({ "online": online }))?;
    client
        .publish(topic, to_qos(qos), true, payload)
        .await
        .with_context(|| format!("publish cli status failed: {topic}"))?;
    Ok(())
}
