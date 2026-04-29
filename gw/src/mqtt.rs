use std::sync::Arc;

use anyhow::Context;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use serde::Deserialize;
use serde_json::json;
use tokio::{
    sync::{mpsc, oneshot},
    time::{Duration, sleep},
};
use tracing::{error, info, warn};

use crate::{
    cli::MqttConfig,
    control::{execute_command_via_api, resp_to_json_value},
    state::AppState,
};

/// MQTT 发布事件，表示一条待发送到 broker 的遥测消息。
#[derive(Debug, Clone)]
pub struct MqttPublishEvent {
    pub device_id: String,
    pub sub_topic: String,
    pub payload: Vec<u8>,
}

/// MQTT 后台发布任务句柄，用于投递消息与优雅停机。
pub struct MqttWorkerHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl MqttWorkerHandle {
    /// 发送停机信号并等待后台发布任务退出。
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Err(e) = self.join_handle.await {
            error!(error = ?e, "mqtt worker join failed");
        }
    }
}

/// MQTT 控制请求结构，承载来自 Web 侧的控制命令。
#[derive(Debug, Clone, Deserialize)]
struct MqttControlRequest {
    req_id: Option<String>,
    client_id: Option<String>,
    cmd: String,
    #[serde(default)]
    args: serde_json::Value,
}

/// 启动 MQTT 后台任务（遥测发布 + 命令订阅），并返回句柄。
pub fn start_mqtt_worker(
    cfg: MqttConfig,
    state: Arc<AppState>,
    rx: mpsc::Receiver<MqttPublishEvent>,
) -> Option<MqttWorkerHandle> {
    if !cfg.enabled {
        info!("mqtt publisher disabled by config");
        return None;
    }
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join_handle = tokio::spawn(async move {
        if let Err(e) = mqtt_worker_loop(cfg, state, rx, shutdown_rx).await {
            error!(error = ?e, "mqtt worker exited with error");
        }
    });
    Some(MqttWorkerHandle {
        shutdown_tx: Some(shutdown_tx),
        join_handle,
    })
}

/// 运行 MQTT 主循环，负责发布遥测并处理命令订阅。
async fn mqtt_worker_loop(
    cfg: MqttConfig,
    state: Arc<AppState>,
    mut rx: mpsc::Receiver<MqttPublishEvent>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let mut options = MqttOptions::new(cfg.client_id.clone(), cfg.host.clone(), cfg.port);
    options.set_keep_alive(Duration::from_secs(30));
    if let Some(username) = cfg.username.as_deref() {
        options.set_credentials(username, cfg.password.as_deref().unwrap_or(""));
    }

    let (client, mut eventloop) = AsyncClient::new(options, cfg.queue_capacity);
    let cmd_topic = format!("{}/cmd/#", cfg.topic_prefix);
    info!(
        host = %cfg.host,
        port = cfg.port,
        topic_prefix = %cfg.topic_prefix,
        qos = cfg.qos,
        "mqtt worker started"
    );

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                break;
            }
            maybe_evt = rx.recv() => {
                let Some(evt) = maybe_evt else {
                    break;
                };
                let topic = format!("{}/{}/{}", cfg.topic_prefix, evt.device_id, evt.sub_topic);
                if let Err(e) = client.publish(topic, to_qos(cfg.qos), false, evt.payload).await {
                    warn!(error = %e, "mqtt publish failed");
                }
            }
            poll = eventloop.poll() => {
                match poll {
                    Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                        info!("mqtt connected");
                        if let Err(e) = client.subscribe(cmd_topic.clone(), to_qos(cfg.qos)).await {
                            warn!(error = %e, topic = %cmd_topic, "mqtt subscribe command topic failed");
                        } else {
                            info!(topic = %cmd_topic, "mqtt command topic subscribed");
                        }
                    }
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        let payload = publish.payload.to_vec();
                        if let Err(e) = handle_control_message(&cfg, &client, &state, &publish.topic, &payload).await {
                            warn!(error = ?e, topic = %publish.topic, "mqtt control message handling failed");
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        warn!(error = %e, "mqtt eventloop error, retrying");
                        sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        }
    }

    while let Ok(evt) = rx.try_recv() {
        let topic = format!("{}/{}/{}", cfg.topic_prefix, evt.device_id, evt.sub_topic);
        let _ = client
            .publish(topic, to_qos(cfg.qos), false, evt.payload)
            .await
            .context("drain publish failed");
    }

    info!("mqtt worker stopped");
    Ok(())
}

/// 处理 MQTT 控制命令消息：解析请求、执行控制命令并发布响应。
async fn handle_control_message(
    cfg: &MqttConfig,
    client: &AsyncClient,
    state: &Arc<AppState>,
    topic: &str,
    payload: &[u8],
) -> anyhow::Result<()> {
    let request: MqttControlRequest = serde_json::from_slice(payload)
        .with_context(|| "parse mqtt control request json failed")?;
    let req_id = request
        .req_id
        .clone()
        .unwrap_or_else(|| format!("req-{}", state.request_seq.load(std::sync::atomic::Ordering::Relaxed)));
    let client_id = request
        .client_id
        .clone()
        .or_else(|| topic.rsplit('/').next().map(|v| v.to_string()))
        .unwrap_or_else(|| "default".to_string());
    let args = build_command_args(&request)?;
    let resp = execute_command_via_api(state, args).await;
    let ok = !matches!(resp, common::resp::RespValue::Error(_));
    let response_payload = json!({
        "req_id": req_id,
        "ok": ok,
        "resp": resp_to_json_value(&resp),
    });
    let resp_topic = format!("{}/resp/{}", cfg.topic_prefix, client_id);
    client
        .publish(
            resp_topic,
            to_qos(cfg.qos),
            false,
            serde_json::to_vec(&response_payload)?,
        )
        .await?;
    Ok(())
}

/// 将 MQTT 控制请求转换为网关命令参数列表。
fn build_command_args(request: &MqttControlRequest) -> anyhow::Result<Vec<String>> {
    let cmd = request.cmd.trim().to_ascii_uppercase();
    let mut args = vec![cmd.clone()];
    match cmd.as_str() {
        "PING" | "LIST" => {}
        "CONNECT" => {
            let sim_addr = request
                .args
                .get("sim_addr")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("CONNECT requires args.sim_addr"))?;
            args.push(sim_addr.to_string());
        }
        "STATUS" | "KICK" => {
            let device_id = request
                .args
                .get("device_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("{cmd} requires args.device_id"))?;
            args.push(device_id.to_string());
        }
        "SEND" => {
            let device_id = request
                .args
                .get("device_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("SEND requires args.device_id"))?;
            let command_code = request
                .args
                .get("command_code")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("SEND requires args.command_code"))?;
            args.push(device_id.to_string());
            args.push(command_code.to_string());
            if let Some(timeout_ms) = request.args.get("timeout_ms").and_then(|v| v.as_u64()) {
                args.push(timeout_ms.to_string());
            }
        }
        _ => return Err(anyhow::anyhow!("unsupported cmd: {cmd}")),
    }
    Ok(args)
}

/// 将配置中的 QoS 数值转换为 `rumqttc::QoS`。
fn to_qos(qos: u8) -> QoS {
    match qos {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}
