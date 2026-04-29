use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use serde::Deserialize;
use serde_json::json;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Duration, sleep},
};
use tracing::{error, info, warn};

use crate::{
    cli::MqttBridgeConfig,
    command::BrokerCommand,
    control::{execute_command_via_api, resp_to_json_value},
    state::AppState,
};

/// MQTT 发布事件，表示一条待发送到嵌入 broker 的遥测消息。
#[derive(Debug, Clone)]
pub struct MqttPublishEvent {
    pub device_id: String,
    pub sub_topic: String,
    pub full_topic: Option<String>,
    pub payload: Vec<u8>,
}

/// MQTT 桥接任务句柄，用于优雅停机。
pub struct MqttWorkerHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl MqttWorkerHandle {
    /// 发送停机信号并等待后台桥接任务退出。
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Err(e) = self.join_handle.await {
            error!(error = ?e, "mqtt bridge worker join failed");
        }
    }
}

/// MQTT 控制请求结构，承载来自客户端的控制命令。
#[derive(Debug, Clone, Deserialize)]
struct MqttControlRequest {
    req_id: Option<String>,
    client_id: Option<String>,
    cmd: String,
    #[serde(default)]
    args: serde_json::Value,
}

/// CLI 在线状态上报结构。
#[derive(Debug, Clone, Deserialize)]
struct CliStatusPayload {
    online: bool,
}

/// 启动 MQTT 桥接任务（遥测发布 + 命令订阅），并返回句柄。
pub fn start_mqtt_worker(
    cfg: MqttBridgeConfig,
    state: Arc<AppState>,
    rx: mpsc::Receiver<MqttPublishEvent>,
) -> Option<MqttWorkerHandle> {
    if !cfg.enabled {
        info!("mqtt bridge disabled by config");
        return None;
    }
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join_handle = tokio::spawn(async move {
        if let Err(e) = mqtt_worker_loop(cfg, state, rx, shutdown_rx).await {
            error!(error = ?e, "mqtt bridge worker exited with error");
        }
    });
    Some(MqttWorkerHandle {
        shutdown_tx: Some(shutdown_tx),
        join_handle,
    })
}

/// 运行 MQTT 桥接主循环，负责发布遥测并处理命令订阅。
async fn mqtt_worker_loop(
    cfg: MqttBridgeConfig,
    state: Arc<AppState>,
    mut rx: mpsc::Receiver<MqttPublishEvent>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let mut options = MqttOptions::new(cfg.client_id.clone(), cfg.host.clone(), cfg.port);
    options.set_keep_alive(Duration::from_secs(30));
    // 放宽 MQTT 包大小，避免大型 SUBCFG_SET 请求被丢弃。
    options.set_max_packet_size(1024 * 1024, 1024 * 1024);
    if let Some(username) = cfg.username.as_deref() {
        options.set_credentials(username, cfg.password.as_deref().unwrap_or(""));
    }

    let (client, mut eventloop) = AsyncClient::new(options, cfg.queue_capacity);
    let cmd_topic = format!("{}/cmd/#", cfg.topic_prefix);
    let cli_status_topic = format!("{}/cli/status/+", cfg.topic_prefix);
    let mut pending_cleanup_tasks: HashMap<String, JoinHandle<()>> = HashMap::new();
    info!(
        host = %cfg.host,
        port = cfg.port,
        topic_prefix = %cfg.topic_prefix,
        qos = cfg.qos,
        trace_topics = cfg.trace_topics,
        trace_payload_preview_bytes = cfg.trace_payload_preview_bytes,
        "mqtt bridge worker started"
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
                let topic = evt
                    .full_topic
                    .unwrap_or_else(|| format!("{}/{}/{}", cfg.topic_prefix, evt.device_id, evt.sub_topic));
                trace_topic_io(&cfg, "out", &topic, &evt.payload, "telemetry");
                if let Err(e) = client.publish(topic, to_qos(cfg.qos), false, evt.payload).await {
                    warn!(error = %e, "mqtt publish failed");
                }
            }
            poll = eventloop.poll() => {
                match poll {
                    Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                        info!("mqtt bridge connected");
                        if let Err(e) = client.subscribe(cmd_topic.clone(), to_qos(cfg.qos)).await {
                            warn!(error = %e, topic = %cmd_topic, "mqtt subscribe command topic failed");
                        } else {
                            info!(topic = %cmd_topic, "mqtt command topic subscribed");
                        }
                        if let Err(e) = client.subscribe(cli_status_topic.clone(), to_qos(cfg.qos)).await {
                            warn!(error = %e, topic = %cli_status_topic, "mqtt subscribe cli status topic failed");
                        } else {
                            info!(topic = %cli_status_topic, "mqtt cli status topic subscribed");
                        }
                    }
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        let payload = publish.payload.to_vec();
                        trace_topic_io(&cfg, "in", &publish.topic, &payload, "control_or_status");
                        if is_cli_status_topic(&cfg.topic_prefix, &publish.topic) {
                            handle_cli_status_message(
                                &cfg,
                                &state,
                                &publish.topic,
                                &payload,
                                &mut pending_cleanup_tasks,
                            );
                            continue;
                        }
                        if let Err(e) = handle_control_message(
                            &cfg,
                            &client,
                            &state,
                            &publish.topic,
                            &payload,
                            &mut pending_cleanup_tasks,
                        )
                        .await
                        {
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
        let topic = evt
            .full_topic
            .unwrap_or_else(|| format!("{}/{}/{}", cfg.topic_prefix, evt.device_id, evt.sub_topic));
        trace_topic_io(&cfg, "out", &topic, &evt.payload, "drain");
        let _ = client
            .publish(topic, to_qos(cfg.qos), false, evt.payload)
            .await
            .context("drain publish failed");
    }

    info!("mqtt bridge worker stopped");
    Ok(())
}

/// 判断是否为 CLI 在线状态主题。
fn is_cli_status_topic(topic_prefix: &str, topic: &str) -> bool {
    topic.starts_with(&format!("{topic_prefix}/cli/status/"))
}

/// 处理 CLI 在线状态消息，离线时清理会话与订阅。
fn handle_cli_status_message(
    cfg: &MqttBridgeConfig,
    state: &Arc<AppState>,
    topic: &str,
    payload: &[u8],
    pending_cleanup_tasks: &mut HashMap<String, JoinHandle<()>>,
) {
    let Some(client_id) = topic.strip_prefix(&format!("{}/cli/status/", cfg.topic_prefix)) else {
        return;
    };
    let Ok(status) = serde_json::from_slice::<CliStatusPayload>(payload) else {
        return;
    };
    cancel_cleanup_task(pending_cleanup_tasks, client_id);
    if status.online {
        state.touch_cli_session(client_id, "STATUS");
        return;
    }
    schedule_cleanup_task(
        pending_cleanup_tasks,
        state.clone(),
        client_id.to_string(),
        cfg.cli_offline_grace_secs,
    );
}

/// 处理 MQTT 控制命令消息：解析请求、执行控制命令并发布响应。
async fn handle_control_message(
    cfg: &MqttBridgeConfig,
    client: &AsyncClient,
    state: &Arc<AppState>,
    topic: &str,
    payload: &[u8],
    pending_cleanup_tasks: &mut HashMap<String, JoinHandle<()>>,
) -> anyhow::Result<()> {
    let request: MqttControlRequest = serde_json::from_slice(payload)
        .with_context(|| "parse mqtt control request json failed")?;
    let req_id = request.req_id.clone().unwrap_or_else(|| {
        format!(
            "req-{}",
            state.request_seq.load(std::sync::atomic::Ordering::Relaxed)
        )
    });
    let client_id = request
        .client_id
        .clone()
        .or_else(|| topic.rsplit('/').next().map(|v| v.to_string()))
        .unwrap_or_else(|| "default".to_string());
    cancel_cleanup_task(pending_cleanup_tasks, &client_id);
    state.touch_cli_session(&client_id, &request.cmd);
    let args = build_command_args(&request)?;
    let resp = execute_command_via_api(state, args).await;
    let ok = !matches!(resp, common::resp::RespValue::Error(_));
    let response_payload = json!({
        "req_id": req_id,
        "ok": ok,
        "resp": resp_to_json_value(&resp),
    });
    let resp_topic = format!("{}/resp/{}", cfg.topic_prefix, client_id);
    let resp_bytes = serde_json::to_vec(&response_payload)?;
    trace_topic_io(cfg, "out", &resp_topic, &resp_bytes, "control_response");
    client
        .publish(
            resp_topic,
            to_qos(cfg.qos),
            false,
            resp_bytes,
        )
        .await?;
    Ok(())
}

/// 记录 topic 方向日志（输入/输出），用于链路排查。
fn trace_topic_io(cfg: &MqttBridgeConfig, direction: &str, topic: &str, payload: &[u8], kind: &str) {
    if !cfg.trace_topics {
        return;
    }
    let preview = payload_preview(payload, cfg.trace_payload_preview_bytes);
    info!(
        direction = %direction,
        kind = %kind,
        topic = %topic,
        payload_len = payload.len(),
        payload_preview = %preview,
        "mqtt topic trace"
    );
}

/// 生成 payload 摘要：优先按 UTF-8 文本截断，避免日志过大。
fn payload_preview(payload: &[u8], max_bytes: usize) -> String {
    if payload.is_empty() || max_bytes == 0 {
        return String::new();
    }
    let take = payload.len().min(max_bytes);
    let mut text = String::from_utf8_lossy(&payload[..take]).to_string();
    text = text.replace('\n', "\\n").replace('\r', "\\r");
    if payload.len() > take {
        text.push_str("...(truncated)");
    }
    text
}

/// 取消指定 CLI 的延迟清理任务（若存在）。
fn cancel_cleanup_task(tasks: &mut HashMap<String, JoinHandle<()>>, client_id: &str) {
    if let Some(handle) = tasks.remove(client_id) {
        handle.abort();
    }
}

/// 为离线 CLI 安排延迟清理，避免瞬断导致误删订阅。
fn schedule_cleanup_task(
    tasks: &mut HashMap<String, JoinHandle<()>>,
    state: Arc<AppState>,
    client_id: String,
    grace_secs: u64,
) {
    if let Some(handle) = tasks.remove(&client_id) {
        handle.abort();
    }
    let task_client_id = client_id.clone();
    let handle = tokio::spawn(async move {
        sleep(Duration::from_secs(grace_secs)).await;
        let removed = state.cleanup_cli_session(&task_client_id);
        info!(
            client_id = %task_client_id,
            grace_secs = grace_secs,
            removed_subscriptions = removed,
            "cli offline grace elapsed, cleaned related topics"
        );
    });
    tasks.insert(client_id, handle);
}

/// 将 MQTT 控制请求转换为控制命令参数列表。
fn build_command_args(request: &MqttControlRequest) -> anyhow::Result<Vec<String>> {
    let Some(cmd) = BrokerCommand::parse(&request.cmd) else {
        return Err(anyhow::anyhow!("unsupported cmd: {}", request.cmd.trim()));
    };
    let mut args = vec![cmd.as_str().to_string()];
    match cmd {
        BrokerCommand::Ping | BrokerCommand::List | BrokerCommand::CliList => {}
        BrokerCommand::Connect => {
            let sim_addr = request
                .args
                .get("sim_addr")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("CONNECT requires args.sim_addr"))?;
            args.push(sim_addr.to_string());
        }
        BrokerCommand::Status | BrokerCommand::Kick => {
            let device_id = request
                .args
                .get("device_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("{} requires args.device_id", cmd.as_str()))?;
            args.push(device_id.to_string());
        }
        BrokerCommand::Key => {
            let device_id = request
                .args
                .get("device_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("KEY requires args.device_id"))?;
            args.push(device_id.to_string());
            if let Some(param_ids) = request.args.get("param_ids").and_then(|v| v.as_array()) {
                for item in param_ids {
                    let Some(param_id) = item.as_str() else {
                        return Err(anyhow::anyhow!("KEY args.param_ids must be string array"));
                    };
                    args.push(param_id.to_string());
                }
            }
        }
        BrokerCommand::Send => {
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
        BrokerCommand::SubcfgSet => {
            let client_id = request
                .args
                .get("client_id")
                .and_then(|v| v.as_str())
                .or(request.client_id.as_deref())
                .ok_or_else(|| anyhow::anyhow!("SUBCFG_SET requires args.client_id or request.client_id"))?;
            let device_id = request
                .args
                .get("device_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("SUBCFG_SET requires args.device_id"))?;
            let param_ids = request
                .args
                .get("param_ids")
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow::anyhow!("SUBCFG_SET requires args.param_ids(array)"))?;
            args.push(client_id.to_string());
            args.push(device_id.to_string());
            for item in param_ids {
                let Some(param_id) = item.as_str() else {
                    return Err(anyhow::anyhow!("SUBCFG_SET args.param_ids must be string array"));
                };
                args.push(param_id.to_string());
            }
        }
        BrokerCommand::SubcfgGet | BrokerCommand::SubcfgDel => {
            let client_id = request
                .args
                .get("client_id")
                .and_then(|v| v.as_str())
                .or(request.client_id.as_deref())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "{} requires args.client_id or request.client_id",
                        cmd.as_str()
                    )
                })?;
            let device_id = request
                .args
                .get("device_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("{} requires args.device_id", cmd.as_str()))?;
            args.push(client_id.to_string());
            args.push(device_id.to_string());
        }
        BrokerCommand::SubcfgList => {
            if let Some(client_id) = request.args.get("client_id").and_then(|v| v.as_str()) {
                args.push(client_id.to_string());
            } else if let Some(client_id) = request.client_id.as_deref() {
                args.push(client_id.to_string());
            }
        }
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{MqttControlRequest, build_command_args};

    /// 验证 KEY 命令应把 `param_ids` 数组转换为顺序参数列表。
    #[test]
    fn build_command_args_should_expand_key_param_ids() {
        let req = MqttControlRequest {
            req_id: None,
            client_id: Some("cli-1".to_string()),
            cmd: "KEY".to_string(),
            args: json!({
                "device_id": "dev-1",
                "param_ids": ["A00001", "A00002"]
            }),
        };
        let got = build_command_args(&req).expect("key request should be valid");
        assert_eq!(got, vec!["KEY", "dev-1", "A00001", "A00002"]);
    }

    /// 验证 SUBCFG_SET 在未提供 args.client_id 时应回退到 request.client_id。
    #[test]
    fn build_command_args_should_fallback_client_id_for_subcfg_set() {
        let req = MqttControlRequest {
            req_id: None,
            client_id: Some("cli-9".to_string()),
            cmd: "SUBCFG_SET".to_string(),
            args: json!({
                "device_id": "dev-9",
                "param_ids": ["A00010", "A00011"]
            }),
        };
        let got = build_command_args(&req).expect("subcfg_set request should be valid");
        assert_eq!(got, vec!["SUBCFG_SET", "cli-9", "dev-9", "A00010", "A00011"]);
    }

    /// 验证未知命令应返回错误，避免误执行到默认分支。
    #[test]
    fn build_command_args_should_fail_for_unknown_cmd() {
        let req = MqttControlRequest {
            req_id: None,
            client_id: None,
            cmd: "BAD_CMD".to_string(),
            args: json!({}),
        };
        let err = build_command_args(&req).expect_err("unknown cmd should fail");
        assert!(err.to_string().contains("unsupported cmd"));
    }
}
