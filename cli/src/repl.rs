use std::{
    collections::HashMap,
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use common::resp::RespValue;
use crossterm::{
    cursor::MoveToColumn,
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    style::Print,
    terminal::{self, Clear, ClearType},
};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    cli::Config,
    device_config::{find_device_addr, list_device_targets},
    local_commands::{LocalCommandAction, handle_local_command},
    mqtt_client::{MqttSession, TelemetryMessage},
    resp_output::{print_clilist_resp, print_list_resp, print_resp},
};

/// 规范化后的远程命令请求。
struct RemoteCommand {
    cmd: String,
    args: Value,
    wait_timeout_ms: u64,
}

/// 本地订阅类型：整包遥测或参数聚合流。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalSubscriptionKind {
    Full,
    Param,
    Wildcard,
}

/// 本地订阅项，带可用于 UNSUB 的订阅 ID。
#[derive(Debug, Clone)]
struct LocalSubscription {
    id: String,
    kind: LocalSubscriptionKind,
    device_id: String,
    topic: String,
}

/// 管理当前 CLI 会话内的全部订阅。
struct LocalSubscriptions {
    items: HashMap<String, LocalSubscription>,
    key_to_id: HashMap<String, String>,
}

impl LocalSubscriptions {
    /// 创建空订阅表。
    fn new() -> Self {
        Self {
            items: HashMap::new(),
            key_to_id: HashMap::new(),
        }
    }

    /// 按业务键生成索引键（类型 + 设备）。
    fn make_key(kind: LocalSubscriptionKind, device_id: &str) -> String {
        let tag = match kind {
            LocalSubscriptionKind::Full => "full",
            LocalSubscriptionKind::Param => "param",
            LocalSubscriptionKind::Wildcard => "wildcard",
        };
        format!("{tag}:{device_id}")
    }

    /// 生成新的全局唯一订阅 ID（UUID v4）。
    fn new_subscription_id() -> String {
        Uuid::new_v4().to_string()
    }

    /// 新增或更新订阅，返回订阅 ID 和是否新建。
    fn upsert(&mut self, kind: LocalSubscriptionKind, device_id: &str, topic: String) -> (String, bool) {
        let key = Self::make_key(kind, device_id);
        if let Some(id) = self.key_to_id.get(&key).cloned() {
            if let Some(item) = self.items.get_mut(&id) {
                item.topic = topic;
            }
            return (id, false);
        }
        let id = Self::new_subscription_id();
        self.items.insert(
            id.clone(),
            LocalSubscription {
                id: id.clone(),
                kind,
                device_id: device_id.to_string(),
                topic,
            },
        );
        self.key_to_id.insert(key, id.clone());
        (id, true)
    }

    /// 通过订阅 ID 删除并返回订阅项。
    fn remove_by_id(&mut self, id: &str) -> Option<LocalSubscription> {
        let item = self.items.remove(id)?;
        let key = Self::make_key(item.kind, &item.device_id);
        self.key_to_id.remove(&key);
        Some(item)
    }

    /// 按类型+设备删除并返回订阅项。
    fn remove_by_kind_device(
        &mut self,
        kind: LocalSubscriptionKind,
        device_id: &str,
    ) -> Option<LocalSubscription> {
        let key = Self::make_key(kind, device_id);
        let id = self.key_to_id.remove(&key)?.to_string();
        self.items.remove(&id)
    }

    /// 返回全部订阅（按 ID 升序）。
    fn list(&self) -> Vec<&LocalSubscription> {
        let mut out = self.items.values().collect::<Vec<_>>();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    /// 清空并返回全部订阅项。
    fn drain_all(&mut self) -> Vec<LocalSubscription> {
        let items = self.items.drain().map(|(_, v)| v).collect::<Vec<_>>();
        self.key_to_id.clear();
        items
    }

    /// 返回当前本地参数订阅涉及的设备列表（去重）。
    fn param_device_ids(&self) -> Vec<String> {
        let mut out = Vec::<String>::new();
        for item in self.items.values() {
            if item.kind == LocalSubscriptionKind::Param && !out.contains(&item.device_id) {
                out.push(item.device_id.clone());
            }
        }
        out
    }
}

/// 运行 REPL 循环，支持实时遥测输出与按键驱动输入。
pub async fn run_repl_loop(cfg: &Config, session: &MqttSession) -> anyhow::Result<()> {
    let mut telemetry_rx = session.telemetry_receiver();
    let mut telemetry_subscriptions = LocalSubscriptions::new();
    let mut observed_topics = HashMap::<String, u64>::new();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let (key_tx, mut key_rx) = mpsc::unbounded_channel::<KeyEvent>();
    let stop_for_reader = stop_flag.clone();
    let key_reader = tokio::task::spawn_blocking(move || {
        while !stop_for_reader.load(Ordering::Relaxed) {
            let Ok(has_event) = event::poll(Duration::from_millis(100)) else {
                continue;
            };
            if !has_event {
                continue;
            }
            let Ok(Event::Key(key)) = event::read() else {
                continue;
            };
            if key.kind == KeyEventKind::Press && key_tx.send(key).is_err() {
                break;
            }
        }
    });

    terminal::enable_raw_mode()?;
    render_prompt("")?;
    let mut input_buffer = String::new();

    let result = async {
        loop {
            tokio::select! {
                key = key_rx.recv() => {
                    let Some(key) = key else {
                        break;
                    };
                    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('q') {
                        unsubscribe_all_and_clear_remote(cfg, session, &mut telemetry_subscriptions)
                            .await?;
                        println!();
                        println!(
                            "[local] Ctrl+Q -> unsubscribed all telemetry topics and cleared param subscriptions"
                        );
                        render_prompt(&input_buffer)?;
                        continue;
                    }
                    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
                        println!();
                        println!("received Ctrl+C, graceful shutdown");
                        break;
                    }
                    match key.code {
                        KeyCode::Enter => {
                            println!();
                            let line = input_buffer.trim().to_string();
                            input_buffer.clear();
                            let should_exit = process_input_line(
                                cfg,
                                session,
                                &line,
                                &mut telemetry_subscriptions,
                                &observed_topics,
                            )
                            .await?;
                            if should_exit {
                                break;
                            }
                            render_prompt(&input_buffer)?;
                        }
                        KeyCode::Backspace => {
                            input_buffer.pop();
                            render_prompt(&input_buffer)?;
                        }
                        KeyCode::Char(c) => {
                            if !key.modifiers.contains(KeyModifiers::CONTROL) {
                                input_buffer.push(c);
                                render_prompt(&input_buffer)?;
                            }
                        }
                        _ => {}
                    }
                }
                message = telemetry_rx.recv() => {
                    match message {
                        Ok(msg) => {
                            *observed_topics.entry(msg.topic.clone()).or_insert(0) += 1;
                            print_telemetry_and_restore_input(&msg, &input_buffer)?;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            println!();
                            println!("[telemetry] warning: skipped {skipped} messages");
                            render_prompt(&input_buffer)?;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    stop_flag.store(true, Ordering::Relaxed);
    let _ = key_reader.await;
    terminal::disable_raw_mode()?;
    result
}

/// 渲染当前输入提示符与编辑中的文本。
fn render_prompt(input: &str) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout();
    execute!(
        stdout,
        MoveToColumn(0),
        Clear(ClearType::CurrentLine),
        Print(format!("gw> {input}"))
    )?;
    stdout.flush()?;
    Ok(())
}

/// 打印一条遥测消息并恢复用户当前输入行。
fn print_telemetry_and_restore_input(msg: &TelemetryMessage, input: &str) -> anyhow::Result<()> {
    let payload = String::from_utf8_lossy(&msg.payload);
    println!();
    println!("[telemetry] topic={} payload={}", msg.topic, payload);
    render_prompt(input)
}

/// 处理单行输入，返回是否需要退出 REPL。
async fn process_input_line(
    cfg: &Config,
    session: &MqttSession,
    line: &str,
    telemetry_subscriptions: &mut LocalSubscriptions,
    observed_topics: &HashMap<String, u64>,
) -> anyhow::Result<bool> {
    if line.is_empty() {
        return Ok(false);
    }
    match handle_local_command(line) {
        LocalCommandAction::Exit => return Ok(true),
        LocalCommandAction::Continue => return Ok(false),
        LocalCommandAction::NotLocal => {}
    }

    let raw_cmd = parse_line_to_command(line);
    if raw_cmd.is_empty() {
        return Ok(false);
    }

    match handle_sub_unsub_command(
        cfg,
        session,
        &raw_cmd,
        telemetry_subscriptions,
        observed_topics,
    )
    .await
    {
        Ok(true) => return Ok(false),
        Ok(false) => {}
        Err(e) => {
            eprintln!("(error) {e}");
            return Ok(false);
        }
    }

    if raw_cmd.len() == 1 && raw_cmd[0].eq_ignore_ascii_case("CONA") {
        run_connect_all(cfg, session).await?;
        return Ok(false);
    }
    if raw_cmd.len() == 1 && raw_cmd[0].eq_ignore_ascii_case("KICA") {
        run_kick_all(cfg, session).await?;
        return Ok(false);
    }

    let command = match normalize_command(cfg, raw_cmd).await {
        Ok(v) => v,
        Err(e) => {
            let message = e.to_string();
            if let Some(unknown) = message.strip_prefix("unknown command: ") {
                eprintln!(
                    "(error) Unknown command: {}. Type HELP for available commands.",
                    unknown
                );
            } else {
                eprintln!("(error) {message}");
            }
            return Ok(false);
        }
    };
    let command_name = command.cmd.clone();
    let command_device_id = command
        .args
        .get("device_id")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    if let Err(e) = send_and_print(session, cfg, command).await {
        eprintln!("(error) {e}");
        return Ok(false);
    }
    if command_name == "SUBCFG_SET" {
        if let Some(device_id) = command_device_id {
            ensure_param_topic_subscribed(cfg, session, telemetry_subscriptions, &device_id).await?;
        }
    } else if command_name == "SUBCFG_DEL" && let Some(device_id) = command_device_id {
        try_unsubscribe_param_topic(cfg, session, telemetry_subscriptions, &device_id).await?;
    }
    Ok(false)
}

/// 取消当前所有遥测订阅。
async fn unsubscribe_all(
    _cfg: &Config,
    session: &MqttSession,
    subscriptions: &mut LocalSubscriptions,
) -> anyhow::Result<()> {
    for item in subscriptions.drain_all() {
        session.unsubscribe_topic(&item.topic).await?;
    }
    Ok(())
}

/// 取消本地全部遥测订阅并清理当前 client_id 的参数订阅配置。
async fn unsubscribe_all_and_clear_remote(
    cfg: &Config,
    session: &MqttSession,
    subscriptions: &mut LocalSubscriptions,
) -> anyhow::Result<()> {
    let param_device_ids = subscriptions.param_device_ids();
    unsubscribe_all(cfg, session, subscriptions).await?;
    clear_remote_param_subscriptions_for_devices(cfg, session, &param_device_ids).await
}

/// 处理本地 SUB/UNSUB 命令，返回是否已消费当前输入。
async fn handle_sub_unsub_command(
    cfg: &Config,
    session: &MqttSession,
    raw_cmd: &[String],
    subscriptions: &mut LocalSubscriptions,
    observed_topics: &HashMap<String, u64>,
) -> anyhow::Result<bool> {
    if raw_cmd.is_empty() {
        return Ok(true);
    }
    let verb = raw_cmd[0].to_ascii_uppercase();
    if verb == "SUB" {
        if raw_cmd.len() != 2 {
            anyhow::bail!("usage: SUB <device_id>");
        }
        let device_id = raw_cmd[1].clone();
        let topic = telemetry_topic(cfg, &device_id);
        session.subscribe_topic(&topic, cfg.mqtt_qos).await?;
        let (sub_id, created) =
            subscriptions.upsert(LocalSubscriptionKind::Full, &device_id, topic.clone());
        if created {
            println!("subscribed: id={} {} -> {}", sub_id, device_id, topic);
        } else {
            println!("updated subscription: id={} {} -> {}", sub_id, device_id, topic);
        }
        return Ok(true);
    }
    if verb == "SUBALL" {
        if raw_cmd.len() > 2 {
            anyhow::bail!("usage: SUBALL [topic_filter]");
        }
        let topic = if raw_cmd.len() == 2 {
            let filter = raw_cmd[1].trim();
            if filter.is_empty() {
                anyhow::bail!("invalid topic_filter: {}", raw_cmd[1]);
            }
            filter.to_string()
        } else {
            "#".to_string()
        };
        session.subscribe_topic(&topic, cfg.mqtt_qos).await?;
        let (sub_id, created) = subscriptions.upsert(
            LocalSubscriptionKind::Wildcard,
            &topic,
            topic.clone(),
        );
        if created {
            println!("subscribed all: id={} topic={}", sub_id, topic);
        } else {
            println!("updated all-subscription: id={} topic={}", sub_id, topic);
        }
        return Ok(true);
    }
    if verb == "UNSUB" {
        if raw_cmd.len() > 2 {
            anyhow::bail!("usage: UNSUB [subscription_id]");
        }
        if raw_cmd.len() == 1 {
            unsubscribe_all_and_clear_remote(cfg, session, subscriptions).await?;
            println!("unsubscribed all telemetry topics");
            return Ok(true);
        }
        let sub_id = raw_cmd[1].trim();
        if sub_id.is_empty() {
            anyhow::bail!("invalid subscription_id: {}", raw_cmd[1]);
        }
        let Some(item) = subscriptions.remove_by_id(sub_id) else {
            anyhow::bail!("subscription not found: {}", sub_id);
        };
        session.unsubscribe_topic(&item.topic).await?;
        println!(
            "unsubscribed: id={} kind={} device={} topic={}",
            item.id,
            kind_label(item.kind),
            item.device_id,
            item.topic
        );
        if item.kind == LocalSubscriptionKind::Param {
            remove_remote_param_subscription(cfg, session, &item.device_id).await?;
        }
        return Ok(true);
    }
    if verb == "PL" {
        print_local_subscriptions(subscriptions);
        return Ok(true);
    }
    if verb == "TL" || verb == "TOPICS" {
        print_observed_topics(observed_topics);
        return Ok(true);
    }
    Ok(false)
}

/// 生成设备遥测主题。
fn telemetry_topic(cfg: &Config, device_id: &str) -> String {
    format!("{}/{}/telemetry", cfg.mqtt_topic_prefix, device_id)
}

/// 生成按 client_id + device_id 聚合后的参数订阅输出主题。
fn param_payload_topic(cfg: &Config, device_id: &str) -> String {
    format!(
        "{}/{}/{}/payload",
        cfg.mqtt_topic_prefix, cfg.mqtt_client_id, device_id
    )
}

/// 确保参数聚合主题已订阅，避免仅配置了 PS 但收不到数据。
async fn ensure_param_topic_subscribed(
    cfg: &Config,
    session: &MqttSession,
    subscriptions: &mut LocalSubscriptions,
    device_id: &str,
) -> anyhow::Result<()> {
    let topic = param_payload_topic(cfg, device_id);
    session.subscribe_topic(&topic, cfg.mqtt_qos).await?;
    let (sub_id, created) = subscriptions.upsert(LocalSubscriptionKind::Param, device_id, topic.clone());
    if created {
        println!(
            "subscribed param stream: id={} {} -> {}",
            sub_id, device_id, topic
        );
    } else {
        println!(
            "updated param stream: id={} {} -> {}",
            sub_id, device_id, topic
        );
    }
    Ok(())
}

/// 尝试取消参数聚合主题订阅，用于删除参数订阅后的本地清理。
async fn try_unsubscribe_param_topic(
    cfg: &Config,
    session: &MqttSession,
    subscriptions: &mut LocalSubscriptions,
    device_id: &str,
) -> anyhow::Result<()> {
    let item = subscriptions
        .remove_by_kind_device(LocalSubscriptionKind::Param, device_id)
        .unwrap_or(LocalSubscription {
            id: String::new(),
            kind: LocalSubscriptionKind::Param,
            device_id: device_id.to_string(),
            topic: param_payload_topic(cfg, device_id),
        });
    session.unsubscribe_topic(&item.topic).await?;
    if !item.id.is_empty() {
        println!(
            "unsubscribed param stream: id={} {} -> {}",
            item.id, device_id, item.topic
        );
    } else {
        println!("unsubscribed param stream: {} -> {}", device_id, item.topic);
    }
    Ok(())
}

/// 将订阅类型转为可读标签。
fn kind_label(kind: LocalSubscriptionKind) -> &'static str {
    match kind {
        LocalSubscriptionKind::Full => "SUB",
        LocalSubscriptionKind::Param => "PS",
        LocalSubscriptionKind::Wildcard => "ALL",
    }
}

/// 输出当前会话的全部订阅列表。
fn print_local_subscriptions(subscriptions: &LocalSubscriptions) {
    let rows = subscriptions.list();
    if rows.is_empty() {
        println!("  (no subscriptions)");
        return;
    }
    println!("{:<36} {:<6} {:<12} TOPIC", "ID", "TYPE", "DEVICE");
    println!("{}", "-".repeat(112));
    for item in rows {
        println!(
            "{:<36} {:<6} {:<12} {}",
            item.id,
            kind_label(item.kind),
            item.device_id,
            item.topic
        );
    }
}

/// 输出当前会话已观测到的 topic 列表与消息计数。
fn print_observed_topics(observed_topics: &HashMap<String, u64>) {
    if observed_topics.is_empty() {
        println!("  (no topics observed yet)");
        return;
    }
    let mut rows = observed_topics.iter().collect::<Vec<_>>();
    rows.sort_by(|a, b| a.0.cmp(b.0));
    println!("{:<8} TOPIC", "COUNT");
    println!("{}", "-".repeat(96));
    for (topic, count) in rows {
        println!("{:<8} {}", count, topic);
    }
}

/// 批量连接设备清单中的全部设备。
async fn run_connect_all(cfg: &Config, session: &MqttSession) -> anyhow::Result<()> {
    let targets = list_device_targets(&cfg.device_config_path).await?;
    if targets.is_empty() {
        println!("no devices found in config");
        return Ok(());
    }
    for (device_id, addr) in targets {
        println!("CONA -> CONNECT {} ({})", device_id, addr);
        let command = RemoteCommand {
            cmd: "CONNECT".to_string(),
            args: json!({ "sim_addr": addr }),
            wait_timeout_ms: cfg.req_timeout_ms,
        };
        let value = send_command(session, cfg, command).await?;
        print_resp(&value);
    }
    Ok(())
}

/// 批量踢除设备清单中的全部设备。
async fn run_kick_all(cfg: &Config, session: &MqttSession) -> anyhow::Result<()> {
    let targets = list_device_targets(&cfg.device_config_path).await?;
    if targets.is_empty() {
        println!("no devices found in config");
        return Ok(());
    }
    for (device_id, _) in targets {
        println!("KICA -> KICK {}", device_id);
        let command = RemoteCommand {
            cmd: "KICK".to_string(),
            args: json!({ "device_id": device_id }),
            wait_timeout_ms: cfg.req_timeout_ms,
        };
        let value = send_command(session, cfg, command).await?;
        print_resp(&value);
    }
    Ok(())
}

/// 发送单条命令并根据命令类型输出响应。
async fn send_and_print(
    session: &MqttSession,
    cfg: &Config,
    command: RemoteCommand,
) -> anyhow::Result<()> {
    let cmd_name = command.cmd.clone();
    let value = send_command(session, cfg, command).await?;
    if cmd_name == "LIST" {
        print_list_resp(&value);
    } else if cmd_name == "CLILIST" {
        print_clilist_resp(&value);
    } else {
        print_resp(&value);
    }
    Ok(())
}

/// 发送单条命令并将 JSON 响应转换为 RESP 值。
async fn send_command(
    session: &MqttSession,
    cfg: &Config,
    command: RemoteCommand,
) -> anyhow::Result<RespValue> {
    let timeout_ms = command.wait_timeout_ms.max(cfg.req_timeout_ms);
    let raw = session
        .request(&command.cmd, command.args, timeout_ms, cfg.mqtt_qos)
        .await?;
    json_resp_to_resp_value(&raw)
}

/// 将一行输入解析为命令参数列表（空白符分隔）。
fn parse_line_to_command(line: &str) -> Vec<String> {
    line.split_whitespace()
        .map(|s| {
            s.chars()
                .filter(|c| !c.is_control() && *c != '\u{feff}')
                .collect::<String>()
        })
        .filter(|s| !s.is_empty())
        .collect()
}

/// 规范化命令关键字（支持别名）。
fn canonical_command(cmd: &str) -> String {
    match cmd.to_uppercase().as_str() {
        "CONN" => "CONNECT".to_string(),
        "STAT" => "STATUS".to_string(),
        "PS" => "SUBCFG_SET".to_string(),
        other => other.to_string(),
    }
}

/// 规范化命令，支持简化输入。
async fn normalize_command(cfg: &Config, mut command: Vec<String>) -> anyhow::Result<RemoteCommand> {
    if command.is_empty() {
        anyhow::bail!("empty command");
    }

    let cmd = command[0].to_uppercase();
    let canonical_cmd = canonical_command(&cmd);

    // CONN/CONNECT <device_id> -> CONNECT <ip:port>
    if canonical_cmd == "CONNECT" {
        if command.len() != 2 {
            anyhow::bail!("usage: CONN|CONNECT <device_id>");
        }
        let device_id = &command[1];
        let addr = find_device_addr(&cfg.device_config_path, device_id).await?;
        return Ok(RemoteCommand {
            cmd: "CONNECT".to_string(),
            args: json!({ "sim_addr": addr }),
            wait_timeout_ms: cfg.req_timeout_ms,
        });
    }

    // 所有远程命令统一按大写处理，确保大小写输入行为一致。
    command[0] = canonical_cmd;

    match command[0].as_str() {
        "PING" => Ok(RemoteCommand {
            cmd: "PING".to_string(),
            args: json!({}),
            wait_timeout_ms: cfg.req_timeout_ms,
        }),
        "LIST" => Ok(RemoteCommand {
            cmd: "LIST".to_string(),
            args: json!({}),
            wait_timeout_ms: cfg.req_timeout_ms,
        }),
        "CLILIST" => Ok(RemoteCommand {
            cmd: "CLILIST".to_string(),
            args: json!({}),
            wait_timeout_ms: cfg.req_timeout_ms,
        }),
        "KEY" => {
            if command.len() < 2 {
                anyhow::bail!("usage: KEY <device_id> [param_id...]");
            }
            let device_id = command[1].trim().to_string();
            if device_id.is_empty() {
                anyhow::bail!("device_id must not be empty");
            }
            let mut param_ids = Vec::<String>::new();
            for raw in command.iter().skip(2) {
                let param_id = raw.trim().to_ascii_uppercase();
                validate_param_code_like(&param_id)?;
                param_ids.push(param_id);
            }
            Ok(RemoteCommand {
                cmd: "KEY".to_string(),
                args: json!({
                    "device_id": device_id,
                    "param_ids": param_ids,
                }),
                wait_timeout_ms: cfg.req_timeout_ms,
            })
        }
        "STATUS" => {
            if command.len() != 2 {
                anyhow::bail!("usage: STAT|STATUS <device_id>");
            }
            Ok(RemoteCommand {
                cmd: "STATUS".to_string(),
                args: json!({ "device_id": command[1] }),
                wait_timeout_ms: cfg.req_timeout_ms,
            })
        }
        "KICK" => {
            if command.len() != 2 {
                anyhow::bail!("usage: KICK <device_id>");
            }
            Ok(RemoteCommand {
                cmd: "KICK".to_string(),
                args: json!({ "device_id": command[1] }),
                wait_timeout_ms: cfg.req_timeout_ms,
            })
        }
        "SEND" => {
            let (device_id, command_code, timeout_ms) = validate_send_command(&command)?;
            let wait_timeout_ms = timeout_ms.unwrap_or(cfg.req_timeout_ms).saturating_add(2000);
            let mut args = json!({
                "device_id": device_id,
                "command_code": command_code
            });
            if let Some(v) = timeout_ms {
                args["timeout_ms"] = json!(v);
            }
            Ok(RemoteCommand {
                cmd: "SEND".to_string(),
                args,
                wait_timeout_ms,
            })
        }
        "SUBCFG_SET" => {
            if command.len() < 3 {
                anyhow::bail!("usage: SUBCFG_SET <device_id> <param_id...>");
            }
            let param_ids = normalize_param_tokens(&command[2..])?;
            Ok(RemoteCommand {
                cmd: "SUBCFG_SET".to_string(),
                args: json!({
                    "client_id": cfg.mqtt_client_id,
                    "device_id": command[1],
                    "param_ids": param_ids,
                }),
                wait_timeout_ms: cfg.req_timeout_ms,
            })
        }
        "SUBCFG_DEL" => {
            if command.len() != 2 {
                anyhow::bail!("usage: SUBCFG_DEL <device_id>");
            }
            Ok(RemoteCommand {
                cmd: "SUBCFG_DEL".to_string(),
                args: json!({
                    "client_id": cfg.mqtt_client_id,
                    "device_id": command[1],
                }),
                wait_timeout_ms: cfg.req_timeout_ms,
            })
        }
        "SUBCFG_GET" => anyhow::bail!(
            "SUBCFG_GET has been removed, use PL to list subscriptions"
        ),
        "SUBCFG_LIST" => {
            if command.len() > 1 {
                anyhow::bail!("usage: SUBCFG_LIST");
            }
            Ok(RemoteCommand {
                cmd: "SUBCFG_LIST".to_string(),
                args: json!({ "client_id": cfg.mqtt_client_id }),
                wait_timeout_ms: cfg.req_timeout_ms,
            })
        }
        _ => anyhow::bail!("unknown command: {}", command[0]),
    }
}

/// 删除指定设备在当前 client_id 下的参数订阅配置。
async fn remove_remote_param_subscription(
    cfg: &Config,
    session: &MqttSession,
    device_id: &str,
) -> anyhow::Result<()> {
    let value = send_command(
        session,
        cfg,
        RemoteCommand {
            cmd: "SUBCFG_DEL".to_string(),
            args: json!({
                "client_id": cfg.mqtt_client_id,
                "device_id": device_id,
            }),
            wait_timeout_ms: cfg.req_timeout_ms,
        },
    )
    .await?;
    print_resp(&value);
    Ok(())
}

/// 按设备列表清理当前 client_id 下参数订阅配置。
async fn clear_remote_param_subscriptions_for_devices(
    cfg: &Config,
    session: &MqttSession,
    device_ids: &[String],
) -> anyhow::Result<()> {
    for device_id in device_ids {
        remove_remote_param_subscription(cfg, session, device_id).await?;
    }
    Ok(())
}

/// 规范化参数令牌列表，支持单点与区间混写且不在 CLI 侧展开区间。
fn normalize_param_tokens(tokens: &[String]) -> anyhow::Result<Vec<String>> {
    let mut out = Vec::new();
    for raw in tokens {
        let token = raw.trim().to_ascii_uppercase();
        if token.is_empty() {
            continue;
        }
        if let Some((start, end)) = token.split_once('~') {
            validate_param_range_token(start.trim(), end.trim())?;
            out.push(format!(
                "{}~{}",
                start.trim().to_ascii_uppercase(),
                end.trim().to_ascii_uppercase()
            ));
            continue;
        }
        validate_param_code_like(&token)?;
        out.push(token);
    }
    if out.is_empty() {
        anyhow::bail!("param list must not be empty");
    }
    Ok(out)
}

/// 校验参数区间令牌（如 `P00003~P00010`）的合法性。
fn validate_param_range_token(start: &str, end: &str) -> anyhow::Result<()> {
    validate_param_code_like(start)?;
    validate_param_code_like(end)?;
    let sp = start
        .chars()
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid range start: {start}"))?;
    let ep = end
        .chars()
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid range end: {end}"))?;
    if sp != ep {
        anyhow::bail!("range prefix mismatch: {start}~{end}");
    }
    let s = start[1..]
        .parse::<u32>()
        .map_err(|_| anyhow::anyhow!("invalid range start: {start}"))?;
    let e = end[1..]
        .parse::<u32>()
        .map_err(|_| anyhow::anyhow!("invalid range end: {end}"))?;
    if s > e {
        anyhow::bail!("invalid range order: {start}~{end}");
    }
    Ok(())
}

/// 校验参数编码形如 `P00001`（首字母 + 5 位数字）。
fn validate_param_code_like(code: &str) -> anyhow::Result<()> {
    if code.len() != 6 {
        anyhow::bail!("invalid param code: {code}");
    }
    let mut chars = code.chars();
    let Some(prefix) = chars.next() else {
        anyhow::bail!("invalid param code: {code}");
    };
    if !prefix.is_ascii_uppercase() || !chars.all(|c| c.is_ascii_digit()) {
        anyhow::bail!("invalid param code: {code}");
    }
    Ok(())
}

/// 校验 SEND 命令参数格式及命令编码范围。
fn validate_send_command(command: &[String]) -> anyhow::Result<(String, String, Option<u64>)> {
    if command.len() != 3 && command.len() != 4 {
        anyhow::bail!("usage: SEND <device_id> <command_code:C00001~C99999> [timeout_ms]");
    }
    if !is_valid_c_command_code(&command[2]) {
        anyhow::bail!(
            "invalid command_code: {} (expected C00001~C99999)",
            command[2]
        );
    }
    let timeout_ms = if let Some(timeout) = command.get(3) {
        Some(
            timeout
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("invalid timeout_ms: {timeout}"))?,
        )
    } else {
        None
    };
    Ok((command[1].clone(), command[2].to_ascii_uppercase(), timeout_ms))
}

/// 判断命令编码是否符合 `C00001~C99999`。
fn is_valid_c_command_code(code: &str) -> bool {
    if code.len() != 6 {
        return false;
    }
    let upper = code.to_ascii_uppercase();
    if !upper.starts_with('C') {
        return false;
    }
    let digits = &upper[1..];
    if !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return false;
    }
    let Ok(value) = digits.parse::<u32>() else {
        return false;
    };
    (1..=99_999).contains(&value)
}

/// 将 MQTT JSON 响应结构转换为 RESP 值。
fn json_resp_to_resp_value(v: &Value) -> anyhow::Result<RespValue> {
    let obj = v
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("invalid response format"))?;
    let resp_type = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("response type missing"))?;
    match resp_type {
        "simple_string" => Ok(RespValue::SimpleString(
            obj.get("value")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        )),
        "error" => Ok(RespValue::Error(
            obj.get("value")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        )),
        "integer" => Ok(RespValue::Integer(
            obj.get("value").and_then(Value::as_i64).unwrap_or_default(),
        )),
        "bulk_string" => Ok(RespValue::BulkString(
            obj.get("value")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .as_bytes()
                .to_vec(),
        )),
        "null_bulk_string" => Ok(RespValue::NullBulkString),
        "array" => {
            let mut out = Vec::new();
            if let Some(arr) = obj.get("value").and_then(Value::as_array) {
                for item in arr {
                    out.push(json_resp_to_resp_value(item)?);
                }
            }
            Ok(RespValue::Array(out))
        }
        _ => anyhow::bail!("unknown response type: {resp_type}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        canonical_command, is_valid_c_command_code, normalize_param_tokens, parse_line_to_command,
        validate_send_command,
    };

    /// 验证输入中的 BOM 与控制字符可被清理。
    #[test]
    fn parse_line_cleans_control_chars() {
        let parts = parse_line_to_command("SEND dev001 \u{feff}A001\u{0007}");
        assert_eq!(parts, vec!["SEND", "dev001", "A001"]);
    }

    /// 验证常用命令别名映射。
    #[test]
    fn canonical_aliases() {
        assert_eq!(canonical_command("conn"), "CONNECT");
        assert_eq!(canonical_command("STAT"), "STATUS");
        assert_eq!(canonical_command("ps"), "SUBCFG_SET");
        assert_eq!(canonical_command("pl"), "PL");
        assert_eq!(canonical_command("PING"), "PING");
    }

    /// 验证参数区间可被接受并按原样保留给 broker 侧展开。
    #[test]
    fn normalize_param_tokens_should_support_mixed_tokens() {
        let input = vec![
            "P00001".to_string(),
            "P00003~P00005".to_string(),
            "P00010".to_string(),
        ];
        let out = normalize_param_tokens(&input).expect("normalize should succeed");
        assert_eq!(out, vec!["P00001", "P00003~P00005", "P00010"]);
    }

    /// 验证 C 指令编码合法性判断。
    #[test]
    fn validate_c_command_code() {
        assert!(is_valid_c_command_code("C00001"));
        assert!(is_valid_c_command_code("c99999"));
        assert!(!is_valid_c_command_code("C00000"));
        assert!(!is_valid_c_command_code("A00001"));
    }

    /// 验证 SEND 参数校验逻辑。
    #[test]
    fn validate_send_command_args() {
        assert!(validate_send_command(&[
            "SEND".to_string(),
            "dev001".to_string(),
            "C00001".to_string()
        ])
        .is_ok());
        assert!(validate_send_command(&[
            "SEND".to_string(),
            "dev001".to_string(),
            "A00001".to_string()
        ])
        .is_err());
    }
}
