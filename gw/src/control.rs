use std::{sync::Arc, time::Duration};

use common::{
    device_proto::command_frame,
    resp::{RespValue, as_command_args, decode_value, encode_value},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};
use tracing::{error, info, warn};

use crate::{
    device::connect_one_sim,
    state::AppState,
};

/// 控制连接允许的最大待解析缓冲，防止异常半包导致内存持续增长。
const MAX_PENDING_BYTES: usize = 64 * 1024;
/// 标准错误码：无效参数。
const ERR_INVALID_ARGUMENT: &str = "ERR_INVALID_ARGUMENT";
/// 标准错误码：协议错误。
const ERR_BAD_PROTOCOL: &str = "ERR_BAD_PROTOCOL";
/// 标准错误码：未知命令。
const ERR_UNKNOWN_COMMAND: &str = "ERR_UNKNOWN_COMMAND";
/// 标准错误码：未找到目标。
const ERR_NOT_FOUND: &str = "ERR_NOT_FOUND";
/// 标准错误码：冲突状态。
const ERR_CONFLICT: &str = "ERR_CONFLICT";
/// 标准错误码：连接失败。
const ERR_CONNECT_FAILED: &str = "ERR_CONNECT_FAILED";
/// 标准错误码：超时。
const ERR_TIMEOUT: &str = "ERR_TIMEOUT";
/// 标准错误码：设备离线。
const ERR_DEVICE_OFFLINE: &str = "ERR_DEVICE_OFFLINE";
/// 标准错误码：内部错误。
const ERR_INTERNAL: &str = "ERR_INTERNAL";
/// 标准错误码：输入帧过大。
const ERR_FRAME_TOO_LARGE: &str = "ERR_FRAME_TOO_LARGE";
/// 标准错误码：设备回令格式错误。
const ERR_BAD_REPLY: &str = "ERR_BAD_REPLY";
/// 指令执行成功文本。
const COMMAND_RESULT_SUCCESS: &str = "SUCCESS";
/// 指令执行失败文本。
const COMMAND_RESULT_FAIL: &str = "FAIL";

/// 构造统一格式的 RESP 错误响应，便于客户端按错误码处理。
fn err_resp(code: &str, detail: impl AsRef<str>) -> RespValue {
    RespValue::Error(format!("{code} {}", detail.as_ref()))
}

/// 将编码统一为大写，避免大小写差异导致规则不命中。
fn normalize_code(code: &str) -> String {
    code.trim().to_ascii_uppercase()
}

/// 校验编码格式是否符合 `A/Z + 5位数字`。
fn is_valid_code(code: &str) -> bool {
    if code.len() != 6 {
        return false;
    }
    let mut chars = code.chars();
    let Some(prefix) = chars.next() else {
        return false;
    };
    (prefix == 'A' || prefix == 'Z') && chars.all(|ch| ch.is_ascii_digit())
}

/// 将设备回令解析为统一结果字符串（仅 SUCCESS / FAIL）。
fn parse_command_result(reply: Vec<u8>) -> Result<&'static str, &'static str> {
    let text = std::str::from_utf8(&reply).map_err(|_| "reply is not valid utf8")?;
    match text.trim().to_ascii_uppercase().as_str() {
        COMMAND_RESULT_SUCCESS => Ok(COMMAND_RESULT_SUCCESS),
        COMMAND_RESULT_FAIL => Ok(COMMAND_RESULT_FAIL),
        _ => Err("reply must be SUCCESS or FAIL"),
    }
}

/// 运行控制端口监听循环。
pub async fn run_control_listener(listener: TcpListener, state: Arc<AppState>) -> anyhow::Result<()> {
    loop {
        let (socket, peer) = listener.accept().await?;
        let s = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_control_connection(socket, peer, s).await {
                error!(peer = %peer, error = ?e, "control connection error");
            }
        });
    }
}

/// 处理单个控制客户端连接。
async fn handle_control_connection(
    mut socket: TcpStream,
    peer: std::net::SocketAddr,
    state: Arc<AppState>,
) -> anyhow::Result<()> {
    info!(peer = %peer, "control connected");
    let mut read_buf = vec![0_u8; 4096];
    let mut pending = Vec::<u8>::new();

    loop {
        let n = socket.read(&mut read_buf).await?;
        if n == 0 {
            break;
        }
        pending.extend_from_slice(&read_buf[..n]);
        if pending.len() > MAX_PENDING_BYTES {
            warn!(peer = %peer, pending_len = pending.len(), "control frame too large");
            let out = encode_value(&err_resp(ERR_FRAME_TOO_LARGE, "protocol frame too large"));
            socket.write_all(&out).await?;
            break;
        }

        loop {
            let decoded = match decode_value(&pending) {
                Ok(v) => v,
                Err(e) => {
                    warn!(peer = %peer, error = %e, "bad control protocol");
                    let out = encode_value(&err_resp(ERR_BAD_PROTOCOL, format!("bad protocol: {e}")));
                    socket.write_all(&out).await?;
                    return Ok(());
                }
            };
            let Some((value, consumed)) = decoded else {
                break;
            };
            pending.drain(0..consumed);

            let args = match as_command_args(value) {
                Ok(v) => v,
                Err(e) => {
                    let out = encode_value(&err_resp(ERR_INVALID_ARGUMENT, format!("bad command: {e}")));
                    socket.write_all(&out).await?;
                    continue;
                }
            };
            let resp = execute_command(&state, args).await;
            let out = encode_value(&resp);
            socket.write_all(&out).await?;
        }
    }

    info!(peer = %peer, "control disconnected");
    Ok(())
}

/// 执行控制命令并返回 RESP 结果。
async fn execute_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.is_empty() {
        return err_resp(ERR_INVALID_ARGUMENT, "empty command");
    }

    let cmd = args[0].to_uppercase();
    match cmd.as_str() {
        "PING" => RespValue::SimpleString("PONG".to_string()),
        "CONNECT" => handle_connect_command(state, args).await,
        "LIST" => {
            let mut ids = state
                .all_devices
                .iter()
                .map(|entry| entry.key().clone())
                .collect::<Vec<_>>();
            ids.sort_unstable();
            let devices: Vec<RespValue> = ids
                .iter()
                .map(|id| {
                    let info = state.all_devices.get(id).expect("device must exist");
                    let ts = info
                        .last_seen_ts
                        .load(std::sync::atomic::Ordering::Relaxed)
                        .to_string();
                    let ip = info.ip.clone();
                    let port = info.port;
                    let addr = format!("{}:{}", ip, port);
                    RespValue::Array(vec![
                        RespValue::BulkString(b"device_id".to_vec()),
                        RespValue::BulkString(id.as_bytes().to_vec()),
                        RespValue::BulkString(b"online".to_vec()),
                        RespValue::BulkString(if info.online {
                            b"1".to_vec()
                        } else {
                            b"0".to_vec()
                        }),
                        RespValue::BulkString(b"last_seen_ts".to_vec()),
                        RespValue::BulkString(ts.into_bytes()),
                        RespValue::BulkString(b"addr".to_vec()),
                        RespValue::BulkString(addr.into_bytes()),
                    ])
                })
                .collect();
            RespValue::Array(devices)
        }
        "STATUS" => {
            if args.len() != 2 {
                return err_resp(ERR_INVALID_ARGUMENT, "usage: STATUS <device_id>");
            }
            let id = &args[1];
            if let Some(info) = state.all_devices.get(id) {
                let ts = info
                    .last_seen_ts
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .to_string();
                let addr = format!("{}:{}", info.ip, info.port);
                RespValue::Array(vec![
                    RespValue::BulkString(b"device_id".to_vec()),
                    RespValue::BulkString(id.as_bytes().to_vec()),
                    RespValue::BulkString(b"online".to_vec()),
                    RespValue::BulkString(if info.online {
                        b"1".to_vec()
                    } else {
                        b"0".to_vec()
                    }),
                    RespValue::BulkString(b"last_seen_ts".to_vec()),
                    RespValue::BulkString(ts.into_bytes()),
                    RespValue::BulkString(b"addr".to_vec()),
                    RespValue::BulkString(addr.into_bytes()),
                ])
            } else {
                err_resp(ERR_NOT_FOUND, "device not found")
            }
        }
        "KICK" => {
            if args.len() != 2 {
                return err_resp(ERR_INVALID_ARGUMENT, "usage: KICK <device_id>");
            }
            let id = &args[1];
            if let Some((_, h)) = state.device_handles.remove(id) {
                let _ = h.cancel_tx.send(true);
                if let Some((_, sim_addr)) = state.device_to_sim.remove(id) {
                    state.sim_connections.remove(&sim_addr);
                    state.pending_sims.remove(&sim_addr);
                }
            }
            if let Some(mut info) = state.all_devices.get_mut(id) {
                info.online = false;
            }
            RespValue::SimpleString("OK".to_string())
        }
        "SEND" => handle_send_command(state, args).await,
        _ => err_resp(ERR_UNKNOWN_COMMAND, format!("unknown command: {}", args[0])),
    }
}

/// 对外暴露控制命令执行入口，供 MQTT 控制通道复用。
pub async fn execute_command_via_api(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    execute_command(state, args).await
}

/// 将 RESP 值转换为 JSON 值，便于在 HTTP/MQTT 等通道统一返回结构化结果。
pub fn resp_to_json_value(value: &RespValue) -> serde_json::Value {
    match value {
        RespValue::SimpleString(s) => serde_json::json!({ "type": "simple_string", "value": s }),
        RespValue::Error(s) => serde_json::json!({ "type": "error", "value": s }),
        RespValue::Integer(v) => serde_json::json!({ "type": "integer", "value": v }),
        RespValue::BulkString(v) => serde_json::json!({
            "type": "bulk_string",
            "value": String::from_utf8_lossy(v).to_string()
        }),
        RespValue::NullBulkString => serde_json::json!({ "type": "null_bulk_string" }),
        RespValue::Array(arr) => serde_json::json!({
            "type": "array",
            "value": arr.iter().map(resp_to_json_value).collect::<Vec<_>>()
        }),
    }
}

/// 处理 CONNECT 命令，触发网关主动连接 sim。
async fn handle_connect_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.len() != 2 {
        return err_resp(ERR_INVALID_ARGUMENT, "usage: CONNECT <sim_addr>");
    }

    let sim_addr = args[1].clone();
    if state.sim_connections.contains_key(&sim_addr) {
        return err_resp(ERR_CONFLICT, "sim already connected");
    }
    if state.pending_sims.contains_key(&sim_addr) {
        return err_resp(ERR_CONFLICT, "sim is connecting");
    }
    state.pending_sims.insert(sim_addr.clone(), ());

    if let Err(e) = connect_one_sim(state.clone(), &sim_addr).await {
        state.pending_sims.remove(&sim_addr);
        return err_resp(ERR_CONNECT_FAILED, format!("connect failed: {e}"));
    }

    RespValue::Array(vec![
        RespValue::BulkString(b"connecting".to_vec()),
        RespValue::BulkString(sim_addr.into_bytes()),
    ])
}

/// 处理 SEND 指令下发与同步等待回令流程。
async fn handle_send_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.len() < 3 || args.len() > 4 {
        return err_resp(
            ERR_INVALID_ARGUMENT,
            "usage: SEND <device_id> <command_code> [timeout_ms]",
        );
    }
    let device_id = &args[1];
    let command_code = normalize_code(&args[2]);
    if !is_valid_code(&command_code) {
        return err_resp(
            ERR_INVALID_ARGUMENT,
            "invalid command_code, expected A00001/A99999/Z00001/Z99999",
        );
    }
    let cmd_payload = command_code.into_bytes();
    let timeout_ms = if args.len() == 4 {
        match args[3].parse::<u64>() {
            Ok(v) => v,
            Err(e) => return err_resp(ERR_INVALID_ARGUMENT, format!("invalid timeout: {e}")),
        }
    } else {
        3000
    };

    let Some(handle) = state.device_handles.get(device_id).map(|v| v.clone()) else {
        return err_resp(ERR_DEVICE_OFFLINE, "device not online");
    };
    let request_id = state
        .request_seq
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let frame = command_frame(request_id, cmd_payload);
    let (tx, rx) = oneshot::channel();
    state.pending.insert(request_id, tx);

    if let Err(e) = handle.tx.send(frame).await {
        state.pending.remove(&request_id);
        return err_resp(ERR_INTERNAL, format!("send to device failed: {e}"));
    }

    match timeout(Duration::from_millis(timeout_ms), rx).await {
        Ok(Ok(reply)) => match parse_command_result(reply) {
            Ok(result) => RespValue::SimpleString(result.to_string()),
            Err(e) => err_resp(ERR_BAD_REPLY, e),
        },
        Ok(Err(_)) => err_resp(ERR_INTERNAL, "reply channel closed"),
        Err(_) => {
            state.pending.remove(&request_id);
            err_resp(ERR_TIMEOUT, "timeout")
        }
    }
}
