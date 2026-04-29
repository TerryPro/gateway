use std::sync::Arc;

use common::{
    resp::RespValue,
};

use crate::{command::BrokerCommand, state::AppState};

mod commands;
mod protocol;

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
pub async fn run_control_listener(
    listener: tokio::net::TcpListener,
    state: Arc<AppState>,
) -> anyhow::Result<()> {
    protocol::run_control_listener(listener, state).await
}

/// 执行控制命令并返回 RESP 结果。
async fn execute_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.is_empty() {
        return err_resp(ERR_INVALID_ARGUMENT, "empty command");
    }

    let Some(cmd) = BrokerCommand::parse(&args[0]) else {
        return err_resp(ERR_UNKNOWN_COMMAND, format!("unknown command: {}", args[0]));
    };
    match cmd {
        BrokerCommand::Ping => RespValue::SimpleString("PONG".to_string()),
        BrokerCommand::Connect => commands::handle_connect_command(state, args).await,
        BrokerCommand::List => commands::handle_list_command(state),
        BrokerCommand::Status => commands::handle_status_command(state, args),
        BrokerCommand::Key => commands::handle_key_command(state, args),
        BrokerCommand::Kick => commands::handle_kick_command(state, args),
        BrokerCommand::Send => commands::handle_send_command(state, args).await,
        BrokerCommand::CliList => commands::handle_clilist_command(state, args),
        BrokerCommand::SubcfgSet => commands::handle_subcfg_set_command(state, args),
        BrokerCommand::SubcfgGet => commands::handle_subcfg_get_command(state, args),
        BrokerCommand::SubcfgDel => commands::handle_subcfg_del_command(state, args),
        BrokerCommand::SubcfgList => commands::handle_subcfg_list_command(state, args),
    }
}

/// 对外暴露控制命令执行入口，供 MQTT 控制通道复用。
pub async fn execute_command_via_api(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    execute_command(state, args).await
}

/// 将 RESP 值转换为 JSON 值，便于在 MQTT 通道统一返回结构化结果。
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
