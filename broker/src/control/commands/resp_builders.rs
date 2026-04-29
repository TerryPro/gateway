use common::resp::RespValue;

use crate::state::{CliSessionInfo, ParamCurrentValue};

/// 构建设备状态行，统一 LIST/STATUS 的字段顺序与编码方式。
pub(crate) fn build_device_status_row(
    device_id: &str,
    online: bool,
    last_seen_ts: u64,
    addr: &str,
) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"device_id".to_vec()),
        RespValue::BulkString(device_id.as_bytes().to_vec()),
        RespValue::BulkString(b"online".to_vec()),
        RespValue::BulkString(if online { b"1".to_vec() } else { b"0".to_vec() }),
        RespValue::BulkString(b"last_seen_ts".to_vec()),
        RespValue::BulkString(last_seen_ts.to_string().into_bytes()),
        RespValue::BulkString(b"addr".to_vec()),
        RespValue::BulkString(addr.as_bytes().to_vec()),
    ])
}

/// 构建 KEY 命令行（命中值）。
pub(crate) fn build_key_row(item: &ParamCurrentValue) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"device_id".to_vec()),
        RespValue::BulkString(item.device_id.as_bytes().to_vec()),
        RespValue::BulkString(b"param_id".to_vec()),
        RespValue::BulkString(item.param_id.as_bytes().to_vec()),
        RespValue::BulkString(b"value".to_vec()),
        RespValue::BulkString(item.value.to_string().into_bytes()),
        RespValue::BulkString(b"ts_ms".to_vec()),
        RespValue::BulkString(item.ts_ms.to_string().into_bytes()),
    ])
}

/// 构建 KEY 命令行（未命中值）。
pub(crate) fn build_key_row_with_null(device_id: &str, param_id: &str) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"device_id".to_vec()),
        RespValue::BulkString(device_id.as_bytes().to_vec()),
        RespValue::BulkString(b"param_id".to_vec()),
        RespValue::BulkString(param_id.as_bytes().to_vec()),
        RespValue::BulkString(b"value".to_vec()),
        RespValue::NullBulkString,
        RespValue::BulkString(b"ts_ms".to_vec()),
        RespValue::NullBulkString,
    ])
}

/// 构建 CLI 会话行，统一 CLILIST 输出字段顺序。
pub(crate) fn build_cli_session_row(item: &CliSessionInfo) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"client_id".to_vec()),
        RespValue::BulkString(item.client_id.as_bytes().to_vec()),
        RespValue::BulkString(b"last_seen_ts".to_vec()),
        RespValue::BulkString(item.last_seen_ts.to_string().into_bytes()),
        RespValue::BulkString(b"last_cmd".to_vec()),
        RespValue::BulkString(item.last_cmd.as_bytes().to_vec()),
    ])
}

/// 构建 SUBCFG 输出行，统一 GET/LIST 的字段拼装逻辑。
pub(crate) fn build_subcfg_row<'a, I>(client_id: &str, device_id: &str, param_ids: I) -> RespValue
where
    I: IntoIterator<Item = &'a str>,
{
    let params = param_ids
        .into_iter()
        .map(|id| RespValue::BulkString(id.as_bytes().to_vec()))
        .collect::<Vec<_>>();
    RespValue::Array(vec![
        RespValue::BulkString(b"client_id".to_vec()),
        RespValue::BulkString(client_id.as_bytes().to_vec()),
        RespValue::BulkString(b"device_id".to_vec()),
        RespValue::BulkString(device_id.as_bytes().to_vec()),
        RespValue::BulkString(b"param_ids".to_vec()),
        RespValue::Array(params),
    ])
}

/// 构建 SUBCFG_SET 响应，返回写入后的参数数量。
pub(crate) fn build_subcfg_set_ack(client_id: &str, device_id: &str, count: usize) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"client_id".to_vec()),
        RespValue::BulkString(client_id.as_bytes().to_vec()),
        RespValue::BulkString(b"device_id".to_vec()),
        RespValue::BulkString(device_id.as_bytes().to_vec()),
        RespValue::BulkString(b"param_count".to_vec()),
        RespValue::BulkString(count.to_string().into_bytes()),
    ])
}

/// 构建 SUBCFG_DEL 响应，返回删除结果标记。
pub(crate) fn build_subcfg_removed(removed: bool) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(b"removed".to_vec()),
        RespValue::BulkString(if removed { b"1".to_vec() } else { b"0".to_vec() }),
    ])
}
