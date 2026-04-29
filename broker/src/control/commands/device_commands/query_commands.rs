use std::sync::Arc;

use common::resp::RespValue;

use crate::state::{AppState, ParamCurrentValue};

use super::super::resp_builders::{
    build_cli_session_row, build_device_status_row, build_key_row, build_key_row_with_null,
};
use super::super::validators::{normalize_non_empty, normalize_param_id};
use super::super::super::{ERR_INTERNAL, ERR_INVALID_ARGUMENT, ERR_NOT_FOUND, err_resp};

/// 处理 LIST：返回全部设备在线状态与地址信息。
pub(crate) fn handle_list_command(state: &Arc<AppState>) -> RespValue {
    let mut ids = state
        .all_devices
        .iter()
        .map(|entry| entry.key().clone())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    let devices: Vec<RespValue> = ids
        .iter()
        .map(|id| {
            let Some(info) = state.all_devices.get(id) else {
                return RespValue::Error(format!(
                    "{ERR_INTERNAL} device disappeared while listing: {id}"
                ));
            };
            let ts = info
                .last_seen_ts
                .load(std::sync::atomic::Ordering::Relaxed);
            let ip = info.ip.clone();
            let port = info.port;
            let addr = format!("{ip}:{port}");
            build_device_status_row(id, info.online, ts, &addr)
        })
        .collect();
    RespValue::Array(devices)
}

/// 处理 STATUS：返回单设备在线状态与地址信息。
pub(crate) fn handle_status_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.len() != 2 {
        return err_resp(ERR_INVALID_ARGUMENT, "usage: STATUS <device_id>");
    }
    let id = &args[1];
    if let Some(info) = state.all_devices.get(id) {
        let ts = info
            .last_seen_ts
            .load(std::sync::atomic::Ordering::Relaxed);
        let addr = format!("{}:{}", info.ip, info.port);
        build_device_status_row(id, info.online, ts, &addr)
    } else {
        err_resp(ERR_NOT_FOUND, "device not found")
    }
}

/// 处理 KEY：查询设备参数当前值缓存。
pub(crate) fn handle_key_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.len() < 2 {
        return err_resp(ERR_INVALID_ARGUMENT, "usage: KEY <device_id> [param_id...]");
    }
    let device_id = match normalize_non_empty(&args[1], "device_id") {
        Ok(v) => v,
        Err(e) => return err_resp(ERR_INVALID_ARGUMENT, e.to_string()),
    };

    if args.len() == 2 {
        let mut items = state.list_param_current_values_for_device(&device_id);
        if items.is_empty() {
            return err_resp(ERR_NOT_FOUND, "no cached values for device");
        }
        items.sort_by(|a, b| a.param_id.cmp(&b.param_id));
        let rows = items.iter().map(build_key_row).collect::<Vec<_>>();
        return RespValue::Array(rows);
    }

    let mut rows = Vec::new();
    for raw_param_id in args.iter().skip(2) {
        let param_id = match normalize_param_id(raw_param_id) {
            Ok(v) => v,
            Err(e) => return err_resp(ERR_INVALID_ARGUMENT, e.to_string()),
        };
        let item = state.get_param_current_value(&device_id, &param_id);
        rows.push(key_row_from_optional(&device_id, &param_id, item.as_ref()));
    }
    RespValue::Array(rows)
}

/// 将缓存项转换为 KEY 命令输出行。
fn key_row_from_item(item: &ParamCurrentValue) -> RespValue {
    build_key_row(item)
}

/// 将可选缓存项转换为 KEY 命令输出行（未命中返回空值）。
fn key_row_from_optional(
    device_id: &str,
    param_id: &str,
    item: Option<&ParamCurrentValue>,
) -> RespValue {
    let Some(found) = item else {
        return build_key_row_with_null(device_id, param_id);
    };
    key_row_from_item(found)
}

/// 处理 CLILIST：查看当前已登记的 CLI 会话列表。
pub(crate) fn handle_clilist_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.len() != 1 {
        return err_resp(ERR_INVALID_ARGUMENT, "usage: CLILIST");
    }
    let mut items = state.list_cli_sessions();
    items.sort_by(|a, b| a.client_id.cmp(&b.client_id));
    let rows = items
        .into_iter()
        .map(|item| build_cli_session_row(&item))
        .collect::<Vec<_>>();
    RespValue::Array(rows)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::resp::RespValue;
    use dashmap::DashMap;

    use super::handle_key_command;
    use crate::state::{AppState, DeviceInfo};

    /// 构建仅用于 KEY 命令测试的最小状态对象。
    fn build_test_state() -> Arc<AppState> {
        let all_devices = DashMap::<String, DeviceInfo>::new();
        Arc::new(AppState::new(all_devices, None, None, true, true))
    }

    /// 验证 KEY 在设备 ID 为空时应返回参数错误。
    #[test]
    fn key_should_reject_empty_device_id() {
        let state = build_test_state();
        let resp = handle_key_command(&state, vec!["KEY".to_string(), "  ".to_string()]);
        match resp {
            RespValue::Error(msg) => {
                assert!(msg.contains("ERR_INVALID_ARGUMENT"));
                assert!(msg.contains("device_id must not be empty"));
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    /// 验证 KEY 在参数编码非法时应返回参数错误。
    #[test]
    fn key_should_reject_invalid_param_id() {
        let state = build_test_state();
        let resp = handle_key_command(
            &state,
            vec!["KEY".to_string(), "dev-1".to_string(), "BAD".to_string()],
        );
        match resp {
            RespValue::Error(msg) => {
                assert!(msg.contains("ERR_INVALID_ARGUMENT"));
                assert!(msg.contains("invalid param_id"));
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    /// 验证 KEY 在指定参数未命中缓存时应返回空值行而不是错误。
    #[test]
    fn key_should_return_null_row_for_missing_param() {
        let state = build_test_state();
        let resp = handle_key_command(
            &state,
            vec!["KEY".to_string(), "dev-1".to_string(), "A00001".to_string()],
        );
        match resp {
            RespValue::Array(rows) => assert_eq!(rows.len(), 1),
            other => panic!("unexpected response: {other:?}"),
        }
    }
}
