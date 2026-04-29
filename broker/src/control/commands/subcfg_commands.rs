use std::sync::Arc;

use common::resp::RespValue;

use crate::state::AppState;

use super::resp_builders::{build_subcfg_removed, build_subcfg_row, build_subcfg_set_ack};
use super::validators::{normalize_non_empty, parse_param_specs};
use super::super::{ERR_INVALID_ARGUMENT, ERR_NOT_FOUND, err_resp};

/// 处理 SUBCFG_SET：设置 client_id + dev_id 的参数订阅集合。
pub(crate) fn handle_subcfg_set_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.len() < 4 {
        return err_resp(
            ERR_INVALID_ARGUMENT,
            "usage: SUBCFG_SET <client_id> <device_id> <param_id...>",
        );
    }
    let client_id = match normalize_non_empty(&args[1], "client_id") {
        Ok(v) => v,
        Err(e) => return err_resp(ERR_INVALID_ARGUMENT, e.to_string()),
    };
    let device_id = match normalize_non_empty(&args[2], "device_id") {
        Ok(v) => v,
        Err(e) => return err_resp(ERR_INVALID_ARGUMENT, e.to_string()),
    };
    let param_ids = match parse_param_specs(args.iter().skip(3).map(|s| s.as_str())) {
        Ok(v) => v,
        Err(e) => return err_resp(ERR_INVALID_ARGUMENT, e.to_string()),
    };
    if param_ids.is_empty() {
        return err_resp(ERR_INVALID_ARGUMENT, "param_ids must not be empty");
    }
    let count = state.upsert_param_subscription(&client_id, &device_id, param_ids);
    build_subcfg_set_ack(&client_id, &device_id, count)
}

/// 处理 SUBCFG_GET：查询指定 client_id + dev_id 的参数订阅集合。
pub(crate) fn handle_subcfg_get_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.len() != 3 {
        return err_resp(
            ERR_INVALID_ARGUMENT,
            "usage: SUBCFG_GET <client_id> <device_id>",
        );
    }
    let client_id = match normalize_non_empty(&args[1], "client_id") {
        Ok(v) => v,
        Err(e) => return err_resp(ERR_INVALID_ARGUMENT, e.to_string()),
    };
    let device_id = match normalize_non_empty(&args[2], "device_id") {
        Ok(v) => v,
        Err(e) => return err_resp(ERR_INVALID_ARGUMENT, e.to_string()),
    };
    let Some(item) = state.get_param_subscription(&client_id, &device_id) else {
        return err_resp(ERR_NOT_FOUND, "subscription not found");
    };
    build_subcfg_row(
        &item.client_id,
        &item.device_id,
        item.param_ids.iter().map(|id| id.as_str()),
    )
}

/// 处理 SUBCFG_DEL：删除指定 client_id + dev_id 的参数订阅集合。
pub(crate) fn handle_subcfg_del_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.len() != 3 {
        return err_resp(
            ERR_INVALID_ARGUMENT,
            "usage: SUBCFG_DEL <client_id> <device_id>",
        );
    }
    let client_id = match normalize_non_empty(&args[1], "client_id") {
        Ok(v) => v,
        Err(e) => return err_resp(ERR_INVALID_ARGUMENT, e.to_string()),
    };
    let device_id = match normalize_non_empty(&args[2], "device_id") {
        Ok(v) => v,
        Err(e) => return err_resp(ERR_INVALID_ARGUMENT, e.to_string()),
    };
    let removed = state.remove_param_subscription(&client_id, &device_id);
    build_subcfg_removed(removed)
}

/// 处理 SUBCFG_LIST：列出参数订阅，可按 client_id 过滤。
pub(crate) fn handle_subcfg_list_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.len() > 2 {
        return err_resp(
            ERR_INVALID_ARGUMENT,
            "usage: SUBCFG_LIST [client_id]",
        );
    }
    let filter_client_id = if args.len() == 2 {
        Some(args[1].as_str())
    } else {
        None
    };
    let mut items = state.list_param_subscriptions(filter_client_id);
    items.sort_by(|a, b| {
        a.client_id
            .cmp(&b.client_id)
            .then_with(|| a.device_id.cmp(&b.device_id))
    });
    let rows = items
        .into_iter()
        .map(|item| {
            build_subcfg_row(
                &item.client_id,
                &item.device_id,
                item.param_ids.iter().map(|id| id.as_str()),
            )
        })
        .collect::<Vec<_>>();
    RespValue::Array(rows)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::resp::RespValue;
    use dashmap::DashMap;

    use super::handle_subcfg_set_command;
    use crate::state::{AppState, DeviceInfo};

    /// 构建仅用于控制命令测试的最小状态对象。
    fn build_test_state() -> Arc<AppState> {
        let all_devices = DashMap::<String, DeviceInfo>::new();
        Arc::new(AppState::new(all_devices, None, None, true, true))
    }

    /// 验证 SUBCFG_SET 应支持参数区间并写入订阅状态。
    #[test]
    fn subcfg_set_should_expand_ranges_and_store_subscription() {
        let state = build_test_state();
        let args = vec![
            "SUBCFG_SET".to_string(),
            "cli-1".to_string(),
            "dev-1".to_string(),
            "A00001~A00003".to_string(),
            "A00005".to_string(),
        ];

        let resp = handle_subcfg_set_command(&state, args);
        match resp {
            RespValue::Array(fields) => {
                assert!(!fields.is_empty(), "response fields should not be empty");
            }
            other => panic!("unexpected response: {other:?}"),
        }

        let sub = state
            .get_param_subscription("cli-1", "dev-1")
            .expect("subscription should exist");
        assert_eq!(sub.param_ids.len(), 4);
        assert!(sub.param_ids.contains("A00001"));
        assert!(sub.param_ids.contains("A00003"));
        assert!(sub.param_ids.contains("A00005"));
    }

    /// 验证 SUBCFG_SET 遇到非法参数编码时应返回错误响应。
    #[test]
    fn subcfg_set_should_reject_invalid_param_id() {
        let state = build_test_state();
        let args = vec![
            "SUBCFG_SET".to_string(),
            "cli-1".to_string(),
            "dev-1".to_string(),
            "BAD".to_string(),
        ];
        let resp = handle_subcfg_set_command(&state, args);
        match resp {
            RespValue::Error(msg) => {
                assert!(msg.contains("ERR_INVALID_ARGUMENT"));
                assert!(msg.contains("invalid param_id"));
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }
}
