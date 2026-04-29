use std::{sync::Arc, time::Duration};

use common::{device_proto::command_frame, resp::RespValue};
use tokio::{sync::oneshot, time::timeout};

use crate::{device::connect_one_sim, state::AppState};

use super::super::validators::normalize_non_empty;
use super::super::super::{
    ERR_BAD_REPLY, ERR_CONNECT_FAILED, ERR_CONFLICT, ERR_DEVICE_OFFLINE, ERR_INTERNAL,
    ERR_INVALID_ARGUMENT, ERR_TIMEOUT, err_resp, is_valid_code, normalize_code,
    parse_command_result,
};

/// 处理 CONNECT 命令，触发 Broker 主动连接 sim。
pub(crate) async fn handle_connect_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.len() != 2 {
        return err_resp(ERR_INVALID_ARGUMENT, "usage: CONNECT <sim_addr>");
    }

    let sim_addr = match normalize_non_empty(&args[1], "sim_addr") {
        Ok(v) => v,
        Err(e) => return err_resp(ERR_INVALID_ARGUMENT, e.to_string()),
    };
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

/// 处理 KICK：断开指定设备连接并清理运行态映射。
pub(crate) fn handle_kick_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.len() != 2 {
        return err_resp(ERR_INVALID_ARGUMENT, "usage: KICK <device_id>");
    }
    let id = match normalize_non_empty(&args[1], "device_id") {
        Ok(v) => v,
        Err(e) => return err_resp(ERR_INVALID_ARGUMENT, e.to_string()),
    };
    if let Some((_, h)) = state.device_handles.remove(&id) {
        let _ = h.cancel_tx.send(true);
    }
    state.mark_device_disconnected(&id);
    RespValue::SimpleString("OK".to_string())
}

/// 处理 SEND 指令下发与同步等待回令流程。
pub(crate) async fn handle_send_command(state: &Arc<AppState>, args: Vec<String>) -> RespValue {
    if args.len() < 3 || args.len() > 4 {
        return err_resp(
            ERR_INVALID_ARGUMENT,
            "usage: SEND <device_id> <command_code> [timeout_ms]",
        );
    }
    let device_id = match normalize_non_empty(&args[1], "device_id") {
        Ok(v) => v,
        Err(e) => return err_resp(ERR_INVALID_ARGUMENT, e.to_string()),
    };
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

    let Some(handle) = state.device_handles.get(&device_id).map(|v| v.clone()) else {
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
