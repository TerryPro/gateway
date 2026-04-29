use std::sync::Arc;

use common::resp::{as_command_args, decode_value, encode_value};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{error, info, warn};

use crate::state::AppState;

use super::{ERR_BAD_PROTOCOL, ERR_FRAME_TOO_LARGE, ERR_INVALID_ARGUMENT, MAX_PENDING_BYTES, err_resp, execute_command};

/// 运行控制端口监听循环，并为每个连接派生独立任务。
pub(super) async fn run_control_listener(
    listener: TcpListener,
    state: Arc<AppState>,
) -> anyhow::Result<()> {
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

/// 处理单个控制客户端连接的收包、解码、执行与回包流程。
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
