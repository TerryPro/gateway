use crate::config::ApiConfig;
use crate::config::StorageConfig;
use crate::flush::flusher;
use crate::mem::time_window_buffer::TimeWindowBuffer;
use crate::model::{IngestBatch, IngestPacket, QueryRequest};
use crate::query::executor::QueryExecutor;
use crate::wal::recovery;
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

/// 启动 HTTP 服务并挂载健康检查与查询接口。
pub async fn run_http_server(
    cfg: ApiConfig,
    query: QueryExecutor,
    ingest_tx: mpsc::Sender<IngestBatch>,
    mem: TimeWindowBuffer,
    storage: StorageConfig,
    wal_dir: String,
) -> anyhow::Result<()> {
    let state = Arc::new(ApiState {
        query,
        ingest_tx,
        mem,
        storage,
        wal_dir,
    });
    let app = Router::new()
        .route("/health", get(health))
        .route("/ingest", post(ingest_handler))
        .route("/query", post(query_handler))
        .route("/query/history", post(query_history_handler))
        .route("/admin/flush", post(admin_flush_handler))
        .route("/admin/recover", post(admin_recover_handler))
        .route("/admin/stats", get(admin_stats_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    info!("http listen {}", cfg.listen);
    axum::serve(listener, app).await?;
    Ok(())
}

/// API 共享状态：当前仅持有查询执行器。
#[derive(Clone)]
struct ApiState {
    query: QueryExecutor,
    ingest_tx: mpsc::Sender<IngestBatch>,
    mem: TimeWindowBuffer,
    storage: StorageConfig,
    wal_dir: String,
}

/// 健康检查接口，用于探活与运维监控。
async fn health() -> &'static str {
    "ok"
}

/// 查询接口：执行查询并返回结构化结果。
async fn query_handler(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<crate::model::QueryResponse>, axum::http::StatusCode> {
    state
        .query
        .query_disk_only(req)
        .await
        .map(Json)
        .map_err(|e| {
            tracing::error!("query error: {}", e);
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        })
}

/// 历史数据查询接口：使用 DataFusion 查询 Parquet 文件。
async fn query_history_handler(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<crate::model::QueryRequest>,
) -> Result<Json<crate::model::QueryResponse>, axum::http::StatusCode> {
    // 使用 DataFusion 查询历史数据
    state
        .query
        .execute(req)
        .await
        .map(Json)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)
}

/// 写入接口：将单个输入包转为批次后投递到 ingest 队列。
async fn ingest_handler(
    State(state): State<Arc<ApiState>>,
    Json(packet): Json<IngestPacket>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let batch = IngestBatch {
        device_id: packet.device_id,
        recv_ts: packet.recv_ts,
        points: packet.points,
    };
    state
        .ingest_tx
        .send(batch)
        .await
        .map_err(|_| axum::http::StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(serde_json::json!({
        "ok": true
    })))
}

/// 手动 flush 接口：立即触发一次"内存到磁盘"的落盘流程。
async fn admin_flush_handler(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    flusher::flush_by_hour_window(&state.storage, &state.mem)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({
        "ok": true
    })))
}

/// 手动恢复接口：先清空内存，再从 WAL 目录回放重建窗口数据。
async fn admin_recover_handler(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    state.mem.clear();
    let mut replayed_batches = 0usize;
    recovery::replay_wal_dir(Path::new(&state.wal_dir), |batch| {
        state.mem.insert_recovered_batch(batch);
        replayed_batches += 1;
    })
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let mem_stats = state.mem.snapshot_stats();
    Ok(Json(serde_json::json!({
        "ok": true,
        "replayed_batches": replayed_batches,
        "mem_device_count": mem_stats.device_count,
        "mem_total_points": mem_stats.active_points + mem_stats.flushing_points
    })))
}

/// 运维统计接口：返回内存窗口与 WAL 目录的基础状态。
async fn admin_stats_handler(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
    let mem_stats = state.mem.snapshot_stats();
    let wal_stats = recovery::wal_dir_stats(Path::new(&state.wal_dir))
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({
        "ok": true,
        "mem": mem_stats,
        "wal": wal_stats
    })))
}
