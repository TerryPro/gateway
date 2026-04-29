use crate::mem::time_window_buffer::TimeWindowBuffer;
use crate::model::{QueryRequest, QueryResponse};
use crate::query::planner::build_plan;
use crate::query::duckdb_executor;

/// 查询执行器：统一合并磁盘层与内存层结果。
#[derive(Clone)]
pub struct QueryExecutor {
    mem: TimeWindowBuffer,
    mem_window_sec: u64,
    duckdb: duckdb_executor::QueryExecutor,
}

impl QueryExecutor {
    /// 创建查询执行器，注入存储根目录与内存窗口。
    pub fn new(_root: String, mem: TimeWindowBuffer, duckdb: duckdb_executor::QueryExecutor) -> Self {
        let mem_window_sec = 3600; // 固定 1 小时窗口
        Self {
            mem,
            mem_window_sec,
            duckdb,
        }
    }

    /// 执行查询：使用 DuckDB 统一查询磁盘和内存数据。
    pub async fn execute(&self, req: QueryRequest) -> anyhow::Result<QueryResponse> {
        let now_ts = chrono::Local::now().timestamp().max(0) as u64;
        let plan = build_plan(&req, self.mem_window_sec, now_ts);

        // 获取内存数据的 Arrow 表示
        let mem_batch = self.mem.to_record_batch(
            &req.device_id,
            plan.mem_from_ts,
            plan.mem_to_ts,
            &req.params,
        );

        // 记录内存数据行数（用于统计）
        let mem_n = mem_batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);

        // 使用 DuckDB 统一查询
        let results = self.duckdb.query_unified(
            &req.device_id,
            plan.disk_from_ts,
            plan.disk_to_ts,
            plan.mem_from_ts,
            plan.mem_to_ts,
            &req.params,
            req.limit,
            mem_batch,
        )?;

        let disk_n = results.len().saturating_sub(mem_n);

        Ok(QueryResponse {
            rows: results,
            source_disk_rows: disk_n,
            source_mem_rows: mem_n,
        })
    }

    /// 简单查询：只查询磁盘 Parquet 数据（不合并内存），用于 API 快速响应
    pub async fn query_disk_only(&self, req: QueryRequest) -> anyhow::Result<QueryResponse> {
        let results = self.duckdb.query(
            &req.device_id,
            req.from_ts,
            req.to_ts,
            &req.params,
        )?;

        let results = if let Some(limit) = req.limit {
            results.into_iter().take(limit).collect()
        } else {
            results
        };

        Ok(QueryResponse {
            rows: results,
            source_disk_rows: 0,
            source_mem_rows: 0,
        })
    }
}


