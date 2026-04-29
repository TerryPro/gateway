use crate::mem::time_window_buffer::TimeWindowBuffer;
use crate::model::{QueryRequest, QueryResponse};
use crate::query::planner::build_plan;
use crate::query::datafusion_executor::QueryExecutor as DfQueryExecutor;

#[derive(Clone)]
pub struct QueryExecutor {
    mem: TimeWindowBuffer,
    mem_window_sec: u64,
    df: DfQueryExecutor,
}

impl QueryExecutor {
    pub fn new(_root: String, mem: TimeWindowBuffer, df: DfQueryExecutor) -> Self {
        let mem_window_sec = 3600;
        Self {
            mem,
            mem_window_sec,
            df,
        }
    }

    pub async fn execute(&self, req: QueryRequest) -> anyhow::Result<QueryResponse> {
        let now_ts = chrono::Local::now().timestamp().max(0) as u64;
        let plan = build_plan(&req, self.mem_window_sec, now_ts);

        let mem_batch = self.mem.to_record_batch(
            &req.device_id,
            plan.mem_from_ts,
            plan.mem_to_ts,
            &req.params,
        );

        let mem_n = mem_batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);

        let results = self.df.query_unified(
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

    pub async fn query_disk_only(&self, req: QueryRequest) -> anyhow::Result<QueryResponse> {
        let results = self.df.query(
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
