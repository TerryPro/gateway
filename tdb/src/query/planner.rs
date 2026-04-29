use crate::model::QueryRequest;

/// 查询计划：将请求切分为磁盘区间与内存区间。
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub disk_from_ts: u64,
    pub disk_to_ts: u64,
    pub mem_from_ts: u64,
    pub mem_to_ts: u64,
}

/// 根据“最近窗口”边界拆分查询范围，避免漏查未落盘数据。
pub fn build_plan(req: &QueryRequest, mem_window_sec: u64, now_ts: u64) -> QueryPlan {
    let mem_boundary = now_ts.saturating_sub(mem_window_sec);
    let disk_from = req.from_ts;
    let disk_to = req.to_ts.min(mem_boundary.saturating_sub(1));
    let mem_from = req.from_ts.max(mem_boundary);
    let mem_to = req.to_ts;
    QueryPlan {
        disk_from_ts: disk_from,
        disk_to_ts: disk_to,
        mem_from_ts: mem_from,
        mem_to_ts: mem_to,
    }
}
