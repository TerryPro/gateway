use serde::{Deserialize, Serialize};

/// 单点时序值，采用 long-row 风格。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPoint {
    pub ts: u64,
    pub param_id: String,
    pub value: f32,
}

/// MQTT 解包后的输入数据包。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestPacket {
    pub device_id: String,
    pub recv_ts: u64,
    pub points: Vec<DataPoint>,
}

/// 进入写入链路的批次结构。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestBatch {
    pub device_id: String,
    pub recv_ts: u64,
    pub points: Vec<DataPoint>,
}

/// HTTP 查询请求体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    pub device_id: String,
    pub from_ts: u64,
    pub to_ts: u64,
    pub params: Vec<String>,
    pub limit: Option<usize>,
}

/// HTTP 查询返回体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub rows: Vec<DataPoint>,
    pub source_disk_rows: usize,
    pub source_mem_rows: usize,
}
