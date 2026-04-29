[cfg(test)]
mod tests;

use std::{
    collections::HashMap,
    fmt::Debug,
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use arrow::{
    array::{ArrayRef, Float64Builder, UInt64Builder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use chrono::{Datelike, Timelike, Utc};
use duckdb::{params, Connection, Database};
use parking_lot::RwLock;
use parquet::{
    arrow::ArrowWriter,
    basic::{Encoding, Compression},
    file::{
        properties::{WriterProperties, WriterVersion},
        statistics::Statistics,
    },
};

pub const MAX_POINTS: usize = 10000;
pub const FLUSH_ROWS_THRESHOLD: usize = 7200;
pub const FLUSH_TIME_THRESHOLD_MS: u64 = 3600_000;

#[derive(Debug, Clone)]
pub struct SparseRecord {
    pub timestamp: i64,
    pub values: HashMap<u16, f64>,
}

#[derive(Debug, Clone)]
pub struct DeviceData {
    pub device_id: String,
    pub timestamp: i64,
    pub values: HashMap<String, f64>,
}

pub struct DoubleBuffer {
    active: RwLock<Vec<SparseRecord>>,
    flush: RwLock<Vec<SparseRecord>>,
    last_flush_time: RwLock<Instant>,
    row_count: RwLock<usize>,
}

impl DoubleBuffer {
    pub fn new() -> Self {
        Self {
            active: RwLock::new(Vec::with_capacity(FLUSH_ROWS_THRESHOLD * 2)),
            flush: RwLock::new(Vec::with_capacity(FLUSH_ROWS_THRESHOLD * 2)),
            last_flush_time: RwLock::new(Instant::now()),
            row_count: RwLock::new(0),
        }
    }

    pub fn push(&self, record: SparseRecord) -> bool {
        {
            let mut active = self.active.write();
            active.push(record);
            let count = active.len();
            *self.row_count.write() = count;
        }
        self.should_flush()
    }

    pub fn should_flush(&self) -> bool {
        let row_count = *self.row_count.read();
        let elapsed = self.last_flush_time.read().elapsed().as_millis() as u64;
        row_count >= FLUSH_ROWS_THRESHOLD || elapsed >= FLUSH_TIME_THRESHOLD_MS
    }

    pub fn swap(&self) -> Vec<SparseRecord> {
        {
            let mut last_flush = self.last_flush_time.write();
            *last_flush = Instant::now();
        }
        {
            let mut row_count = self.row_count.write();
            *row_count = 0;
        }
        let mut active = self.active.write();
        let mut flush = self.flush.write();
        std::mem::swap(&mut active, &mut flush);
        let mut result = Vec::with_capacity(flush.len());
        std::mem::swap(&mut result, &mut flush);
        result
    }

    pub fn get_active_snapshot(&self) -> Vec<SparseRecord> {
        self.active.read().clone()
    }
}

impl Default for DoubleBuffer {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ParquetWriter {
    base_path: PathBuf,
    row_group_size: usize,
}

impl ParquetWriter {
    pub fn new(base_path: impl Into<PathBuf>, row_group_size: usize) -> Self {
        Self {
            base_path: base_path.into(),
            row_group_size,
        }
    }

    pub fn write_batch(&self, records: &[SparseRecord], device_id: &str) -> anyhow::Result<Vec<PathBuf>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let batches = self.split_into_batches(records, self.row_group_size);
        let mut written_files = Vec::new();
        for batch in batches {
            let file_path = self.get_file_path(device_id, batch[0].timestamp)?;
            self.write_single_batch(&batch, &file_path)?;
            written_files.push(file_path);
        }
        Ok(written_files)
    }

    fn split_into_batches(&self, records: &[SparseRecord], batch_size: usize) -> Vec<Vec<SparseRecord>> {
        let mut batches = Vec::new();
        for chunk in records.chunks(batch_size) {
            batches.push(chunk.to_vec());
        }
        batches
    }

    fn get_file_path(&self, device_id: &str, timestamp: i64) -> anyhow::Result<PathBuf> {
        let dt = chrono::DateTime::from_timestamp_millis(timestamp)
            .unwrap_or_else(|| Utc::now());
        let year = dt.year();
        let month = dt.month();
        let day = dt.day();
        let hour = dt.hour();

        let path = self.base_path
            .join(format!("device_id={}", device_id))
            .join(format!("year={}", year))
            .join(format!("month={:02}", month))
            .join(format!("day={:02}", day))
            .join(format!("hour_{:02}.parquet", hour));
        Ok(path)
    }

    fn write_single_batch(&self, records: &[SparseRecord], file_path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(file_path)?;
        let schema = self.build_schema();
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_compression(Compression::ZSTD)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        let batch = self.records_to_record_batch(records)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn build_schema(&self) -> Arc<Schema> {
        let mut fields = vec![Field::new("timestamp", DataType::Int64, false)];
        for i in 1..=MAX_POINTS {
            fields.push(Field::new(
                format!("point_{:04}", i),
                DataType::Float64,
                true,
            ));
        }
        Arc::new(Schema::new(fields))
    }

    fn records_to_record_batch(&self, records: &[SparseRecord]) -> anyhow::Result<RecordBatch> {
        let num_rows = records.len();
        let mut ts_builder = UInt64Builder::with_capacity(num_rows);
        let mut point_builders: Vec<Float64Builder> = (0..MAX_POINTS)
            .map(|_| Float64Builder::with_capacity(num_rows))
            .collect();

        for record in records {
            ts_builder.append_value(record.timestamp as u64);
            for (point_idx, builder) in point_builders.iter_mut().enumerate() {
                let point_id = (point_idx + 1) as u16;
                if let Some(value) = record.values.get(&point_id) {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
            }
        }

        let ts_array: ArrayRef = Arc::new(ts_builder.finish());
        let point_arrays: Vec<ArrayRef> = point_builders
            .into_iter()
            .map(|b| Arc::new(b.finish()) as ArrayRef)
            .collect();

        let mut arrays = vec![ts_array];
        arrays.extend(point_arrays);

        let schema = self.build_schema();
        RecordBatch::try_new(schema, arrays).map_err(Into::into)
    }
}

pub struct DuckDbQueryService {
    conn: RwLock<Connection>,
    base_path: PathBuf,
}

impl DuckDbQueryService {
    pub fn new(base_path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        let service = Self {
            conn: RwLock::new(conn),
            base_path: base_path.into(),
        };
        service.init_duckdb()?;
        Ok(service)
    }

    fn init_duckdb(&self) -> anyhow::Result<()> {
        let conn = self.conn.read();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS device_points (
                device_id VARCHAR,
                timestamp BIGINT,
                point_id VARCHAR,
                value DOUBLE
            )",
            [],
        )?;
        conn.execute(
            "CREATE SEQUENCE IF NOT EXISTS seq_id START 1",
            [],
        )?;
        Ok(())
    }

    pub fn query_point(
        &self,
        device_id: &str,
        point_id: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> anyhow::Result<Vec<(i64, Option<f64>)>> {
        let conn = self.conn.read();
        let sql = format!(
            "SELECT timestamp, \"{}\" as value
             FROM '{
             base_path}/device_id={}/year=*/month=*/day=*/hour_*.parquet'
             WHERE timestamp BETWEEN $1 AND $2
             ORDER BY timestamp",
            point_id,
            device_id,
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![start_ts, end_ts], |row| {
            let ts: i64 = row.get(0)?;
            let value: Option<f64> = row.get(1)?;
            Ok((ts, value))
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn query_point_with_buffer(
        &self,
        device_id: &str,
        point_id: &str,
        start_ts: i64,
        end_ts: i64,
        buffer: &[SparseRecord],
    ) -> anyhow::Result<Vec<(i64, Option<f64>)>> {
        let point_idx = Self::parse_point_id(point_id)?;
        self.register_buffer_table(device_id, point_idx, buffer)?;
        let conn = self.conn.read();
        let sql = format!(
            "SELECT timestamp, value FROM memory_buffer
             WHERE timestamp BETWEEN $1 AND $2
             UNION ALL
             SELECT timestamp, \"{}\" as value
             FROM '{{}}
             /device_id={}/year=*/month=*/day=*/hour_*.parquet'
             WHERE timestamp BETWEEN $1 AND $2
             ORDER BY timestamp",
            point_id,
            device_id,
        );
        let pattern = self.base_path.to_string_lossy();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![start_ts, end_ts, pattern], |row| {
            let ts: i64 = row.get(0)?;
            let value: Option<f64> = row.get(1)?;
            Ok((ts, value))
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    fn register_buffer_table(
        &self,
        device_id: &str,
        point_idx: u16,
        buffer: &[SparseRecord],
    ) -> anyhow::Result<()> {
        let conn = self.conn.read();
        conn.execute("DROP TABLE IF EXISTS memory_buffer", [])?;
        conn.execute(
            "CREATE TABLE memory_buffer (
                device_id VARCHAR,
                timestamp BIGINT,
                value DOUBLE
            )",
            [],
        )?;
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare("INSERT INTO memory_buffer VALUES ($1, $2, $3)")?;
            for record in buffer {
                if record.timestamp >= i64::MIN && record.timestamp <= i64::MAX {
                    let value = record.values.get(&point_idx);
                    stmt.execute(params![
                        device_id,
                        record.timestamp,
                        value
                    ])?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn parse_point_id(point_id: &str) -> anyhow::Result<u16> {
        let stripped = point_id.strip_prefix("point_").unwrap_or(point_id);
        let idx: u16 = stripped.parse()
            .map_err(|_| anyhow::anyhow!("Invalid point_id format: {}", point_id))?;
        if idx == 0 || idx > MAX_POINTS as u16 {
            anyhow::bail!("Point ID out of range: {} (valid: 1-{})", idx, MAX_POINTS);
        }
        Ok(idx)
    }
}

pub fn parse_jsonl_record(line: &str) -> Option<DeviceData> {
    let json: serde_json::Value = serde_json::from_str(line).ok()?;
    let id = json.get("id")?.as_str()?.to_string();
    let t = json.get("t")?.as_i64()?;
    let p = json.get("p")?.as_object()?;
    let mut values = HashMap::new();
    for (k, v) in p {
        if let Some(num) = v.as_f64() {
            values.insert(k.clone(), num);
        }
    }
    Some(DeviceData {
        device_id: id,
        timestamp: t,
        values,
    })
}

pub fn parse_point_code(code: &str) -> Option<u16> {
    let stripped = code.strip_prefix('P')?;
    stripped.parse().ok()
}

pub fn device_data_to_sparse_record(data: &DeviceData) -> SparseRecord {
    let mut values = HashMap::new();
    for (k, v) in &data.values {
        if let Some(idx) = parse_point_code(k) {
            values.insert(idx, *v);
        }
    }
    SparseRecord {
        timestamp: data.timestamp,
        values,
    }
}

pub struct TsdbService {
    buffer: Arc<DoubleBuffer>,
    writer: Arc<ParquetWriter>,
    query_service: Arc<DuckDbQueryService>,
    flush_handle: Option<thread::JoinHandle<()>>,
    shutdown_rx: Arc<RwLock<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl TsdbService {
    pub fn new(base_path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let buffer = Arc::new(DoubleBuffer::new());
        let writer = Arc::new(ParquetWriter::new(base_path.clone(), 1000));
        let query_service = Arc::new(DuckDbQueryService::new(base_path)?);
        let shutdown_rx = Arc::new(RwLock::new(None));
        Ok(Self {
            buffer,
            writer,
            query_service,
            flush_handle: None,
            shutdown_rx,
        })
    }

    pub fn start_background_flush(&mut self, device_id: String) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.shutdown_rx.write() = Some(tx);
        let buffer = self.buffer.clone();
        let writer = self.writer.clone();
        let device_id_clone = device_id.clone();
        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                Self::flush_loop(buffer, writer, device_id_clone, rx).await;
            });
        });
        self.flush_handle = Some(handle);
    }

    async fn flush_loop(
        buffer: Arc<DoubleBuffer>,
        writer: Arc<ParquetWriter>,
        device_id: String,
        mut rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if buffer.should_flush() {
                        let records = buffer.swap();
                        if !records.is_empty() {
                            if let Err(e) = writer.write_batch(&records, &device_id) {
                                tracing::error!("Flush error: {}", e);
                            }
                        }
                    }
                }
                _ = &mut rx => {
                    let records = buffer.swap();
                    if !records.is_empty() {
                        let _ = writer.write_batch(&records, &device_id);
                    }
                    break;
                }
            }
        }
    }

    pub fn push(&self, data: DeviceData) -> bool {
        let record = device_data_to_sparse_record(&data);
        self.buffer.push(record)
    }

    pub fn query_point(
        &self,
        point_id: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> anyhow::Result<Vec<(i64, Option<f64>)>> {
        let buffer_snapshot = self.buffer.get_active_snapshot();
        self.query_service.query_point_with_buffer(
            "dev001",
            point_id,
            start_ts,
            end_ts,
            &buffer_snapshot,
        )
    }

    pub fn flush(&self) -> anyhow::Result<Vec<PathBuf>> {
        let records = self.buffer.swap();
        self.writer.write_batch(&records, "dev001")
    }

    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_rx.write().take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.flush_handle.take() {
            let _ = handle.join();
        }
    }
}

pub fn load_jsonl_to_parquet(
    jsonl_path: &Path,
    base_path: &Path,
    batch_size: usize,
) -> anyhow::Result<Vec<PathBuf>> {
    let writer = ParquetWriter::new(base_path, 1000);
    let content = std::fs::read_to_string(jsonl_path)?;
    let lines: Vec<&str> = content.lines().collect();
    let mut all_records: Vec<SparseRecord> = Vec::new();
    let mut written_files = Vec::new();
    for line in lines {
        if let Some(data) = parse_jsonl_record(line) {
            let record = device_data_to_sparse_record(&data);
            all_records.push(record);
        }
        if all_records.len() >= batch_size {
            let device_id = "dev001";
            let files = writer.write_batch(&all_records, device_id)?;
            written_files.extend(files);
            all_records.clear();
        }
    }
    if !all_records.is_empty() {
        let files = writer.write_batch(&all_records, "dev001")?;
        written_files.extend(files);
    }
    Ok(written_files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_point_code() {
        assert_eq!(parse_point_code("P0001"), Some(1));
        assert_eq!(parse_point_code("P1000"), Some(1000));
        assert_eq!(parse_point_code("P1234"), Some(1234));
        assert_eq!(parse_point_code("P0000"), Some(0));
        assert_eq!(parse_point_code("invalid"), None);
    }

    #[test]
    fn test_double_buffer_push() {
        let buffer = DoubleBuffer::new();
        let record = SparseRecord {
            timestamp: 1000,
            values: HashMap::from([(1, 1.0), (100, 100.0)]),
        };
        let should_flush = buffer.push(record);
        assert!(!should_flush);
        assert_eq!(*buffer.row_count.read(), 1);
    }

    #[test]
    fn test_double_buffer_swap() {
        let buffer = DoubleBuffer::new();
        for i in 0..5 {
            buffer.push(SparseRecord {
                timestamp: i as i64 * 1000,
                values: HashMap::from([(i as u16, i as f64)]),
            });
        }
        let swapped = buffer.swap();
        assert_eq!(swapped.len(), 5);
        assert_eq!(*buffer.row_count.read(), 0);
    }

    #[test]
    fn test_parse_jsonl_record() {
        let line = r#"{"id":"dev001","t":1777050000000,"s":1,"p":{"P0001":100.5,"P1000":200.5}}"#;
        let data = parse_jsonl_record(line).unwrap();
        assert_eq!(data.device_id, "dev001");
        assert_eq!(data.timestamp, 1777050000000);
        assert_eq!(data.values.get("P0001"), Some(&100.5));
        assert_eq!(data.values.get("P1000"), Some(&200.5));
    }
}