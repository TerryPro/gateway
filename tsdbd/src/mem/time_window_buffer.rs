//! 时序时间窗口双缓冲存储器
//!
//! 设计目标：
//! - 支持高并发写入（采集线程）
//! - 支持低延迟查询（查询线程）
//! - 支持后台 flush（刷盘线程）
//! - flush 期间不阻塞写入和查询

use crate::model::{DataPoint, IngestBatch};
use arrow::array::{Float32Builder, StringBuilder, UInt64Builder};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use chrono::{Datelike, Local, TimeZone, Timelike};
use parking_lot::RwLock;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// 小时窗口键（年 - 月 - 日 - 时）。
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct HourKey {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
}

impl HourKey {
    /// 从时间戳（毫秒）创建小时键。
    pub fn from_ts(ts_millis: u64) -> Self {
        let dt = Local.timestamp_millis_opt(ts_millis as i64).unwrap();
        Self {
            year: dt.year(),
            month: dt.month(),
            day: dt.day(),
            hour: dt.hour(),
        }
    }

    /// 转换为小时起始时间戳（毫秒）。
    pub fn to_ts_millis(&self) -> u64 {
        Local
            .with_ymd_and_hms(self.year, self.month, self.day, self.hour, 0, 0)
            .unwrap()
            .timestamp_millis() as u64
    }

    /// 转换为下一个小时的键。
    pub fn next_hour(&self) -> Self {
        let mut next = *self;
        next.hour += 1;
        if next.hour >= 24 {
            next.hour = 0;
            next.day += 1;
            if next.day >= 28 {
                next.day = 1;
                next.month += 1;
                if next.month > 12 {
                    next.month = 1;
                    next.year += 1;
                }
            }
        }
        next
    }
}

/// 设备级数据缓冲（按小时窗口组织）。
type DeviceBuffer = HashMap<String, Vec<DataPoint>>;

/// 小时窗口缓冲（多个设备）。
type HourWindowBuffer = HashMap<HourKey, DeviceBuffer>;

/// 时序时间窗口双缓冲存储器。
///
/// 架构：
/// ```text
/// ┌─────────────────────────────────────┐
/// │  Active Buffer (活跃缓冲)            │
/// │  - 接收新数据                         │
/// │  - 支持查询                           │
/// └─────────────────────────────────────┘
///              ↓ swap()
/// ┌─────────────────────────────────────┐
/// │  Flushing Buffer (待刷盘缓冲)        │
/// │  - 存储待落盘数据                     │
/// │  - 支持查询                           │
/// │  - flush 完成后清空                   │
/// └─────────────────────────────────────┘
/// ```
pub struct TimeWindowBuffer {
    /// 活跃缓冲区（可写入，可查询）
    active: Arc<RwLock<HourWindowBuffer>>,
    /// 待刷盘缓冲区（只读，可查询，等待 flush）
    flushing: Arc<RwLock<HourWindowBuffer>>,
    /// 当前活跃的小时窗口（用于快速判断）
    current_hour: Arc<RwLock<HourKey>>,
}

/// 内存窗口快照统计。
#[derive(Debug, Clone, Serialize)]
pub struct WindowStats {
    pub active_hour_count: usize,
    pub flushing_hour_count: usize,
    pub device_count: usize,
    pub active_points: usize,
    pub flushing_points: usize,
}

impl TimeWindowBuffer {
    /// 创建双缓冲存储器。
    pub fn new() -> Self {
        let current_hour = HourKey::from_ts(now_ms());
        Self {
            active: Arc::new(RwLock::new(HashMap::new())),
            flushing: Arc::new(RwLock::new(HashMap::new())),
            current_hour: Arc::new(RwLock::new(current_hour)),
        }
    }

    /// 插入一批数据（来自 ingest）。
    pub fn insert_batch(&self, batch: IngestBatch) {
        let mut guard = self.active.write();
        for point in batch.points {
            self.insert_point_into_guard(&mut guard, batch.device_id.clone(), point);
        }
    }

    /// 插入回放数据（来自 WAL 恢复）。
    pub fn insert_recovered_batch(&self, batch: IngestBatch) {
        let mut guard = self.active.write();
        for point in batch.points {
            self.insert_point_into_guard(&mut guard, batch.device_id.clone(), point);
        }
    }

    /// 插入单个数据点。
    #[allow(dead_code)]
    pub fn insert(&self, device_id: String, point: DataPoint) {
        let mut guard = self.active.write();
        self.insert_point_into_guard(&mut guard, device_id, point);
    }

    /// 查询指定设备/参数/时间范围的数据（合并 active + flushing）。
    pub fn query(
        &self,
        device_id: &str,
        from_ts: u64,
        to_ts: u64,
        params: &[String],
        limit: Option<usize>,
    ) -> Vec<DataPoint> {
        let active_guard = self.active.read();
        let flushing_guard = self.flushing.read();
        
        let mut out = Vec::new();
        let pset: HashSet<&str> = params.iter().map(String::as_str).collect();

        // 查询 active buffer
        self.query_buffer(&active_guard, device_id, from_ts, to_ts, &pset, &mut out, limit);
        
        // 如果还需要更多数据，查询 flushing buffer
        if limit.map_or(true, |n| out.len() < n) {
            self.query_buffer(&flushing_guard, device_id, from_ts, to_ts, &pset, &mut out, limit);
        }

        out.sort_by_key(|x| x.ts);
        
        // 应用 limit
        if let Some(n) = limit {
            out.truncate(n);
        }
        
        out
    }

    /// 将内存数据转换为 Arrow RecordBatch。
    ///
    /// 用于与 DuckDB 集成，实现统一查询。
    pub fn to_record_batch(
        &self,
        device_id: &str,
        from_ts: u64,
        to_ts: u64,
        params: &[String],
    ) -> Option<RecordBatch> {
        // 先查询数据
        let rows = self.query(device_id, from_ts, to_ts, params, None);
        
        if rows.is_empty() {
            return None;
        }
        
        // 构建 Arrow 数组
        let mut ts_builder = UInt64Builder::with_capacity(rows.len());
        let mut param_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 10);
        let mut value_builder = Float32Builder::with_capacity(rows.len());
        
        for row in rows {
            ts_builder.append_value(row.ts);
            param_builder.append_value(&row.param_id);
            value_builder.append_value(row.value);
        }
        
        // 构建 schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::UInt64, false),
            Field::new("param_id", DataType::Utf8, false),
            Field::new("value", DataType::Float32, false),
        ]));
        
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ts_builder.finish()),
                Arc::new(param_builder.finish()),
                Arc::new(value_builder.finish()),
            ],
        ).ok()?;
        
        Some(batch)
    }

    /// 执行交换：将 active buffer 转移到 flushing buffer，返回需要 flush 的数据。
    ///
    /// 这个操作应该快速完成（毫秒级），只涉及指针交换。
    pub fn swap(&self) -> HashMap<HourKey, HashMap<String, Vec<DataPoint>>> {
        let mut active_guard = self.active.write();
        let mut flushing_guard = self.flushing.write();
        
        // 更新当前小时窗口
        let new_hour = HourKey::from_ts(now_ms());
        *self.current_hour.write() = new_hour;
        
        // 交换缓冲区
        let mut to_flush = HashMap::new();
        std::mem::swap(&mut *active_guard, &mut to_flush);
        
        // 清空 flushing buffer（之前的数据应该已经被 flush 了）
        *flushing_guard = to_flush.clone();
        
        to_flush
    }

    /// 获取待 flush 的数据（从 flushing buffer 提取）。
    #[allow(dead_code)]
    pub fn get_flushable(&self) -> HashMap<HourKey, HashMap<String, Vec<DataPoint>>> {
        let flushing_guard = self.flushing.read();
        (*flushing_guard).clone()
    }

    /// 清空 flushing buffer（flush 完成后调用）。
    pub fn clear_flushing(&self) {
        let mut flushing_guard = self.flushing.write();
        flushing_guard.clear();
    }

    /// 清空所有数据。
    pub fn clear(&self) {
        let mut active_guard = self.active.write();
        let mut flushing_guard = self.flushing.write();
        active_guard.clear();
        flushing_guard.clear();
    }

    /// 获取统计信息。
    pub fn snapshot_stats(&self) -> WindowStats {
        let active_guard = self.active.read();
        let flushing_guard = self.flushing.read();
        
        let (active_points, active_devices) = self.count_points(&active_guard);
        let (flushing_points, flushing_devices) = self.count_points(&flushing_guard);
        
        let mut all_devices = HashSet::new();
        all_devices.extend(active_devices);
        all_devices.extend(flushing_devices);

        WindowStats {
            active_hour_count: active_guard.len(),
            flushing_hour_count: flushing_guard.len(),
            device_count: all_devices.len(),
            active_points,
            flushing_points,
        }
    }

    /// 内部方法：在已持有写锁的情况下插入数据点。
    fn insert_point_into_guard(
        &self,
        guard: &mut HourWindowBuffer,
        device_id: String,
        point: DataPoint,
    ) {
        let hour_key = HourKey::from_ts(point.ts);
        let device_map = guard.entry(hour_key).or_insert_with(HashMap::new);
        let points = device_map.entry(device_id).or_insert_with(Vec::new);
        points.push(point);
    }

    /// 内部方法：查询缓冲区。
    fn query_buffer(
        &self,
        buffer: &HourWindowBuffer,
        device_id: &str,
        from_ts: u64,
        to_ts: u64,
        params: &HashSet<&str>,
        out: &mut Vec<DataPoint>,
        limit: Option<usize>,
    ) {
        for (hour_key, device_map) in buffer {
            if let Some(points) = device_map.get(device_id) {
                let hour_start = hour_key.to_ts_millis();
                let hour_end = hour_key.next_hour().to_ts_millis();

                // 快速跳过不相关的小时窗口
                if hour_end < from_ts || hour_start > to_ts {
                    continue;
                }

                for point in points {
                    if point.ts < from_ts || point.ts > to_ts {
                        continue;
                    }
                    if !params.is_empty() && !params.contains(point.param_id.as_str()) {
                        continue;
                    }
                    out.push(point.clone());
                    
                    if let Some(n) = limit {
                        if out.len() >= n {
                            return;
                        }
                    }
                }
            }
        }
    }

    /// 内部方法：统计点数和设备数。
    fn count_points(&self, buffer: &HourWindowBuffer) -> (usize, HashSet<String>) {
        let mut total = 0;
        let mut devices = HashSet::new();
        for device_map in buffer.values() {
            for (device_id, points) in device_map {
                total += points.len();
                devices.insert(device_id.clone());
            }
        }
        (total, devices)
    }
}

impl Default for TimeWindowBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TimeWindowBuffer {
    fn clone(&self) -> Self {
        Self {
            active: Arc::clone(&self.active),
            flushing: Arc::clone(&self.flushing),
            current_hour: Arc::clone(&self.current_hour),
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_query() {
        let buffer = TimeWindowBuffer::new();

        // 插入数据
        let ts = now_ms();
        buffer.insert(
            "dev001".to_string(),
            DataPoint {
                ts,
                param_id: "P00001".to_string(),
                value: 25.5,
            },
        );

        // 查询
        let results = buffer.query("dev001", ts - 1000, ts + 1000, &[], None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, 25.5);
    }

    #[test]
    fn test_swap_and_query() {
        let buffer = TimeWindowBuffer::new();

        // 插入数据
        let ts = now_ms();
        buffer.insert(
            "dev001".to_string(),
            DataPoint {
                ts,
                param_id: "P00001".to_string(),
                value: 25.5,
            },
        );

        // 执行交换
        let flushable = buffer.swap();
        assert!(!flushable.is_empty());

        // 查询应该仍然能查到数据（从 flushing buffer）
        let results = buffer.query("dev001", ts - 1000, ts + 1000, &[], None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, 25.5);

        // 清空 flushing 后查询不到
        buffer.clear_flushing();
        let results = buffer.query("dev001", ts - 1000, ts + 1000, &[], None);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_to_record_batch() {
        let buffer = TimeWindowBuffer::new();

        // 插入多条数据
        let ts = now_ms();
        buffer.insert(
            "dev001".to_string(),
            DataPoint {
                ts,
                param_id: "P00001".to_string(),
                value: 25.5,
            },
        );
        buffer.insert(
            "dev001".to_string(),
            DataPoint {
                ts: ts + 1000,
                param_id: "P00002".to_string(),
                value: 30.0,
            },
        );
        buffer.insert(
            "dev001".to_string(),
            DataPoint {
                ts: ts + 2000,
                param_id: "P00001".to_string(),
                value: 26.5,
            },
        );

        // 转换为 Arrow RecordBatch
        let batch = buffer.to_record_batch("dev001", ts, ts + 3000, &[]);
        assert!(batch.is_some());
        
        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 3);
        
        // 验证 schema
        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "ts");
        assert_eq!(schema.field(1).name(), "param_id");
        assert_eq!(schema.field(2).name(), "value");
    }
}
