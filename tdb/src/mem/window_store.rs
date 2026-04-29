use crate::model::{DataPoint, IngestBatch};
use chrono::{Datelike, Local, TimeZone, Timelike};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

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

/// 最近窗口内存存储：按 device 分桶，提供时间和参数过滤查询。
#[derive(Clone)]
pub struct WindowStore {
    window_sec: u64,
    inner: Arc<RwLock<HashMap<String, Vec<DataPoint>>>>,
}

/// 内存窗口快照统计，用于运维观测。
#[derive(Debug, Clone, Serialize)]
pub struct WindowStats {
    pub device_count: usize,
    pub total_points: usize,
}

impl WindowStore {
    /// 创建窗口存储，`window_sec` 控制保留范围。
    pub fn new(window_sec: u64) -> Self {
        Self {
            window_sec,
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 返回配置的内存窗口秒数。
    pub fn window_sec(&self) -> u64 {
        self.window_sec
    }

    /// 插入一批数据并执行基于事件时间的过期清理。
    pub fn insert_batch(&self, batch: IngestBatch) {
        let mut guard = self.inner.write().expect("window lock poisoned");
        insert_points(&mut guard, batch.device_id, batch.points);
    }

    /// 回放写入：用于启动恢复时注入历史 WAL 数据。
    pub fn insert_recovered_batch(&self, batch: IngestBatch) {
        let mut guard = self.inner.write().expect("window lock poisoned");
        insert_points(&mut guard, batch.device_id, batch.points);
    }

    /// 在内存中查询指定设备/参数/时间范围的数据。
    pub fn query(
        &self,
        device_id: &str,
        from_ts: u64,
        to_ts: u64,
        params: &[String],
        limit: Option<usize>,
    ) -> Vec<DataPoint> {
        let guard = self.inner.read().expect("window lock poisoned");
        let Some(points) = guard.get(device_id) else {
            return Vec::new();
        };
        let pset: HashSet<&str> = params.iter().map(String::as_str).collect();
        let mut out = Vec::new();
        for p in points {
            if p.ts < from_ts || p.ts > to_ts {
                continue;
            }
            if !pset.is_empty() && !pset.contains(p.param_id.as_str()) {
                continue;
            }
            out.push(p.clone());
            if let Some(n) = limit && out.len() >= n {
                break;
            }
        }
        out
    }

    /// 提取 `flush_before_ts` 之前的数据并从内存中移除，用于后台落盘。
    pub fn drain_flushable(&self, flush_before_ts: u64) -> HashMap<String, Vec<DataPoint>> {
        let mut guard = self.inner.write().expect("window lock poisoned");
        let mut out: HashMap<String, Vec<DataPoint>> = HashMap::new();
        for (device_id, points) in guard.iter_mut() {
            let mut keep = Vec::with_capacity(points.len());
            let mut flush = Vec::new();
            for p in points.drain(..) {
                if p.ts < flush_before_ts {
                    flush.push(p);
                } else {
                    keep.push(p);
                }
            }
            *points = keep;
            if !flush.is_empty() {
                out.insert(device_id.clone(), flush);
            }
        }
        out
    }

    /// 清空内存窗口数据，通常用于 WAL 全量回放前的重建。
    pub fn clear(&self) {
        let mut guard = self.inner.write().expect("window lock poisoned");
        guard.clear();
    }

    /// 返回当前内存窗口的设备数与点数统计。
    pub fn snapshot_stats(&self) -> WindowStats {
        let guard = self.inner.read().expect("window lock poisoned");
        let total_points = guard.values().map(Vec::len).sum();
        WindowStats {
            device_count: guard.len(),
            total_points,
        }
    }
}

/// 将点数据写入设备槽位，等待 flush 阶段统一迁移到磁盘。
fn insert_points(guard: &mut HashMap<String, Vec<DataPoint>>, device_id: String, points: Vec<DataPoint>) {
    let slot = guard.entry(device_id).or_default();
    slot.extend(points);
}
