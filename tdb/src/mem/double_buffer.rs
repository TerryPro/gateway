use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use parking_lot::RwLock;
use crate::model::DataPoint;

/// 双缓冲区结构，用于采集线程与刷盘线程之间的无锁/低锁数据交换。
///
/// 设计要点：
/// - `active` 与 `flush` 两个 Vec 通过 RwLock 保护，交换时仅持锁极短时间。
/// - 采集线程只向 active buffer 追加数据。
/// - 当 active 达到行数阈值或时间阈值时，触发交换，刷盘线程异步处理 flush buffer。
#[derive(Debug)]
pub struct DoubleBuffer {
    inner: RwLock<DoubleBufferInner>,
    /// 上次刷盘时间戳（毫秒）
    last_flush_ms: AtomicU64,
    /// 行数阈值，默认 7200 行（约 1 小时，500ms 间隔）
    row_threshold: usize,
    /// 时间阈值（毫秒），默认 1 小时
    time_threshold_ms: u64,
}

#[derive(Debug)]
struct DoubleBufferInner {
    active: Vec<DataPoint>,
    flush: Vec<DataPoint>,
}

impl DoubleBuffer {
    pub fn new(row_threshold: usize, time_threshold: Duration) -> Self {
        Self {
            inner: RwLock::new(DoubleBufferInner {
                active: Vec::with_capacity(row_threshold),
                flush: Vec::with_capacity(row_threshold),
            }),
            last_flush_ms: AtomicU64::new(now_ms()),
            row_threshold,
            time_threshold_ms: time_threshold.as_millis() as u64,
        }
    }

    /// 向 active buffer 追加一条记录，持写锁时间极短。
    pub fn push(&self, point: DataPoint) {
        let mut guard = self.inner.write();
        guard.active.push(point);
    }

    /// 批量追加记录。
    pub fn extend(&self, points: Vec<DataPoint>) {
        let mut guard = self.inner.write();
        guard.active.extend(points);
    }

    /// 检查是否需要触发刷盘（行数或时间阈值）。
    pub fn should_flush(&self) -> bool {
        let guard = self.inner.read();
        if guard.active.len() >= self.row_threshold {
            return true;
        }
        drop(guard);

        let elapsed = now_ms().saturating_sub(self.last_flush_ms.load(Ordering::Relaxed));
        elapsed >= self.time_threshold_ms
    }

    /// 执行双缓冲交换，返回需要刷盘的数据。
    ///
    /// 交换后：
    /// - active 变为空的 Vec（保留容量复用）。
    /// - flush 持有之前 active 的数据，由调用者处理。
    pub fn swap(&self) -> Vec<DataPoint> {
        let mut guard = self.inner.write();
        let mut new_active = Vec::with_capacity(self.row_threshold);
        let mut new_flush = Vec::with_capacity(self.row_threshold);
        std::mem::swap(&mut guard.active, &mut new_flush);
        std::mem::swap(&mut guard.active, &mut new_active);
        drop(guard);

        self.last_flush_ms.store(now_ms(), Ordering::Relaxed);
        new_flush
    }

    /// 获取当前 active buffer 的快照（用于查询）。
    ///
    /// 持读锁拷贝数据，返回后锁已释放。
    pub fn snapshot(&self) -> Vec<DataPoint> {
        let guard = self.inner.read();
        guard.active.clone()
    }

    /// 获取当前 active buffer 中的行数。
    pub fn active_len(&self) -> usize {
        let guard = self.inner.read();
        guard.active.len()
    }
}

/// 按设备 ID 分区的双缓冲管理器。
#[derive(Debug, Clone)]
pub struct DeviceBufferManager {
    buffers: Arc<RwLock<HashMap<String, Arc<DoubleBuffer>>>>,
    row_threshold: usize,
    time_threshold: Duration,
}

impl DeviceBufferManager {
    pub fn new(row_threshold: usize, time_threshold: Duration) -> Self {
        Self {
            buffers: Arc::new(RwLock::new(HashMap::new())),
            row_threshold,
            time_threshold,
        }
    }

    /// 获取或创建指定设备的双缓冲。
    pub fn get_or_create(&self, device_id: &str) -> Arc<DoubleBuffer> {
        let guard = self.buffers.read();
        if let Some(buf) = guard.get(device_id) {
            return buf.clone();
        }
        drop(guard);

        let mut guard = self.buffers.write();
        guard
            .entry(device_id.to_string())
            .or_insert_with(|| {
                Arc::new(DoubleBuffer::new(self.row_threshold, self.time_threshold))
            })
            .clone()
    }

    /// 获取指定设备的双缓冲（如果不存在则返回 None）。
    pub fn get(&self, device_id: &str) -> Option<Arc<DoubleBuffer>> {
        let guard = self.buffers.read();
        guard.get(device_id).cloned()
    }

    /// 返回所有需要刷盘的设备 ID 列表。
    pub fn devices_to_flush(&self) -> Vec<String> {
        let guard = self.buffers.read();
        guard
            .iter()
            .filter(|(_, buf)| buf.should_flush())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// 返回当前管理的所有设备 ID。
    pub fn device_ids(&self) -> Vec<String> {
        let guard = self.buffers.read();
        guard.keys().cloned().collect()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
