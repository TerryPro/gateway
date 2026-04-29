//! 直接使用 common::storage 写入 Parquet 文件，替代调用 tst 工具。

use anyhow::Result;
use chrono::{Datelike, Local, TimeZone, Timelike};
use common::storage::{
    CompressionArg, LongRow, LongRowSegmentWriter, PacketWideRow, PacketWideSegmentWriter,
    SegmentWriterConfig, StorageMeta, StorageMode,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// 设备级写入器，管理该设备所有小时分段的写入。
pub struct DeviceWriter {
    device_id: String,
    root: PathBuf,
    #[allow(dead_code)]
    storage_meta: StorageMeta,
    segment_config: SegmentWriterConfig,
    packet_wide_writers: HashMap<HourKey, PacketWideSegmentWriter>,
    long_row_writers: HashMap<HourKey, LongRowSegmentWriter>,
    next_segment_seq: HashMap<HourKey, u32>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct HourKey {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
}

impl DeviceWriter {
    /// 创建新的设备写入器。
    pub fn new(device_id: String, root: &Path) -> Result<Self> {
        let root = root.to_path_buf();
        
        // 加载或创建存储配置
        let storage_meta = if root.join("_meta").join("storage.toml").exists() {
            StorageMeta::load_from(&root)?
        } else {
            let meta = StorageMeta::new(
                StorageMode::LongRow,
                CompressionArg::Zstd,
                1800, // 30 分钟分段
            );
            meta.write_to(&root)?;
            info!("created storage meta at {}", root.display());
            meta
        };

        let segment_config = SegmentWriterConfig {
            segment_sec: storage_meta.segment_sec,
            segment_max_rows: storage_meta.segment_max_rows,
            row_group_rows: storage_meta.row_group_rows,
            compression: storage_meta.compression.to_compression(),
        };

        Ok(Self {
            device_id,
            root,
            storage_meta,
            segment_config,
            packet_wide_writers: HashMap::new(),
            long_row_writers: HashMap::new(),
            next_segment_seq: HashMap::new(),
        })
    }

    /// 写入一批数据点（宽表模式）。
    #[allow(dead_code)]
    pub fn write_packet_wide(&mut self, rows: Vec<PacketWideRow>) -> Result<u64> {
        let mut written = 0u64;
        
        for row in rows {
            let hour_key = ts_to_hour_key(row.ts);
            let hour_dir = self.get_hour_dir(&hour_key);
            
            // 获取或创建分段写入器
            let writer = self.packet_wide_writers.entry(hour_key.clone()).or_insert_with(|| {
                if !hour_dir.exists() {
                    std::fs::create_dir_all(&hour_dir).expect("create hour dir failed");
                }
                println!("DEBUG: hour_dir={}, exists={}", hour_dir.display(), hour_dir.exists());
                let seq = *self.next_segment_seq.entry(hour_key.clone()).or_insert(1);
                let segment_start_ts = hour_key_to_ts(&hour_key);
                let w = PacketWideSegmentWriter::new(
                    &hour_dir,
                    seq,
                    segment_start_ts,
                    self.segment_config.clone(),
                ).expect("create segment writer failed");
                *self.next_segment_seq.entry(hour_key.clone()).or_insert(1) += 1;
                w
            });
            
            let need_flush = writer.append(row)?;
            written += 1;
            
            if need_flush {
                self.flush_hour(&hour_key)?;
            }
        }
        
        Ok(written)
    }

    /// 写入一批数据点（长表模式）。
    pub fn write_long_row(&mut self, rows: Vec<LongRow>) -> Result<u64> {
        let mut written = 0u64;
        
        for row in rows {
            let hour_key = ts_to_hour_key(row.ts);
            let hour_dir = self.get_hour_dir(&hour_key);
            
            // 获取或创建分段写入器
            let writer = self.long_row_writers.entry(hour_key.clone()).or_insert_with(|| {
                if !hour_dir.exists() {
                    std::fs::create_dir_all(&hour_dir).expect("create hour dir failed");
                }
                let seq = *self.next_segment_seq.entry(hour_key.clone()).or_insert(1);
                let segment_start_ts = hour_key_to_ts(&hour_key);
                let w = LongRowSegmentWriter::new(
                    &hour_dir,
                    seq,
                    segment_start_ts,
                    self.segment_config.clone(),
                ).expect("create segment writer failed");
                *self.next_segment_seq.entry(hour_key.clone()).or_insert(1) += 1;
                w
            });
            
            let need_flush = writer.append(row)?;
            written += 1;
            
            if need_flush {
                self.flush_hour(&hour_key)?;
            }
        }
        
        Ok(written)
    }

    /// 刷新指定小时的所有分段。
    fn flush_hour(&mut self, hour_key: &HourKey) -> Result<()> {
        if let Some(writer) = self.packet_wide_writers.remove(hour_key) {
            let hour_dir = self.get_hour_dir(hour_key);
            let _ = writer.flush_and_close(&hour_dir);
            debug!("flushed packet wide segment for hour {:?}", hour_key);
        }
        
        if let Some(writer) = self.long_row_writers.remove(hour_key) {
            let hour_dir = self.get_hour_dir(hour_key);
            let _ = writer.flush_and_close(&hour_dir);
            debug!("flushed long row segment for hour {:?}", hour_key);
        }
        
        Ok(())
    }

    /// 刷新所有缓存的分段。
    #[allow(dead_code)]
    pub fn flush_all(&mut self) -> Result<()> {
        let hour_keys: Vec<_> = self.packet_wide_writers.keys().cloned().collect();
        for hour_key in hour_keys {
            self.flush_hour(&hour_key)?;
        }
        
        let hour_keys: Vec<_> = self.long_row_writers.keys().cloned().collect();
        for hour_key in hour_keys {
            self.flush_hour(&hour_key)?;
        }
        
        Ok(())
    }

    /// 获取小时目录路径。
    fn get_hour_dir(&self, hour_key: &HourKey) -> PathBuf {
        self.root
            .join(&self.device_id)
            .join(format!("{:04}-{:02}-{:02}", hour_key.year, hour_key.month, hour_key.day))
            .join(format!("{:02}", hour_key.hour))
    }

    /// 获取存储模式。
    #[allow(dead_code)]
    pub fn storage_mode(&self) -> StorageMode {
        self.storage_meta.default_mode
    }
}

/// 将时间戳转换为小时键。
fn ts_to_hour_key(ts_millis: u64) -> HourKey {
    let dt = Local.timestamp_millis_opt(ts_millis as i64).unwrap();
    HourKey {
        year: dt.year(),
        month: dt.month(),
        day: dt.day(),
        hour: dt.hour(),
    }
}

/// 将小时键转换为时间戳（毫秒）。
fn hour_key_to_ts(hour_key: &HourKey) -> u64 {
    Local
        .with_ymd_and_hms(hour_key.year, hour_key.month, hour_key.day, hour_key.hour, 0, 0)
        .unwrap()
        .timestamp_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_device_writer_lifecycle() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let root = temp_dir.path().join("tsdata");
        
        // 创建设备写入器
        let mut writer = DeviceWriter::new("dev001".to_string(), &root)?;
        
        // 写入一些测试数据
        let rows = vec![
            LongRow { ts: 1714000000000, param_id: "P00001".to_string(), value: 25.5 },
            LongRow { ts: 1714000001000, param_id: "P00002".to_string(), value: 30.2 },
        ];
        
        writer.write_long_row(rows)?;
        writer.flush_all()?;
        
        // 验证文件已创建
        assert!(root.join("_meta").join("storage.toml").exists());
        assert!(root.join("dev001").exists());
        
        Ok(())
    }
}
