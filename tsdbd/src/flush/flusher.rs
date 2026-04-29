use crate::config::{FlushConfig, StorageConfig};
use crate::mem::time_window_buffer::TimeWindowBuffer;
use std::path::Path;
use tracing::{info, warn};

use super::parquet_writer::DeviceWriter;

/// 周期性 flush 调度器：按小时窗口自动落盘。
pub async fn run_flush_scheduler(
    cfg: FlushConfig,
    storage: StorageConfig,
    mem: TimeWindowBuffer,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(cfg.interval_ms));
    loop {
        ticker.tick().await;
        flush_by_hour_window(&storage, &mem).await?;
    }
}

/// 按小时窗口执行 flush：将超过 1 小时的数据落盘。
///
/// 双缓存工作流程：
/// 1. 执行 swap()：将 active buffer 转移到 flushing buffer（毫秒级，加锁）
/// 2. 从 flushing buffer 读取数据写入 Parquet（耗时，无锁）
/// 3. 清空 flushing buffer
///
/// 优势：
/// - swap 期间不阻塞写入（active buffer 清空后立即接收新数据）
/// - flush 期间不阻塞查询（flushing buffer 保持可查询）
/// - 数据完整性：flush 完成前数据始终在 flushing buffer 中
pub async fn flush_by_hour_window(storage: &StorageConfig, mem: &TimeWindowBuffer) -> anyhow::Result<()> {
    // 步骤 1：执行交换（快速，加锁）
    let flushable = mem.swap();
    if flushable.is_empty() {
        return Ok(());
    }

    let mut total_rows = 0u64;
    
    // 步骤 2：写入 Parquet（耗时，无锁）
    // 注意：这里使用的是 swap 返回的数据副本，不依赖 flushing buffer
    // 这样可以避免 flush 期间 flushing buffer 被修改
    for (hour_key, device_data) in flushable {
        for (device_id, points) in device_data {
            // 将 DataPoint 转换为 LongRow
            let long_rows: Vec<_> = points.into_iter()
                .map(|p| common::storage::LongRow {
                    ts: p.ts,
                    param_id: p.param_id,
                    value: p.value,
                })
                .collect();
            
            let row_count = long_rows.len();
            total_rows += row_count as u64;
            
            // 直接使用 DeviceWriter 写入 Parquet 文件
            match DeviceWriter::new(device_id.clone(), Path::new(&storage.root)) {
                Ok(mut writer) => {
                    if let Err(e) = writer.write_long_row(long_rows) {
                        warn!("flush write failed for {} hour={:?}: {:?}", device_id, hour_key, e);
                    } else {
                        info!("flushed device={} hour={:?} rows={}", device_id, hour_key, row_count);
                    }
                }
                Err(e) => {
                    warn!("flush create writer failed for {} hour={:?}: {:?}", device_id, hour_key, e);
                }
            }
        }
    }
    
    // 步骤 3：清空 flushing buffer
    mem.clear_flushing();
    
    if total_rows > 0 {
        info!("flush cycle done total_rows={}", total_rows);
    }
    
    Ok(())
}

