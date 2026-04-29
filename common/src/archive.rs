use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use chrono::{Datelike, Local, NaiveDate, TimeZone, Timelike};
use serde::{Deserialize, Serialize};

/// 归档 schema 版本，用于后续演进兼容。
pub const ARCHIVE_SCHEMA_VERSION: i16 = 1;
/// 归档清单文件名。
pub const MANIFEST_FILE_NAME: &str = "manifest.jsonl";

/// 单个 Parquet 分段的元信息，用于快速剪枝查询范围。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub schema_version: i16,
    pub device_id: String,
    pub file_name: String,
    pub day_key: String,
    pub hour: u32,
    pub part: u32,
    pub min_ts_ms: u64,
    pub max_ts_ms: u64,
    pub rows: u64,
    pub payload_bytes: u64,
    pub file_size_bytes: u64,
    pub sealed: bool,
    pub created_at_ms: u64,
}

/// 本地时区时间戳转换为日期键与小时。
pub fn format_day_key_hour_local(ts_ms: u64) -> anyhow::Result<(String, u32)> {
    let ts = i64::try_from(ts_ms).context("timestamp too large")?;
    let dt = Local
        .timestamp_millis_opt(ts)
        .single()
        .context("invalid timestamp")?;
    let day_key = format!("{:04}{:02}{:02}", dt.year(), dt.month(), dt.day());
    Ok((day_key, dt.hour()))
}

/// 构建设备归档目录路径。
pub fn build_device_dir(root: &str, device_id: &str) -> PathBuf {
    Path::new(root).join(device_id)
}

/// 构建设备日归档目录路径。
pub fn build_day_dir(root: &str, device_id: &str, day_key: &str) -> PathBuf {
    build_device_dir(root, device_id).join(day_key)
}

/// 生成标准 Parquet 文件名。
pub fn parquet_file_name(hour: u32, part: u32) -> String {
    format!("h{hour:02}_p{part:03}.parquet")
}

/// 生成临时写入文件名。
pub fn parquet_tmp_file_name(hour: u32, part: u32) -> String {
    format!("h{hour:02}_p{part:03}.parquet.tmp")
}

/// 解析标准 Parquet 文件名中的小时与分段编号。
pub fn parse_parquet_file_name(name: &str) -> Option<(u32, u32)> {
    if !name.ends_with(".parquet") {
        return None;
    }
    let stem = name.trim_end_matches(".parquet");
    if stem.starts_with('h') && stem.contains("_p") {
        if let Some((h_raw, p_raw)) = stem.split_once("_p") {
            if let (Some(hour_raw), Ok(part)) = (h_raw.strip_prefix('h'), p_raw.parse::<u32>()) {
                if let Ok(hour) = hour_raw.parse::<u32>() {
                    return Some((hour, part));
                }
            }
        }
    }
    if stem.starts_with("hour_") {
        let parts: Vec<&str> = stem.split('_').collect();
        if parts.len() != 4 || parts[0] != "hour" || parts[2] != "part" {
            return None;
        }
        let hour = parts[1].parse::<u32>().ok()?;
        let part = parts[3].parse::<u32>().ok()?;
        return Some((hour, part));
    }
    None
}

/// 返回清单文件绝对路径。
pub fn manifest_path(day_dir: &Path) -> PathBuf {
    day_dir.join(MANIFEST_FILE_NAME)
}

/// 向日目录清单追加一条分段记录。
pub fn append_manifest_entry(day_dir: &Path, entry: &ManifestEntry) -> anyhow::Result<()> {
    std::fs::create_dir_all(day_dir)
        .with_context(|| format!("create day dir failed: {}", day_dir.display()))?;
    let path = manifest_path(day_dir);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open manifest failed: {}", path.display()))?;
    let line = serde_json::to_string(entry)?;
    file.write_all(line.as_bytes())?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

/// 读取清单中的所有分段记录。
pub fn load_manifest_entries(day_dir: &Path) -> anyhow::Result<Vec<ManifestEntry>> {
    let path = manifest_path(day_dir);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file =
        std::fs::File::open(&path).with_context(|| format!("open manifest failed: {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read manifest line {} failed", idx + 1))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let entry = serde_json::from_str::<ManifestEntry>(trimmed).with_context(|| {
            format!(
                "parse manifest line {} failed: {}",
                idx + 1,
                trimmed.chars().take(128).collect::<String>()
            )
        })?;
        out.push(entry);
    }
    Ok(out)
}

/// 判断时间段是否有交集。
pub fn intersects(min_ts: u64, max_ts: u64, from_ts: u64, to_ts: u64) -> bool {
    !(max_ts < from_ts || min_ts > to_ts)
}

/// 将 `YYYYMMDD` 解析为日期。
pub fn parse_day_key(day_key: &str) -> Option<NaiveDate> {
    if day_key.len() != 8 {
        return None;
    }
    let y = day_key[0..4].parse::<i32>().ok()?;
    let m = day_key[4..6].parse::<u32>().ok()?;
    let d = day_key[6..8].parse::<u32>().ok()?;
    NaiveDate::from_ymd_opt(y, m, d)
}

/// 计算某个 `YYYYMMDD` 的本地时区毫秒时间范围。
pub fn day_ts_bounds_ms(day_key: &str) -> anyhow::Result<(u64, u64)> {
    let date = parse_day_key(day_key).context("invalid day key")?;
    let day_start = date
        .and_hms_opt(0, 0, 0)
        .context("invalid day start")?;
    let next_day_start = date
        .succ_opt()
        .context("invalid next day")?
        .and_hms_opt(0, 0, 0)
        .context("invalid next day start")?;
    let start = Local
        .from_local_datetime(&day_start)
        .single()
        .context("invalid local day start")?
        .timestamp_millis();
    let next_start = Local
        .from_local_datetime(&next_day_start)
        .single()
        .context("invalid local next day start")?
        .timestamp_millis();
    let end = next_start - 1;
    if start < 0 || end < 0 {
        bail!("negative timestamp is not supported");
    }
    Ok((start as u64, end as u64))
}

/// 扫描并返回指定设备时间范围内命中的清单文件集合。
pub fn collect_manifest_candidates(
    root: &str,
    device_id: &str,
    from_ts: u64,
    to_ts: u64,
) -> anyhow::Result<Vec<(PathBuf, ManifestEntry)>> {
    let mut out = Vec::new();
    let device_dir = build_device_dir(root, device_id);
    if !device_dir.exists() {
        return Ok(out);
    }
    let rd = std::fs::read_dir(&device_dir)
        .with_context(|| format!("read device dir failed: {}", device_dir.display()))?;
    for entry in rd {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let day_key = entry.file_name().to_string_lossy().to_string();
        let (day_min, day_max) = match day_ts_bounds_ms(&day_key) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if !intersects(day_min, day_max, from_ts, to_ts) {
            continue;
        }
        let day_dir = entry.path();
        let records = load_manifest_entries(&day_dir)?;
        for item in records {
            if !item.sealed {
                continue;
            }
            if !intersects(item.min_ts_ms, item.max_ts_ms, from_ts, to_ts) {
                continue;
            }
            out.push((day_dir.join(&item.file_name), item));
        }
    }
    out.sort_by_key(|(_, e)| (e.day_key.clone(), e.hour, e.part));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{build_device_dir, format_day_key_hour_local, parse_parquet_file_name, parquet_file_name};

    /// 验证 Parquet 文件名可往返解析。
    #[test]
    fn parquet_file_name_roundtrip_should_work() {
        let name = parquet_file_name(7, 12);
        assert_eq!(name, "h07_p012.parquet");
        let parsed = parse_parquet_file_name(&name).expect("file name parse should succeed");
        assert_eq!(parsed.0, 7);
        assert_eq!(parsed.1, 12);
    }

    /// 验证旧命名仍可被解析，便于平滑兼容。
    #[test]
    fn parse_legacy_parquet_file_name_should_work() {
        let parsed = parse_parquet_file_name("hour_12_part_001.parquet")
            .expect("legacy file name parse should succeed");
        assert_eq!(parsed.0, 12);
        assert_eq!(parsed.1, 1);
    }

    /// 验证时间戳可正确映射到本地天和小时。
    #[test]
    fn format_day_key_hour_local_should_work() {
        let (day, hour) =
            format_day_key_hour_local(1_776_911_711_886).expect("format day/hour should succeed");
        assert!(!day.is_empty());
        assert!(hour <= 23);
    }

    /// 验证设备目录命名应使用简化的设备 ID 目录。
    #[test]
    fn build_device_dir_should_use_plain_device_id() {
        let dir = build_device_dir("data", "dev001");
        assert_eq!(dir, Path::new("data").join("dev001"));
    }
}
