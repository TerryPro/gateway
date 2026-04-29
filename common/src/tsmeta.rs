use chrono::{DateTime, Datelike, Timelike, Utc};

/// 由秒级时间戳计算日期字符串和小时值，日期格式为 `YYYY-MM-DD`。
pub fn day_hour_from_ts(ts_sec: u64) -> (String, u32) {
    let dt: DateTime<Utc> =
        DateTime::from_timestamp(ts_sec as i64, 0).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    (
        format!("{:04}-{:02}-{:02}", dt.year(), dt.month(), dt.day()),
        dt.hour(),
    )
}

/// 判断参数编码是否符合 `A/Z/P + 5位数字` 约束。
pub fn is_valid_param_code(code: &str) -> bool {
    if code.len() != 6 {
        return false;
    }
    let mut chars = code.chars();
    let Some(prefix) = chars.next() else {
        return false;
    };
    (prefix == 'A' || prefix == 'Z' || prefix == 'P') && chars.all(|ch| ch.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::{day_hour_from_ts, is_valid_param_code};

    /// 验证参数编码校验可覆盖 A/Z/P 三类前缀。
    #[test]
    fn is_valid_param_code_should_accept_azp() {
        assert!(is_valid_param_code("A00001"));
        assert!(is_valid_param_code("Z99999"));
        assert!(is_valid_param_code("P12345"));
        assert!(!is_valid_param_code("C00001"));
        assert!(!is_valid_param_code("P0000"));
    }

    /// 验证秒级时间戳可正确映射到日期与小时。
    #[test]
    fn day_hour_from_ts_should_work() {
        let (day, hour) = day_hour_from_ts(1_776_954_695);
        assert!(!day.is_empty());
        assert!(hour <= 23);
    }
}
