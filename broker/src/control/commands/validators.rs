use common::tsmeta::is_valid_param_code;

/// 规范化并校验非空字段，返回去首尾空白后的值。
pub(crate) fn normalize_non_empty(raw: &str, field: &str) -> anyhow::Result<String> {
    let value = raw.trim();
    if value.is_empty() {
        anyhow::bail!("{field} must not be empty");
    }
    Ok(value.to_string())
}

/// 规范化并校验单个参数编码，返回大写编码。
pub(crate) fn normalize_param_id(raw: &str) -> anyhow::Result<String> {
    let param_id = raw.trim().to_ascii_uppercase();
    if !is_valid_param_code(&param_id) {
        anyhow::bail!("invalid param_id: {raw}");
    }
    Ok(param_id)
}

/// 解析参数列表，支持单点参数和区间参数（如 `A00001~A00010`）。
pub(crate) fn parse_param_specs<'a, I>(raw_tokens: I) -> anyhow::Result<Vec<String>>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut param_ids = Vec::new();
    for raw in raw_tokens {
        let token = raw.trim().to_ascii_uppercase();
        if token.is_empty() {
            continue;
        }
        if let Some((start, end)) = token.split_once('~') {
            let range_items = expand_param_range(start.trim(), end.trim())?;
            param_ids.extend(range_items);
            continue;
        }
        param_ids.push(normalize_param_id(&token)?);
    }
    Ok(param_ids)
}

/// 展开参数区间（如 `A00003~A00010`）为离散参数 ID 列表。
pub(crate) fn expand_param_range(start: &str, end: &str) -> anyhow::Result<Vec<String>> {
    if !is_valid_param_code(start) || !is_valid_param_code(end) {
        anyhow::bail!("invalid range endpoint");
    }
    let sp = start
        .chars()
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid range start"))?;
    let ep = end
        .chars()
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid range end"))?;
    if sp != ep {
        anyhow::bail!("range prefix mismatch");
    }
    let s = start[1..].parse::<u32>()?;
    let e = end[1..].parse::<u32>()?;
    if s > e {
        anyhow::bail!("range order invalid");
    }
    let mut out = Vec::with_capacity((e - s + 1) as usize);
    for v in s..=e {
        out.push(format!("{sp}{v:05}"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{expand_param_range, normalize_non_empty, parse_param_specs};

    /// 验证参数区间展开应包含两端并保持递增。
    #[test]
    fn expand_param_range_should_include_both_ends() {
        let got = expand_param_range("A00001", "A00003").expect("range should be valid");
        assert_eq!(
            got,
            vec![
                "A00001".to_string(),
                "A00002".to_string(),
                "A00003".to_string()
            ]
        );
    }

    /// 验证非法参数区间（前缀不一致）应直接返回错误。
    #[test]
    fn expand_param_range_should_fail_on_prefix_mismatch() {
        let err = expand_param_range("A00001", "Z00003").expect_err("range should be invalid");
        assert!(err.to_string().contains("prefix mismatch"));
    }

    /// 验证参数列表解析应支持混合单点与区间，并自动跳过空 token。
    #[test]
    fn parse_param_specs_should_handle_mixed_tokens() {
        let got = parse_param_specs(["A00001~A00002", "  ", "Z00001"]).expect("valid specs");
        assert_eq!(
            got,
            vec![
                "A00001".to_string(),
                "A00002".to_string(),
                "Z00001".to_string()
            ]
        );
    }

    /// 验证非空字段规范化应去首尾空白并在空值时报错。
    #[test]
    fn normalize_non_empty_should_trim_and_validate() {
        let got = normalize_non_empty(" dev-1 ", "device_id").expect("value should be valid");
        assert_eq!(got, "dev-1");

        let err = normalize_non_empty("   ", "device_id").expect_err("value should be invalid");
        assert!(err.to_string().contains("device_id must not be empty"));
    }
}
