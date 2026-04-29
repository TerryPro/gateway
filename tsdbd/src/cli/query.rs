use clap::ValueEnum;
use duckdb::{Connection, params};
use serde::{Serialize, Deserialize};
use anyhow::Context;

/// 输出格式枚举
#[derive(Debug, Clone, ValueEnum)]
pub enum OutputFormat {
    Wide,
    Long,
    Json,
}



/// JSON 格式输出行
#[derive(Debug, Serialize)]
struct JsonRow {
    ts: u64,
    param_ids: Vec<String>,
    values: Vec<f32>,
}

/// API 查询请求
#[derive(Debug, Serialize)]
struct ApiQueryRequest {
    device_id: String,
    from_ts: u64,
    to_ts: u64,
    params: Vec<String>,
    limit: Option<usize>,
}

/// API 查询响应
#[derive(Debug, Deserialize)]
struct ApiQueryResponse {
    rows: Vec<ApiDataPoint>,
    source_disk_rows: usize,
    source_mem_rows: usize,
}

/// API 数据点
#[derive(Debug, Deserialize)]
struct ApiDataPoint {
    ts: u64,
    param_id: String,
    value: f32,
}

/// 查询统计
#[derive(Debug, Default)]
struct QueryProfile {
    rows_read: usize,
    rows_returned: usize,
    elapsed_ms: u128,
}

/// 执行 CLI 查询
pub async fn run(
    root: Option<String>,
    device: Option<String>,
    from: Option<u64>,
    to: Option<u64>,
    params: Option<Vec<String>>,
    limit: usize,
    format: OutputFormat,
    sql: Option<String>,
    sql_template: Option<String>,
    profile: bool,
    api_url: Option<String>,
) -> anyhow::Result<()> {
    let start_time = std::time::Instant::now();
    
    // 如果有 API URL，使用远程查询
    if let Some(url) = api_url {
        return execute_remote_query(&url, device, from, to, params, limit, format, profile).await;
    }
    
    // 确定存储根目录
    let root = root.unwrap_or_else(|| "data/store".to_string());

    // 确定查询方式：SQL 优先，否则使用基础查询
    if let Some(ref sql_str) = sql {
        execute_sql_and_print(&root, sql_str, device.as_deref())?;
        return Ok(());
    } else if let Some(ref template) = sql_template {
        let device = device.ok_or_else(|| anyhow::anyhow!("--device is required for SQL template"))?;
        let from = from.ok_or_else(|| anyhow::anyhow!("--from is required for SQL template"))?;
        let to = to.ok_or_else(|| anyhow::anyhow!("--to is required for SQL template"))?;

        let sql = build_sql_from_template(template, &device, from, to, params.as_ref())?;
        execute_sql_and_print(&root, &sql, Some(&device))?;
        return Ok(());
    }
    
    // 基础查询
    let device = device.ok_or_else(|| anyhow::anyhow!("--device is required"))?;
    let from = from.unwrap_or(0);
    let to = to.unwrap_or(u64::MAX);
    
    let results = execute_basic_query(&root, &device, from, to, params.as_ref(), limit)?;
    
    let elapsed = start_time.elapsed();
    
    // 输出结果
    let profile_info = if profile {
        let p = QueryProfile {
            rows_read: results.len(),
            rows_returned: results.len(),
            elapsed_ms: elapsed.as_millis(),
        };
        Some(p)
    } else {
        None
    };
    
    output_results(&results, &format, profile_info)?;
    
    Ok(())
}

/// 执行基础查询
fn execute_basic_query(
    root: &str,
    device: &str,
    from: u64,
    to: u64,
    params: Option<&Vec<String>>,
    limit: usize,
) -> anyhow::Result<Vec<crate::model::DataPoint>> {
    let conn = Connection::open_in_memory()?;
    
    // 构建参数过滤
    let param_filter = if let Some(ps) = params {
        if ps.is_empty() {
            String::new()
        } else {
            let param_list = ps.iter()
                .map(|p| format!("'{}'", p))
                .collect::<Vec<_>>()
                .join(",");
            format!("AND param_id IN ({})", param_list)
        }
    } else {
        String::new()
    };
    
    // 构建 Parquet 文件路径模式
    let parquet_pattern = format!("{}/{}/**/*.parquet", root, device);
    
    // 构建 SQL
    let sql = format!(
        "SELECT ts, param_id, value 
         FROM read_parquet('{}', hive_partitioning=1)
         WHERE ts >= {} AND ts <= {} {}
         ORDER BY ts
         LIMIT {}",
        parquet_pattern,
        from,
        to,
        param_filter,
        limit
    );
    
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![])?;
    
    let mut results = Vec::new();
    while let Some(row) = rows.next()? {
        let ts: u64 = row.get(0)?;
        let param_id: String = row.get(1)?;
        let value: f64 = row.get(2)?;
        
        results.push(crate::model::DataPoint {
            ts,
            param_id,
            value: value as f32,
        });
    }
    
    Ok(results)
}

/// 执行 SQL 查询（直接输出结果，不转换为 DataPoint）
fn execute_sql_and_print(
    root: &str,
    sql: &str,
    device: Option<&str>,
) -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;

    let mut final_sql = sql.replace("{{root}}", root);

    if let Some(device_id) = device {
        let parquet_path = format!("read_parquet('{}/{}/**/*.parquet', hive_partitioning=1)", root, device_id);
        final_sql = final_sql.replace("device_data", &parquet_path);
    }

    let mut stmt = conn.prepare(&final_sql)?;
    let mut rows = stmt.query(params![])?;

    let mut results: Vec<(u64, String, f64)> = Vec::new();
    while let Some(row) = rows.next()? {
        let ts: u64 = row.get(0)?;
        let param_id: String = row.get(1)?;
        let value: f64 = row.get(2)?;
        results.push((ts, param_id, value));
    }

    println!("ts,param_id,value");
    for (ts, param_id, value) in results {
        println!("{},{},{:.6}", ts, param_id, value);
    }

    Ok(())
}

/// 执行远程查询（调用 HTTP API）
async fn execute_remote_query(
    api_url: &str,
    device: Option<String>,
    from: Option<u64>,
    to: Option<u64>,
    params: Option<Vec<String>>,
    limit: usize,
    format: OutputFormat,
    profile: bool,
) -> anyhow::Result<()> {
    let start_time = std::time::Instant::now();
    let device = device.ok_or_else(|| anyhow::anyhow!("--device is required for remote query"))?;
    let from = from.unwrap_or(0);
    let to = to.unwrap_or(u64::MAX);
    
    // 构建查询请求
    let req = ApiQueryRequest {
        device_id: device,
        from_ts: from,
        to_ts: to,
        params: params.unwrap_or_default(),
        limit: Some(limit),
    };
    
    // 发送 HTTP POST 请求
    let client = reqwest::Client::new();
    let url = format!("{}/query", api_url.trim_end_matches('/'));
    
    let response = client.post(&url)
        .json(&req)
        .send()
        .await
        .context("failed to send request to server")?;
    
    if !response.status().is_success() {
        let status = response.status();
        let error_text: String = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned error {}: {}", status, error_text);
    }
    
    let resp: ApiQueryResponse = response.json().await.context("failed to parse response")?;
    
    // 转换为 DataPoint 格式
    let results: Vec<crate::model::DataPoint> = resp.rows
        .into_iter()
        .map(|row| crate::model::DataPoint {
            ts: row.ts,
            param_id: row.param_id,
            value: row.value,
        })
        .collect();
    
    // 输出结果
    let profile_info = if profile {
        let p = QueryProfile {
            rows_read: results.len(),
            rows_returned: results.len(),
            elapsed_ms: start_time.elapsed().as_millis(),
        };
        Some(p)
    } else {
        None
    };
    
    output_results(&results, &format, profile_info)?;
    
    Ok(())
}

/// 从模板构建 SQL
fn build_sql_from_template(
    template: &str,
    device: &str,
    from: u64,
    to: u64,
    params: Option<&Vec<String>>,
) -> anyhow::Result<String> {
    let mut sql = template.to_string();
    
    // 替换 {{table}}
    let table_pattern = format!("{}/**/*.parquet", device);
    sql = sql.replace("{{table}}", &table_pattern);
    
    // 替换 {{from}} 和 {{to}}
    sql = sql.replace("{{from}}", &from.to_string());
    sql = sql.replace("{{to}}", &to.to_string());
    
    // 替换 {{params}}（如果有）
    if let Some(ps) = params {
        if !ps.is_empty() {
            let param_list = ps.iter()
                .map(|p| format!("'{}'", p))
                .collect::<Vec<_>>()
                .join(",");
            sql = sql.replace("{{params}}", &param_list);
        } else {
            sql = sql.replace("{{params}}", "");
        }
    } else {
        sql = sql.replace("{{params}}", "");
    }
    
    Ok(sql)
}

/// 输出结果
fn output_results(
    results: &[crate::model::DataPoint],
    format: &OutputFormat,
    profile: Option<QueryProfile>,
) -> anyhow::Result<()> {
    match format {
        OutputFormat::Wide => output_wide(results)?,
        OutputFormat::Long => output_long(results)?,
        OutputFormat::Json => output_json(results)?,
    }
    
    // 输出统计信息
    if let Some(p) = profile {
        eprintln!("\nQuery Profile:");
        eprintln!("  Rows read: {}", p.rows_read);
        eprintln!("  Rows returned: {}", p.rows_returned);
        eprintln!("  Elapsed: {}ms", p.elapsed_ms);
    }
    
    Ok(())
}

/// 宽表格式输出
fn output_wide(results: &[crate::model::DataPoint]) -> anyhow::Result<()> {
    println!("ts,param_id,value");

    for row in results {
        println!("{},{},{:.3}", row.ts, row.param_id, row.value);
    }

    Ok(())
}

/// 长表格式输出
fn output_long(results: &[crate::model::DataPoint]) -> anyhow::Result<()> {
    // 输出表头
    println!("ts,param_id,value");
    
    // 输出每一行
    for row in results {
        println!("{},{},{:.3}", row.ts, row.param_id, row.value);
    }
    
    Ok(())
}

/// JSON 格式输出
fn output_json(results: &[crate::model::DataPoint]) -> anyhow::Result<()> {
    use std::collections::BTreeMap;
    
    // 按 ts 分组
    let mut grouped: BTreeMap<u64, Vec<&crate::model::DataPoint>> = BTreeMap::new();
    for row in results {
        grouped.entry(row.ts).or_default().push(row);
    }
    
    // 输出每一行
    for (ts, rows) in grouped {
        let param_ids: Vec<String> = rows.iter().map(|r| r.param_id.clone()).collect();
        let values: Vec<f32> = rows.iter().map(|r| r.value).collect();
        
        let json_row = JsonRow {
            ts,
            param_ids,
            values,
        };
        
        let json_str = serde_json::to_string(&json_row)?;
        println!("{}", json_str);
    }
    
    Ok(())
}
