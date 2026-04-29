use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use anyhow::Context;

use crate::model::DataPoint;
use crate::query::datafusion_executor::QueryExecutor as DfExecutor;

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputFormat {
    Wide,
    Long,
    Json,
}

#[derive(Debug, Serialize)]
struct JsonRow {
    ts: u64,
    param_ids: Vec<String>,
    values: Vec<f32>,
}

#[derive(Debug, Serialize)]
struct ApiQueryRequest {
    device_id: String,
    from_ts: u64,
    to_ts: u64,
    params: Vec<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ApiQueryResponse {
    rows: Vec<ApiDataPoint>,
    source_disk_rows: usize,
    source_mem_rows: usize,
}

#[derive(Debug, Deserialize)]
struct ApiDataPoint {
    ts: u64,
    param_id: String,
    value: f32,
}

#[derive(Debug, Default)]
struct QueryProfile {
    rows_read: usize,
    rows_returned: usize,
    elapsed_ms: u128,
}

pub async fn run(
    root: Option<String>,
    device: Option<String>,
    from: Option<u64>,
    to: Option<u64>,
    params: Option<Vec<String>>,
    limit: usize,
    format: OutputFormat,
    sql: Option<String>,
    _sql_template: Option<String>,
    profile: bool,
    api_url: Option<String>,
) -> anyhow::Result<()> {
    let start_time = std::time::Instant::now();

    if let Some(url) = api_url {
        return execute_remote_query(&url, device, from, to, params, limit, format, profile).await;
    }

    let root = root.unwrap_or_else(|| "data/store".to_string());
    let device = device.ok_or_else(|| anyhow::anyhow!("--device is required"))?;

    let executor = DfExecutor::new(root.clone());

    if let Some(sql_query) = sql {
        let from_ts = from.unwrap_or(0);
        let to_ts = to.unwrap_or(u64::MAX);
        let sql = sql_query
            .replace("{{table}}", &format!("t_{}", device.replace("-", "_")))
            .replace("{{from}}", &from_ts.to_string())
            .replace("{{to}}", &to_ts.to_string());
        let result = executor.query_sql_generic_async(&sql, Some(&device)).await?;
        println!("{}", result);
        return Ok(());
    }

    let from = from.unwrap_or(0);
    let to = to.unwrap_or(u64::MAX);
    let mut results = executor.query_async(&device, from, to, &params.unwrap_or_default()).await?;

    if results.len() > limit {
        results.truncate(limit);
    }

    let elapsed = start_time.elapsed();

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

    let req = ApiQueryRequest {
        device_id: device,
        from_ts: from,
        to_ts: to,
        params: params.unwrap_or_default(),
        limit: Some(limit),
    };

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

    let results: Vec<DataPoint> = resp.rows.into_iter().map(|r| DataPoint {
        ts: r.ts,
        param_id: r.param_id,
        value: r.value,
    }).collect();

    let elapsed = start_time.elapsed();

    let profile_info = if profile {
        let p = QueryProfile {
            rows_read: resp.source_disk_rows + resp.source_mem_rows,
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

fn output_results(
    results: &[DataPoint],
    format: &OutputFormat,
    profile: Option<QueryProfile>,
) -> anyhow::Result<()> {
    match format {
        OutputFormat::Wide => output_wide(results),
        OutputFormat::Long => output_long(results),
        OutputFormat::Json => output_json(results),
    }

    if let Some(p) = profile {
        eprintln!(
            "[profile] rows_read={} rows_returned={} elapsed={}ms",
            p.rows_read, p.rows_returned, p.elapsed_ms
        );
    }

    Ok(())
}

fn output_long(results: &[DataPoint]) {
    println!("ts,param_id,value");
    for p in results {
        println!("{},{},{:.6}", p.ts, p.param_id, p.value);
    }
}

fn output_wide(results: &[DataPoint]) {
    if results.is_empty() {
        println!("(no data)");
        return;
    }

    let mut param_ids: Vec<String> = results.iter().map(|p| p.param_id.clone()).collect();
    param_ids.sort_by(|a, b| a.cmp(b));
    param_ids.dedup();

    println!("ts,{}", param_ids.join(","));

    let mut current_ts: Option<u64> = None;
    let mut row_values: Vec<Option<f32>> = Vec::new();

    for p in results {
        if Some(p.ts) != current_ts {
            if let Some(ts) = current_ts {
                let values_str: Vec<String> = row_values.iter().map(|v| {
                    v.map(|val| format!("{:.6}", val)).unwrap_or_default()
                }).collect();
                println!("{},{}", ts, values_str.join(","));
            }
            current_ts = Some(p.ts);
            row_values = vec![None; param_ids.len()];
        }

        if let Some(idx) = param_ids.iter().position(|pid| pid == &p.param_id) {
            row_values[idx] = Some(p.value);
        }
    }

    if let Some(ts) = current_ts {
        let values_str: Vec<String> = row_values.iter().map(|v| {
            v.map(|val| format!("{:.6}", val)).unwrap_or_default()
        }).collect();
        println!("{},{}", ts, values_str.join(","));
    }
}

fn output_json(results: &[DataPoint]) {
    let rows: Vec<JsonRow> = results.iter().map(|p| JsonRow {
        ts: p.ts,
        param_ids: vec![p.param_id.clone()],
        values: vec![p.value],
    }).collect();

    serde_json::to_writer_pretty(std::io::stdout(), &rows).ok();
    println!();
}
