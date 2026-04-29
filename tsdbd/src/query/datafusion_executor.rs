use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{as_primitive_array, Array, StringViewArray};
use arrow::datatypes::{UInt64Type, Float32Type, UInt8Type, UInt16Type, UInt32Type, Int8Type, Int16Type, Int32Type, Int64Type, Float64Type};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::prelude::*;
use glob::glob;

use crate::model::DataPoint;

pub struct QueryExecutor {
    storage_root: String,
    ctx: SessionContext,
}

impl Clone for QueryExecutor {
    fn clone(&self) -> Self {
        Self {
            storage_root: self.storage_root.clone(),
            ctx: SessionContext::new(),
        }
    }
}

impl QueryExecutor {
    pub fn new(storage_root: String) -> Self {
        Self {
            storage_root,
            ctx: SessionContext::new(),
        }
    }

    fn build_parquet_paths(&self, device_id: &str) -> Vec<PathBuf> {
        let pattern = format!("{}/{}/**/*.parquet", self.storage_root, device_id);
        glob(&pattern)
            .map(|g| g.into_iter().filter_map(|p| p.ok()).collect())
            .unwrap_or_default()
    }

    pub fn query(
        &self,
        device_id: &str,
        from_ts: u64,
        to_ts: u64,
        params: &[String],
    ) -> Result<Vec<DataPoint>> {
        let parquet_paths = self.build_parquet_paths(device_id);
        if parquet_paths.is_empty() {
            return Ok(Vec::new());
        }

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("create runtime")?;

        runtime.block_on(self.query_async(device_id, from_ts, to_ts, params))
    }

    pub async fn query_async(
        &self,
        device_id: &str,
        from_ts: u64,
        to_ts: u64,
        params: &[String],
    ) -> Result<Vec<DataPoint>> {
        let parquet_paths = self.build_parquet_paths(device_id);
        if parquet_paths.is_empty() {
            return Ok(Vec::new());
        }

        let table_name = format!("t_{}", device_id.replace("-", "_"));
        let mut first = true;
        for path in &parquet_paths {
            let path_str = path.to_string_lossy().replace("\\", "/");
            let url = ListingTableUrl::parse(format!("file:///{}", path_str))?;
            let table_name = if first { table_name.as_str() } else { "next_table" };
            self.ctx
                .register_parquet(table_name, url, ParquetReadOptions::default())
                .await
                .with_context(|| format!("register parquet: {}", path.display()))?;
            if !first {
                self.ctx.deregister_table("next_table").ok();
            }
            first = false;
        }

        let param_filter = if params.is_empty() {
            String::new()
        } else {
            let param_list = params
                .iter()
                .map(|p| format!("'{}'", p.to_uppercase()))
                .collect::<Vec<_>>()
                .join(",");
            format!("AND param_id IN ({})", param_list)
        };

        let sql = format!(
            "SELECT ts, CAST(param_id AS VARCHAR) as param_id, value FROM {} WHERE ts >= {from_ts} AND ts <= {to_ts} {param_filter} ORDER BY ts",
            table_name
        );

        let df = self.ctx.sql(&sql).await.with_context(|| format!("sql error: {}", sql))?;
        let batches = df.collect().await.with_context(|| "collect failed")?;

        let mut results = Vec::new();
        for batch in batches {
            let ts_arr = as_primitive_array::<UInt64Type>(batch.column(0));
            let param_arr = batch.column(1);
            let value_arr = as_primitive_array::<Float32Type>(batch.column(2));

            for i in 0..batch.num_rows() {
                let param_id = if let Some(arr) = param_arr.as_any().downcast_ref::<StringViewArray>() {
                    arr.value(i).to_string()
                } else {
                    return Err(anyhow::anyhow!("Unsupported param_id type: {:?}", param_arr.data_type()).into());
                };

                results.push(DataPoint {
                    ts: ts_arr.value(i),
                    param_id,
                    value: value_arr.value(i),
                });
            }
        }

        Ok(results)
    }

    pub fn query_sql(&self, sql: &str) -> Result<Vec<DataPoint>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("create runtime")?;

        runtime.block_on(self.query_sql_async(sql, None))
    }

    pub async fn query_sql_async(&self, sql: &str, device_id: Option<&str>) -> Result<Vec<DataPoint>> {
        if let Some(device) = device_id {
            let parquet_paths = self.build_parquet_paths(device);
            if !parquet_paths.is_empty() {
                let table_name = format!("t_{}", device.replace("-", "_"));
                let mut first = true;
                for path in &parquet_paths {
                    let path_str = path.to_string_lossy().replace("\\", "/");
                    let url = ListingTableUrl::parse(format!("file:///{}", path_str))?;
                    let tbl_name = if first { table_name.as_str() } else { "next_table" };
                    self.ctx
                        .register_parquet(tbl_name, url, ParquetReadOptions::default())
                        .await
                        .with_context(|| format!("register parquet: {}", path.display()))?;
                    if !first {
                        self.ctx.deregister_table("next_table").ok();
                    }
                    first = false;
                }
            }
        }

        let df = self.ctx.sql(sql).await.with_context(|| format!("sql error: {}", sql))?;
        let batches = df.collect().await.with_context(|| "collect failed")?;

        let mut results = Vec::new();
        for batch in batches {
            let ts_arr = as_primitive_array::<UInt64Type>(batch.column(0));
            let param_arr = batch.column(1);
            let value_arr = as_primitive_array::<Float32Type>(batch.column(2));

            for i in 0..batch.num_rows() {
                let param_id = if let Some(arr) = param_arr.as_any().downcast_ref::<StringViewArray>() {
                    arr.value(i).to_string()
                } else {
                    return Err(anyhow::anyhow!("Unsupported param_id type: {:?}", param_arr.data_type()).into());
                };

                results.push(DataPoint {
                    ts: ts_arr.value(i),
                    param_id,
                    value: value_arr.value(i),
                });
            }
        }

        Ok(results)
    }

    pub fn query_sql_generic(&self, sql: &str, device_id: Option<&str>) -> Result<String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("create runtime")?;

        runtime.block_on(self.query_sql_generic_async(sql, device_id))
    }

    pub async fn query_sql_generic_async(&self, sql: &str, device_id: Option<&str>) -> Result<String> {
        if let Some(device) = device_id {
            let parquet_paths = self.build_parquet_paths(device);
            if !parquet_paths.is_empty() {
                let table_name = format!("t_{}", device.replace("-", "_"));
                let mut first = true;
                for path in &parquet_paths {
                    let path_str = path.to_string_lossy().replace("\\", "/");
                    let url = ListingTableUrl::parse(format!("file:///{}", path_str))?;
                    let tbl_name = if first { table_name.as_str() } else { "next_table" };
                    self.ctx
                        .register_parquet(tbl_name, url, ParquetReadOptions::default())
                        .await
                        .with_context(|| format!("register parquet: {}", path.display()))?;
                    if !first {
                        self.ctx.deregister_table("next_table").ok();
                    }
                    first = false;
                }
            }
        }

        let df = self.ctx.sql(sql).await.with_context(|| format!("sql error: {}", sql))?;
        let batches = df.collect().await.with_context(|| "collect failed")?;

        if batches.is_empty() {
            return Ok(String::new());
        }

        let schema = batches[0].schema();
        let num_columns = schema.fields().len();

        let mut output = String::new();

        let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        output.push_str(&headers.join(","));
        output.push('\n');

        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let mut values = Vec::new();
                for col_idx in 0..num_columns {
                    let col = batch.column(col_idx);
                    let val = Self::format_scalar(&col, row_idx);
                    values.push(val);
                }
                output.push_str(&values.join(","));
                output.push('\n');
            }
        }

        Ok(output)
    }

    fn format_scalar(col: &dyn Array, row_idx: usize) -> String {
        use arrow::array::*;
        use arrow::datatypes::DataType;

        match col.data_type() {
            DataType::UInt8 => col.as_primitive::<UInt8Type>().value(row_idx).to_string(),
            DataType::UInt16 => col.as_primitive::<UInt16Type>().value(row_idx).to_string(),
            DataType::UInt32 => col.as_primitive::<UInt32Type>().value(row_idx).to_string(),
            DataType::UInt64 => col.as_primitive::<UInt64Type>().value(row_idx).to_string(),
            DataType::Int8 => col.as_primitive::<Int8Type>().value(row_idx).to_string(),
            DataType::Int16 => col.as_primitive::<Int16Type>().value(row_idx).to_string(),
            DataType::Int32 => col.as_primitive::<Int32Type>().value(row_idx).to_string(),
            DataType::Int64 => col.as_primitive::<Int64Type>().value(row_idx).to_string(),
            DataType::Float32 => col.as_primitive::<Float32Type>().value(row_idx).to_string(),
            DataType::Float64 => col.as_primitive::<Float64Type>().value(row_idx).to_string(),
            DataType::Utf8 => col.as_string::<i32>().value(row_idx).to_string(),
            DataType::LargeUtf8 => col.as_string::<i64>().value(row_idx).to_string(),
            DataType::Utf8View => {
                if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                    arr.value(row_idx).to_string()
                } else {
                    format!("{:?}", col.data_type())
                }
            }
            DataType::Boolean => col.as_boolean().value(row_idx).to_string(),
            _ => format!("{:?}", col.data_type()),
        }
    }

    pub async fn query_disk_async(
        &self,
        device_id: &str,
        from_ts: u64,
        to_ts: u64,
        params: &[String],
        _limit: Option<usize>,
    ) -> Result<Vec<DataPoint>> {
        self.query_async(device_id, from_ts, to_ts, params).await
    }

    #[allow(dead_code)]
    pub fn query_point(
        &self,
        device_id: &str,
        param_id: &str,
        from_ts: u64,
        to_ts: u64,
    ) -> Result<Vec<DataPoint>> {
        self.query(device_id, from_ts, to_ts, &[param_id.to_string()])
    }

    #[allow(dead_code)]
    pub fn query_wide(
        &self,
        device_id: &str,
        param_ids: &[String],
        from_ts: u64,
        to_ts: u64,
    ) -> Result<Vec<(u64, Vec<f64>)>> {
        let points = self.query(device_id, from_ts, to_ts, param_ids)?;

        let mut map: std::collections::HashMap<u64, Vec<f64>> = std::collections::HashMap::new();
        for p in &points {
            let entry = map.entry(p.ts).or_insert_with(|| vec![f64::NAN; param_ids.len()]);
            if let Some(idx) = param_ids.iter().position(|x| x.eq_ignore_ascii_case(&p.param_id)) {
                entry[idx] = p.value as f64;
            }
        }

        let mut results: Vec<(u64, Vec<f64>)> = map.into_iter().collect();
        results.sort_by_key(|(ts, _)| *ts);
        Ok(results)
    }

    pub fn query_unified(
        &self,
        device_id: &str,
        disk_from: u64,
        disk_to: u64,
        mem_from: u64,
        mem_to: u64,
        params: &[String],
        _limit: Option<usize>,
        mem_batch: Option<RecordBatch>,
    ) -> Result<Vec<DataPoint>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("create runtime")?;

        runtime.block_on(self.query_unified_async(
            device_id,
            disk_from,
            disk_to,
            mem_from,
            mem_to,
            params,
            mem_batch,
        ))
    }

    async fn query_unified_async(
        &self,
        device_id: &str,
        disk_from: u64,
        disk_to: u64,
        mem_from: u64,
        mem_to: u64,
        params: &[String],
        mem_batch: Option<RecordBatch>,
    ) -> Result<Vec<DataPoint>> {
        let param_filter = if params.is_empty() {
            String::new()
        } else {
            let param_list = params
                .iter()
                .map(|p| format!("'{}'", p.to_uppercase()))
                .collect::<Vec<_>>()
                .join(",");
            format!("AND param_id IN ({})", param_list)
        };

        let mut all_results = Vec::new();

        if disk_from <= disk_to {
            let parquet_paths = self.build_parquet_paths(device_id);
            if !parquet_paths.is_empty() {
                let table_name = format!("disk_t_{}", device_id.replace("-", "_"));
                for (idx, path) in parquet_paths.iter().enumerate() {
                    let path_str = path.to_string_lossy().replace("\\", "/");
                    let url = ListingTableUrl::parse(format!("file:///{}", path_str))?;
                    let tbl = if idx == 0 { table_name.as_str() } else { "next_table" };
                    self.ctx
                        .register_parquet(tbl, url, ParquetReadOptions::default())
                        .await
                        .ok();
                    if idx > 0 {
                        self.ctx.deregister_table("next_table").ok();
                    }
                }

                let sql = format!(
                    "SELECT ts, param_id, value FROM {} WHERE ts >= {} AND ts <= {} {} ORDER BY ts",
                    table_name, disk_from, disk_to, param_filter
                );

                if let Ok(df) = self.ctx.sql(&sql).await {
                    let batches = df.collect().await.unwrap_or_default();
                    for batch in batches {
                        let ts_arr = as_primitive_array::<UInt64Type>(batch.column(0));
                        let param_arr = batch.column(1);
                        let value_arr = as_primitive_array::<Float32Type>(batch.column(2));

                        for i in 0..batch.num_rows() {
                            let param_id = if let Some(arr) = param_arr.as_any().downcast_ref::<StringViewArray>() {
                                arr.value(i).to_string()
                            } else {
                                continue;
                            };
                            all_results.push(DataPoint {
                                ts: ts_arr.value(i),
                                param_id,
                                value: value_arr.value(i),
                            });
                        }
                    }
                }
                self.ctx.deregister_table(&table_name).ok();
            }
        }

        if let Some(batch) = mem_batch {
            let schema = batch.schema();
            let fields: Vec<arrow::datatypes::Field> =
                schema.fields().iter().map(|f| f.as_ref().clone()).collect();
            let schema = Arc::new(arrow::datatypes::Schema::new(fields));

            let partitions = vec![vec![batch]];

            let provider = datafusion::datasource::MemTable::try_new(
                schema,
                partitions,
            )?;

            self.ctx
                .register_table("mem_table", Arc::new(provider))
                .ok();

            let sql = format!(
                "SELECT ts, param_id, value FROM mem_table WHERE ts >= {} AND ts <= {} {} ORDER BY ts",
                mem_from, mem_to, param_filter
            );

            if let Ok(df) = self.ctx.sql(&sql).await {
                let batches = df.collect().await.unwrap_or_default();
                for batch in batches {
                    let ts_arr = as_primitive_array::<UInt64Type>(batch.column(0));
                    let param_arr = batch.column(1);
                    let value_arr = as_primitive_array::<Float32Type>(batch.column(2));

                    for i in 0..batch.num_rows() {
                        let param_id = if let Some(arr) = param_arr.as_any().downcast_ref::<StringViewArray>() {
                            arr.value(i).to_string()
                        } else {
                            continue;
                        };
                        all_results.push(DataPoint {
                            ts: ts_arr.value(i),
                            param_id,
                            value: value_arr.value(i),
                        });
                    }
                }
            }
            self.ctx.deregister_table("mem_table").ok();
        }

        all_results.sort_by_key(|p| p.ts);
        Ok(all_results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::UInt64, false),
            Field::new("param_id", DataType::Utf8, false),
            Field::new("value", DataType::Float32, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![1000, 2000, 3000])),
                Arc::new(StringArray::from(vec!["P001", "P002", "P001"])),
                Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_query_executor_creation() {
        let executor = QueryExecutor::new("data/store".to_string());
        assert_eq!(executor.storage_root, "data/store");
    }

    #[test]
    fn test_build_parquet_paths() {
        let executor = QueryExecutor::new(".".to_string());
        let paths = executor.build_parquet_paths("nonexistent");
        assert!(paths.is_empty());
    }
}
