use crate::api;
use crate::config::AppConfig;
use crate::flush;
use crate::ingress;
use crate::mem;
use crate::model::IngestBatch;
use crate::query;
use crate::wal;
use anyhow::Context;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

/// Server 运行态：写入路径和查询路径共用这组句柄。
pub struct RuntimeState {
    pub wal: wal::writer::WalWriter,
    pub mem: mem::time_window_buffer::TimeWindowBuffer,
    pub query_engine: query::executor::QueryExecutor,
}

/// 启动 server（可选导入测试数据）
pub async fn run(config_path: String, test_file: Option<String>) -> anyhow::Result<()> {
    init_tracing();
    
    let cfg = AppConfig::load_or_create_default(&config_path)
        .with_context(|| format!("load config failed: {}", config_path))?;
    info!("tdb server start with root={}", cfg.storage.root);

    let wal = wal::writer::WalWriter::open(&cfg.wal)?;
    
    // 时序时间窗口双缓冲存储器
    let mem = mem::time_window_buffer::TimeWindowBuffer::new();
    replay_from_wal(&cfg.wal.dir, &mem)?;
    
    // 如果有测试文件，先导入数据
    if let Some(ref file) = test_file {
        info!("import test data from file: {}", file);
        import_test_data(file, &cfg, &wal, &mem)?;
    }
    
    // DataFusion 查询引擎
    let df_engine = query::datafusion_executor::QueryExecutor::new(cfg.storage.root.clone());
    
    // 查询执行器
    let query_engine = query::executor::QueryExecutor::new(
        cfg.storage.root.clone(), 
        mem.clone(),
        df_engine,
    );
    
    let shared = Arc::new(RuntimeState { 
        wal, 
        mem,
        query_engine,
    });

    let (ingest_tx, rx) = mpsc::channel::<IngestBatch>(cfg.ingest.channel_capacity);
    let mut ingest_task = tokio::spawn(run_ingest_loop(shared.clone(), rx));
    let mut mqtt_task = tokio::spawn(ingress::mqtt_consumer::run_mqtt_consumer(
        cfg.mqtt.clone(),
        ingest_tx.clone(),
    ));
    let mut flush_task = tokio::spawn(flush::flusher::run_flush_scheduler(
        cfg.flush.clone(),
        cfg.storage.clone(),
        shared.mem.clone(),
    ));
    let mut api_task = tokio::spawn(api::http::run_http_server(
        cfg.api.clone(),
        shared.query_engine.clone(),
        ingest_tx.clone(),
        shared.mem.clone(),
        cfg.storage.clone(),
        cfg.wal.dir.clone(),
    ));

    let mut ingest_finished = false;
    let mut mqtt_finished = false;
    let mut flush_finished = false;
    let mut api_finished = false;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown signal received");
        }
        res = &mut ingest_task => {
            ingest_finished = true;
            log_join_result("ingest", res);
        }
        res = &mut mqtt_task => {
            mqtt_finished = true;
            log_join_result("mqtt", res);
        }
        res = &mut flush_task => {
            flush_finished = true;
            log_join_result("flush", res);
        }
        res = &mut api_task => {
            api_finished = true;
            log_join_result("api", res);
        }
    }

    info!("shutdown begin: stop ingress tasks");
    if !mqtt_finished {
        mqtt_task.abort();
        let _ = mqtt_task.await;
    }
    if !api_finished {
        api_task.abort();
        let _ = api_task.await;
    }
    if !flush_finished {
        flush_task.abort();
        let _ = flush_task.await;
    }

    info!("shutdown continue: drain ingest queue to wal/mem");
    drop(ingest_tx);
    if !ingest_finished {
        let res = ingest_task.await;
        log_join_result("ingest", res);
    }

    info!("shutdown continue: flush all mem window to store");
    if let Err(e) = flush::flusher::flush_by_hour_window(&cfg.storage, &shared.mem).await {
        error!("shutdown flush_all failed: {:?}", e);
    }

    info!("shutdown continue: sync wal file");
    if let Err(e) = shared.wal.sync_all() {
        error!("shutdown wal sync failed: {:?}", e);
    }

    info!("shutdown done");
    Ok(())
}

/// 启动时从 WAL 目录回放到内存窗口
fn replay_from_wal(wal_dir: &str, mem: &mem::time_window_buffer::TimeWindowBuffer) -> anyhow::Result<()> {
    let n = wal::recovery::replay_wal_dir(Path::new(wal_dir), |batch| {
        mem.insert_recovered_batch(batch);
    })?;
    info!("wal replay done batches={} dir={}", n, wal_dir);
    Ok(())
}

/// 导入测试数据文件
fn import_test_data(
    file_path: &str,
    _cfg: &AppConfig,
    wal: &wal::writer::WalWriter,
    mem: &mem::time_window_buffer::TimeWindowBuffer,
) -> anyhow::Result<()> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut count = 0;
    
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        
        // 解析测试数据格式：{"id":"dev001","t":1777050000500,"s":2,"p":{"P06539":-75135,...}}
        let json: serde_json::Value = serde_json::from_str(&line)?;
        
        let device_id = json["id"].as_str().unwrap_or("unknown");
        let recv_ts = json["t"].as_u64().unwrap_or(0);
        
        // 解析 points
        let mut points = Vec::new();
        if let Some(params_obj) = json["p"].as_object() {
            for (param_id, value) in params_obj {
                if let Some(v) = value.as_f64() {
                    points.push(crate::model::DataPoint {
                        ts: recv_ts,
                        param_id: param_id.clone(),
                        value: v as f32,
                    });
                }
            }
        }
        
        if points.is_empty() {
            continue;
        }
        
        // 构造 IngestBatch
        let batch = IngestBatch {
            device_id: device_id.to_string(),
            recv_ts,
            points,
        };
        
        // 写入 WAL 和内存
        wal.append_batch(&batch)?;
        mem.insert_batch(batch);
        count += 1;
    }
    
    info!("import test data done: {} batches", count);
    Ok(())
}

/// 串行消费 ingest 批次
async fn run_ingest_loop(
    shared: Arc<RuntimeState>,
    mut rx: mpsc::Receiver<IngestBatch>,
) -> anyhow::Result<()> {
    while let Some(batch) = rx.recv().await {
        shared.wal.append_batch(&batch)?;
        shared.mem.insert_batch(batch);
    }
    Ok(())
}

/// 统一初始化日志
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();
}

/// 输出任务退出原因
fn log_join_result(name: &str, res: Result<anyhow::Result<()>, tokio::task::JoinError>) {
    match res {
        Ok(Ok(())) => info!("task {} exited", name),
        Ok(Err(e)) => error!("task {} failed: {:?}", name, e),
        Err(e) => error!("task {} panicked: {:?}", name, e),
    }
}
