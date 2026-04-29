# EdgeTSDB 可执行设计方案（v1）

## 1. 目标与范围
- 目标：构建一套适配网关场景的时序数据库，满足 MQTT 接入、WAL 保证、分段落盘、最近 1 小时内存保留、统一查询（内存 + 磁盘）。
- 范围：单机版本（先不做分布式），优先保证写入可靠性与查询可用性，再逐步做性能优化。
- 约束：沿用当前存储方案（Parquet + 小时分段 + redb 小时索引 + pidx 参数索引），并将分段策略固定为 1 小时。

## 2. 总体架构
- `ingress`：MQTT 消费器，负责订阅网关 topic、解包、基本校验、投递写入队列。
- `wal`：顺序追加日志，写入成功后才确认 ingestion 成功（至少一次可恢复）。
- `memtable`：最近 1 小时热数据内存存储，支持按设备/参数/时间快速过滤。
- `flusher`：将已封窗数据批量写入 Parquet 小时分段，同时生成/更新 pidx、redb、manifest。
- `query`：统一查询入口，自动路由到磁盘查询与内存查询，并做结果归并。
- `recovery`：启动时基于 WAL 回放恢复 memtable 与未完成落盘状态。

数据流：
1. MQTT 收包 -> 解析标准行（device_id, ts, param_id, value, meta）
2. 先写 WAL（append + fsync 策略）
3. 再写 memtable（最近 1 小时窗口）
4. 到达 flush 条件后，批量落 Parquet + 构建索引
5. 查询时按时间切分：落盘区间查文件，热区间查内存，最后归并输出

## 3. 数据模型与目录布局
逻辑模型（long-row）：
- `ts: u64`
- `device_id: string`
- `param_id: string`
- `value: f32`（可扩展为联合类型）
- `quality/status/source`（预留可选列）

目录布局（建议）：
- `root/<device_id>/YYYY-MM-DD/HH/seg_xxxx.parquet`
- `root/<device_id>/YYYY-MM-DD/HH/seg_xxxx.parquet.pidx`
- `root/_index/tsindex.redb`
- `root/<device_id>/YYYY-MM-DD/HH/manifest.jsonl`
- `root/_wal/YYYY-MM-DD/HH/wal_xxxx.log`
- `root/_meta/storage.toml`

分段策略：
- `segment_sec = 3600`（1小时）
- `segment_max_rows`：建议先 100w（再按压测调优）
- `row_group_rows`：建议先 5w（便于 RowSelection 生效）

## 4. WAL 设计（可恢复）
WAL 记录格式（建议二进制）：
- `magic(4) + version(1) + crc32(4) + payload_len(4) + payload(N)`
- payload 为批次编码（建议 protobuf 或紧凑二进制结构）

WAL 写入策略：
- 默认：批量 `append`，每 `flush_interval_ms` 或每 `N` 条 `fdatasync`。
- 安全模式：每批次落盘后立即 `fsync`（更安全，吞吐下降）。

WAL 生命周期：
- `active`：当前写入日志
- `sealed`：窗口结束待 flush
- `checkpointed`：确认已落盘并建立 checkpoint 后可归档/删除

恢复流程：
1. 扫描 `_wal` 未 checkpoint 文件
2. 校验每条记录 CRC，跳过损坏尾部
3. 回放到 memtable，并标记需要重新 flush 的小时窗口
4. 完成后进入正常服务

## 5. 内存层（最近 1 小时）
目标：
- 保留每设备最近 1 小时明细数据，支撑“未落盘数据可查”。

结构建议：
- 一级：`HashMap<device_id, DeviceWindow>`
- 二级：`BTreeMap<ts_bucket, Vec<Row>>` 或 `Vec<Row>` + 时间索引
- 可选参数索引：`HashMap<param_id, Vec<offset>>`（提升点查）

淘汰策略：
- 基于 watermark：`now - 3600s` 之前的数据一旦确认落盘，即可从内存回收。

并发模型：
- 写入：单写线程/actor（避免锁竞争）
- 查询：多读（RWLock 或分片锁）

## 6. 落盘与索引构建
触发条件：
- 到达小时边界
- memtable 达到阈值（行数/内存）
- 手动 flush（运维命令）

落盘步骤（单小时窗口）：
1. 冻结窗口快照（写侧继续写新窗口）
2. 生成 Parquet（按 `param_id, ts` 排序）
3. 生成 pidx（二进制，已采用）
4. 更新 redb 小时索引（文件级候选定位）
5. 追加 manifest（可审计）
6. 写 checkpoint，标记对应 WAL 可清理

一致性规则：
- 先落数据文件，再更新索引，最后 checkpoint WAL。
- 任一步失败不删 WAL，保证可重试。

## 7. 查询设计（磁盘 + 内存）
查询输入：
- `device_id`
- 时间范围：`from_ts/to_ts` 或 `--all`
- 参数过滤：`param_ids`
- 返回模式：flat / grouped

查询执行计划：
1. 计算时间切片：`disk_range`（已落盘）+ `mem_range`（最近1小时）
2. 磁盘层：
- 用 redb 拿候选文件
- 读取 pidx 做 row-group/row-range 过滤
- 用 Parquet RowSelection 解码
3. 内存层：
- 直接在 memtable 按时间+参数过滤
4. 归并：
- 按 `ts` 合并并排序（同 ts 可按 param_id 次序）
5. 输出：
- 支持 limit/分页（建议 cursor）

正确性优先级：
- 同一条数据不能重复（避免 WAL 回放与落盘重叠重复返回）
- 同一时间段优先使用“已确认最新状态”视图

## 8. 对外接口（最小可用）
写入（内部）：
- `ingest.append(batch) -> Result<ack_id>`

查询（HTTP/gRPC 二选一，建议先 HTTP）：
- `POST /query`
- 请求：`device_id, from_ts, to_ts, param_ids, limit, flat`
- 响应：`rows + stats(profile可选)`

运维接口：
- `POST /admin/flush`
- `POST /admin/recover`
- `GET /admin/health`
- `GET /admin/stats`（WAL队列、memtable大小、flush延迟）

## 9. 配置项（建议）
- `mqtt.brokers/topic/qos/client_id`
- `wal.dir/sync_mode/sync_interval_ms/max_file_mb`
- `mem.window_sec=3600/max_rows/max_bytes`
- `storage.root/segment_sec=3600/segment_max_rows/row_group_rows/compression`
- `query.default_limit/max_limit/enable_profile`
- `flush.interval_ms/max_pending_windows`

## 10. 可执行开发计划（里程碑）
M1（1-2周）：写入可靠链路打通
- MQTT -> WAL -> memtable
- 启动恢复（WAL回放）
- 基础健康检查

M2（1-2周）：小时落盘闭环
- memtable 冻结与 Parquet 落盘
- pidx + redb + manifest 更新
- WAL checkpoint 与清理

M3（1周）：统一查询
- 磁盘查询 + 内存查询 + 归并
- `--profile` 统计输出（已具备可复用基础）

M4（1周）：稳定性与压测
- 长稳写入（24h）
- 宕机恢复演练
- 查询性能回归（P50/P95/P99）

## 11. 验收标准（可直接执行）
功能验收：
- 能持续消费 MQTT 数据并写入 WAL
- 进程异常退出后，重启可回放恢复，无丢失（允许“至少一次”重复）
- 每小时自动生成 Parquet 与索引
- 查询同一时间范围可同时命中内存和磁盘，并正确合并

性能验收（首版建议）：
- 写入：单机稳定处理目标包速率（按现场实际设定）
- 查询：`--all` 单参数在目标硬件下达到既定阈值（例如 < 300ms 稳态）
- 恢复：WAL 回放速度达到目标（例如 > 10万行/秒）

可靠性验收：
- 断电/崩溃后可恢复到最近 checkpoint 后状态
- 损坏 WAL 尾部可自动截断并继续服务

## 12. 风险与针对性改进
- 风险1：`--all` 场景 pidx 可能收益不稳定
- 方案：增加查询策略开关（auto/use/skip pidx），按 profile 自适应。

- 风险2：pidx 读取与规划开销偏高
- 方案：进程内 pidx 缓存（LRU）+ 单次查询内复用。

- 风险3：redb 每小时重复打开成本
- 方案：单查询复用 DB 句柄与读事务。

- 风险4：输出层耗时影响体感
- 方案：分页/limit 默认值、流式压缩输出、减少终端打印。

## 13. 首版实施清单（可开工）
- 新建 crate：`tsdbd`（服务进程）
- 模块拆分：
- `src/ingress/mqtt_consumer.rs`
- `src/wal/writer.rs`
- `src/wal/recovery.rs`
- `src/mem/window_store.rs`
- `src/flush/flusher.rs`
- `src/query/planner.rs`
- `src/query/executor.rs`
- `src/api/http.rs`
- 复用现有：
- `ingest` 的 Parquet + pidx 生成逻辑
- `tsd` 的查询解析、row-group 选择、profile 统计

---
本方案先确保“可运行、可恢复、可查询”，再迭代“更快、更省、更稳”。如确认该版本，我可以下一步给出 `tsdbd` 的代码骨架与接口定义（包含函数级注释）。
