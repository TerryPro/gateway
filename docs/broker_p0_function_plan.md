# Broker P0 功能方案报告（仅功能性）

## 1. 文档目的

本方案基于当前 `broker` 代码现状，输出一版可落地的 P0 功能建设计划，覆盖以下目标能力：

- 多设备接入与会话管理
- 数据采集与标准化
- 本地状态缓存
- 规则引擎 MVP
- 指令下发闭环
- 离线缓存与补传
- 可观测基础

说明：本版本不包含安全、多租户、权限与合规要求。

## 2. 现状盘点（基于当前代码）

### 2.1 已具备能力

1. 主流程编排完整
   - 已在启动流程中完成日志初始化、归档任务、MQTT 桥接、控制监听与设备配置加载。
   - 参考：`broker/src/main.rs`

2. 全局运行态容器具备基础字段
   - 设备列表、连接句柄、请求 pending、参数订阅、参数当前值、CLI 会话等已具备。
   - 参考：`broker/src/state.rs`

3. 遥测处理链路可用
   - 已支持设备上报解析、点位提取、参数当前值缓存、归档投递、MQTT 发布。
   - 参考：`broker/src/device.rs`

4. 归档链路可用
   - 已支持 Parquet 分段写入、滚动封段、manifest 记录。
   - 参考：`broker/src/archive.rs`

5. 控制命令链路可用
   - TCP 控制协议可执行命令，MQTT 控制也复用同一命令执行入口。
   - 参考：`broker/src/control/mod.rs`、`broker/src/control/protocol.rs`、`broker/src/mqtt_bridge.rs`

6. 指令闭环基础可用
   - `SEND` 命令支持请求 ID、回令等待、超时返回。
   - 参考：`broker/src/control/commands/device_commands/action_commands.rs`

### 2.2 主要缺口

1. 无规则引擎模块（条件判断、冷却、告警动作缺失）。
2. 无参数窗口态缓存（仅当前值，缺最近 N 次/N 秒统计）。
3. MQTT 发布失败存在丢弃路径（无本地补传队列）。
4. 指令下发无标准重试策略、执行历史持久化。
5. 可观测主要依赖日志，缺统一关键指标汇总。
6. 自动重连能力不足（依赖手工 `CONNECT`）。

## 3. P0 目标与边界

## 3.1 目标

在单进程 `broker` 内形成可上线的功能闭环：

- 能稳定接设备并维护会话状态；
- 能标准化处理遥测并维护当前值与窗口值；
- 能通过规则做告警与联动；
- 能可靠下发指令并追踪结果；
- 能在上行异常时本地缓存并恢复补传；
- 能输出关键运行状态与计数。

### 3.2 边界

1. 维持单进程内嵌架构，不拆独立服务。
2. 配置先采用 TOML。
3. 不做 GUI，仅提供命令与 MQTT 状态主题。

## 4. 总体架构设计（P0）

### 4.1 数据链路

`DeviceIngest -> Normalize -> StateCache(Current+Window) -> RuleEngine -> ActionExecutor -> Uplink(Store&Forward)`

### 4.2 控制链路

`TCP/MQTT Command -> CommandOrchestrator -> DeviceSession -> ResultRecord`

### 4.3 架构原则

1. 主流程不阻塞：重计算/重 IO 通过内部队列异步化。
2. 动作解耦：规则只产生命中的动作，动作由 Rust 执行器统一执行。
3. 先内嵌后可拆：模块接口按服务边界设计，保留后续外拆可能。

## 5. 模块方案与改造点

### 5.1 多设备接入与会话管理

#### 目标

- 强化连接生命周期管理，新增自动重连与退避策略。

#### 设计

1. 在现有连接状态基础上增加字段：
   - `last_disconnect_reason`
   - `reconnect_attempts`
   - `next_reconnect_at`
2. 新增 `reconnect_scheduler`：
   - 对曾成功连接的设备维护重连任务。
   - 采用指数退避（例如 1s/2s/4s/8s，上限 60s）。
3. 连接成功后清空重连计数，连接失败记录原因并继续调度。

#### 改造文件（建议）

- `broker/src/state.rs`（状态字段扩展）
- `broker/src/device.rs`（断链事件回调）
- `broker/src/main.rs`（启动重连调度任务）

### 5.2 数据采集与标准化

#### 目标

- 将原始上报统一为标准点结构，支持基础换算与质量控制。

#### 设计

1. 新增结构 `NormalizedPoint`：
   - `device_id`
   - `param_id`
   - `value`
   - `unit`（可选）
   - `quality`（默认 good）
   - `ts_ms`
2. 新增 `normalize_pipeline`：
   - 参数 ID 规范化（大写）
   - 非法值过滤（NaN/inf）
   - 按设备模板做 scale/offset 单位换算（可选）
3. 失败/过滤计数进入指标。

#### 改造文件（建议）

- `broker/src/device.rs`（替换原始点解析输出）
- 新增 `broker/src/normalize/mod.rs`
- 配置新增 `broker.toml` 标准化项（可选）

### 5.3 本地状态缓存

#### 目标

- 在现有“当前值缓存”基础上增加“窗口缓存与统计快照”。

#### 设计

1. 保留现有 `param_current_values`。
2. 新增 `window_cache[(device_id,param_id)] -> VecDeque<(ts_ms, value)>`。
3. 维护窗口统计：
   - `count`
   - `min`
   - `max`
   - `avg`
4. 提供查询命令：
   - `KEYWIN <device_id> <param_id> [window_s]`

#### 改造文件（建议）

- `broker/src/state.rs`（新增缓存结构）
- 新增 `broker/src/window/mod.rs`
- `broker/src/control/commands/...`（新增查询命令）

### 5.4 规则引擎 MVP（Rhai）

#### 目标

- 支持阈值、连续超限、组合条件、冷却时间、告警输出。

#### 设计

1. 规则定义（TOML）：
   - `id`
   - `enabled`
   - `priority`
   - `cooldown_secs`
   - `condition`（Rhai）
   - `actions`
2. 规则执行边界：
   - Rhai 只做条件计算，返回 bool。
   - Rust 统一执行动作（发 MQTT、记告警、触发命令）。
3. 内置函数（第一版）：
   - `p("A00001")`：读取当前值
   - `consec_over("A00001", 80.0, 3)`：连续超限判定
4. 冷却机制：
   - 以 `(rule_id, device_id)` 维度控制重复触发间隔。

#### 改造文件（建议）

- 新增 `broker/src/engine/rules/model.rs`
- 新增 `broker/src/engine/rules/runtime.rs`
- 新增 `broker/src/engine/rules/action.rs`
- `broker/src/device.rs`（遥测流程中调用规则评估）
- `broker.toml` 新增规则文件路径

### 5.5 指令下发闭环增强

#### 目标

- 在现有超时与回令基础上增加标准重试和执行记录。

#### 设计

1. 扩展 `SEND` 策略参数：
   - `timeout_ms`
   - `max_retries`
   - `retry_backoff_ms`
2. 执行状态记录 `command_runs`：
   - `req_id`
   - `device_id`
   - `command_code`
   - `start_ts_ms/end_ts_ms`
   - `status`（success/timeout/fail）
   - `retry_count`
   - `error_msg`
3. 控制命令新增：
   - `CMDHIST [N]`（最近 N 条执行记录）

#### 改造文件（建议）

- `broker/src/control/commands/device_commands/action_commands.rs`
- 新增 `broker/src/command_history/mod.rs`
- `broker/src/control/commands/device_commands/query_commands.rs`

### 5.6 离线缓存与补传

#### 目标

- 上行异常不丢数，恢复后自动补传。

#### 设计

1. 新增 `uplink store-forward` 组件（建议 SQLite）：
   - 发布失败写入本地队列。
   - 重连后按创建顺序补传。
   - 补传成功 ACK 删除。
2. 队列字段：
   - `id`
   - `topic`
   - `payload`
   - `qos`
   - `created_ts_ms`
   - `retry_count`
   - `next_retry_ts_ms`
   - `status`
3. 回压策略：
   - 队列达到上限时按策略丢弃最旧或拒绝新写入，并打点日志。

#### 改造文件（建议）

- 新增 `broker/src/uplink/store_forward.rs`
- `broker/src/mqtt_bridge.rs`（publish 失败时落库，恢复后 drain）
- `broker/src/cli.rs`、`broker.toml`（新增队列配置）

### 5.7 可观测基础

#### 目标

- 在不引入复杂监控系统前提下，提供足够运维可见性。

#### 设计

1. 关键指标（内存计数器）：
   - `device_online_count`
   - `telemetry_in_total`
   - `telemetry_drop_total`
   - `rule_eval_total`
   - `rule_hit_total`
   - `command_send_total`
   - `command_success_total`
   - `command_timeout_total`
   - `uplink_backlog`
   - `uplink_replay_total`
2. 周期输出状态日志（例如每 10 秒）。
3. 通过 MQTT 发布系统状态主题：
   - `gw/sys/metrics`
   - `gw/sys/uplink/backlog`
   - `gw/sys/rules/hit`
4. 控制命令新增：
   - `METRICS`
   - `UPLINK_STATUS`

#### 改造文件（建议）

- 新增 `broker/src/metrics/mod.rs`
- `broker/src/main.rs`（启动周期指标任务）
- `broker/src/control/commands/...`（指标查询）

## 6. 配置方案（新增建议）

在 `broker.toml` 增加：

1. 规则引擎
   - `rules_enabled`
   - `rules_file_path`
   - `rules_reload_interval_ms`（可选）

2. 离线补传
   - `uplink_store_enabled`
   - `uplink_db_path`
   - `uplink_max_queue`
   - `uplink_replay_batch`
   - `uplink_retry_backoff_ms`

3. 指令重试
   - `command_default_timeout_ms`
   - `command_default_retries`
   - `command_retry_backoff_ms`

4. 可观测
   - `metrics_emit_interval_ms`

## 7. 命令与主题扩展建议

### 7.1 控制命令（TCP/MQTT 同步扩展）

1. 规则相关
   - `RULE_LIST`
   - `RULE_RELOAD`
   - `RULE_ENABLE <rule_id>`
   - `RULE_DISABLE <rule_id>`

2. 指令历史与运行态
   - `CMDHIST [N]`
   - `METRICS`
   - `UPLINK_STATUS`

3. 缓存查询
   - `KEYWIN <device_id> <param_id> [window_s]`

### 7.2 MQTT 状态主题

1. `gw/sys/metrics`
2. `gw/sys/rules/hit`
3. `gw/sys/uplink/backlog`

## 8. 里程碑计划（6 周）

### 第 1 周：状态与标准化

1. 增加窗口缓存数据结构和查询接口。
2. 落地标准化流水线（点位过滤、时间戳统一、可选换算）。

### 第 2-3 周：规则引擎 MVP

1. 引入 Rhai 依赖并实现规则加载、编译缓存。
2. 支持 `p`、`consec_over`、组合条件、冷却。
3. 输出告警动作（先 MQTT + 日志）。

### 第 4 周：指令闭环增强

1. `SEND` 增加可配置重试。
2. 落地 `command_runs` 历史记录与查询命令。

### 第 5 周：离线补传

1. 落地 store-forward 本地队列。
2. 打通 publish 失败落库与连接恢复补传。

### 第 6 周：可观测收口与回归

1. 指标汇总、状态主题、运行态查询命令补齐。
2. 压测、稳定性回归、文档与操作手册输出。

## 9. 验收标准（P0）

1. 连接与会话
   - 多设备并发接入稳定，在线/离线状态与实际一致。

2. 数据处理
   - 遥测解析稳定，非法数据可统计可追踪。

3. 规则能力
   - 阈值、连续超限、组合条件、冷却生效。
   - 规则命中有可查询日志。

4. 指令闭环
   - 支持超时与重试，执行结果可追溯。

5. 补传能力
   - 断网期间数据落地，恢复后自动补传。

6. 可观测
   - 能查询核心计数器和积压状态，具备基础问题定位能力。

## 10. 实施优先级建议

优先顺序建议如下：

1. 规则引擎 MVP（直接提升业务价值）
2. 离线补传（直接提升可靠性）
3. 指令重试与历史（直接提升可运维性）
4. 窗口缓存与指标收口（提升可观察与调参效率）

---

本报告可作为 P0 评审基线；评审后建议输出两份配套文档：

1. 《模块接口定义（Rust 结构体与 trait）》
2. 《联调与验收用例清单（命令、主题、异常场景）》
