# Rust Gateway Workspace

一个用于设备网关联调的 Rust 工作区，包含：
- `gw`：网关服务端（控制面 + 设备连接管理）
- `dev`：设备模拟器（可模拟多设备、遥测上报、指令回包）
- `cli`：控制台客户端（REPL 交互，支持命令别名）
- `tsd`：`tsdata` 查询工具（query/stat/export）
- `common`：共享协议与 RESP 编解码库

适合本地联调、协议验证、控制面回归测试。

## 1. 工作区结构

```text
gateway/
├─ Cargo.toml          # workspace
├─ config.toml         # gw 控制监听配置
├─ dev.toml            # 设备清单（gw/dev/cli 共用）
├─ common/             # 协议与编解码
├─ gw/                 # 网关服务
├─ dev/                # 设备模拟器
├─ cli/                # REPL 客户端
└─ tsd/                # tsdata 查询工具
```

## 2. 架构与数据流

### 2.1 总体流程

1. `dev` 按 `dev.toml` 启动模拟设备监听端口（如 `127.0.0.1:7101`）。
2. `gw` 启动控制端口（默认 `0.0.0.0:7002`），等待控制客户端指令。
3. `cli` 连接 `gw` 控制端口，通过 `CONNECT` 触发 `gw` 主动连接某个模拟设备。
4. `gw` 与设备连接建立后，处理：
   - 设备上报（Telemetry）
   - 下行指令（Command）
   - 回令匹配（CommandReply）

### 2.2 连接方向

- 控制面：`cli -> gw`（RESP 文本协议）
- 设备面：`gw -> dev`（`common::device_proto` 私有二进制协议）

## 3. 核心能力

### 3.1 `gw`（网关服务）

- 控制命令：`PING` / `LIST` / `STATUS` / `CONNECT` / `SEND` / `KICK`
- 同一 `device_id` 唯一在线连接（避免状态键不一致）
- 控制连接防护：
  - 协议解析失败返回标准错误码并断开
  - 待解析缓冲上限（`64KiB`）防止异常半包撑爆内存
- 错误响应标准化：`ERR_XXXX detail`
- 已接入 `tracing` 结构化日志（支持 `config.toml` 与 `RUST_LOG`）
- 遥测归档：固定 Parquet 分段 + `manifest.jsonl` 清单，支持按时间/大小自动切段

### 3.2 `dev`（设备模拟器）

- 支持 `--all` 或 `--device-id` 选择启动设备
- 遥测参数不依赖 `dev.toml`，每周期随机上报参数
- 默认每 `500ms` 上报 `2000` 个随机参数，编码范围 `P00001~P99999`
- 每个参数值随机为整型或浮点型
- 指令不依赖 `dev.toml`，`C00001~C99999` 视为合法指令
- 指令回包仅有 `SUCCESS` / `FAIL` 两种状态

### 3.3 `cli`（REPL）

- 本地命令：`HELP` / `QUIT` / `EXIT`
- 命令别名：
  - `CONN <device_id>` -> `CONNECT <ip:port>`
  - `STAT <device_id>` -> `STATUS <device_id>`
  - `CONA` -> 依次对 `dev.toml` 全部设备执行 `CONNECT <ip:port>`
  - `KICA` -> 依次对 `dev.toml` 全部设备执行 `KICK <device_id>`
- `CONNECT` 时可直接输入设备 ID，`cli` 会用 `dev.toml` 解析地址

## 4. 环境要求

- Rust stable（建议使用最新稳定版）
- Windows / Linux / macOS 均可（本文示例为 Windows PowerShell）

检查工具链：

```powershell
rustc -V
cargo -V
```

## 5. 快速开始（Windows）

### 5.1 构建

在仓库根目录执行：

```powershell
cargo build
```

### 5.2 启动设备模拟器（终端 A）

```powershell
cargo run -p dev -- --all --config dev.toml
```

### 5.3 启动网关（终端 B）

```powershell
cargo run -p gw -- --config config.toml --device-config dev.toml
```

可选：临时覆盖日志级别（优先于 `config.toml`）

```powershell
$env:RUST_LOG="debug"
cargo run -p gw -- --config config.toml --device-config dev.toml
```

### 5.4 启动控制客户端（终端 C）

```powershell
cargo run -p cli -- --addr 127.0.0.1:7002 --device-config dev.toml
```

进入 REPL 后示例：

```text
gw> PING
gw> LIST
gw> CONN dev001
gw> STAT dev001
gw> SEND dev001 C00001 1500
gw> KICK dev001
gw> QUIT
```

## 6. 配置说明

## 6.1 `config.toml`（网关控制面）

```toml
control_addr = "0.0.0.0:7002"
log_level = "info"
archive_enabled = true
archive_root = "data"
archive_rotate_mode = "time"
archive_rotate_size_mb = 64
archive_queue_capacity = 10000
archive_flush_interval_ms = 1000
```

字段说明：
- `control_addr`：`gw` 控制端口监听地址
- `log_level`：网关日志级别，支持 `trace/debug/info/warn/error`
- `archive_enabled`：是否启用遥测归档
- `archive_root`：归档根目录
- `archive_rotate_mode`：切段策略，`time|size|hybrid`
- `archive_rotate_size_mb`：按大小切段阈值（`size/hybrid` 生效）
- `archive_queue_capacity`：归档队列容量，队列满时会丢弃新遥测并告警
- `archive_flush_interval_ms`：归档刷盘周期（毫秒）

归档目录示例（Parquet）：

```text
data/
└─ dev003/
   └─ 20260423/
      ├─ h10_p001.parquet
      ├─ h10_p002.parquet
      └─ manifest.jsonl
```

## 6.2 `dev.toml`（设备清单）

示例（节选）：

```toml
[[devices]]
id = "dev001"
ip = "127.0.0.1"
port = 7101
telemetry_interval_ms = 500
```

说明：
- `gw` 与 `cli` 仅使用 `id/ip/port` 字段
- `dev` 使用 `telemetry_interval_ms` 控制上报周期
- 遥测参数与指令规则均为内置随机/校验逻辑，不再从 `dev.toml` 读取
- `cli` 通过 `id/ip/port` 将 `CONN <device_id>` 映射为 `CONNECT <ip:port>`

## 7. 控制命令参考（`gw`）

### 7.1 命令列表

- `PING`
  - 用途：连通性探测
  - 返回：`+PONG`
- `LIST`
  - 用途：列出设备状态
  - 返回：数组，每项含 `device_id/online/last_seen_ts/addr`
- `STATUS <device_id>`
  - 用途：查询单设备状态
- `CONNECT <sim_addr>`
  - 用途：触发网关主动连接设备模拟端，如 `127.0.0.1:7101`
- `SEND <device_id> <command_code> [timeout_ms]`
  - 用途：向在线设备下发指令并等待回令
  - 命令编码：建议使用 `C00001~C99999`（与当前 `dev` 约定一致）
  - 默认超时：`3000ms`
- `KICK <device_id>`
  - 用途：踢除设备连接并标记离线

### 7.2 错误响应格式

统一格式：

```text
-ERR_XXXX detail
```

常见错误码：
- `ERR_INVALID_ARGUMENT`
- `ERR_BAD_PROTOCOL`
- `ERR_UNKNOWN_COMMAND`
- `ERR_NOT_FOUND`
- `ERR_CONFLICT`
- `ERR_CONNECT_FAILED`
- `ERR_DEVICE_OFFLINE`
- `ERR_TIMEOUT`
- `ERR_INTERNAL`
- `ERR_FRAME_TOO_LARGE`

## 8. 协议说明

### 8.1 控制面协议（RESP）

`cli` 与 `gw` 使用 RESP（类似 Redis 协议）进行命令交互。

### 8.2 设备面协议（私有二进制）

定义位于 `common::device_proto`：
- Header 长度：12 字节
- 魔数：`0xCAFE`
- 版本：`1`
- 消息类型：
  - `Hello(1)`
  - `Telemetry(2)`
  - `Command(3)`
  - `CommandReply(4)`
  - `Heartbeat(5)`
  - `Error(255)`
- 负载上限：`1MiB`

## 9. 测试与质量检查

运行 `gw` 测试：

```powershell
cargo test -p gw
```

运行全工作区测试：

```powershell
cargo test
```

当前 `gw` 已覆盖的重点测试方向：
- CLI 参数优先级（默认值/配置文件/命令行覆盖）
- 参数错误处理（未知参数、缺失值、显式缺失配置）

## 10. 常见问题

### 10.1 `CONNECT` 报连接失败

排查顺序：
1. `dev` 是否已启动并监听目标端口
2. `dev.toml` 中 `ip/port` 是否正确
3. 本机防火墙是否拦截

### 10.2 `SEND` 报超时

排查顺序：
1. 设备是否在线（先 `STATUS`）
2. `command_code` 是否符合 `C00001~C99999`
3. 增加超时参数，如 `SEND dev001 C00001 5000`

### 10.3 `ERR_BAD_PROTOCOL` / `ERR_FRAME_TOO_LARGE`

- 控制端发送的 RESP 数据格式不合法，或单条输入累计过大
- 建议优先使用 `cli`，避免手写原始字节流

## 11. 开发建议

- 新增控制命令时，同步更新：
  - `gw/src/control.rs` 命令分发
  - `cli/src/local_commands.rs` 帮助文案
  - `README.md` 命令章节
- 保持错误码稳定，便于上层自动化处理
- 优先增加“行为测试”，如超时、重复连接、异常输入

---

如需继续扩展（例如接入 Prometheus 指标、增加认证、引入持久化设备状态），建议在 `gw` 中按“控制面/设备会话/状态存储”分层演进。 
