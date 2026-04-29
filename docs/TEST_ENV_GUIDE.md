# 测试环境启动使用说明（Windows）

本文用于在本项目中快速启动一套可联调的测试环境，目标链路为：

`dev(模拟设备) -> gw(网关解析测点) -> rmqtt(消息中间件) -> Web 前端(mqtt_viewer.html)`

## 1. 环境与目录

- 操作系统：Windows
- Rust：建议 stable 最新版
- 项目根目录：`D:\Develop\Rust\gateway`
- 本文所有命令默认在项目根目录执行

## 2. 端口与组件约定

- `rmqtt MQTT/TCP`：`1883`
- `rmqtt MQTT/WS`：`8080`
- `rmqtt HTTP API`：`6060`
- `gw 控制端口`：`7002`
- `dev` 设备监听端口：来自 `dev.toml`（如 `7101`、`7102`...）

## 3. 配置检查（首次必做）

### 3.1 rmqtt 配置

文件：`tools\rmqtt\rmqtt-0.19.0-x86_64-pc-windows\etc\rmqtt.toml`

确保至少包含：

```toml
listener.tcp.external.addr = "0.0.0.0:1883"
listener.tcp.external.allow_anonymous = true
listener.ws.external.addr = "0.0.0.0:8080"
```

说明：
- `allow_anonymous = true` 便于本地联调（生产环境建议关闭并启用认证）
- `listener.ws.external` 用于浏览器通过 WebSocket 订阅 MQTT

### 3.2 gw 配置

文件：`config.toml`

确保 MQTT 发布配置可用：

```toml
mqtt_enabled = true
mqtt_host = "127.0.0.1"
mqtt_port = 1883
mqtt_client_id = "gw-publisher"
mqtt_topic_prefix = "gw"
mqtt_queue_capacity = 10000
mqtt_qos = 1
```

## 4. 启动步骤（按顺序）

建议打开 4 个终端窗口（PowerShell 或 CMD 均可）。

### 终端 A：启动 rmqtt

```powershell
cd D:\Develop\Rust\gateway\tools\rmqtt\rmqtt-0.19.0-x86_64-pc-windows
.\start.bat
```

看到类似日志表示成功：
- `MQTT Broker Listening on external/tcp 0.0.0.0:1883`
- `MQTT Broker Listening on external/ws 0.0.0.0:8080`

### 终端 B：启动模拟设备 dev

```powershell
cd D:\Develop\Rust\gateway
cargo run -p dev -- --all --config dev.toml
```

### 终端 C：启动网关 gw

```powershell
cd D:\Develop\Rust\gateway
cargo run -p gw -- --config config.toml --device-config dev.toml
```

看到类似日志表示成功：
- `gateway started`
- `control listener ready control_addr=0.0.0.0:7002`
- `mqtt worker started ...`
- `mqtt connected`

### 终端 D：启动控制客户端 cli

```powershell
cd D:\Develop\Rust\gateway
cargo run -p cli -- --addr 127.0.0.1:7002 --device-config dev.toml
```

在 `cli` 里执行：

```text
PING
LIST
CONA
STAT dev001
```

说明：
- `CONA` 会按 `dev.toml` 批量连接设备
- 设备连接后，`gw` 才会收到并处理上行测点

## 5. 前端查看实时测点

浏览器打开文件：

- `mqtt_viewer.html`

页面默认参数：
- WS 地址：`ws://127.0.0.1:8080/`
- Topic：`gw/+/telemetry`

点击“连接并订阅”后，应看到：
- 连接日志：`MQTT 连接成功`、`订阅成功`
- 实时数据区持续出现 JSON 消息

## 6. 数据格式说明

`gw` 发布到 rmqtt 的 Topic：

- `gw/{device_id}/telemetry`

Payload 示例：

```json
{
  "device_id": "dev001",
  "ts_ms": 1713945307000,
  "points": [
    { "id": "P00001", "value": 12.34 },
    { "id": "P00002", "value": 56.78 }
  ]
}
```

## 7. 快速验收清单

- `rmqtt` 正常监听 `1883` 和 `8080`
- `gw` 日志出现 `mqtt connected`
- `cli` 执行 `CONA` 成功
- 页面已连接并成功订阅 `gw/+/telemetry`
- 页面实时刷出 `points` 数据

## 8. 停止与重启

- 停止单个组件：在对应终端按 `Ctrl + C`
- 重启建议顺序：
1. 先停 `cli`
2. 再停 `gw`
3. 再停 `dev`
4. 最后停 `rmqtt`
5. 启动时按本文第 4 节顺序重启

## 9. 常见问题排查

### 9.1 页面连接不上 WS

- 检查 `rmqtt` 是否已启动
- 检查 `rmqtt.toml` 是否开启 `listener.ws.external.addr`
- 检查防火墙是否拦截 `8080`

### 9.2 gw 日志反复出现 mqtt 连接错误

- 检查 `mqtt_host/mqtt_port` 是否正确（默认 `127.0.0.1:1883`）
- 检查 `rmqtt` 是否已启动
- 若 `allow_anonymous = false`，需在 `config.toml` 配置 `mqtt_username/mqtt_password`

### 9.3 页面无数据但已连接成功

- 确认 `cli` 已执行 `CONA` 或 `CONNECT`
- 确认 `dev` 正在运行并持续上报
- 确认订阅主题是 `gw/+/telemetry`

