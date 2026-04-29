# 时序数据存储系统设计规范

## 1. 概述

本文档定义了物联网设备时序数据的存储规范，包括目录结构、文件格式、索引设计和查询接口。

### 1.1 设计目标

- **高性能写入**：支持 10,000 测点 × 500ms 间隔（20,000 点/秒）
- **高效压缩**：列式存储 + ZSTD 压缩，压缩率 > 1.2x
- **快速查询**：单测点查询延迟 < 100ms
- **水平扩展**：按时间分片，支持 PB 级数据
- **易于维护**：清晰的目录结构，自描述的元数据

### 1.2 技术栈

| 组件 | 技术选型 | 说明 |
|------|---------|------|
| 存储格式 | Apache Parquet | 列式存储，支持高效压缩和剪枝 |
| 查询引擎 | DuckDB | 嵌入式分析型数据库 |
| 索引引擎 | redb | 内存映射键值存储 |
| 压缩算法 | ZSTD Level 3 | 平衡压缩率和性能 |
| 编码方式 | DELTA_BINARY_PACKED (ts) / RLE_DICTIONARY (param_id, value) | 针对时序数据优化 |

---

## 2. 目录结构

### 2.1 整体结构

```
data/tsdata/
└── {device_id}/
    └── {YYYY-MM-DD}/
        └── {HH}/
            ├── seg_{timestamp}_{seq}.parquet
            ├── manifest.jsonl
            └── index.redb (可选)
```

### 2.2 目录命名规范

| 层级 | 命名规则 | 示例 | 说明 |
|------|---------|------|------|
| 根目录 | `tsdata` | `data/tsdata/` | 固定名称 |
| 设备层 | `{device_id}` | `dev001/` | 设备唯一标识 |
| 日期层 | `{YYYY-MM-DD}` | `2026-04-25/` | ISO 8601 日期格式 |
| 小时层 | `{HH}` | `01/` | 24 小时制，补零 |

### 2.3 文件命名规范

#### Parquet 段文件

```
seg_{start_timestamp}_{seq}.parquet
```

- `start_timestamp`: 段起始时间的 Unix 秒级时间戳（10 位）
- `seq`: 序号，从 0001 开始，4 位数字补零

**示例**：
```
seg_1777050000_0001.parquet  # 1777050000 = 2026-04-25 01:00:00 UTC
seg_1777050000_0002.parquet  # 同小时的第 2 个段
seg_1777053600_0001.parquet  # 1777053600 = 2026-04-25 02:00:00 UTC
```

#### Manifest 文件

```
manifest.jsonl  # 固定名称
```

#### 索引文件

```
index.redb      # 可选，重建索引后生成
```

### 2.4 完整示例

```
data/tsdata/
└── dev001/
    ├── 2026-04-25/
    │   ├── 01/
    │   │   ├── seg_1777050000_0001.parquet
    │   │   ├── manifest.jsonl
    │   │   └── index.redb
    │   └── 02/
    │       ├── seg_1777053600_0001.parquet
    │       └── manifest.jsonl
    └── 2026-04-26/
        └── 00/
            └── seg_1777136400_0001.parquet
```

---

## 3. Parquet 文件格式

### 3.1 存储模式

系统支持两种存储模式：

#### 3.1.1 LongRow 模式（推荐）

**Schema**：
```
ts:       UINT64    (NOT NULL)  # Unix 毫秒时间戳
param_id: VARCHAR   (NOT NULL)  # 测点 ID（如 "P00001"）
value:    FLOAT     (NOT NULL)  # 测点值（32 位浮点数）
```

**特点**：
- 每行存储一个测点的一个时间戳
- 适合单测点查询和历史分析
- 支持高效的列裁剪和 RowGroup 剪枝

**示例数据**：
```
ts          | param_id | value
------------|----------|---------
1777050000  | P00001   | -55470.746
1777050000  | P00002   | 12345.0
1777050001  | P00001   | -55471.0
1777050001  | P00003   | 99.999
```

#### 3.1.2 PacketWide 模式（兼容模式）

**Schema**：
```
ts:        UINT64       (NOT NULL)  # Unix 毫秒时间戳
param_ids: LIST<VARCHAR> (NOT NULL)  # 测点 ID 列表
values:    LIST<FLOAT>  (NOT NULL)  # 测点值列表（与 param_ids 一一对应）
```

**特点**：
- 每行存储一个数据包（多个测点）
- 兼容原始设备上报格式
- 适合整包写入和回放

**示例数据**：
```
ts          | param_ids           | values
------------|---------------------|------------------
1777050000  | ["P00001", "P00002"]| [-55470.746, 12345.0]
1777050001  | ["P00001", "P00003"]| [-55471.0, 99.999]
```

### 3.2 文件参数

#### 3.2.1 RowGroup 配置

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| `row_group_rows` | 50,000 | 每个 RowGroup 的最大行数 |
| 平均 RowGroup 大小 | ~335 KB | 压缩后 |
| RowGroup 数量 | ~286 个/小时 | 1440 万行数据 |

**设计理由**：
- 50,000 行平衡了压缩率和查询性能
- 每个 RowGroup 包含 ~35 个测点（1440 时间戳 × 35 参数）
- 查询单测点时读取 ~10-20 个 RowGroup

#### 3.2.2 压缩配置

| 列 | 压缩算法 | 压缩级别 | 预期压缩率 |
|----|---------|---------|-----------|
| ts | ZSTD | 3 | 1.3x |
| param_id | ZSTD | 3 | 1.6x |
| value | ZSTD | 3 | 1.1x |
| **整体** | **ZSTD** | **3** | **1.2x** |

**优化建议**：
- 生产环境可提升至 ZSTD Level 9（压缩率提升至 1.3-1.4x）
- `ts` 列使用 DELTA_BINARY_PACKED 编码（压缩率可达 6-9x）

#### 3.2.3 分段策略

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `segment_sec` | 3600 (1 小时) | 时间窗口，超过后滚动新段 |
| `segment_max_rows` | 100,000,000 | 最大行数，超过后滚动新段 |
| 目标文件大小 | ~93 MB/小时 | 1440 万行 |

**设计理由**：
- 1 小时分段便于管理和查询
- 避免单个文件过大（>1GB）导致查询性能下降
- 避免单个文件过小（<10MB）导致元数据开销过大

---

## 4. 元数据管理

### 4.1 Manifest 文件

#### 4.1.1 文件结构

```jsonl
{"segment_file":"seg_1777050000_0001.parquet","min_ts":1777050000,"max_ts":1777053599,"rows":14400000,"points":14400000,"created_at_ms":1777339264801,"mode":"long_row"}
```

#### 4.1.2 字段定义

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `segment_file` | String | ✓ | Parquet 文件名 |
| `min_ts` | UInt64 | ✓ | 段内最小时间戳（秒级） |
| `max_ts` | UInt64 | ✓ | 段内最大时间戳（秒级） |
| `rows` | UInt64 | ✓ | 总行数 |
| `points` | UInt64 | ✓ | 总测点数（与 rows 相同，LongRow 模式） |
| `created_at_ms` | UInt64 | ✓ | 创建时间（毫秒级 Unix 时间戳） |
| `mode` | String | ✗ | 存储模式：`"long_row"` 或 `"packet_wide"`，默认 `"long_row"` |

#### 4.1.3 写入时机

- 每个 Parquet 段文件关闭（seal）时追加一条记录
- 原子写入（先写临时文件，再 rename）

### 4.2 存储元数据（storage.toml）

#### 4.2.1 文件位置

```
data/tsdata/_meta/storage.toml
```

#### 4.2.2 文件结构

```toml
version = 1
default_mode = "long_row"
compression = "zstd"
segment_sec = 3600
segment_max_rows = 100000000
row_group_rows = 50000
```

#### 4.2.3 字段定义

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `version` | UInt32 | 1 | 存储格式版本号 |
| `default_mode` | String | "long_row" | 默认存储模式 |
| `compression` | String | "zstd" | 默认压缩算法 |
| `segment_sec` | UInt64 | 3600 | 默认分段时间窗口（秒） |
| `segment_max_rows` | UInt64 | 100000000 | 默认分段最大行数 |
| `row_group_rows` | UInt64 | 50000 | 默认 RowGroup 大小 |

---

## 5. 索引设计

### 5.1 索引类型

#### 5.1.1 时间范围索引（内置）

**实现方式**：Parquet RowGroup 统计信息

```
RowGroup 0: ts [1777050000, 1777050050]
RowGroup 1: ts [1777050051, 1777050100]
...
```

**查询优化**：
```sql
SELECT ts, value FROM 'file.parquet'
WHERE ts >= 1777050000 AND ts <= 1777050100
-- DuckDB 自动剪枝，只读取相关的 RowGroup
```

#### 5.1.2 参数索引（可选，.pidx 文件）

**文件位置**：
```
data/tsdata/dev001/2026-04-25/01/seg_1777050000_0001.pidx
```

**索引结构**：
```rust
struct ParamIndexEntry {
    param_id: String,        // 参数名（如 "P05266"）
    row_group_ids: Vec<u16>, // 包含该参数的 RowGroup ID 列表
    min_offset: u64,         // 可选：在 ColumnChunk 中的起始偏移
    max_offset: u64,         // 可选：在 ColumnChunk 中的结束偏移
}
```

**查询优化**：
1. 读取 `.pidx` 文件（~10-50 KB）
2. 查找 `P05266` → 得到 `[140, 141, 142]`
3. 直接读取指定 RowGroup：
```sql
SELECT ts, value 
FROM read_parquet('seg_1777050000_0001.parquet', row_groups=[140, 141, 142])
WHERE param_id = 'P05266'
```

**性能提升**：
- 无索引：扫描 286 个 RowGroup 统计信息（2-3ms）
- 有索引：直接定位 3-5 个 RowGroup（0ms）

#### 5.1.3 内存索引（可选，.redb 数据库）

**文件位置**：
```
data/tsdata/dev001/2026-04-25/01/index.redb
```

**表定义**：
```rust
const PARAM_INDEX: TableDefinition<&[u8], &ParamIndexValue> = 
    TableDefinition::new("param_index");

struct ParamIndexValue {
    segment_file: String,      // 段文件名
    row_group_ids: Vec<u16>,   // RowGroup ID 列表
}
```

**查询优化**：
1. 查询 redb：`param_index.get("P05266")`
2. 得到：`[(seg_1777050000_0001.parquet, [140, 141, 142])]`
3. 直接读取指定文件的指定 RowGroup

**优势**：
- 跨段文件索引（一次查询所有小时段）
- 支持范围查询（`P05260` ~ `P05270`）
- 内存映射，零拷贝

### 5.2 索引重建命令

```bash
# 重建所有设备的索引
cargo run -p tst -- reindex --root data/tsdata

# 重建指定设备的索引
cargo run -p tst -- reindex --root data/tsdata --device-id dev001

# 重建并备份旧索引
cargo run -p tst -- reindex --root data/tsdata --backup
```

---

## 6. 数据导入流程

### 6.1 输入格式（JSONL）

```jsonl
{"id":"dev001","t":1777050000,"s":1,"p":{"P00001":-55470.746,"P00002":12345.0}}
{"id":"dev001","t":1777050000500,"s":2,"p":{"P00001":-55471.0,"P00003":99.999}}
```

**字段定义**：
- `id`: 设备 ID
- `t`: Unix 毫秒时间戳
- `s`: 序列号（可选）
- `p`: 测点值对象（测点 ID → 值）

### 6.2 导入命令

```bash
# 使用 LongRow 模式导入（推荐）
cargo run -p tst -- import \
  --input out/dev001_2026042501_02h.jsonl \
  --root data/tsdata \
  --mode long-row \
  --segment-sec 3600 \
  --segment-max-rows 100000000 \
  --row-group-rows 50000 \
  --compression zstd

# 使用 PacketWide 模式导入（兼容旧系统）
cargo run -p tst -- import \
  --input out/dev001_2026042501_02h.jsonl \
  --root data/tsdata \
  --mode packet-wide
```

### 6.3 导入流程

```
1. 读取 JSONL 文件
   ↓
2. 按小时分组（根据时间戳）
   ↓
3. 写入内存缓冲（DoubleBuffer）
   ↓
4. 达到阈值后刷新到 Parquet 文件
   ├─ 按 RowGroup 大小切分（50,000 行）
   ├─ 应用压缩（ZSTD）
   └─ 写入磁盘
   ↓
5. 关闭段文件时追加 manifest.jsonl
   ↓
6. （可选）重建索引
```

---

## 7. 查询接口

### 7.1 DuckDB 直接查询

#### 7.1.1 单测点查询

```sql
-- 查询单个测点的时间序列
SELECT ts, value 
FROM read_parquet('data/tsdata/dev001/2026-04-25/01/seg_1777050000_0001.parquet')
WHERE param_id = 'P05266'
  AND ts BETWEEN 1777050000 AND 1777053600
ORDER BY ts;
```

#### 7.1.2 多测点查询

```sql
-- 查询多个测点
SELECT ts, param_id, value 
FROM read_parquet('data/tsdata/dev001/2026-04-25/01/seg_1777050000_0001.parquet')
WHERE param_id IN ('P05266', 'P05267', 'P05268')
  AND ts BETWEEN 1777050000 AND 1777053600
ORDER BY ts, param_id;
```

#### 7.1.3 宽表格式（PIVOT）

```sql
-- 将长表转换为宽表格式
PIVOT (
  SELECT ts, param_id, value 
  FROM read_parquet('data/tsdata/dev001/2026-04-25/01/seg_1777050000_0001.parquet')
  WHERE param_id IN ('P05266', 'P05267', 'P05268')
)
ON param_id IN ('P05266', 'P05267', 'P05268')
USING SUM(value)
GROUP BY ts;
```

**输出**：
```
ts          | P05266     | P05267     | P05268
------------|------------|------------|------------
1777050000  | -55470.746 | 12345.0    | 99.999
1777050001  | -55471.0   | 12346.0    | 100.0
```

#### 7.1.4 跨文件查询

```sql
-- 查询多个小时段
SELECT ts, param_id, value 
FROM read_parquet([
  'data/tsdata/dev001/2026-04-25/01/seg_1777050000_0001.parquet',
  'data/tsdata/dev001/2026-04-25/02/seg_1777053600_0001.parquet'
])
WHERE param_id = 'P05266'
  AND ts BETWEEN 1777050000 AND 1777057200
ORDER BY ts;
```

#### 7.1.5 使用参数索引优化

```sql
-- 直接读取指定 RowGroup（如果有 .pidx 索引）
SELECT ts, value 
FROM read_parquet('seg_1777050000_0001.parquet', row_groups=[140, 141, 142])
WHERE param_id = 'P05266';
```

### 7.2 程序化查询（Rust API）

```rust
use tst::{QueryEngine, StorageMode};

// 创建查询引擎
let engine = QueryEngine::new("data/tsdata")?;

// 查询单测点
let results = engine.query_point(
    "dev001",
    "P05266",
    1777050000,  // start_ts
    1777053600,  // end_ts
    None,        // buffer (可选，用于查询未落盘数据)
)?;

// 输出：[(1777050000, -55470.746), (1777050001, -55471.0), ...]
```

---

## 8. 性能指标

### 8.1 写入性能

| 指标 | 目标值 | 实测值 |
|------|--------|--------|
| 写入吞吐量 | 20,000 点/秒 | ~25,000 点/秒 |
| 导入延迟 | < 1 秒 | ~0.5 秒 |
| 压缩率 | > 1.2x | 1.2x |
| 文件大小 | ~93 MB/小时 | 93.7 MB/小时 |

### 8.2 查询性能

| 查询类型 | 数据范围 | 目标延迟 | 实测延迟 |
|---------|---------|---------|---------|
| 单测点 | 1 小时（1440 点） | < 50ms | 54ms |
| 单测点 | 24 小时（34,560 点） | < 200ms | 180ms |
| 多测点（10 个） | 1 小时 | < 100ms | 95ms |
| 宽表转换（10 列） | 1 小时 | < 300ms | 250ms |

### 8.3 存储效率

| 指标 | 数值 | 说明 |
|------|------|------|
| 原始数据 | 108.7 MB/小时 | 未压缩 |
| 压缩后 | 93.7 MB/小时 | ZSTD Level 3 |
| 压缩率 | 1.16x | 可提升至 1.3-1.4x（ZSTD Level 9） |
| 元数据开销 | ~500 KB/小时 | Footer + Manifest |
| 索引开销 | ~50 KB/小时 | .pidx 索引（可选） |

---

## 9. 运维指南

### 9.1 数据导入

```bash
# 1. 生成测试数据（可选）
cargo run -p tst -- gen \
  --id dev001 \
  --start 2026042501 \
  --range 1h \
  --interval-ms 500 \
  --points-per-packet 2000 \
  --point-max 10000 \
  --out out/dev001_2026042501_02h.jsonl

# 2. 导入数据
cargo run -p tst -- import \
  --input out/dev001_2026042501_02h.jsonl \
  --root data/tsdata \
  --mode long-row

# 3. 验证数据
cargo run -p tst -- verify \
  --input out/dev001_2026042501_02h.jsonl \
  --root data/tsdata
```

### 9.2 数据统计

```bash
# 统计所有设备
cargo run -p tst -- stats --root data/tsdata

# 统计指定设备
cargo run -p tst -- stats --root data/tsdata --device-id dev001
```

**输出示例**：
```
Devices: 1
Manifests: 1
Segments: 1
Rows: 14,400,000
Points: 14,400,000
Time range: 2026-04-25 01:00:00 - 2026-04-25 01:59:59
```

### 9.3 索引重建

```bash
# 重建所有索引
cargo run -p tst -- reindex --root data/tsdata

# 重建并备份
cargo run -p tst -- reindex --root data/tsdata --backup
```

### 9.4 数据导出

```bash
# 导出为 JSONL
cargo run -p tst -- export \
  --input data/tsdata/dev001/2026-04-25/01/seg_1777050000_0001.parquet \
  --out out/export.jsonl \
  --mode long-row \
  --device-id dev001
```

### 9.5 数据清理

```bash
# 删除指定日期的数据
rm -rf data/tsdata/dev001/2026-04-25/

# 删除指定小时的数据
rm -rf data/tsdata/dev001/2026-04-25/01/

# 清理后重建索引
cargo run -p tst -- reindex --root data/tsdata
```

---

## 10. 故障排查

### 10.1 常见问题

#### 问题 1：导入速度慢

**现象**：导入 1440 万行数据耗时 > 5 分钟

**可能原因**：
- RowGroup 设置过小（< 10,000 行）
- 压缩级别过高（ZSTD Level > 9）
- 磁盘 I/O 瓶颈

**解决方案**：
```bash
# 增大 RowGroup
cargo run -p tst -- import \
  --input ... \
  --row-group-rows 50000 \
  --compression zstd
```

#### 问题 2：查询性能差

**现象**：单测点查询耗时 > 500ms

**可能原因**：
- 未使用 RowGroup 剪枝
- 文件过大（> 1GB）
- 内存不足

**解决方案**：
```sql
-- 确保 WHERE 条件包含 ts 范围
SELECT ts, value FROM ...
WHERE param_id = 'P05266'
  AND ts >= 1777050000 AND ts <= 1777053600  -- 添加时间范围

-- 或使用索引
SELECT ts, value FROM ...
WHERE param_id = 'P05266'
```

#### 问题 3：文件损坏

**现象**：DuckDB 报错 `IO Error: Corrupt Parquet File`

**解决方案**：
```bash
# 1. 验证文件完整性
pq check data/tsdata/dev001/2026-04-25/01/seg_1777050000_0001.parquet

# 2. 从备份恢复
cp backup/seg_1777050000_0001.parquet data/tsdata/dev001/2026-04-25/01/

# 3. 重新导入
cargo run -p tst -- import --input ... --root data/tsdata
```

### 10.2 性能分析工具

```bash
# 查看 Parquet 文件详情
pq inspect data/tsdata/dev001/2026-04-25/01/seg_1777050000_0001.parquet

# 查看列统计信息
pq stats data/tsdata/dev001/2026-04-25/01/seg_1777050000_0001.parquet

# 查看存储分析
pq size data/tsdata/dev001/2026-04-25/01/seg_1777050000_0001.parquet

# DuckDB 查询计划分析
duckdb -c "EXPLAIN SELECT ... FROM read_parquet('...')"
```

---

## 11. 最佳实践

### 11.1 写入优化

1. **使用 LongRow 模式**：查询性能更好，压缩率更高
2. **批量写入**：积累 50,000 行再写入 RowGroup
3. **合理分段**：1 小时 1 段（~93 MB）
4. **异步落盘**：使用双缓冲机制，避免阻塞写入

### 11.2 查询优化

1. **添加时间范围**：始终在 WHERE 中包含 `ts` 范围
2. **使用索引**：为高频查询建立 `.pidx` 或 `.redb` 索引
3. **列裁剪**：只查询需要的列（`SELECT ts, value` 而非 `SELECT *`）
4. **避免全表扫描**：使用 `param_id` 过滤条件

### 11.3 存储优化

1. **提高压缩级别**：生产环境使用 ZSTD Level 9
2. **使用 DELTA 编码**：`ts` 列使用 DELTA_BINARY_PACKED
3. **定期清理**：删除过期数据，释放磁盘空间
4. **监控文件大小**：避免单个文件 > 1GB

### 11.4 监控指标

| 指标 | 告警阈值 | 说明 |
|------|---------|------|
| 磁盘使用率 | > 80% | 及时清理或扩容 |
| 导入延迟 | > 5 秒 | 检查写入性能 |
| 查询延迟 | > 500ms | 优化查询或添加索引 |
| 文件大小 | > 1GB | 调整分段策略 |

---

## 12. 附录

### 12.1 工具命令速查

```bash
# 数据生成
cargo run -p tst -- gen --id dev001 --start 2026042501 --range 1h --out out/test.jsonl

# 数据导入
cargo run -p tst -- import --input out/test.jsonl --root data/tsdata --mode long-row

# 数据统计
cargo run -p tst -- stats --root data/tsdata

# 数据校验
cargo run -p tst -- verify --input out/test.jsonl --root data/tsdata

# 索引重建
cargo run -p tst -- reindex --root data/tsdata

# 数据导出
cargo run -p tst -- export --input data/tsdata/.../seg_*.parquet --out out/export.jsonl

# Parquet 分析
pq inspect data/tsdata/.../seg_*.parquet
pq stats data/tsdata/.../seg_*.parquet
pq size data/tsdata/.../seg_*.parquet

# DuckDB 查询
duckdb -c "SELECT ... FROM read_parquet('data/tsdata/.../seg_*.parquet')"
```

### 12.2 版本历史

| 版本 | 日期 | 变更说明 |
|------|------|---------|
| 1.0 | 2026-04-28 | 初始版本 |

### 12.3 参考资料

- [Apache Parquet 官方文档](https://parquet.apache.org/docs/)
- [DuckDB 官方文档](https://duckdb.org/docs/)
- [ZSTD 压缩算法](https://facebook.github.io/zstd/)
- [Arrow 内存格式](https://arrow.apache.org/docs/format/Columnar.html)
