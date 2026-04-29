<script setup lang="ts">
import { computed, h, onBeforeUnmount, onMounted, ref } from "vue";
import mqtt, { type IClientOptions, type MqttClient } from "mqtt";
import { invoke } from "@tauri-apps/api/core";
import {
  type DataTableColumns,
  darkTheme,
  NButton,
  NCard,
  NConfigProvider,
  NDataTable,
  NForm,
  NFormItem,
  NGrid,
  NGridItem,
  NInput,
  NLayout,
  NLayoutContent,
  NLayoutHeader,
  NLayoutSider,
  NModal,
  NSpace,
  NStatistic,
  NTabPane,
  NTabs,
  NTag,
  NText,
} from "naive-ui";

type ConnectLogRow = {
  id: number;
  time: string;
  level: string;
  message: string;
};

type ResponseLogRow = {
  id: number;
  time: string;
  topic: string;
  type: string;
  bytes: number;
  payload: string;
};

type SubscriptionDataRow = {
  id: number;
  time: string;
  deviceId: string;
  values: Record<string, string>;
  tsMs: number;
};

type RealtimePacketRow = {
  id: number;
  time: string;
  deviceId: string;
  paramId: string;
  topic: string;
  bytes: number;
  payload: string;
};

type HistoryRawRow = {
  ts: number;
  value: number;
};

type HistoryQueryResponse = {
  total: number;
  rows: HistoryRawRow[];
  root: string;
};

type HistoryTableRow = {
  id: number;
  time: string;
  ts: number;
  value: string;
};

type DeviceStatus = "connected" | "disconnected" | "connecting" | "disconnecting";

type DeviceConnectionItem = {
  id: string;
  simAddr: string;
  status: DeviceStatus;
  updatedAt: string;
};

const wsUrl = ref("ws://127.0.0.1:8080/");
const clientId = ref("desktop-console-1");
const newDeviceId = ref("");
const subDeviceId = ref("dev001");
const subParamsInput = ref("P00001,P00002");
const paramSubscriptions = ref<Array<{ deviceId: string; params: string[] }>>([
  { deviceId: "dev001", params: ["P00001", "P00002"] },
]);
const deviceConnections = ref<DeviceConnectionItem[]>([
  { id: "dev001", simAddr: "127.0.0.1:7101", status: "disconnected", updatedAt: "-" },
  { id: "dev002", simAddr: "127.0.0.1:7102", status: "disconnected", updatedAt: "-" },
  { id: "dev003", simAddr: "127.0.0.1:7103", status: "disconnected", updatedAt: "-" },
]);

const connected = ref(false);
const realtimeSubscribed = ref(false);
const realtimePaused = ref(false);
const activeLogTab = ref<"connect" | "response" | "realtime">("connect");
const realtimeViewMode = ref<"param" | "packet">("param");
const showConfigModal = ref(false);
const draftWsUrl = ref("");
const draftClientId = ref("");

const client = ref<MqttClient | null>(null);
const connectLogs = ref<ConnectLogRow[]>([]);
const responseLogs = ref<ResponseLogRow[]>([]);
const subscriptionDataRows = ref<SubscriptionDataRow[]>([]);
const realtimePacketRows = ref<RealtimePacketRow[]>([]);
const historyRows = ref<HistoryTableRow[]>([]);
const historyTotal = ref(0);
const historyLoading = ref(false);
const historyError = ref("");
const historyDeviceId = ref("dev001");
const historyParamId = ref("P00001");
const historyFrom = ref("");
const historyTo = ref("");
const historyRoot = ref("");
const logSeq = ref(1);

const MAX_LOG_LINES = 500;
const SUB_ROW_MERGE_WINDOW_MS = 300;
const CONFIG_STORAGE_KEY = "gw-console-connection-config";
const themeOverrides = {
  common: {
    fontSize: "14px",
    fontSizeMini: "14px",
    fontSizeSmall: "14px",
    fontSizeMedium: "14px",
    fontSizeLarge: "14px",
    fontSizeHuge: "14px",
  },
};

/**
 * 返回当前日志时间字符串，统一各日志表格时间格式。
 */
function nowTime(): string {
  return new Date().toLocaleTimeString();
}

/**
 * 生成日志主键，确保表格渲染稳定。
 */
function nextLogId(): number {
  const id = logSeq.value;
  logSeq.value += 1;
  return id;
}

/**
 * 追加日志并限制最大行数，避免高频场景下内存无限增长。
 */
function pushBounded<T>(target: { value: T[] }, row: T): void {
  target.value.push(row);
  if (target.value.length > MAX_LOG_LINES) {
    target.value.splice(0, target.value.length - MAX_LOG_LINES);
  }
}

/**
 * 记录连接类日志，用于连接生命周期与错误排查。
 */
function pushConnectLog(message: string, level = "INFO"): void {
  pushBounded(connectLogs, {
    id: nextLogId(),
    time: nowTime(),
    level,
    message,
  });
}

/**
 * 记录命令应答日志，保留完整 payload 供展开查看。
 */
function pushResponseLog(topic: string, type: string, bytes: number, payload: string): void {
  pushBounded(responseLogs, {
    id: nextLogId(),
    time: nowTime(),
    topic,
    type,
    bytes,
    payload,
  });
}

/**
 * 记录订阅参数数据行，并在短窗口内按设备合并，降低行级稀疏度。
 */
function pushSubscriptionDataRow(deviceId: string, values: Record<string, string>): void {
  const now = Date.now();
  const rows = subscriptionDataRows.value;
  for (let i = rows.length - 1; i >= 0; i -= 1) {
    const row = rows[i];
    if (row.deviceId !== deviceId) continue;
    if (now - row.tsMs > SUB_ROW_MERGE_WINDOW_MS) break;
    rows[i] = {
      ...row,
      values: { ...row.values, ...values },
      tsMs: now,
    };
    return;
  }
  pushBounded(subscriptionDataRows, {
    id: nextLogId(),
    time: nowTime(),
    deviceId,
    values,
    tsMs: now,
  });
}

/**
 * 记录原始实时数据包，供“数据包模式”按接收粒度展示。
 */
function pushRealtimePacketRow(
  topic: string,
  deviceId: string,
  paramId: string,
  bytes: number,
  payload: string,
): void {
  pushBounded(realtimePacketRows, {
    id: nextLogId(),
    time: nowTime(),
    deviceId,
    paramId,
    topic,
    bytes,
    payload,
  });
}

/**
 * 将 payload 尝试格式化为可读文本，若非 JSON 则原样返回。
 */
function formatPayload(raw: string): string {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

/**
 * 识别消息类型，用于实时区摘要展示。
 */
function detectMessageType(raw: string): string {
  try {
    const parsed = JSON.parse(raw);
    if (parsed && Array.isArray(parsed.points)) return "telemetry";
    if (parsed && typeof parsed === "object") return "json";
    return typeof parsed;
  } catch {
    return "text/binary";
  }
}

/**
 * 统一参数标识格式，避免大小写不一致导致匹配失败。
 */
function normalizeParamId(raw: string): string {
  return raw.trim().toUpperCase();
}

/**
 * 将秒级时间戳格式化为 `datetime-local` 可用字符串。
 */
function toDatetimeLocalValue(tsSec: number): string {
  const date = new Date(tsSec * 1000);
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  const hh = String(date.getHours()).padStart(2, "0");
  const mi = String(date.getMinutes()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}`;
}

/**
 * 将 `datetime-local` 输入转换为秒级时间戳。
 */
function parseDatetimeLocalToTs(value: string): number | null {
  if (!value.trim()) return null;
  const ms = new Date(value).getTime();
  if (!Number.isFinite(ms)) return null;
  return Math.floor(ms / 1000);
}

/**
 * 将秒级时间戳格式化为界面可读时间文本。
 */
function formatTsLabel(tsSec: number): string {
  return new Date(tsSec * 1000).toLocaleString();
}

/**
 * 初始化历史查询默认时间窗口（最近 1 小时）。
 */
function initHistoryRange(): void {
  const nowSec = Math.floor(Date.now() / 1000);
  historyTo.value = toDatetimeLocalValue(nowSec);
  historyFrom.value = toDatetimeLocalValue(nowSec - 3600);
}

/**
 * 展开单个参数片段，支持 `P00001` 与 `P00001~P00100` 两种输入格式。
 */
function expandParamToken(token: string): string[] {
  const normalized = normalizeParamId(token);
  if (!normalized) return [];
  if (!normalized.includes("~")) return [normalized];

  const [startRaw, endRaw] = normalized.split("~").map((item) => item.trim());
  if (!startRaw || !endRaw) return [];
  const rangeRegex = /^([A-Z_]+)(\d+)$/;
  const startMatch = startRaw.match(rangeRegex);
  const endMatch = endRaw.match(rangeRegex);
  if (!startMatch || !endMatch) return [];

  const [, startPrefix, startNumText] = startMatch;
  const [, endPrefix, endNumText] = endMatch;
  if (startPrefix !== endPrefix) return [];

  const startNum = Number(startNumText);
  const endNum = Number(endNumText);
  if (!Number.isFinite(startNum) || !Number.isFinite(endNum) || startNum > endNum) return [];

  const width = Math.max(startNumText.length, endNumText.length);
  const expanded: string[] = [];
  for (let i = startNum; i <= endNum; i += 1) {
    expanded.push(`${startPrefix}${String(i).padStart(width, "0")}`);
  }
  return expanded;
}

/**
 * 从订阅面板输入解析参数列表，自动去重并过滤空项。
 */
function parseParamList(raw: string): string[] {
  const set = new Set<string>();
  raw.split(",").forEach((item) => {
    expandParamToken(item).forEach((param) => set.add(param));
  });
  return Array.from(set);
}

/**
 * 生成订阅参数摘要文本，参数过多时仅展示前几项并附加剩余数量。
 */
function summarizeParams(params: string[], maxVisible = 10): string {
  if (params.length <= maxVisible) {
    return params.join(", ");
  }
  const head = params.slice(0, maxVisible).join(", ");
  const rest = params.length - maxVisible;
  return `${head} ... +${rest}`;
}

/**
 * 解析设备实时 topic，提取设备 ID 作为订阅匹配键。
 */
function extractDeviceIdFromTopic(topic: string): string {
  const parts = topic.split("/");
  if (parts.length >= 3 && parts[0] === "gw") {
    return parts[1];
  }
  return "";
}

/**
 * 解析参数级实时 topic，提取参数 ID（如 `gw/dev001/P00001`）。
 */
function extractParamIdFromTopic(topic: string): string {
  const parts = topic.split("/");
  if (parts.length === 3 && parts[0] === "gw") {
    const maybeParam = normalizeParamId(parts[2]);
    if (maybeParam !== "TELEMETRY") {
      return maybeParam;
    }
  }
  return "";
}

/**
 * 根据设备 ID 与参数 ID 生成参数级实时主题。
 */
function telemetryParamTopicOf(deviceId: string, paramId: string): string {
  return `gw/${deviceId}/${normalizeParamId(paramId)}`;
}

/**
 * 从实时 payload 提取参数值，兼容 points 数组和对象字段两种常见结构。
 */
function pickParamValue(payload: Record<string, unknown>, paramId: string): string {
  const normalized = normalizeParamId(paramId);
  const direct = payload[normalized] ?? payload[paramId] ?? (payload.data as Record<string, unknown> | undefined)?.[normalized];
  if (direct !== undefined) {
    return typeof direct === "string" ? direct : JSON.stringify(direct);
  }
  const points = payload.points;
  if (Array.isArray(points)) {
    for (const item of points) {
      if (!item || typeof item !== "object") continue;
      const point = item as Record<string, unknown>;
      const key = point.point_id ?? point.id ?? point.code ?? point.name;
      const keyText = typeof key === "string" ? normalizeParamId(key) : "";
      if (keyText === normalized) {
        const value = point.value ?? point.val ?? point.v ?? "-";
        return typeof value === "string" ? value : JSON.stringify(value);
      }
    }
  }
  return "-";
}

/**
 * 根据当前订阅规则处理实时消息，并生成参数表格行。
 */
function handleRealtimeTelemetry(topic: string, raw: string): void {
  if (realtimePaused.value) return;
  const bytes = new TextEncoder().encode(raw).byteLength;
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(raw) as Record<string, unknown>;
  } catch {
    return;
  }
  const topicDeviceId = extractDeviceIdFromTopic(topic);
  const payloadDeviceId = typeof parsed.device_id === "string" ? parsed.device_id : "";
  const deviceId = topicDeviceId || payloadDeviceId;
  if (!deviceId) return;
  const matched = paramSubscriptions.value.find((item) => item.deviceId === deviceId);
  if (!matched) return;
  const topicParamId = extractParamIdFromTopic(topic);
  if (topicParamId) {
    if (!matched.params.includes(topicParamId)) return;
    const value = parsed.value;
    const valueText = value === undefined ? "-" : typeof value === "string" ? value : JSON.stringify(value);
    pushRealtimePacketRow(topic, deviceId, topicParamId, bytes, formatPayload(raw));
    pushSubscriptionDataRow(deviceId, {
      [topicParamId]: valueText,
    });
    return;
  }
  pushRealtimePacketRow(topic, deviceId, "-", bytes, formatPayload(raw));
  const values: Record<string, string> = {};
  matched.params.forEach((paramId) => {
    values[paramId] = pickParamValue(parsed, paramId);
  });
  pushSubscriptionDataRow(deviceId, values);
}

/**
 * 路由 MQTT 消息：命令应答显示完整内容，实时消息仅显示摘要。
 */
function routeMessage(topic: string, payload: Buffer): void {
  const raw = payload.toString();
  if (topic.startsWith("gw/resp/")) {
    const msgType = detectMessageType(raw);
    const bytes = payload.byteLength ?? raw.length;
    pushResponseLog(topic, msgType, bytes, formatPayload(raw));
    return;
  }
  handleRealtimeTelemetry(topic, raw);
}

/**
 * 清理并断开当前 MQTT 客户端，重置连接与订阅状态。
 */
function disconnectClient(): void {
  if (client.value) {
    client.value.end(true);
    client.value = null;
  }
  connected.value = false;
  realtimeSubscribed.value = false;
  deviceConnections.value.forEach((item) => {
    item.status = "disconnected";
    item.updatedAt = nowTime();
  });
  pushConnectLog("已断开连接");
}

/**
 * 将设备状态转换为可读中文文案，用于界面展示。
 */
function deviceStatusText(status: DeviceStatus): string {
  if (status === "connected") return "已连接";
  if (status === "connecting") return "连接中";
  if (status === "disconnecting") return "断开中";
  return "未连接";
}

/**
 * 将设备状态映射为标签颜色类型，便于快速识别风险状态。
 */
function deviceStatusType(status: DeviceStatus): "success" | "error" | "warning" | "default" {
  if (status === "connected") return "success";
  if (status === "disconnected") return "error";
  if (status === "connecting" || status === "disconnecting") return "warning";
  return "default";
}

/**
 * 更新指定设备的连接状态，并记录状态更新时间。
 */
function updateDeviceStatus(deviceId: string, status: DeviceStatus): void {
  const device = deviceConnections.value.find((item) => item.id === deviceId);
  if (!device) return;
  device.status = status;
  device.updatedAt = nowTime();
}

/**
 * 添加新的受管设备，便于在面板中进行逐设备连接控制。
 */
function addManagedDevice(): void {
  const id = newDeviceId.value.trim();
  if (!id) {
    pushConnectLog("设备 ID 不能为空", "WARN");
    return;
  }
  if (deviceConnections.value.some((item) => item.id === id)) {
    pushConnectLog(`设备已存在: ${id}`, "WARN");
    return;
  }
  deviceConnections.value.push({
    id,
    simAddr: "127.0.0.1:7101",
    status: "disconnected",
    updatedAt: nowTime(),
  });
  newDeviceId.value = "";
  pushConnectLog(`设备已加入管理列表: ${id}`);
}

/**
 * 从管理列表中移除设备，仅影响前端面板，不会触发后端命令。
 */
function removeManagedDevice(deviceId: string): void {
  const index = deviceConnections.value.findIndex((item) => item.id === deviceId);
  if (index < 0) return;
  deviceConnections.value.splice(index, 1);
  pushConnectLog(`设备已移除: ${deviceId}`);
}

/**
 * 添加设备参数订阅项，支持按设备维护多参数订阅列表。
 */
function addParamSubscription(): void {
  const deviceId = subDeviceId.value.trim();
  if (!deviceId) {
    pushConnectLog("订阅设备 ID 不能为空", "WARN");
    return;
  }
  const params = parseParamList(subParamsInput.value);
  if (params.length === 0) {
    pushConnectLog("至少填写一个参数，例如 P00001", "WARN");
    return;
  }
  const existing = paramSubscriptions.value.find((item) => item.deviceId === deviceId);
  if (existing) {
    existing.params = Array.from(new Set([...existing.params, ...params]));
    pushConnectLog(`订阅参数已更新: ${deviceId} -> ${existing.params.join(", ")}`);
  } else {
    paramSubscriptions.value.push({ deviceId, params });
    pushConnectLog(`订阅项已添加: ${deviceId} -> ${params.join(", ")}`);
  }
}

/**
 * 移除设备参数订阅项，避免无效订阅占用带宽与渲染资源。
 */
function removeParamSubscription(deviceId: string): void {
  if (!deviceId.trim()) {
    pushConnectLog("请先输入要移除的设备 ID", "WARN");
    return;
  }
  const index = paramSubscriptions.value.findIndex((item) => item.deviceId === deviceId);
  if (index < 0) {
    pushConnectLog(`未找到订阅项: ${deviceId}`, "WARN");
    return;
  }
  paramSubscriptions.value.splice(index, 1);
  pushConnectLog(`订阅项已移除: ${deviceId}`);
}

/**
 * 发送设备生命周期控制命令，并同步更新本地设备连接状态。
 */
function publishDeviceLifecycleCommand(deviceId: string, cmd: "CONNECT" | "KICK"): void {
  if (!client.value) {
    pushConnectLog("请先连接 MQTT", "WARN");
    return;
  }
  const device = deviceConnections.value.find((item) => item.id === deviceId);
  if (!device) {
    pushConnectLog(`未找到设备: ${deviceId}`, "WARN");
    return;
  }
  const sim = device.simAddr.trim();
  if (cmd === "CONNECT" && !sim) {
    pushConnectLog(`设备 ${deviceId} 的 sim 地址不能为空`, "WARN");
    return;
  }
  const prevStatus = device.status;
  const pendingStatus: DeviceStatus = cmd === "CONNECT" ? "connecting" : "disconnecting";
  const successStatus: DeviceStatus = cmd === "CONNECT" ? "connected" : "disconnected";
  const topic = `gw/cmd/${clientId.value}`;
  const reqId = `req-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
  const args = cmd === "CONNECT" ? { device_id: deviceId, sim_addr: sim } : { device_id: deviceId };
  const body = JSON.stringify({ req_id: reqId, client_id: clientId.value, cmd, args });

  updateDeviceStatus(deviceId, pendingStatus);
  client.value.publish(topic, body, { qos: 1 }, (err) => {
    if (err) {
      updateDeviceStatus(deviceId, prevStatus);
      pushConnectLog(`设备 ${deviceId} ${cmd} 发送失败: ${err.message}`, "ERROR");
      return;
    }
    updateDeviceStatus(deviceId, successStatus);
    pushConnectLog(`设备 ${deviceId} ${cmd} 已发送: ${topic} req_id=${reqId}`);
  });
}

/**
 * 触发指定设备的连接请求，连接参数来自设备管理列表中的 sim 地址。
 */
function connectManagedDevice(deviceId: string): void {
  publishDeviceLifecycleCommand(deviceId, "CONNECT");
}

/**
 * 触发指定设备的断开请求，使用 KICK 指令实现会话剔除。
 */
function disconnectManagedDevice(deviceId: string): void {
  publishDeviceLifecycleCommand(deviceId, "KICK");
}

/**
 * 建立 MQTT 连接并仅订阅命令应答主题，避免默认接入高频实时流。
 */
function connectOnly(): void {
  if (!wsUrl.value.trim() || !clientId.value.trim()) {
    pushConnectLog("WS 地址、客户端 ID 不能为空", "WARN");
    return;
  }
  disconnectClient();

  const options: IClientOptions = {
    clientId: `web-${clientId.value}-${Math.random().toString(16).slice(2)}`,
    clean: true,
    keepalive: 20,
    reconnectPeriod: 2000,
    connectTimeout: 5000,
    reschedulePings: true,
  };
  const mqttClient = mqtt.connect(wsUrl.value, options);
  client.value = mqttClient;

  mqttClient.on("connect", () => {
    connected.value = true;
    pushConnectLog("MQTT 连接成功");
    const respTopic = `gw/resp/${clientId.value}`;
    mqttClient.subscribe(respTopic, { qos: 1 }, (err) => {
      if (err) {
        pushConnectLog(`应答主题订阅失败: ${err.message}`, "ERROR");
        return;
      }
      pushConnectLog(`应答主题订阅成功: ${respTopic}`);
    });
  });

  mqttClient.on("reconnect", () => pushConnectLog("正在重连...", "WARN"));
  mqttClient.on("offline", () => pushConnectLog("连接离线(offline)", "WARN"));
  mqttClient.on("end", () => pushConnectLog("连接结束(end)", "WARN"));
  mqttClient.on("close", () => {
    connected.value = false;
    pushConnectLog("连接关闭(close)", "WARN");
  });
  mqttClient.on("error", (err) => pushConnectLog(`连接错误: ${err.message}`, "ERROR"));
  mqttClient.on("message", (topic, payload) => routeMessage(topic, payload));
}

/**
 * 按参数级实时主题批量订阅，仅接收已配置参数的数据流。
 */
function subscribeRealtime(): void {
  if (!client.value) {
    pushConnectLog("请先连接 MQTT", "WARN");
    return;
  }
  if (paramSubscriptions.value.length === 0) {
    pushConnectLog("请先添加至少一个设备参数订阅项", "WARN");
    return;
  }
  const topics = Array.from(
    new Set(
      paramSubscriptions.value.flatMap((item) => item.params.map((param) => telemetryParamTopicOf(item.deviceId, param))),
    ),
  );
  client.value.subscribe(topics, { qos: 0 }, (err) => {
    realtimeSubscribed.value = !err;
    if (err) {
      pushConnectLog(`设备参数订阅失败: ${err.message}`, "ERROR");
      return;
    }
    pushConnectLog(`设备参数订阅成功: ${topics.join(", ")}`);
  });
}

/**
 * 取消参数级实时主题订阅，停止参数流接收。
 */
function unsubscribeRealtime(): void {
  if (!client.value) {
    pushConnectLog("请先连接 MQTT", "WARN");
    return;
  }
  if (paramSubscriptions.value.length === 0) {
    pushConnectLog("当前没有可取消的订阅项", "WARN");
    return;
  }
  const topics = Array.from(
    new Set(
      paramSubscriptions.value.flatMap((item) => item.params.map((param) => telemetryParamTopicOf(item.deviceId, param))),
    ),
  );
  client.value.unsubscribe(topics, (err) => {
    if (err) {
      pushConnectLog(`设备参数取消订阅失败: ${err.message}`, "ERROR");
      return;
    }
    realtimeSubscribed.value = false;
    pushConnectLog(`设备参数订阅已取消: ${topics.join(", ")}`);
  });
}

/**
 * 切换实时渲染开关，暂停时保留订阅但不渲染新消息。
 */
function toggleRealtimePause(): void {
  realtimePaused.value = !realtimePaused.value;
  pushConnectLog(realtimePaused.value ? "已暂停实时数据显示" : "已恢复实时数据显示");
}

/**
 * 清空三类日志面板，用于快速重新观察当前问题窗口。
 */
function clearPanels(): void {
  connectLogs.value = [];
  responseLogs.value = [];
  subscriptionDataRows.value = [];
  realtimePacketRows.value = [];
  historyRows.value = [];
  historyTotal.value = 0;
  historyError.value = "";
}

/**
 * 调用 Tauri 后端执行历史参数查询，并刷新表格数据。
 */
async function runHistoryQuery(): Promise<void> {
  const deviceId = historyDeviceId.value.trim();
  const paramId = normalizeParamId(historyParamId.value);
  const fromTs = parseDatetimeLocalToTs(historyFrom.value);
  const toTs = parseDatetimeLocalToTs(historyTo.value);
  if (!deviceId) {
    historyError.value = "设备 ID 不能为空";
    return;
  }
  if (!paramId) {
    historyError.value = "参数 ID 不能为空";
    return;
  }
  if (fromTs === null || toTs === null) {
    historyError.value = "时间格式无效，请重新选择开始和结束时间";
    return;
  }
  if (fromTs > toTs) {
    historyError.value = "开始时间不能大于结束时间";
    return;
  }
  historyLoading.value = true;
  historyError.value = "";
  try {
    const resp = await invoke<HistoryQueryResponse>("query_param_history", {
      req: {
        deviceId,
        paramId,
        fromTs,
        toTs,
        limit: 500,
        offset: 0,
        root: historyRoot.value.trim() || undefined,
      },
    });
    historyTotal.value = resp.total;
    historyRows.value = resp.rows.map((item, idx) => ({
      id: idx + 1,
      ts: item.ts,
      time: formatTsLabel(item.ts),
      value: item.value.toFixed(6),
    }));
    pushConnectLog(`历史查询完成: ${deviceId}/${paramId} 共 ${resp.total} 条, root=${resp.root}`);
  } catch (e) {
    historyRows.value = [];
    historyTotal.value = 0;
    historyError.value = (e as Error).message || String(e);
    pushConnectLog(`历史查询失败: ${historyError.value}`, "ERROR");
  } finally {
    historyLoading.value = false;
  }
}

/**
 * 将当前连接配置持久化到本地存储，便于应用重启后自动恢复。
 */
function persistConnectionConfig(): void {
  if (typeof window === "undefined") {
    return;
  }
  const payload = {
    wsUrl: wsUrl.value,
    clientId: clientId.value,
  };
  window.localStorage.setItem(CONFIG_STORAGE_KEY, JSON.stringify(payload));
}

/**
 * 从本地存储加载连接配置，若配置有效则覆盖默认值。
 */
function loadConnectionConfig(): void {
  if (typeof window === "undefined") {
    return;
  }
  const raw = window.localStorage.getItem(CONFIG_STORAGE_KEY);
  if (!raw) {
    return;
  }
  try {
    const parsed = JSON.parse(raw) as { wsUrl?: string; clientId?: string };
    if (parsed.wsUrl && parsed.wsUrl.trim()) {
      wsUrl.value = parsed.wsUrl.trim();
    }
    if (parsed.clientId && parsed.clientId.trim()) {
      clientId.value = parsed.clientId.trim();
    }
    pushConnectLog("已加载本地连接配置");
  } catch (e) {
    pushConnectLog(`本地配置解析失败: ${(e as Error).message}`, "WARN");
  }
}

/**
 * 打开配置面板并初始化可编辑草稿值。
 */
function openConfigPanel(): void {
  draftWsUrl.value = wsUrl.value;
  draftClientId.value = clientId.value;
  showConfigModal.value = true;
}

/**
 * 保存配置面板中的连接参数并关闭弹窗。
 */
function saveConfigPanel(): void {
  const ws = draftWsUrl.value.trim();
  const cid = draftClientId.value.trim();
  if (!ws || !cid) {
    pushConnectLog("配置保存失败：WS 地址和客户端 ID 不能为空", "WARN");
    return;
  }
  wsUrl.value = ws;
  clientId.value = cid;
  persistConnectionConfig();
  showConfigModal.value = false;
  pushConnectLog("配置已保存（WS 地址、客户端 ID）");
}

/**
 * 组件卸载时释放 MQTT 连接，避免后台悬挂连接影响测试。
 */
function cleanup(): void {
  if (client.value) {
    client.value.end(true);
    client.value = null;
  }
}

const statusType = computed(() => (connected.value ? "success" : "error"));
const statusText = computed(() => (connected.value ? "已连接" : "未连接"));
const pauseText = computed(() => (realtimePaused.value ? "恢复实时显示" : "暂停实时显示"));
const responseCount = computed(() => responseLogs.value.length);
const realtimeCount = computed(() =>
  realtimeViewMode.value === "param" ? subscriptionDataRows.value.length : realtimePacketRows.value.length,
);
const connectCount = computed(() => connectLogs.value.length);
const subscribedParamHeaders = computed(() => {
  const set = new Set<string>();
  paramSubscriptions.value.forEach((item) => {
    item.params.forEach((param) => set.add(param));
  });
  return Array.from(set);
});

const connectColumns: DataTableColumns<ConnectLogRow> = [
  { title: "时间", key: "time", width: 100 },
  { title: "级别", key: "level", width: 90 },
  { title: "消息", key: "message" },
];

const responseColumns: DataTableColumns<ResponseLogRow> = [
  { title: "时间", key: "time", width: 100 },
  { title: "Topic", key: "topic", minWidth: 180 },
  { title: "类型", key: "type", width: 90 },
  { title: "字节", key: "bytes", width: 90 },
  {
    title: "完整内容",
    key: "payload",
    render: (row) => h("div", { class: "payload-preview" }, row.payload),
  },
];

const realtimeColumns = computed<DataTableColumns<SubscriptionDataRow>>(() => [
  { title: "时间", key: "time", width: 100 },
  { title: "设备", key: "deviceId", width: 110 },
  ...subscribedParamHeaders.value.map((param) => ({
    title: param,
    key: `param-${param}`,
    minWidth: 120,
    render: (row: SubscriptionDataRow) => row.values[param] ?? "-",
  })),
]);

const realtimePacketColumns: DataTableColumns<RealtimePacketRow> = [
  { title: "时间", key: "time", width: 100 },
  { title: "设备", key: "deviceId", width: 110 },
  { title: "参数", key: "paramId", width: 110 },
  { title: "Topic", key: "topic", minWidth: 220 },
  { title: "字节", key: "bytes", width: 80 },
  {
    title: "数据包",
    key: "payload",
    render: (row) => h("div", { class: "payload-preview" }, row.payload),
  },
];

const historyColumns: DataTableColumns<HistoryTableRow> = [
  { title: "序号", key: "id", width: 80 },
  { title: "时间", key: "time", minWidth: 180 },
  { title: "时间戳(s)", key: "ts", minWidth: 130 },
  { title: "参数值", key: "value", minWidth: 160 },
];

onBeforeUnmount(cleanup);
onMounted(() => {
  loadConnectionConfig();
  initHistoryRange();
});
</script>

<template>
  <n-config-provider :theme="darkTheme" :theme-overrides="themeOverrides">
    <n-layout class="app-root">
      <n-layout-header bordered class="app-header">
        <div class="header-left">
          <div class="title">GW MQTT 专业控制台</div>
          <n-text depth="3">设备控制、命令应答与实时测点统一工作台</n-text>
        </div>
        <n-space>
          <n-tag :type="statusType" size="medium">{{ statusText }}</n-tag>
          <n-tag type="info" size="medium">应答 {{ responseCount }}</n-tag>
          <n-tag type="warning" size="medium">实时 {{ realtimeCount }}</n-tag>
          <n-button size="small" secondary @click="openConfigPanel">配置</n-button>
        </n-space>
      </n-layout-header>
      <n-layout has-sider class="workspace">
        <n-layout-sider bordered width="400" content-style="padding: 12px;">
          <n-space vertical :size="12">
            <n-card title="连接配置" size="small">
              <n-space vertical>
                <n-button type="primary" block @click="connectOnly">连接</n-button>
                <n-button block type="error" @click="disconnectClient">断开</n-button>
                <n-button block secondary @click="clearPanels">清空日志面板</n-button>
              </n-space>
            </n-card>

            <n-card title="设备连接管理" size="small">
              <n-space vertical :size="10">
                <n-space>
                  <n-input v-model:value="newDeviceId" placeholder="新设备 ID，如 dev004" />
                  <n-button @click="addManagedDevice">添加设备</n-button>
                </n-space>
                <div v-for="device in deviceConnections" :key="device.id" class="device-row">
                  <div class="device-row-head">
                    <n-text strong>{{ device.id }}</n-text>
                    <n-tag size="small" :type="deviceStatusType(device.status)">
                      {{ deviceStatusText(device.status) }}
                    </n-tag>
                  </div>
                  <n-input v-model:value="device.simAddr" size="small" placeholder="sim 地址，如 127.0.0.1:7101" />
                  <div class="device-row-foot">
                    <n-text depth="3">最近更新：{{ device.updatedAt }}</n-text>
                    <n-space :size="8">
                      <n-button size="tiny" type="primary" @click="connectManagedDevice(device.id)">连接</n-button>
                      <n-button size="tiny" type="warning" @click="disconnectManagedDevice(device.id)">断开</n-button>
                      <n-button size="tiny" tertiary @click="removeManagedDevice(device.id)">移除</n-button>
                    </n-space>
                  </div>
                </div>
              </n-space>
            </n-card>

            <n-card title="运行指标" size="small" class="metrics-card">
              <n-grid :cols="2" :x-gap="10" :y-gap="10">
                <n-grid-item>
                  <n-statistic label="连接日志" :value="connectCount" />
                </n-grid-item>
                <n-grid-item>
                  <n-statistic label="命令应答" :value="responseCount" />
                </n-grid-item>
                <n-grid-item>
                  <n-statistic label="实时摘要" :value="realtimeCount" />
                </n-grid-item>
                <n-grid-item>
                  <n-statistic label="实时状态" :value="realtimePaused ? 'Paused' : 'Running'" />
                </n-grid-item>
              </n-grid>
            </n-card>
          </n-space>
        </n-layout-sider>

        <n-layout-content class="app-content">
          <div class="right-stack">
            <n-card title="订阅功能" size="small" class="top-panels blend-card">
              <n-form label-placement="top" :show-feedback="false" class="sub-form">
                <n-grid :cols="3" :x-gap="8">
                  <n-grid-item>
                    <n-form-item label="设备 ID">
                      <n-input v-model:value="subDeviceId" placeholder="dev001" />
                    </n-form-item>
                  </n-grid-item>
                  <n-grid-item :span="2">
                    <n-form-item label="参数列表（逗号分隔）">
                      <n-input v-model:value="subParamsInput" placeholder="P00001,P00002" />
                    </n-form-item>
                  </n-grid-item>
                </n-grid>
              </n-form>
              <n-space vertical>
                <div class="sub-actions">
                  <n-space wrap>
                    <n-button type="primary" strong @click="addParamSubscription">添加订阅项</n-button>
                    <n-button type="error" strong @click="removeParamSubscription(subDeviceId)">移除订阅项</n-button>
                    <n-button type="info" strong @click="subscribeRealtime">订阅实时</n-button>
                    <n-button type="warning" strong @click="unsubscribeRealtime">取消订阅</n-button>
                    <n-button :type="realtimePaused ? 'success' : 'info'" strong @click="toggleRealtimePause">
                      {{ pauseText }}
                    </n-button>
                  </n-space>
                </div>
                <div v-for="item in paramSubscriptions" :key="item.deviceId" class="sub-item">
                  <div class="sub-item-head">
                    <n-text strong>{{ item.deviceId }}</n-text>
                    <n-tag size="small" type="info">参数 {{ item.params.length }}</n-tag>
                  </div>
                  <n-text depth="3" class="sub-item-preview">{{ summarizeParams(item.params) }}</n-text>
                </div>
              </n-space>
            </n-card>

            <n-card title="日志中心" size="small" class="logs-card">
              <n-space vertical :size="12" style="margin-bottom: 12px">
                <n-card title="参数历史查询（直连 tsdata）" size="small" class="blend-card">
                  <n-form label-placement="top" :show-feedback="false">
                    <n-grid :cols="4" :x-gap="8" :y-gap="8">
                      <n-grid-item>
                        <n-form-item label="设备 ID">
                          <n-input v-model:value="historyDeviceId" placeholder="dev001" />
                        </n-form-item>
                      </n-grid-item>
                      <n-grid-item>
                        <n-form-item label="参数 ID">
                          <n-input v-model:value="historyParamId" placeholder="P00001" />
                        </n-form-item>
                      </n-grid-item>
                      <n-grid-item>
                        <n-form-item label="开始时间">
                          <input v-model="historyFrom" type="datetime-local" class="native-datetime-input" />
                        </n-form-item>
                      </n-grid-item>
                      <n-grid-item>
                        <n-form-item label="结束时间">
                          <input v-model="historyTo" type="datetime-local" class="native-datetime-input" />
                        </n-form-item>
                      </n-grid-item>
                    </n-grid>
                    <n-form-item label="tsdata 根目录（可选）">
                      <n-input v-model:value="historyRoot" placeholder="例如 D:\\Develop\\Rust\\gateway\\tsdata（不填则自动探测）" />
                    </n-form-item>
                  </n-form>
                  <n-space vertical :size="8">
                    <n-space>
                      <n-button type="primary" :loading="historyLoading" @click="runHistoryQuery">查询</n-button>
                      <n-tag type="info" size="small">结果 {{ historyRows.length }} / 总量 {{ historyTotal }}</n-tag>
                    </n-space>
                    <n-text v-if="historyError" type="error">{{ historyError }}</n-text>
                    <n-data-table
                      :columns="historyColumns"
                      :data="historyRows"
                      size="small"
                      :bordered="false"
                      max-height="280"
                      striped
                    />
                  </n-space>
                </n-card>
              </n-space>
              <n-tabs v-model:value="activeLogTab" type="line" animated>
                <n-tab-pane name="connect" tab="连接日志">
                  <n-data-table
                    :columns="connectColumns"
                    :data="connectLogs"
                    size="small"
                    :bordered="false"
                    max-height="calc(100vh - 320px)"
                    striped
                  />
                </n-tab-pane>
                <n-tab-pane name="response" tab="命令应答（完整）">
                  <n-data-table
                    :columns="responseColumns"
                    :data="responseLogs"
                    size="small"
                    :bordered="false"
                    max-height="calc(100vh - 320px)"
                    striped
                  />
                </n-tab-pane>
                <n-tab-pane name="realtime" tab="实时数据（参数）">
                  <n-space justify="end" style="margin-bottom: 8px">
                    <n-button
                      size="small"
                      :type="realtimeViewMode === 'param' ? 'primary' : 'default'"
                      @click="realtimeViewMode = 'param'"
                    >
                      参数视图
                    </n-button>
                    <n-button
                      size="small"
                      :type="realtimeViewMode === 'packet' ? 'primary' : 'default'"
                      @click="realtimeViewMode = 'packet'"
                    >
                      数据包视图
                    </n-button>
                  </n-space>
                  <n-data-table
                    v-if="realtimeViewMode === 'param'"
                    :columns="realtimeColumns"
                    :data="subscriptionDataRows"
                    size="small"
                    :bordered="false"
                    max-height="calc(100vh - 320px)"
                    striped
                  />
                  <n-data-table
                    v-else
                    :columns="realtimePacketColumns"
                    :data="realtimePacketRows"
                    size="small"
                    :bordered="false"
                    max-height="calc(100vh - 320px)"
                    striped
                  />
                </n-tab-pane>
              </n-tabs>
            </n-card>
          </div>
        </n-layout-content>
      </n-layout>
    </n-layout>

    <n-modal v-model:show="showConfigModal" preset="card" title="连接配置中心" style="width: 560px">
      <n-space vertical :size="12">
        <n-text depth="3">在这里维护连接级配置。保存后，建议重新连接使配置立即生效。</n-text>
        <n-form label-placement="top" :show-feedback="false">
          <n-form-item label="WS 地址">
            <n-input v-model:value="draftWsUrl" placeholder="ws://127.0.0.1:8080/" />
          </n-form-item>
          <n-form-item label="客户端 ID">
            <n-input v-model:value="draftClientId" placeholder="desktop-console-1" />
          </n-form-item>
        </n-form>
        <n-space justify="end">
          <n-button @click="showConfigModal = false">取消</n-button>
          <n-button type="primary" @click="saveConfigPanel">保存配置</n-button>
        </n-space>
      </n-space>
    </n-modal>
  </n-config-provider>
</template>

<style scoped>
.app-root {
  height: 100vh;
  display: flex;
  flex-direction: column;
  font-size: 14px;
}

.app-header {
  padding: 12px 16px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.header-left {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.workspace {
  flex: 1 1 auto;
  min-height: 0;
}

.app-content {
  padding: 12px;
  height: 100%;
  min-height: 0;
}

.right-stack {
  height: 100%;
  display: flex;
  flex-direction: column;
  gap: 12px;
  min-height: 0;
}

.top-panels {
  flex: 0 0 auto;
}

.blend-card {
  background: transparent;
}

.blend-card :deep(.n-card__content),
.blend-card :deep(.n-card-header) {
  background: transparent;
}

.sub-form {
  margin-bottom: 4px;
}

.sub-item {
  border: 1px solid #2d3f61;
  border-radius: 6px;
  padding: 6px 8px;
  background: transparent;
}

.sub-item-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.sub-item-preview {
  display: block;
  margin-top: 4px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.sub-actions {
  border: 1px solid #2d3f61;
  border-radius: 6px;
  padding: 8px;
  background: rgba(17, 25, 40, 0.45);
}

.logs-card {
  flex: 1 1 auto;
  min-height: 0;
}

.title {
  font-size: 14px;
  font-weight: 600;
}

.log-panel {
  height: 320px;
  margin: 0;
  overflow: auto;
  border: 1px solid #2d3f61;
  border-radius: 6px;
  background: #0f1729;
  padding: 10px;
  font-size: 14px;
  line-height: 1.5;
  white-space: pre-wrap;
  word-break: break-word;
}

.metrics-card :deep(.n-statistic .n-statistic__label) {
  font-size: 12px;
}

.metrics-card :deep(.n-statistic .n-statistic-value .n-statistic-value__content) {
  font-size: 18px;
  font-weight: 600;
}

.device-row {
  border: 1px solid #2d3f61;
  border-radius: 6px;
  padding: 8px;
  background: transparent;
}

.device-row-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 8px;
}

.device-row-foot {
  margin-top: 8px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.native-datetime-input {
  width: 100%;
  height: 34px;
  padding: 0 12px;
  border: 1px solid #41506b;
  border-radius: 3px;
  background: #1f2537;
  color: #e6eaf2;
  box-sizing: border-box;
}
</style>
