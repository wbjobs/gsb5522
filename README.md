# 多标签页共享任务队列

纯原生 Web 技术实现(BroadcastChannel + IndexedDB + Web Locks API + DOM + Canvas),无任何框架。

## 运行

需要通过 HTTP 访问(file:// 下 BroadcastChannel/IndexedDB 行为不可靠):

```bash
python3 -m http.server 8000
# 打开 http://localhost:8000 ,并复制 4 个标签页
```

要求支持 Web Locks API 的现代浏览器(Chrome / Edge / Firefox / Safari 15.2+)。

## 架构

- `js/db.js` — IndexedDB 持久层,四个 store:`tasks`(自增 id 保证 FIFO)、`history`、`tabs`(标签页注册表 + 心跳)、`meta`(暂停标志)。
- `js/queue.js` — 核心逻辑。所有写操作经 Web Locks 串行化;BroadcastChannel 只作为"状态已变"的提示,真实数据始终以 IndexedDB 为准。
- `js/viz.js` — Canvas 可视化:待消费队列、消费中任务、各标签页消费数柱状图。
- `js/app.js` — UI 绑定、自动消费循环、模拟消费(可调失败率)。

## 关键约束的实现方式

| 约束 | 实现 |
| --- | --- |
| 同时出队不重复消费 | `navigator.locks.request('stq:dequeue')` 内完成"取第一个 pending + 置为 processing"的读改写,跨标签页互斥 |
| 标签页关闭后队列不丢 | 任务全部持久化在 IndexedDB;心跳(2s)标记活跃标签页,回收器(3s)把离线标签页的 processing 任务重新入队 |
| 离线入队后恢复正确 | 入队直接写 IndexedDB(离线可用);`online` 事件触发重新同步,BroadcastChannel 发送失败被静默忽略 |
| 消息乱序不丢任务 | 广播消息不携带关键状态,任何消息到达都只是触发"从 DB 重新读全量状态" |
| 消费失败重新入队 | `fail()` 将任务置回 pending 并累加 attempts,历史记录可见 |
| 队列为空不能崩 | 出队返回 `{kind:'empty'}``,UI 提示"队列为空",自动消费退避等待 |

## 验收对照

- **4 个标签页同时出队不重复**:开 4 个标签页 → 批量入队 → 全部开启自动消费 → 观察"消费历史"中每个任务 id 只被 dequeue 一次,各标签页消费数之和 = 完成数。
- **关闭标签页不丢**:消费中直接关闭某标签页,约 7 秒内其 processing 任务被其他标签页回收重入队(历史中出现"回收重入队")。
- **离线入队恢复**:DevTools → Network → Offline,入队若干任务,恢复在线后所有标签页状态一致。
- **刷新一致**:刷新任意标签页,队列、统计、历史完全保留。
