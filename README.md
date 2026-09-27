# 多标签页共享任务队列

纯原生 Web 技术实现（无框架）：多个浏览器标签页共享同一个任务队列，协作消费，任务绝不重复、不丢失。

## 运行

```bash
# 需要通过 http(s) 访问（Web Locks / IndexedDB 在 file:// 下受限）
python3 -m http.server 8000
# 打开 4 个标签页访问 http://localhost:8000
```

## 使用

- **入队 1 个 / 10 个**：向共享队列追加任务（离线也能入队，直接写本地 IndexedDB）。
- **出队并消费 1 个**：手动原子出队并模拟消费一次。
- **自动消费**：每个标签页可独立开启，持续出队消费。
- **模拟失败率**：消费时按概率失败，失败任务自动重新入队（保留原 FIFO 序号，重试次数 +1）。
- **暂停 / 继续**：全局暂停出队（入队不受影响），跨标签页生效。
- **清空队列**：清除待消费与消费中的任务，保留历史与统计。

界面实时展示：队列深度折线图（Canvas）、各标签页消费数柱状图（Canvas）、标签页统计表、队首预览、消费历史。

## 架构

```
js/queue-core.js   队列核心逻辑（与存储/锁解耦，可在 Node 中测试）
js/idb-store.js    IndexedDB 存储适配器 + Web Locks 适配器
js/app.js          UI、BroadcastChannel 同步、心跳、消费循环、Canvas 可视化
test/queue-core.test.mjs   Node 并发测试（node test/queue-core.test.mjs）
```

## 关键约束的实现方式

| 约束 | 实现 |
| --- | --- |
| 同时出队不重复 | 所有「取队首 + 标记 processing」在 Web Locks 同名排他锁内完成，跨标签页互斥 |
| 标签页关闭队列不丢 | IndexedDB 是唯一事实源；失联标签页的 processing 任务由「心跳超时 + 任务滞留超时」双条件回收重新入队 |
| 离线入队恢复正确 | 入队只写本地 IndexedDB，不依赖网络；恢复后状态自然一致 |
| 消息乱序不丢任务 | BroadcastChannel 只发送「有变化」提示，不承载数据；接收方收到后从 IndexedDB 全量重读，另有 1s 定时刷新兜底 |
| 消费失败重排 | `fail()` 在锁内把任务改回 pending，保留原 seq（FIFO 不变），retries +1 |
| 队列为空不崩 | 出队返回 `null`，UI 提示「队列为空或已暂停」 |

## 测试

```bash
node test/queue-core.test.mjs
```

9 项测试覆盖：4 标签页并发出队无重复（含无锁对照实验）、失败重排、空队列、暂停/继续、清空、失联回收、持久化恢复、并发入队出队混合压力（seq 唯一连续、无并发重复持有）。
