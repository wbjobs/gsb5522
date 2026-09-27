/**
 * Node 并发测试：用「带随机异步抖动的内存存储」模拟多标签页竞争，
 * 验证 queue-core 在 Web Locks 语义（互斥锁）下的关键约束。
 * 运行：node test/queue-core.test.mjs
 */
import assert from 'node:assert/strict';
import { createQueueCore, TASK_STATUS } from '../js/queue-core.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = () => sleep(Math.random() * 3);

/** 故意不做任何原子性保证的内存存储：没有锁保护时并发出队必然重复 */
function createMemoryStore() {
  const tasks = new Map();
  const meta = new Map();
  const stats = new Map();
  const history = [];
  let hid = 0;
  return {
    async getMeta(key) { await jitter(); return meta.get(key); },
    async setMeta(key, value) { await jitter(); meta.set(key, value); },
    async putTask(task) { await jitter(); tasks.set(task.id, { ...task }); },
    async getTask(id) { await jitter(); return tasks.get(id) ? { ...tasks.get(id) } : undefined; },
    async nextPending() {
      await jitter();
      const pending = [...tasks.values()].filter((t) => t.status === TASK_STATUS.PENDING);
      pending.sort((a, b) => a.seq - b.seq);
      return pending.length ? { ...pending[0] } : null;
    },
    async tasksByStatus(status) {
      await jitter();
      return [...tasks.values()].filter((t) => t.status === status).map((t) => ({ ...t }));
    },
    async clearTasks() {
      await jitter();
      let removed = 0;
      for (const [id, t] of tasks) {
        if (t.status !== TASK_STATUS.DONE) { tasks.delete(id); removed += 1; }
      }
      return removed;
    },
    async addHistory(entry) { await jitter(); history.push({ hid: ++hid, ...entry }); },
    async recentHistory(limit) { await jitter(); return history.slice(-limit).reverse().map((h) => ({ ...h })); },
    async pruneHistory(keep) { if (history.length > keep) history.splice(0, history.length - keep); },
    async getStat(tabId) { await jitter(); return stats.get(tabId) ? { ...stats.get(tabId) } : undefined; },
    async putStat(stat) { await jitter(); stats.set(stat.tabId, { ...stat }); },
    async allStats() { await jitter(); return [...stats.values()].map((s) => ({ ...s })); },
    _dump: { tasks, meta, stats, history },
  };
}

/** 与 Web Locks 同语义的互斥锁 */
function createMutexLock() {
  let tail = Promise.resolve();
  return {
    runExclusive(fn) {
      const run = tail.then(fn, fn);
      tail = run.catch(() => {});
      return run;
    },
  };
}

/** 无锁（错误用法），用于证明测试环境确实能暴露并发问题 */
const noLock = { runExclusive: (fn) => fn() };

let now = 1_000_000;
const advance = (ms) => { now += ms; };

function makeCore(store, lock, staleMs = 1000) {
  return createQueueCore({ store, lock, now: () => now, staleMs });
}

const results = [];
async function test(name, fn) {
  try {
    await fn();
    results.push(['PASS', name]);
  } catch (err) {
    results.push(['FAIL', `${name}\n    ${err.message}`]);
  }
}

// 1. 4 个标签页并发出队：每个任务恰好被一个标签页拿到，无重复、无丢失
await test('4 标签页并发出队不重复消费', async () => {
  const store = createMemoryStore();
  const core = makeCore(store, createMutexLock());
  const N = 120;
  for (let i = 0; i < N; i += 1) await core.enqueue({ i }, 'seed');

  const claimed = new Map(); // taskId -> tabId
  await Promise.all(
    ['tab-1', 'tab-2', 'tab-3', 'tab-4'].map(async (tabId) => {
      for (;;) {
        const task = await core.dequeue(tabId);
        if (!task) break;
        assert(!claimed.has(task.id), `任务 ${task.id} 被 ${tabId} 与 ${claimed.get(task.id)} 重复消费`);
        claimed.set(task.id, tabId);
        await jitter(); // 模拟消费耗时
        await core.complete(task.id, tabId);
      }
    }),
  );
  assert.equal(claimed.size, N, `应消费 ${N} 个，实际 ${claimed.size}`);
  const state = await core.getState();
  assert.equal(state.counts.pending, 0);
  assert.equal(state.counts.done, N);
});

// 1b. 对照实验：无锁时同样的竞争应出现重复（证明测试环境有效）
await test('对照：无锁并发出队会重复（验证测试有效性）', async () => {
  const store = createMemoryStore();
  const core = makeCore(store, noLock);
  for (let i = 0; i < 60; i += 1) await core.enqueue({ i }, 'seed');
  const claimed = new Set();
  let duplicates = 0;
  await Promise.all(
    ['a', 'b', 'c', 'd'].map(async (tabId) => {
      for (let k = 0; k < 30; k += 1) {
        const task = await core.dequeue(tabId);
        if (!task) break;
        if (claimed.has(task.id)) duplicates += 1;
        claimed.add(task.id);
      }
    }),
  );
  assert(duplicates > 0, '无锁环境未出现重复，测试环境无法暴露并发缺陷');
});

// 2. 消费失败重新入队：任务回到 pending、retries 递增、最终可被消费完成
await test('消费失败重新入队', async () => {
  const store = createMemoryStore();
  const core = makeCore(store, createMutexLock());
  await core.enqueue({ x: 1 }, 'tab-1');
  const t1 = await core.dequeue('tab-1');
  assert.equal(t1.status, TASK_STATUS.PROCESSING);
  await core.fail(t1.id, 'tab-1');
  let state = await core.getState();
  assert.equal(state.counts.pending, 1, '失败后应重新入队');
  const t2 = await core.dequeue('tab-2');
  assert.equal(t2.id, t1.id, '应取回同一个任务');
  assert.equal(t2.retries, 1, '重试次数应递增');
  assert.equal(t2.seq, t1.seq, '重新入队应保持原 FIFO 序号');
  await core.complete(t2.id, 'tab-2');
  state = await core.getState();
  assert.equal(state.counts.done, 1);
});

// 3. 空队列出队返回 null，不崩
await test('空队列出队安全返回 null', async () => {
  const core = makeCore(createMemoryStore(), createMutexLock());
  assert.equal(await core.dequeue('tab-1'), null);
  assert.equal(await core.dequeue('tab-2'), null);
});

// 4. 暂停 / 继续
await test('暂停阻止出队，继续后恢复', async () => {
  const core = makeCore(createMemoryStore(), createMutexLock());
  await core.enqueue({ a: 1 }, 'tab-1');
  await core.pause('tab-1');
  assert.equal(await core.dequeue('tab-2'), null, '暂停时不应出队');
  await core.resume('tab-1');
  const task = await core.dequeue('tab-2');
  assert(task, '继续后应能出队');
});

// 5. 清空队列：pending/processing 被清除，done 与历史保留
await test('清空队列', async () => {
  const store = createMemoryStore();
  const core = makeCore(store, createMutexLock());
  await core.enqueue({ a: 1 }, 'tab-1');
  await core.enqueue({ a: 2 }, 'tab-1');
  const t = await core.dequeue('tab-1');
  await core.complete(t.id, 'tab-1');
  await core.enqueue({ a: 3 }, 'tab-1');
  const removed = await core.clear('tab-1');
  assert.equal(removed, 2);
  const state = await core.getState();
  assert.equal(state.counts.pending, 0);
  assert.equal(state.counts.done, 1, '已完成任务应保留');
  assert.equal(await core.dequeue('tab-1'), null);
});

// 6. 标签页崩溃（失联）：其 processing 任务被回收重新入队，不丢任务
await test('标签页关闭后 processing 任务被回收', async () => {
  const store = createMemoryStore();
  const core = makeCore(store, createMutexLock(), 1000);
  await core.enqueue({ a: 1 }, 'tab-1');
  await core.heartbeat('tab-1');
  const t = await core.dequeue('tab-1'); // tab-1 拿到任务后「崩溃」，不再心跳
  advance(5000); // 心跳超时
  await core.heartbeat('tab-2');
  const t2 = await core.dequeue('tab-2'); // tab-2 出队时触发回收
  assert(t2, '任务应被回收并可再次出队');
  assert.equal(t2.id, t.id, '应是同一个任务，任务不丢');
});

// 7. 持久化恢复：新核心实例挂在同一存储上（模拟关标签页/离线后重开）
await test('关闭重开后队列状态一致（离线入队恢复）', async () => {
  const store = createMemoryStore();
  const core1 = makeCore(store, createMutexLock());
  await core1.enqueue({ a: 1 }, 'tab-1');
  await core1.enqueue({ a: 2 }, 'tab-1');
  const t = await core1.dequeue('tab-1');
  await core1.complete(t.id, 'tab-1');
  // 「离线期间」继续入队（直接写存储层，无网络依赖）
  await core1.enqueue({ a: 3 }, 'tab-1');

  const core2 = makeCore(store, createMutexLock()); // 模拟刷新/重开标签页
  const state = await core2.getState();
  assert.equal(state.counts.pending, 2, '重开后待消费数应一致');
  assert.equal(state.counts.done, 1, '重开后已完成数应一致');
  const t2 = await core2.dequeue('tab-9');
  assert.equal(t2.seq, 2, 'FIFO 顺序应保持');
});

// 8. 并发入队 + 出队混合压力：seq 唯一且连续，无并发重复消费。
//    失败重排的任务会被再次出队（合法），因此跟踪「在途」集合：
//    同一任务绝不允许同时被两个消费者持有。
await test('并发入队出队混合压力测试', async () => {
  const store = createMemoryStore();
  const core = makeCore(store, createMutexLock());
  const PRODUCERS = 3;
  const PER_PRODUCER = 40;
  const inflight = new Set();
  const everClaimed = new Set();

  const claim = (task, who) => {
    assert(!inflight.has(task.id), `任务 ${task.id} 被并发重复出队（${who}）`);
    inflight.add(task.id);
    everClaimed.add(task.id);
  };

  const producers = Array.from({ length: PRODUCERS }, (_, p) =>
    (async () => {
      for (let i = 0; i < PER_PRODUCER; i += 1) {
        await core.enqueue({ p, i }, `prod-${p}`);
      }
    })(),
  );
  const consumers = Array.from({ length: 4 }, (_, c) =>
    (async () => {
      let idle = 0;
      while (idle < 5) {
        const task = await core.dequeue(`cons-${c}`);
        if (!task) { idle += 1; await sleep(2); continue; }
        idle = 0;
        claim(task, `cons-${c}`);
        if (Math.random() < 0.2) await core.fail(task.id, `cons-${c}`);
        else await core.complete(task.id, `cons-${c}`);
        inflight.delete(task.id);
      }
    })(),
  );
  await Promise.all([...producers, ...consumers]);

  // 收尾：把失败重排的剩余任务消费完
  for (;;) {
    const task = await core.dequeue('drain');
    if (!task) break;
    claim(task, 'drain');
    await core.complete(task.id, 'drain');
    inflight.delete(task.id);
  }
  const total = PRODUCERS * PER_PRODUCER;
  assert.equal(inflight.size, 0, '不应有滞留在途任务');
  assert.equal(everClaimed.size, total, `应消费 ${total} 个不同任务，实际 ${everClaimed.size}`);
  const seqs = [...store._dump.tasks.values()].map((t) => t.seq).sort((a, b) => a - b);
  assert.deepEqual(seqs, Array.from({ length: total }, (_, i) => i + 1), 'seq 应唯一且连续');
});

for (const [status, name] of results) {
  console.log(`${status === 'PASS' ? '✓' : '✗'} ${name}`);
}
const failed = results.filter(([s]) => s === 'FAIL').length;
console.log(`\n${results.length - failed}/${results.length} 通过`);
process.exit(failed ? 1 : 0);
