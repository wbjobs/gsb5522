/**
 * 队列核心逻辑：与存储 / 锁实现解耦。
 * 浏览器端使用 IndexedDB + Web Locks，测试端使用内存实现。
 *
 * 存储适配器接口（全部返回 Promise）：
 *   getMeta(key) / setMeta(key, value)
 *   putTask(task) / getTask(id) / nextPending() / tasksByStatus(status) / clearTasks()
 *   addHistory(entry) / recentHistory(limit)
 *   getStat(tabId) / putStat(stat) / allStats()
 *
 * 锁适配器接口：
 *   runExclusive(fn)  —— 同一时刻只有一个标签页执行 fn（对应 Web Locks 同名锁）
 */

export const TASK_STATUS = {
  PENDING: 'pending',
  PROCESSING: 'processing',
  DONE: 'done',
};

const HISTORY_LIMIT = 200;

export function createQueueCore(options) {
  const {
    store,
    lock,
    now = () => Date.now(),
    genId = defaultGenId,
    staleMs = 15000,
  } = options;

  async function mutate(fn) {
    return lock.runExclusive(fn);
  }

  async function bumpSeq() {
    const seq = (await store.getMeta('seq')) || 0;
    const next = seq + 1;
    await store.setMeta('seq', next);
    return next;
  }

  async function log(action, taskId, tabId, extra) {
    await store.addHistory({
      action,
      taskId: taskId || null,
      tabId: tabId || null,
      ts: now(),
      extra: extra || null,
    });
    await store.pruneHistory(HISTORY_LIMIT);
  }

  async function touchStat(tabId, patch) {
    const stat = (await store.getStat(tabId)) || {
      tabId,
      consumed: 0,
      failed: 0,
      lastSeen: 0,
    };
    Object.assign(stat, patch);
    await store.putStat(stat);
    return stat;
  }

  /** 入队（离线也可用：直接写本地 IndexedDB，恢复后自然一致） */
  async function enqueue(payload, tabId) {
    return mutate(async () => {
      const seq = await bumpSeq();
      const task = {
        id: genId(),
        seq,
        payload,
        status: TASK_STATUS.PENDING,
        retries: 0,
        tabId: null,
        createdAt: now(),
        updatedAt: now(),
      };
      await store.putTask(task);
      await log('enqueue', task.id, tabId, { seq });
      return task;
    });
  }

  /**
   * 原子出队：整个「取队首 + 标记 processing」在排他锁内完成，
   * 多标签页同时调用也不会拿到同一个任务。队列为空返回 null。
   * 出队前先把「持有标签页已失联」的 processing 任务回收重新入队。
   */
  async function dequeue(tabId) {
    return mutate(async () => {
      if (await store.getMeta('paused')) return null;

      await recoverStaleLocked();

      const task = await store.nextPending();
      if (!task) return null;

      task.status = TASK_STATUS.PROCESSING;
      task.tabId = tabId;
      task.updatedAt = now();
      await store.putTask(task);
      await log('dequeue', task.id, tabId, { seq: task.seq });
      return task;
    });
  }

  /**
   * 锁内回收：owner 标签页失联「且」任务本身已滞留超时的 processing 任务
   * 重新入队。两个条件缺一不可——只凭心跳缺失会误回收其他标签页
   * 刚刚拿到、正在消费的任务，造成重复消费。
   */
  async function recoverStaleLocked() {
    const processing = await store.tasksByStatus(TASK_STATUS.PROCESSING);
    if (!processing.length) return 0;
    const stats = await store.allStats();
    const alive = new Map(stats.map((s) => [s.tabId, s.lastSeen]));
    const cutoff = now() - staleMs;
    let recovered = 0;
    for (const task of processing) {
      const lastSeen = alive.get(task.tabId);
      const ownerGone = lastSeen === undefined || lastSeen < cutoff;
      const tooOld = task.updatedAt < cutoff;
      if (ownerGone && tooOld) {
        const previousOwner = task.tabId;
        task.status = TASK_STATUS.PENDING;
        task.tabId = null;
        task.updatedAt = now();
        await store.putTask(task);
        await log('requeue-stale', task.id, previousOwner, { seq: task.seq });
        recovered += 1;
      }
    }
    return recovered;
  }

  /** 消费成功 */
  async function complete(taskId, tabId) {
    return mutate(async () => {
      const task = await store.getTask(taskId);
      if (!task || task.status !== TASK_STATUS.PROCESSING) return false;
      task.status = TASK_STATUS.DONE;
      task.updatedAt = now();
      await store.putTask(task);
      await touchStat(tabId, {
        consumed: ((await store.getStat(tabId)) || { consumed: 0 }).consumed + 1,
        lastSeen: now(),
      });
      await log('done', task.id, tabId, { seq: task.seq });
      return true;
    });
  }

  /** 消费失败：重新入队（保留原 seq，FIFO 顺序不变），retries + 1 */
  async function fail(taskId, tabId) {
    return mutate(async () => {
      const task = await store.getTask(taskId);
      if (!task || task.status !== TASK_STATUS.PROCESSING) return false;
      task.status = TASK_STATUS.PENDING;
      task.tabId = null;
      task.retries += 1;
      task.updatedAt = now();
      await store.putTask(task);
      await touchStat(tabId, {
        failed: ((await store.getStat(tabId)) || { failed: 0 }).failed + 1,
        lastSeen: now(),
      });
      await log('fail-requeue', task.id, tabId, { seq: task.seq, retries: task.retries });
      return true;
    });
  }

  async function pause(tabId) {
    return mutate(async () => {
      await store.setMeta('paused', true);
      await log('pause', null, tabId);
    });
  }

  async function resume(tabId) {
    return mutate(async () => {
      await store.setMeta('paused', false);
      await log('resume', null, tabId);
    });
  }

  /** 清空队列（pending + processing），保留历史与统计 */
  async function clear(tabId) {
    return mutate(async () => {
      const removed = await store.clearTasks();
      await log('clear', null, tabId, { removed });
      return removed;
    });
  }

  /** 心跳：上报标签页存活，用于失联回收与活跃标签页展示 */
  async function heartbeat(tabId) {
    const stat = (await store.getStat(tabId)) || { tabId, consumed: 0, failed: 0 };
    stat.lastSeen = now();
    await store.putStat(stat);
  }

  /** 聚合状态：供 UI 渲染与跨标签页同步 */
  async function getState() {
    const [paused, pending, processing, done, history, stats] = await Promise.all([
      store.getMeta('paused'),
      store.tasksByStatus(TASK_STATUS.PENDING),
      store.tasksByStatus(TASK_STATUS.PROCESSING),
      store.tasksByStatus(TASK_STATUS.DONE),
      store.recentHistory(50),
      store.allStats(),
    ]);
    const cutoff = now() - staleMs;
    return {
      paused: !!paused,
      counts: {
        pending: pending.length,
        processing: processing.length,
        done: done.length,
      },
      pendingPreview: pending.slice(0, 8).map((t) => ({
        id: t.id,
        seq: t.seq,
        retries: t.retries,
        payload: t.payload,
      })),
      history,
      stats: stats.map((s) => ({ ...s, active: s.lastSeen >= cutoff })),
    };
  }

  return {
    enqueue,
    dequeue,
    complete,
    fail,
    pause,
    resume,
    clear,
    heartbeat,
    getState,
  };
}

let idCounter = 0;
function defaultGenId() {
  idCounter += 1;
  return `t-${Date.now().toString(36)}-${idCounter}-${Math.floor(Math.random() * 1e6).toString(36)}`;
}
