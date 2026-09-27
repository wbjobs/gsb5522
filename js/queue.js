import { openDB, reqToPromise, txDone } from './db.js';

const LOCK_DEQUEUE = 'stq:dequeue';
const LOCK_MUTATE = 'stq:mutate';
const CHANNEL_NAME = 'stq:bus';
const HEARTBEAT_MS = 2000;
const TAB_STALE_MS = 7000;
const RECLAIM_INTERVAL_MS = 3000;
const HISTORY_LIMIT = 200;

export class SharedQueue {
  constructor() {
    if (!('locks' in navigator)) {
      throw new Error('当前浏览器不支持 Web Locks API，无法保证跨标签页出队原子性');
    }
    this.tabId = (crypto.randomUUID ? crypto.randomUUID() : String(Date.now()) + Math.random()).slice(0, 8);
    this.channel = new BroadcastChannel(CHANNEL_NAME);
    this.db = null;
    this._listeners = new Set();
    this._timers = [];
    this._refreshScheduled = false;
  }

  async init() {
    this.db = await openDB();
    this.channel.onmessage = (event) => this._onMessage(event.data);
    await this._heartbeat();
    this._timers.push(setInterval(() => this._heartbeat(), HEARTBEAT_MS));
    this._timers.push(setInterval(() => this.reclaimStale(), RECLAIM_INTERVAL_MS));
    window.addEventListener('beforeunload', () => this._markInactive());
    this._broadcast('hello', {});
    await this._emit();
  }

  onChange(fn) {
    this._listeners.add(fn);
  }

  _onMessage(msg) {
    if (!msg || msg.tabId === this.tabId) return;
    // 消息只作为"状态已变化"的提示,真实数据始终以 IndexedDB 为准,
    // 因此消息乱序/丢失不会丢任务,收到任何消息都重新从 DB 读取。
    this._scheduleEmit();
  }

  _broadcast(type, data) {
    try {
      this.channel.postMessage({ type, tabId: this.tabId, at: Date.now(), data });
    } catch (_) { /* 离线或通道关闭时忽略,DB 仍是真相来源 */ }
  }

  _scheduleEmit() {
    if (this._refreshScheduled) return;
    this._refreshScheduled = true;
    setTimeout(() => {
      this._refreshScheduled = false;
      this._emit();
    }, 30);
  }

  async _emit() {
    const state = await this.getState();
    for (const fn of this._listeners) fn(state);
  }

  // ---------- 标签页注册表 ----------

  async _heartbeat() {
    const tx = this.db.transaction('tabs', 'readwrite');
    const store = tx.objectStore('tabs');
    const rec = (await reqToPromise(store.get(this.tabId))) || { tabId: this.tabId, consumed: 0, failed: 0 };
    rec.lastSeen = Date.now();
    store.put(rec);
    await txDone(tx);
    this._scheduleEmit();
  }

  _markInactive() {
    try {
      const tx = this.db.transaction('tabs', 'readwrite');
      const store = tx.objectStore('tabs');
      const getReq = store.get(this.tabId);
      getReq.onsuccess = () => {
        const rec = getReq.result;
        if (rec) {
          rec.lastSeen = 0;
          store.put(rec);
        }
      };
    } catch (_) { /* 关闭途中失败由 staleness 兜底 */ }
  }

  // ---------- 元信息(暂停/继续) ----------

  async _getPaused() {
    const tx = this.db.transaction('meta', 'readonly');
    const rec = await reqToPromise(tx.objectStore('meta').get('queue'));
    return !!(rec && rec.paused);
  }

  async _setPaused(paused) {
    await navigator.locks.request(LOCK_MUTATE, async () => {
      const tx = this.db.transaction(['meta', 'history'], 'readwrite');
      tx.objectStore('meta').put({ key: 'queue', paused });
      tx.objectStore('history').add({ at: Date.now(), type: paused ? 'pause' : 'resume', tabId: this.tabId });
      await txDone(tx);
    });
    this._broadcast(paused ? 'pause' : 'resume', {});
    await this._emit();
  }

  pause() { return this._setPaused(true); }
  resume() { return this._setPaused(false); }

  // ---------- 入队 ----------

  async enqueue(payload) {
    const task = {
      payload: payload || {},
      status: 'pending',
      attempts: 0,
      owner: null,
      createdAt: Date.now(),
      createdBy: this.tabId,
      startedAt: null,
      finishedAt: null,
    };
    let id;
    await navigator.locks.request(LOCK_MUTATE, async () => {
      const tx = this.db.transaction(['tasks', 'history'], 'readwrite');
      id = await reqToPromise(tx.objectStore('tasks').add(task));
      tx.objectStore('history').add({ at: Date.now(), type: 'enqueue', taskId: id, tabId: this.tabId, detail: describe(task) });
      await txDone(tx);
    });
    this._broadcast('enqueue', { taskId: id });
    await this._emit();
    return id;
  }

  // ---------- 出队(Web Locks 保证跨标签页原子性) ----------

  async dequeue() {
    const result = await navigator.locks.request(LOCK_DEQUEUE, async () => {
      if (await this._getPaused()) return { kind: 'paused' };
      const tx = this.db.transaction(['tasks', 'history'], 'readwrite');
      const store = tx.objectStore('tasks');
      const task = await firstByStatus(store, 'pending');
      if (!task) {
        await txDone(tx);
        return { kind: 'empty' };
      }
      task.status = 'processing';
      task.owner = this.tabId;
      task.startedAt = Date.now();
      store.put(task);
      tx.objectStore('history').add({ at: Date.now(), type: 'dequeue', taskId: task.id, tabId: this.tabId, detail: describe(task) });
      await txDone(tx);
      return { kind: 'ok', task };
    });
    if (result.kind === 'ok') {
      this._broadcast('dequeue', { taskId: result.task.id });
      await this._emit();
    }
    return result;
  }

  // ---------- 消费完成 / 失败重入队 ----------

  async complete(taskId) {
    await navigator.locks.request(LOCK_MUTATE, async () => {
      const tx = this.db.transaction(['tasks', 'history', 'tabs'], 'readwrite');
      const store = tx.objectStore('tasks');
      const task = await reqToPromise(store.get(taskId));
      if (!task || task.status !== 'processing') { await txDone(tx); return; }
      task.status = 'done';
      task.finishedAt = Date.now();
      store.put(task);
      tx.objectStore('history').add({ at: Date.now(), type: 'complete', taskId, tabId: this.tabId, detail: describe(task) });
      const tabRec = (await reqToPromise(tx.objectStore('tabs').get(this.tabId))) || { tabId: this.tabId, consumed: 0, failed: 0 };
      tabRec.consumed += 1;
      tabRec.lastSeen = Date.now();
      tx.objectStore('tabs').put(tabRec);
      await txDone(tx);
    });
    this._broadcast('complete', { taskId });
    await this._emit();
  }

  async fail(taskId, reason) {
    await navigator.locks.request(LOCK_MUTATE, async () => {
      const tx = this.db.transaction(['tasks', 'history', 'tabs'], 'readwrite');
      const store = tx.objectStore('tasks');
      const task = await reqToPromise(store.get(taskId));
      if (!task || task.status !== 'processing') { await txDone(tx); return; }
      task.status = 'pending';
      task.attempts += 1;
      task.owner = null;
      task.startedAt = null;
      store.put(task);
      tx.objectStore('history').add({ at: Date.now(), type: 'requeue', taskId, tabId: this.tabId, detail: `失败重入队(第 ${task.attempts} 次): ${reason || '未知原因'}` });
      const tabRec = (await reqToPromise(tx.objectStore('tabs').get(this.tabId))) || { tabId: this.tabId, consumed: 0, failed: 0 };
      tabRec.failed += 1;
      tabRec.lastSeen = Date.now();
      tx.objectStore('tabs').put(tabRec);
      await txDone(tx);
    });
    this._broadcast('fail', { taskId });
    await this._emit();
  }

  // ---------- 孤儿任务回收(标签页关闭后任务不丢) ----------

  async reclaimStale() {
    const reclaimed = await navigator.locks.request(LOCK_MUTATE, async () => {
      const now = Date.now();
      const tx = this.db.transaction(['tasks', 'tabs', 'history'], 'readwrite');
      const tabs = await reqToPromise(tx.objectStore('tabs').getAll());
      const alive = new Set(tabs.filter((t) => now - t.lastSeen < TAB_STALE_MS).map((t) => t.tabId));
      const processing = await reqToPromise(tx.objectStore('tasks').index('byStatus').getAll('processing'));
      const orphans = processing.filter((t) => !alive.has(t.owner));
      const store = tx.objectStore('tasks');
      for (const task of orphans) {
        const deadOwner = task.owner;
        task.status = 'pending';
        task.owner = null;
        task.startedAt = null;
        store.put(task);
        tx.objectStore('history').add({ at: now, type: 'reclaim', taskId: task.id, tabId: this.tabId, detail: `标签页 ${deadOwner || '?'} 已离线,任务回收重入队` });
      }
      await txDone(tx);
      return orphans.length;
    });
    if (reclaimed > 0) {
      this._broadcast('reclaim', { count: reclaimed });
      await this._emit();
    }
    return reclaimed;
  }

  // ---------- 清空 ----------

  async clear() {
    let removed = 0;
    await navigator.locks.request(LOCK_MUTATE, async () => {
      const tx = this.db.transaction(['tasks', 'history'], 'readwrite');
      const store = tx.objectStore('tasks');
      const all = await reqToPromise(store.getAll());
      for (const task of all) {
        if (task.status === 'pending' || task.status === 'processing') {
          store.delete(task.id);
          removed += 1;
        }
      }
      tx.objectStore('history').add({ at: Date.now(), type: 'clear', tabId: this.tabId, detail: `清空 ${removed} 个待处理任务` });
      await txDone(tx);
    });
    this._broadcast('clear', { removed });
    await this._emit();
    return removed;
  }

  // ---------- 状态读取 ----------

  async getState() {
    const tx = this.db.transaction(['tasks', 'history', 'tabs', 'meta'], 'readonly');
    const [tasks, history, tabs, meta] = await Promise.all([
      reqToPromise(tx.objectStore('tasks').getAll()),
      reqToPromise(tx.objectStore('history').getAll()),
      reqToPromise(tx.objectStore('tabs').getAll()),
      reqToPromise(tx.objectStore('meta').get('queue')),
    ]);
    const now = Date.now();
    const pending = tasks.filter((t) => t.status === 'pending').sort((a, b) => a.id - b.id);
    const processing = tasks.filter((t) => t.status === 'processing');
    const done = tasks.filter((t) => t.status === 'done');
    return {
      tabId: this.tabId,
      paused: !!(meta && meta.paused),
      online: navigator.onLine,
      counts: { pending: pending.length, processing: processing.length, done: done.length },
      pending: pending.slice(0, 100),
      processing,
      tabs: tabs
        .map((t) => ({ ...t, active: now - t.lastSeen < TAB_STALE_MS }))
        .sort((a, b) => b.consumed - a.consumed),
      history: history.slice(-HISTORY_LIMIT).reverse(),
    };
  }
}

function firstByStatus(store, status) {
  return new Promise((resolve, reject) => {
    const req = store.openCursor();
    req.onerror = () => reject(req.error);
    req.onsuccess = () => {
      const cursor = req.result;
      if (!cursor) return resolve(null);
      if (cursor.value.status === status) return resolve(cursor.value);
      cursor.continue();
    };
  });
}

function describe(task) {
  const name = task.payload && task.payload.name ? task.payload.name : `任务 #${task.id}`;
  return `${name}`;
}
