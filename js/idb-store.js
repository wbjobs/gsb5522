/**
 * IndexedDB 存储适配器：队列的唯一事实源。
 * 标签页关闭 / 刷新 / 离线后重开，数据都在这里，队列不丢。
 */

const DB_NAME = 'shared-task-queue';
const DB_VERSION = 1;

function openDb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains('tasks')) {
        const tasks = db.createObjectStore('tasks', { keyPath: 'id' });
        tasks.createIndex('by_status_seq', ['status', 'seq']);
      }
      if (!db.objectStoreNames.contains('meta')) {
        db.createObjectStore('meta', { keyPath: 'key' });
      }
      if (!db.objectStoreNames.contains('stats')) {
        db.createObjectStore('stats', { keyPath: 'tabId' });
      }
      if (!db.objectStoreNames.contains('history')) {
        const history = db.createObjectStore('history', { keyPath: 'hid', autoIncrement: true });
        history.createIndex('by_hid', 'hid');
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function reqToPromise(req) {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function txDone(tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error || new Error('transaction aborted'));
  });
}

export async function createIdbStore() {
  const db = await openDb();

  function store(name, mode) {
    return db.transaction(name, mode).objectStore(name);
  }

  return {
    async getMeta(key) {
      const row = await reqToPromise(store('meta', 'readonly').get(key));
      return row ? row.value : undefined;
    },
    async setMeta(key, value) {
      await reqToPromise(store('meta', 'readwrite').put({ key, value }));
    },

    async putTask(task) {
      await reqToPromise(store('tasks', 'readwrite').put(task));
    },
    async getTask(id) {
      return reqToPromise(store('tasks', 'readonly').get(id));
    },
    async nextPending() {
      const index = store('tasks', 'readonly').index('by_status_seq');
      const range = IDBKeyRange.bound(['pending', 0], ['pending', Infinity]);
      const cursor = await reqToPromise(index.openCursor(range));
      return cursor ? cursor.value : null;
    },
    async tasksByStatus(status) {
      const index = store('tasks', 'readonly').index('by_status_seq');
      const range = IDBKeyRange.bound([status, 0], [status, Infinity]);
      return reqToPromise(index.getAll(range));
    },
    async clearTasks() {
      const tx = db.transaction('tasks', 'readwrite');
      const all = await reqToPromise(tx.objectStore('tasks').getAll());
      const removable = all.filter((t) => t.status !== 'done');
      for (const t of removable) {
        tx.objectStore('tasks').delete(t.id);
      }
      await txDone(tx);
      return removable.length;
    },

    async addHistory(entry) {
      await reqToPromise(store('history', 'readwrite').add(entry));
    },
    async recentHistory(limit) {
      const all = await reqToPromise(store('history', 'readonly').getAll());
      return all.slice(-limit).reverse();
    },
    async pruneHistory(keep) {
      const tx = db.transaction('history', 'readwrite');
      const os = tx.objectStore('history');
      const keys = await reqToPromise(os.getAllKeys());
      const excess = keys.length - keep;
      for (let i = 0; i < excess; i += 1) {
        os.delete(keys[i]);
      }
      await txDone(tx);
    },

    async getStat(tabId) {
      return reqToPromise(store('stats', 'readonly').get(tabId));
    },
    async putStat(stat) {
      await reqToPromise(store('stats', 'readwrite').put(stat));
    },
    async allStats() {
      return reqToPromise(store('stats', 'readonly').getAll());
    },
  };
}

/**
 * Web Locks 适配器：同名排他锁保证「检查 + 出队 + 标记」跨标签页原子执行。
 * 不支持 Web Locks 时退化为页面内 Promise 互斥锁（单标签页仍正确）。
 */
export function createWebLock(lockName) {
  if (navigator.locks && navigator.locks.request) {
    return {
      runExclusive(fn) {
        return navigator.locks.request(lockName, { mode: 'exclusive' }, () => fn());
      },
    };
  }
  let tail = Promise.resolve();
  return {
    runExclusive(fn) {
      const run = tail.then(fn, fn);
      tail = run.catch(() => {});
      return run;
    },
  };
}
