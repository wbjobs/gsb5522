import { createQueueCore } from './queue-core.js';
import { createIdbStore, createWebLock } from './idb-store.js';

const LOCK_NAME = 'shared-task-queue-mutation';
const CHANNEL_NAME = 'shared-task-queue-sync';
const STALE_MS = 15000;
const HEARTBEAT_MS = 2000;
const REFRESH_MS = 1000;
const SAMPLE_MS = 500;
const DEPTH_SAMPLES = 120;

// 标签页身份：sessionStorage 保证每个标签页唯一、刷新后保持不变
const tabId = (() => {
  let id = sessionStorage.getItem('tq-tab-id');
  if (!id) {
    id = `tab-${Math.random().toString(36).slice(2, 8)}`;
    sessionStorage.setItem('tq-tab-id', id);
  }
  return id;
})();

const els = {
  tabBadge: document.getElementById('tabBadge'),
  pausedBadge: document.getElementById('pausedBadge'),
  pendingCount: document.getElementById('pendingCount'),
  processingCount: document.getElementById('processingCount'),
  doneCount: document.getElementById('doneCount'),
  activeTabs: document.getElementById('activeTabs'),
  enqueue1: document.getElementById('enqueue1'),
  enqueue10: document.getElementById('enqueue10'),
  dequeueOnce: document.getElementById('dequeueOnce'),
  autoToggle: document.getElementById('autoToggle'),
  pauseBtn: document.getElementById('pauseBtn'),
  resumeBtn: document.getElementById('resumeBtn'),
  clearBtn: document.getElementById('clearBtn'),
  failRate: document.getElementById('failRate'),
  failRateLabel: document.getElementById('failRateLabel'),
  tabsTable: document.getElementById('tabsTable').querySelector('tbody'),
  historyList: document.getElementById('historyList'),
  pendingPreview: document.getElementById('pendingPreview'),
  depthChart: document.getElementById('depthChart'),
  tabChart: document.getElementById('tabChart'),
  toast: document.getElementById('toast'),
};

let core;
let channel = null;
let autoConsuming = false;
let workerRunning = false;
const depthSeries = [];

function toast(msg) {
  els.toast.textContent = msg;
  els.toast.classList.add('show');
  clearTimeout(toast._t);
  toast._t = setTimeout(() => els.toast.classList.remove('show'), 1800);
}

/**
 * BroadcastChannel 只承担「有变化，请重新读库」的提示角色。
 * 真正的数据永远在 IndexedDB，消息丢失 / 乱序 / 重复都不会丢任务；
 * 另有 1s 定时刷新兜底，任何情况下状态最终一致。
 */
function notify() {
  if (channel) {
    try {
      channel.postMessage({ type: 'changed', ts: Date.now(), from: tabId });
    } catch {
      /* 频道关闭等异常忽略，定时刷新会兜底 */
    }
  }
}

async function mutateAndSync(fn) {
  const result = await fn();
  notify();
  await refresh();
  return result;
}

async function doEnqueue(count) {
  for (let i = 0; i < count; i += 1) {
    const payload = {
      title: `任务 @${new Date().toLocaleTimeString()}`,
      from: tabId,
      n: i + 1,
    };
    // eslint-disable-next-line no-await-in-loop
    await core.enqueue(payload, tabId);
  }
  await mutateAndSync(() => Promise.resolve());
  toast(`已入队 ${count} 个任务`);
}

async function dequeueOnce() {
  const task = await mutateAndSync(() => core.dequeue(tabId));
  if (!task) {
    toast('队列为空或已暂停');
    return;
  }
  await processTask(task);
}

async function processTask(task) {
  const failRate = Number(els.failRate.value) / 100;
  // 模拟消费耗时
  await new Promise((r) => setTimeout(r, 300 + Math.random() * 600));
  if (Math.random() < failRate) {
    await mutateAndSync(() => core.fail(task.id, tabId));
    toast(`消费失败，已重新入队 (seq ${task.seq})`);
  } else {
    await mutateAndSync(() => core.complete(task.id, tabId));
  }
}

async function workerLoop() {
  if (workerRunning) return;
  workerRunning = true;
  while (autoConsuming) {
    // eslint-disable-next-line no-await-in-loop
    const task = await core.dequeue(tabId);
    if (!task) {
      // eslint-disable-next-line no-await-in-loop
      await new Promise((r) => setTimeout(r, 500));
      continue;
    }
    notify();
    // eslint-disable-next-line no-await-in-loop
    await processTask(task);
  }
  workerRunning = false;
}

function fmtTime(ts) {
  return new Date(ts).toLocaleTimeString('zh-CN', { hour12: false });
}

const ACTION_LABELS = {
  enqueue: '入队',
  dequeue: '出队',
  done: '完成',
  'fail-requeue': '失败重排',
  'requeue-stale': '失联回收',
  pause: '暂停',
  resume: '继续',
  clear: '清空',
};

async function refresh() {
  const state = await core.getState();

  els.pendingCount.textContent = state.counts.pending;
  els.processingCount.textContent = state.counts.processing;
  els.doneCount.textContent = state.counts.done;

  const active = state.stats.filter((s) => s.active);
  els.activeTabs.textContent = active.length;

  els.pausedBadge.hidden = !state.paused;
  els.pauseBtn.disabled = state.paused;
  els.resumeBtn.disabled = !state.paused;

  els.tabsTable.replaceChildren(
    ...state.stats
      .slice()
      .sort((a, b) => b.consumed - a.consumed)
      .map((s) => {
        const tr = document.createElement('tr');
        if (s.tabId === tabId) tr.classList.add('me');
        if (!s.active) tr.classList.add('inactive');
        tr.innerHTML = `
          <td>${s.tabId}${s.tabId === tabId ? '（我）' : ''}</td>
          <td>${s.consumed}</td>
          <td>${s.failed}</td>
          <td>${s.active ? '活跃' : '已离开'}</td>`;
        return tr;
      }),
  );

  els.historyList.replaceChildren(
    ...state.history.map((h) => {
      const li = document.createElement('li');
      li.className = `h-${h.action}`;
      const seq = h.extra && h.extra.seq != null ? ` #${h.extra.seq}` : '';
      const retry = h.extra && h.extra.retries ? ` 第${h.extra.retries}次重试` : '';
      li.textContent = `${fmtTime(h.ts)} [${ACTION_LABELS[h.action] || h.action}]${seq}${retry} ${h.tabId || ''}`;
      return li;
    }),
  );

  els.pendingPreview.replaceChildren(
    ...state.pendingPreview.map((t) => {
      const li = document.createElement('li');
      li.textContent = `#${t.seq} ${t.payload.title}${t.retries ? ` (重试${t.retries})` : ''}`;
      return li;
    }),
  );

  drawCharts(state);
}

function fitCanvas(canvas) {
  const dpr = window.devicePixelRatio || 1;
  const { clientWidth, clientHeight } = canvas;
  if (canvas.width !== clientWidth * dpr || canvas.height !== clientHeight * dpr) {
    canvas.width = clientWidth * dpr;
    canvas.height = clientHeight * dpr;
  }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, w: clientWidth, h: clientHeight };
}

function drawCharts(state) {
  drawDepthChart(state);
  drawTabChart(state);
}

function drawDepthChart(state) {
  const { ctx, w, h } = fitCanvas(els.depthChart);
  ctx.clearRect(0, 0, w, h);
  const padL = 28;
  const padB = 18;
  const padT = 8;
  const maxV = Math.max(5, ...depthSeries.map((s) => s.pending + s.processing));

  ctx.strokeStyle = '#3a3f4b';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padL, padT);
  ctx.lineTo(padL, h - padB);
  ctx.lineTo(w - 4, h - padB);
  ctx.stroke();

  ctx.fillStyle = '#8b93a3';
  ctx.font = '10px system-ui';
  ctx.fillText(String(maxV), 4, padT + 8);
  ctx.fillText('0', 16, h - padB);

  if (depthSeries.length > 1) {
    const plotW = w - padL - 6;
    const plotH = h - padT - padB;
    const stepX = plotW / (DEPTH_SAMPLES - 1);
    const offset = DEPTH_SAMPLES - depthSeries.length;

    const line = (key, color) => {
      ctx.strokeStyle = color;
      ctx.lineWidth = 1.6;
      ctx.beginPath();
      depthSeries.forEach((s, i) => {
        const x = padL + (i + offset) * stepX;
        const y = padT + plotH * (1 - s[key] / maxV);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.stroke();
    };
    line('pending', '#f0b429');
    line('processing', '#4f8cff');
  }
}

function drawTabChart(state) {
  const { ctx, w, h } = fitCanvas(els.tabChart);
  ctx.clearRect(0, 0, w, h);
  const stats = state.stats.filter((s) => s.active || s.consumed > 0);
  if (!stats.length) {
    ctx.fillStyle = '#8b93a3';
    ctx.font = '11px system-ui';
    ctx.fillText('暂无消费数据', 10, h / 2);
    return;
  }
  const maxV = Math.max(1, ...stats.map((s) => s.consumed));
  const padB = 26;
  const barAreaH = h - padB - 10;
  const slot = (w - 16) / stats.length;
  const barW = Math.min(46, slot * 0.6);

  stats.forEach((s, i) => {
    const x = 8 + i * slot + (slot - barW) / 2;
    const barH = (s.consumed / maxV) * barAreaH;
    ctx.fillStyle = s.tabId === tabId ? '#4f8cff' : s.active ? '#3aa675' : '#5a6272';
    ctx.fillRect(x, 10 + barAreaH - barH, barW, barH);
    ctx.fillStyle = '#e6e9ef';
    ctx.font = '10px system-ui';
    ctx.textAlign = 'center';
    ctx.fillText(String(s.consumed), x + barW / 2, 10 + barAreaH - barH - 3);
    ctx.fillStyle = '#8b93a3';
    const label = s.tabId.replace('tab-', '');
    ctx.fillText(label, x + barW / 2, h - 12);
    if (s.tabId === tabId) ctx.fillText('我', x + barW / 2, h - 1);
  });
  ctx.textAlign = 'left';
}

function bindEvents() {
  els.enqueue1.addEventListener('click', () => doEnqueue(1));
  els.enqueue10.addEventListener('click', () => doEnqueue(10));
  els.dequeueOnce.addEventListener('click', () => dequeueOnce());
  els.pauseBtn.addEventListener('click', () => mutateAndSync(() => core.pause(tabId)));
  els.resumeBtn.addEventListener('click', () => mutateAndSync(() => core.resume(tabId)));
  els.clearBtn.addEventListener('click', async () => {
    const removed = await mutateAndSync(() => core.clear(tabId));
    toast(`已清空 ${removed} 个任务`);
  });
  els.autoToggle.addEventListener('change', () => {
    autoConsuming = els.autoToggle.checked;
    if (autoConsuming) workerLoop();
  });
  els.failRate.addEventListener('input', () => {
    els.failRateLabel.textContent = `${els.failRate.value}%`;
  });
  window.addEventListener('beforeunload', () => {
    autoConsuming = false;
    if (channel) channel.close();
  });
}

async function init() {
  const store = await createIdbStore();
  core = createQueueCore({
    store,
    lock: createWebLock(LOCK_NAME),
    staleMs: STALE_MS,
  });

  if (typeof BroadcastChannel !== 'undefined') {
    channel = new BroadcastChannel(CHANNEL_NAME);
    channel.onmessage = () => refresh();
  }

  els.tabBadge.textContent = tabId;
  bindEvents();

  await core.heartbeat(tabId);
  setInterval(() => core.heartbeat(tabId), HEARTBEAT_MS);

  // 定时全量刷新：BroadcastChannel 消息丢失 / 乱序时的最终一致性兜底
  setInterval(refresh, REFRESH_MS);

  // 队列深度采样（供折线图）
  setInterval(async () => {
    const state = await core.getState();
    depthSeries.push({
      pending: state.counts.pending,
      processing: state.counts.processing,
    });
    if (depthSeries.length > DEPTH_SAMPLES) depthSeries.shift();
  }, SAMPLE_MS);

  notify();
  await refresh();
}

init().catch((err) => {
  console.error(err);
  toast(`初始化失败: ${err.message}`);
});
