import { SharedQueue } from './queue.js';
import { QueueViz } from './viz.js';

const $ = (sel) => document.querySelector(sel);

const queue = new SharedQueue();
const viz = new QueueViz($('#viz'));

let autoConsume = false;
let consuming = false;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function boot() {
  await queue.init();
  queue.onChange(render);
  bindControls();
  $('#tabId').textContent = queue.tabId;
  window.addEventListener('online', () => { log('网络已恢复,重新同步状态'); queue._emit(); });
  window.addEventListener('offline', () => { log('已离线,入队/出队仍会持久化到本地'); render({}); });
}

function bindControls() {
  $('#enqueueBtn').addEventListener('click', async () => {
    const name = $('#taskName').value.trim() || `任务 ${new Date().toLocaleTimeString()}`;
    await queue.enqueue({ name });
    log(`已入队: ${name}`);
  });

  $('#batchBtn').addEventListener('click', async () => {
    for (let i = 0; i < 10; i += 1) {
      await queue.enqueue({ name: `批量任务 ${Date.now() % 100000}-${i}` });
    }
    log('已批量入队 10 个任务');
  });

  $('#consumeBtn').addEventListener('click', consumeOnce);

  $('#autoBtn').addEventListener('click', () => {
    autoConsume = !autoConsume;
    $('#autoBtn').textContent = autoConsume ? '停止自动消费' : '开始自动消费';
    $('#autoBtn').classList.toggle('active', autoConsume);
    if (autoConsume) autoLoop();
  });

  $('#pauseBtn').addEventListener('click', async () => {
    const paused = $('#pauseBtn').dataset.paused === '1';
    if (paused) { await queue.resume(); log('队列已继续'); }
    else { await queue.pause(); log('队列已暂停'); }
  });

  $('#clearBtn').addEventListener('click', async () => {
    const removed = await queue.clear();
    log(`队列已清空,移除 ${removed} 个任务`);
  });
}

async function consumeOnce() {
  if (consuming) return;
  consuming = true;
  try {
    const res = await queue.dequeue();
    if (res.kind === 'empty') { log('队列为空,无任务可消费'); return; }
    if (res.kind === 'paused') { log('队列已暂停,无法出队'); return; }
    await processTask(res.task);
  } finally {
    consuming = false;
  }
}

async function autoLoop() {
  while (autoConsume) {
    const res = await queue.dequeue();
    if (res.kind === 'ok') {
      await processTask(res.task);
    } else {
      await sleep(600);
    }
  }
}

async function processTask(task) {
  const failRate = Number($('#failRate').value) / 100;
  setStatus(`消费中: #${task.id} ${task.payload.name || ''}`);
  await sleep(400 + Math.random() * 900);
  if (Math.random() < failRate) {
    await queue.fail(task.id, '模拟消费失败');
    log(`任务 #${task.id} 消费失败,已重新入队`);
  } else {
    await queue.complete(task.id);
    log(`任务 #${task.id} 消费完成`);
  }
  setStatus('空闲');
}

function setStatus(text) {
  $('#consumeStatus').textContent = text;
}

function log(text) {
  const el = document.createElement('div');
  el.className = 'log-line';
  el.textContent = `[${new Date().toLocaleTimeString()}] ${text}`;
  const box = $('#log');
  box.prepend(el);
  while (box.children.length > 60) box.lastChild.remove();
}

const TYPE_LABELS = {
  enqueue: '入队', dequeue: '出队', complete: '完成', requeue: '失败重入队',
  reclaim: '回收重入队', clear: '清空', pause: '暂停', resume: '继续',
};

function render(state) {
  if (!state || !state.counts) {
    $('#netStatus').textContent = navigator.onLine ? '在线' : '离线';
    return;
  }
  $('#netStatus').textContent = state.online ? '在线' : '离线';
  $('#statPending').textContent = state.counts.pending;
  $('#statProcessing').textContent = state.counts.processing;
  $('#statDone').textContent = state.counts.done;

  const pauseBtn = $('#pauseBtn');
  pauseBtn.dataset.paused = state.paused ? '1' : '0';
  pauseBtn.textContent = state.paused ? '继续队列' : '暂停队列';
  $('#queueState').textContent = state.paused ? '已暂停' : '运行中';
  $('#queueState').className = state.paused ? 'badge warn' : 'badge ok';

  viz.draw(state);
  renderTabs(state);
  renderHistory(state);
}

function renderTabs(state) {
  const tbody = $('#tabsTable tbody');
  tbody.innerHTML = '';
  for (const tab of state.tabs) {
    const tr = document.createElement('tr');
    if (tab.tabId === state.tabId) tr.className = 'self';
    tr.innerHTML = `
      <td>${tab.tabId}${tab.tabId === state.tabId ? ' (本页)' : ''}</td>
      <td>${tab.active ? '活跃' : '离线'}</td>
      <td>${tab.consumed}</td>
      <td>${tab.failed}</td>`;
    tbody.appendChild(tr);
  }
}

function renderHistory(state) {
  const box = $('#history');
  box.innerHTML = '';
  for (const item of state.history.slice(0, 40)) {
    const el = document.createElement('div');
    el.className = `history-item t-${item.type}`;
    const time = new Date(item.at).toLocaleTimeString();
    const task = item.taskId != null ? `#${item.taskId}` : '-';
    el.textContent = `${time} [${TYPE_LABELS[item.type] || item.type}] 任务${task} 由 ${item.tabId}${item.detail ? ' · ' + item.detail : ''}`;
    box.appendChild(el);
  }
}

boot().catch((err) => {
  document.body.innerHTML = `<p style="color:#f87171;padding:2rem">初始化失败: ${err.message}</p>`;
});
