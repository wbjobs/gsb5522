const COLORS = {
  bg: '#0f1420',
  grid: '#1d2637',
  pending: '#3b82f6',
  pendingRetry: '#f59e0b',
  processing: '#22c55e',
  done: '#475569',
  text: '#cbd5e1',
  bar: '#8b5cf6',
  barSelf: '#ec4899',
};

export class QueueViz {
  constructor(canvas) {
    this.canvas = canvas;
    this.ctx = canvas.getContext('2d');
    this.state = null;
    this._tick = 0;
    setInterval(() => {
      this._tick += 1;
      if (this.state) this.draw(this.state);
    }, 500);
  }

  draw(state) {
    this.state = state;
    const canvas = this.canvas;
    const dpr = window.devicePixelRatio || 1;
    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    if (canvas.width !== width * dpr || canvas.height !== height * dpr) {
      canvas.width = width * dpr;
      canvas.height = height * dpr;
    }
    const ctx = this.ctx;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = COLORS.bg;
    ctx.fillRect(0, 0, width, height);

    this._drawQueueLane(ctx, state, 8, 26, width - 16, 84);
    this._drawProcessingLane(ctx, state, 8, 122, width - 16, 40);
    this._drawTabBars(ctx, state, 8, 174, width - 16, height - 182);
  }

  _label(ctx, text, x, y) {
    ctx.fillStyle = COLORS.text;
    ctx.font = '11px system-ui, sans-serif';
    ctx.textBaseline = 'middle';
    ctx.fillText(text, x, y);
  }

  _drawQueueLane(ctx, state, x, y, w, h) {
    this._label(ctx, `待消费队列 (${state.counts.pending})`, x, y - 8);
    ctx.strokeStyle = COLORS.grid;
    ctx.strokeRect(x, y, w, h);
    const block = 14;
    const gap = 4;
    const perRow = Math.max(1, Math.floor((w - gap) / (block + gap)));
    const maxRows = Math.floor((h - gap) / (block + gap));
    const cap = perRow * maxRows;
    const items = state.pending.slice(0, cap);
    items.forEach((task, i) => {
      const col = i % perRow;
      const row = Math.floor(i / perRow);
      ctx.fillStyle = task.attempts > 0 ? COLORS.pendingRetry : COLORS.pending;
      ctx.fillRect(x + gap + col * (block + gap), y + gap + row * (block + gap), block, block);
    });
    if (state.counts.pending > cap) {
      this._label(ctx, `+${state.counts.pending - cap} 更多`, x + w - 60, y + h - 12);
    }
    if (state.counts.pending === 0) {
      this._label(ctx, '队列为空', x + 10, y + h / 2);
    }
  }

  _drawProcessingLane(ctx, state, x, y, w, h) {
    this._label(ctx, `消费中 (${state.counts.processing})`, x, y - 8);
    ctx.strokeStyle = COLORS.grid;
    ctx.strokeRect(x, y, w, h);
    const pulse = 0.6 + 0.4 * Math.sin(this._tick * 1.2);
    state.processing.forEach((task, i) => {
      const bx = x + 6 + i * 92;
      if (bx + 86 > x + w) return;
      ctx.globalAlpha = pulse;
      ctx.fillStyle = COLORS.processing;
      ctx.fillRect(bx, y + 6, 86, h - 12);
      ctx.globalAlpha = 1;
      ctx.fillStyle = '#052e16';
      ctx.font = '10px system-ui, sans-serif';
      ctx.textBaseline = 'middle';
      ctx.fillText(`#${task.id} → ${task.owner}`, bx + 5, y + h / 2);
    });
  }

  _drawTabBars(ctx, state, x, y, w, h) {
    this._label(ctx, '各标签页消费数', x, y - 8);
    const tabs = state.tabs;
    if (!tabs.length || h <= 10) return;
    const max = Math.max(1, ...tabs.map((t) => t.consumed));
    const barH = Math.min(22, (h - 4) / tabs.length - 6);
    tabs.forEach((tab, i) => {
      const by = y + 4 + i * (barH + 6);
      const bw = (tab.consumed / max) * (w - 150);
      ctx.fillStyle = tab.tabId === state.tabId ? COLORS.barSelf : COLORS.bar;
      ctx.globalAlpha = tab.active ? 1 : 0.35;
      ctx.fillRect(x + 90, by, Math.max(2, bw), barH);
      ctx.globalAlpha = 1;
      this._label(ctx, `${tab.tabId}${tab.active ? '' : ' (离线)'}`, x, by + barH / 2);
      this._label(ctx, String(tab.consumed), x + 96 + Math.max(2, bw), by + barH / 2);
    });
  }
}
