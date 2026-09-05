const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../../src/etf_sentinel/static/monitor.js'), 'utf8');
const NOW = Date.parse('2026-09-05T08:00:00Z');
const tick = () => new Promise(resolve => setImmediate(resolve));

function payload(overrides = {}) {
  return {data_mode: 'DEMO_FIXTURE', disclaimer: '内部研究声明，不构成交易指令。', data: {
    interval_hours: 1, checked_at: new Date(NOW).toISOString(),
    next_check_at: '2026-09-05T09:00:00Z', last_scheduled_at: '2026-09-05T08:00:00Z',
    schedule_status: 'CURRENT', market_data_as_of: '2025-01-30T07:00:00Z',
    market_data_age_hours: 13921.123456, live_data_status: 'COMPLIANCE_BLOCKED',
    analysis_status: 'DEMO_ANALYSIS', blocked_reason: null, headline: '固定 Demo 风险分析',
    findings: ['模型仍未获准用于真实数据候选。'],
    counts: {signals: 60, blocked: 4, watch: 23, candidates: 9},
    source_links: ['fixture://market/demo', 'https://example.com/source'],
    data_snapshot_ids: ['snapshot-1'], code_version: 'tree-example',
    scheduler_note: '每小时检查风险与数据健康，不产生盘中候选。', ...overrides,
  }};
}

function harness(fetcher = () => Promise.resolve({ok: true, json: async () => payload()})) {
  const nodes = new Map();
  const events = new Map();
  const timers = new Map();
  let timerId = 0;
  class Element {
    constructor() { this.dataset = {}; this.children = []; this.textContent = ''; this.hidden = false; this.disabled = false; this.handlers = {}; }
    addEventListener(type, fn) { this.handlers[type] = fn; }
    replaceChildren(...children) { this.children = children; this.textContent = ''; }
    appendChild(child) { this.children.push(child); return child; }
    setAttribute(name, value) { this[name] = value; }
  }
  for (const name of ['panel', 'refresh', 'connection', 'frequency', 'checked', 'next', 'scheduled',
    'schedule', 'as-of', 'age', 'headline', 'findings', 'sources', 'snapshots', 'code',
    'signals', 'blocked', 'watch', 'candidates', 'note', 'disclaimer', 'health-alert']) {
    nodes.set(`monitor-${name}`, new Element());
  }
  const document = {
    body: new Element(), visibilityState: 'visible',
    getElementById: id => nodes.get(id) || null,
    createElement: () => new Element(),
    addEventListener: (name, fn) => events.set(`document:${name}`, fn),
  };
  const calls = [];
  const reloads = [];
  const navigator = {onLine: true};
  const DateClass = class extends Date { static now() { return NOW; } };
  const context = {
    document, navigator, Intl, Date: DateClass, URL, AbortController, console,
    fetch: (...args) => { calls.push(args); return fetcher(...args); },
    setTimeout: (fn, ms) => { timers.set(++timerId, {fn, ms}); return timerId; },
    clearTimeout: id => timers.delete(id),
    window: {addEventListener: (name, fn) => events.set(`window:${name}`, fn),
      location: {reload: () => reloads.push('reload')}},
  };
  vm.runInNewContext(source, context);
  return {nodes, document, navigator, calls, timers, events, reloads,
    node: name => nodes.get(`monitor-${name}`),
    event: (type, name) => events.get(`${type}:${name}`)(),
  };
}

test('success reads same-origin no-cache API, separates checks and fixture times, formats numbers', async () => {
  const h = harness();
  assert.equal(h.document.body.dataset.monitoringState, 'pending');
  await tick();
  assert.equal(h.document.body.dataset.monitoringState, 'ready');
  assert.equal(h.calls[0][0], '/api/v1/monitoring');
  assert.equal(h.calls[0][1].method, 'GET');
  assert.equal(h.calls[0][1].cache, 'no-store');
  assert.equal(h.calls[0][1].redirect, 'error');
  assert.match(h.node('checked').textContent, /2026-09-05 16:00:00/);
  assert.match(h.node('as-of').textContent, /2025-01-30 15:00:00/);
  assert.match(h.node('age').textContent, /13,921\.1/);
  assert.equal(h.node('watch').textContent, '23');
  assert.match(h.node('frequency').textContent, /每小时/);
  assert.equal([...h.timers.values()].filter(t => t.ms === 60000).length, 1);
});

test('reentrant manual refresh does not start a duplicate request', async () => {
  let resolve;
  const h = harness(() => new Promise(done => { resolve = done; }));
  h.node('refresh').handlers.click();
  h.node('refresh').handlers.click();
  assert.equal(h.calls.length, 1);
  resolve({ok: true, json: async () => payload()});
  await tick();
  assert.equal(h.node('refresh').disabled, false);
});

test('network failure removes prior analysis and hides old derived reports', async () => {
  let fail = false;
  const h = harness(() => fail ? Promise.reject(new Error('private transport detail'))
    : Promise.resolve({ok: true, json: async () => payload()}));
  await tick();
  fail = true;
  await h.node('refresh').handlers.click();
  assert.equal(h.document.body.dataset.monitoringState, 'blocked');
  assert.equal(h.node('watch').textContent, '不可用');
  assert.equal(h.node('findings').children.length, 0);
  assert.equal(h.node('sources').children.length, 0);
  assert.equal(h.node('checked').textContent, '不可用');
  assert.doesNotMatch(h.node('headline').textContent, /private transport detail/);
});

test('blocked analysis clears counts but retains checked-at provenance', async () => {
  const h = harness(() => Promise.resolve({ok: true, json: async () => payload({
    analysis_status: 'BLOCKED', blocked_reason: 'DATA_LICENSE_EXPIRED',
  })}));
  await tick();
  assert.equal(h.document.body.dataset.monitoringState, 'blocked');
  assert.equal(h.node('signals').textContent, '不可用');
  assert.match(h.node('headline').textContent, /DATA_LICENSE_EXPIRED/);
  assert.match(h.node('checked').textContent, /2026-09-05/);
});

test('offline fails closed; online refreshes immediately', async () => {
  const h = harness();
  await tick();
  h.navigator.onLine = false;
  h.event('window', 'offline');
  assert.equal(h.document.body.dataset.monitoringState, 'blocked');
  assert.equal(h.node('candidates').textContent, '不可用');
  h.navigator.onLine = true;
  h.event('window', 'online');
  await tick();
  assert.equal(h.calls.length, 2);
  assert.equal(h.document.body.dataset.monitoringState, 'ready');
});

test('background pauses and aborts; delayed old response cannot reopen the gate', async () => {
  let resolve;
  const h = harness(() => new Promise(done => { resolve = done; }));
  const signal = h.calls[0][1].signal;
  h.document.visibilityState = 'hidden';
  h.event('document', 'visibilitychange');
  assert.equal(signal.aborted, true);
  resolve({ok: true, json: async () => payload()});
  await tick();
  assert.equal(h.document.body.dataset.monitoringState, 'blocked');
  assert.equal(h.timers.size, 0);
});

test('visible again immediately verifies current server state', async () => {
  const h = harness();
  await tick();
  h.document.visibilityState = 'hidden'; h.event('document', 'visibilitychange');
  h.document.visibilityState = 'visible'; h.event('document', 'visibilitychange');
  await tick();
  assert.equal(h.calls.length, 2);
  assert.equal(h.document.body.dataset.monitoringState, 'ready');
});

test('request timeout closes gate even if the transport ignores abort', async () => {
  const h = harness(() => new Promise(() => {}));
  [...h.timers.values()].find(t => t.ms === 10000).fn();
  await tick();
  assert.equal(h.calls[0][1].signal.aborted, true);
  assert.equal(h.document.body.dataset.monitoringState, 'blocked');
  assert.equal(h.node('refresh').disabled, false);
});

test('untrusted event text remains text and unsafe source URLs never become links', async () => {
  const injection = '<img src=x onerror=alert(1)>';
  const h = harness(() => Promise.resolve({ok: true, json: async () => payload({
    findings: [injection], source_links: ['javascript:alert(1)', 'https://example.com/source'],
  })}));
  await tick();
  assert.equal(h.node('findings').children[0].textContent, injection);
  assert.equal(h.node('findings').children[0].children.length, 0);
  assert.equal(h.node('sources').children[0].children.length, 0);
  assert.equal(h.node('sources').children[1].children[0].href, 'https://example.com/source');
  assert.equal(h.calls.length, 1);
});

for (const [name, change] of [
  ['stale cached response', {checked_at: '2026-09-05T07:54:59Z'}],
  ['future response', {checked_at: '2026-09-05T08:05:00Z'}],
  ['timezone missing', {checked_at: '2026-09-05T08:00:00'}],
  ['invalid count', {counts: {signals: -1, blocked: 0, watch: 0, candidates: 0}}],
  ['overdue schedule', {schedule_status: 'OVERDUE'}],
  ['failed schedule', {schedule_status: 'FAILED'}],
]) {
  test(`${name} is fail-closed`, async () => {
    const h = harness(() => Promise.resolve({ok: true, json: async () => payload(change)}));
    await tick();
    assert.equal(h.document.body.dataset.monitoringState, 'blocked');
    assert.equal(h.node('candidates').textContent, '不可用');
  });
}

test('two-hour configuration and first scheduled run are explicitly labelled', async () => {
  const h = harness(() => Promise.resolve({ok: true, json: async () => payload({
    interval_hours: 2, schedule_status: 'NOT_RUN', last_scheduled_at: null,
  })}));
  await tick();
  assert.match(h.node('frequency').textContent, /每两小时/);
  assert.match(h.node('schedule').textContent, /尚未首次运行/);
  assert.equal(h.document.body.dataset.monitoringState, 'ready');
});

test('automatic minute poll reads status without submitting or refreshing the page', async () => {
  const h = harness();
  await tick();
  [...h.timers.values()].find(t => t.ms === 60000).fn();
  await tick();
  assert.equal(h.calls.length, 2);
  assert.ok(h.calls.every(([url, options]) => url === '/api/v1/monitoring' && options.method === 'GET'));
});

test('HTTP rejection and wrong data mode fail closed', async () => {
  for (const result of [{ok: false}, {ok: true, json: async () => ({...payload(), data_mode: 'LIVE_LICENSED'})}]) {
    const h = harness(() => Promise.resolve(result));
    await tick();
    assert.equal(h.document.body.dataset.monitoringState, 'blocked');
    assert.equal(h.node('signals').textContent, '不可用');
  }
});

test('templates and CSS are fail-closed before script execution and preserve governance visibility', () => {
  const root = path.join(__dirname, '../../src/etf_sentinel');
  const template = fs.readFileSync(path.join(root, 'templates/dashboard.html'), 'utf8');
  const css = fs.readFileSync(path.join(root, 'static/app.css'), 'utf8');
  assert.match(template, /block body_attributes %}data-monitoring-state="pending"/);
  for (const section of ['overview', 'signals', 'portfolio', 'news', 'events', 'backtests', 'alerts', 'models']) {
    assert.match(template, new RegExp(`<section id="${section}"[^>]*data-monitor-sensitive`));
  }
  for (const section of ['providers', 'governance']) {
    assert.doesNotMatch(template, new RegExp(`<section id="${section}"[^>]*data-monitor-sensitive`));
  }
  assert.match(css, /body\[data-monitoring-state="blocked"\] \[data-monitor-sensitive\] \{ display: none !important; \}/);
  assert.match(template, /每 60 秒只读核验本区服务状态/);
  assert.match(template, /不等于实时行情更新/);
  assert.match(template, /<noscript>/);
});

test('first successful task sets a baseline; a later completed task reloads the dashboard once', async () => {
  let scheduled = '2026-09-05T07:00:00Z';
  const h = harness(() => Promise.resolve({ok: true, json: async () => payload({last_scheduled_at: scheduled})}));
  await tick();
  assert.equal(h.reloads.length, 0);
  await h.node('refresh').handlers.click();
  assert.equal(h.reloads.length, 0);
  scheduled = '2026-09-05T08:00:00Z';
  await h.node('refresh').handlers.click();
  assert.equal(h.reloads.length, 1);
  await h.node('refresh').handlers.click();
  assert.equal(h.reloads.length, 1);
  assert.ok(h.calls.every(([, options]) => options.method === 'GET'));
});

test('the first nonempty task after NOT_RUN only establishes a reload baseline', async () => {
  let change = {schedule_status: 'NOT_RUN', last_scheduled_at: null};
  const h = harness(() => Promise.resolve({ok: true, json: async () => payload(change)}));
  await tick();
  change = {schedule_status: 'CURRENT', last_scheduled_at: '2026-09-05T08:00:00Z'};
  await h.node('refresh').handlers.click();
  assert.equal(h.reloads.length, 0);
});

for (const [name, change] of [
  ['failed analysis', {analysis_status: 'BLOCKED', blocked_reason: 'DATA_HEALTH_FAILED'}],
  ['failed scheduler', {schedule_status: 'FAILED'}],
  ['older cached task', {last_scheduled_at: '2026-09-05T06:00:00Z'}],
  ['older response within freshness window', {checked_at: '2026-09-05T07:59:59Z'}],
  ['expired response', {checked_at: '2026-09-05T07:50:00Z'}],
]) {
  test(`${name} must not trigger dashboard reload`, async () => {
    let overrides = {last_scheduled_at: '2026-09-05T07:00:00Z'};
    const h = harness(() => Promise.resolve({ok: true, json: async () => payload(overrides)}));
    await tick();
    overrides = {last_scheduled_at: '2026-09-05T08:00:00Z', ...change};
    await h.node('refresh').handlers.click();
    assert.equal(h.reloads.length, 0);
    assert.equal(h.document.body.dataset.monitoringState, 'blocked');
  });
}
