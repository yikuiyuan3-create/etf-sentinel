(() => {
  'use strict';
  if (!document.getElementById('monitor-panel')) return;

  const view = name => document.getElementById(`monitor-${name}`);
  const write = (name, value) => { view(name).textContent = value; };
  const counter = new Intl.NumberFormat('zh-CN', {maximumFractionDigits: 0});
  const ageFormat = new Intl.NumberFormat('zh-CN', {minimumFractionDigits: 1, maximumFractionDigits: 1});
  const clock = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
  });
  const countNames = ['signals', 'blocked', 'watch', 'candidates'];
  const provenanceNames = ['frequency', 'checked', 'next', 'scheduled', 'schedule', 'as-of', 'age', 'code', 'note'];
  const scheduleLabels = {
    CURRENT: '调度正常', NOT_RUN: '调度尚未首次运行',
    OVERDUE: '调度已逾期，分析暂停', FAILED: '调度失败，分析暂停',
  };
  let pending = null;
  let retryTimer = null;
  let lastCheckedAt = null;
  let lastScheduledAt = null;
  let reloadRequested = false;

  function clearRetry() {
    if (retryTimer !== null) clearTimeout(retryTimer);
    retryTimer = null;
  }

  function closeGate(message, retainProvenance = false) {
    document.body.dataset.monitoringState = 'blocked';
    view('panel').dataset.status = 'blocked';
    write('connection', '分析不可用 / 已失败关闭');
    write('headline', message);
    write('health-alert', `${message} 旧候选、模拟组合及其他派生分析已暂时隐藏；数据源健康和审计仍可查看。`);
    countNames.forEach(name => write(name, '不可用'));
    ['findings', 'sources', 'snapshots'].forEach(name => view(name).replaceChildren());
    if (!retainProvenance) provenanceNames.forEach(name => write(name, '不可用'));
  }

  function stop(message) {
    clearRetry();
    if (pending) {
      const previous = pending;
      pending = null;
      clearTimeout(previous.timeout);
      previous.controller.abort();
      previous.cancel(new Error('REQUEST_CANCELLED'));
    }
    view('refresh').disabled = false;
    closeGate(message);
  }

  function validText(value, max = 2000) {
    return typeof value === 'string' && value.length <= max;
  }

  function validDate(value, nullable = false) {
    return nullable && value === null || validText(value, 80)
      && /(?:Z|[+-]\d{2}:\d{2})$/.test(value) && Number.isFinite(Date.parse(value));
  }

  function validList(value, maxLength = 100) {
    return Array.isArray(value) && value.length <= maxLength
      && value.every(item => validText(item));
  }

  function validated(payload) {
    const data = payload && payload.data;
    if (!payload || payload.data_mode !== 'DEMO_FIXTURE'
      || !validText(payload.disclaimer, 10000) || !payload.disclaimer || !data
      || ![1, 2].includes(data.interval_hours)
      || !Object.hasOwn(scheduleLabels, data.schedule_status)
      || data.live_data_status !== 'COMPLIANCE_BLOCKED'
      || !['DEMO_ANALYSIS', 'BLOCKED'].includes(data.analysis_status)
      || !validDate(data.checked_at) || !validDate(data.next_check_at)
      || !validDate(data.last_scheduled_at, true) || !validDate(data.market_data_as_of, true)
      || !(data.market_data_age_hours === null
        || Number.isFinite(data.market_data_age_hours) && data.market_data_age_hours >= 0)
      || !validText(data.headline) || !validText(data.scheduler_note)
      || !validText(data.code_version, 256)
      || !(data.blocked_reason === null || validText(data.blocked_reason))
      || !validList(data.findings) || !validList(data.source_links)
      || !validList(data.data_snapshot_ids)
      || !data.counts || !countNames.every(name =>
        Number.isSafeInteger(data.counts[name]) && data.counts[name] >= 0)) {
      throw new Error('INVALID_MONITORING_RESPONSE');
    }
    // A newly fetched response is not necessarily fresh: reject old intermediary caches.
    const responseAge = Date.now() - Date.parse(data.checked_at);
    if (responseAge > 300000 || responseAge < -60000) throw new Error('STALE_CHECK_RESPONSE');
    if (lastCheckedAt !== null && Date.parse(data.checked_at) < lastCheckedAt) {
      throw new Error('OLDER_CHECK_RESPONSE');
    }
    if (data.schedule_status === 'CURRENT' && data.last_scheduled_at !== null
      && lastScheduledAt !== null && Date.parse(data.last_scheduled_at) < lastScheduledAt) {
      throw new Error('OLDER_SCHEDULE_RESPONSE');
    }
    return data;
  }

  function showTime(name, value) {
    write(name, value === null ? '尚无记录' : `${clock.format(new Date(value))} 上海时区`);
    view(name).title = value || '';
  }

  function renderList(name, items, allowLinks = false) {
    const children = items.map(item => {
      const li = document.createElement('li');
      let source;
      if (allowLinks) {
        try {
          source = new URL(item);
          if (source.protocol !== 'https:' || source.username || source.password) source = null;
        } catch (_) { source = null; }
      }
      if (source) {
        const link = document.createElement('a');
        link.textContent = item;
        link.href = source.href;
        link.target = '_blank';
        link.rel = 'noopener noreferrer nofollow';
        li.appendChild(link);
      } else {
        li.textContent = item;
      }
      return li;
    });
    view(name).replaceChildren(...children);
  }

  function render(payload) {
    const data = validated(payload);
    write('frequency', data.interval_hours === 1 ? '每小时检查一次' : '每两小时检查一次');
    showTime('checked', data.checked_at);
    showTime('next', data.next_check_at);
    showTime('scheduled', data.last_scheduled_at);
    showTime('as-of', data.market_data_as_of);
    write('schedule', scheduleLabels[data.schedule_status]);
    write('age', data.market_data_age_hours === null ? '不可用' : `${ageFormat.format(data.market_data_age_hours)} 小时（历史夹具）`);
    write('code', data.code_version);
    write('note', data.scheduler_note);
    write('disclaimer', payload.disclaimer);
    if (data.analysis_status === 'BLOCKED' || ['OVERDUE', 'FAILED'].includes(data.schedule_status)) {
      closeGate(`当前分析已阻断：${data.blocked_reason || scheduleLabels[data.schedule_status]}`, true);
      return;
    }
    document.body.dataset.monitoringState = 'ready';
    view('panel').dataset.status = 'ready';
    write('connection', '服务已核验 · 仅固定 Demo 分析');
    write('headline', data.headline);
    countNames.forEach(name => write(name, counter.format(data.counts[name])));
    renderList('findings', data.findings);
    renderList('sources', data.source_links, true);
    renderList('snapshots', data.data_snapshot_ids);
    // The existing chart may have initialized while its fail-closed section was hidden.
    if (typeof window.dispatchEvent === 'function') window.dispatchEvent(new Event('resize'));
    lastCheckedAt = Date.parse(data.checked_at);
    if (data.schedule_status === 'CURRENT' && data.last_scheduled_at !== null) {
      const scheduledAt = Date.parse(data.last_scheduled_at);
      const newerTask = lastScheduledAt !== null && scheduledAt > lastScheduledAt;
      lastScheduledAt = scheduledAt;
      // Initial page load only establishes a baseline, so a normal reload cannot loop.
      if (newerTask && !reloadRequested) {
        reloadRequested = true;
        write('connection', '新一轮后台检查已完成，正在重新载入整页研究报表');
        window.location.reload();
      }
    }
  }

  async function refresh() {
    if (pending || reloadRequested) return;
    clearRetry();
    if (navigator.onLine === false) {
      stop('网络已离线，无法核验当前数据健康。');
      return;
    }
    if (document.visibilityState === 'hidden') {
      stop('页面处于后台，服务核验已暂停；返回后重新核验。');
      return;
    }
    const request = {controller: new AbortController(), timeout: null, cancel: null};
    pending = request;
    view('refresh').disabled = true;
    write('connection', '正在核验服务，尚未更新分析');
    const deadline = new Promise((_, reject) => {
      request.cancel = reject;
      request.timeout = setTimeout(() => {
        request.controller.abort();
        reject(new Error('MONITORING_TIMEOUT'));
      }, 10000);
    });
    try {
      const response = await Promise.race([
        fetch('/api/v1/monitoring', {
          method: 'GET', headers: {Accept: 'application/json'}, cache: 'no-store',
          mode: 'same-origin', credentials: 'same-origin', redirect: 'error',
          signal: request.controller.signal,
        }).then(async result => {
          if (!result.ok) throw new Error('MONITORING_HTTP_FAILED');
          return result.json();
        }), deadline,
      ]);
      if (pending === request) render(response);
    } catch (_) {
      if (pending === request) closeGate('服务连接、响应完整性或时间校验未通过，请检查数据健康。');
    } finally {
      clearTimeout(request.timeout);
      if (pending === request) {
        pending = null;
        view('refresh').disabled = false;
        if (!reloadRequested) retryTimer = setTimeout(refresh, 60000);
      }
    }
  }

  view('refresh').addEventListener('click', refresh);
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') stop('页面处于后台，服务核验已暂停；返回后重新核验。');
    else refresh();
  });
  window.addEventListener('offline', () => stop('网络已离线，无法核验当前数据健康。'));
  window.addEventListener('online', refresh);
  window.addEventListener('pagehide', () => stop('页面已离开，服务核验已暂停。'));
  window.addEventListener('pageshow', refresh);
  document.body.dataset.monitoringState = 'pending';
  refresh();
})();
