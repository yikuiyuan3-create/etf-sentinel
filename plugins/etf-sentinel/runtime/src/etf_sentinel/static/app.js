(() => {
  "use strict";

  const search = document.querySelector("#signal-search");
  const stateFilter = document.querySelector("#signal-state-filter");
  const sort = document.querySelector("#signal-sort");
  const rows = Array.from(document.querySelectorAll("[data-signal-row]"));
  const emptyMessage = document.querySelector("#signal-filter-empty");
  const chartElement = document.querySelector("#signal-state-chart");
  const chartFallback = document.querySelector("[data-chart-fallback]");
  const songtiFont = '"SimSun", "宋体", "Songti SC", "STSong", serif';

  // Presentation only: preserve the recorded instant in datetime/title and all API values.
  const shanghaiTime = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Shanghai",
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23", timeZoneName: "longOffset",
  });
  document.querySelectorAll("time[datetime]").forEach((element) => {
    const raw = element.getAttribute("datetime");
    if (!raw || !/(?:Z|[+-]\d{2}:\d{2})$/.test(raw)) return;
    const instant = new Date(raw);
    if (Number.isNaN(instant.getTime())) return;
    const parts = Object.fromEntries(shanghaiTime.formatToParts(instant).map(({ type, value }) => [type, value]));
    const zone = parts.timeZoneName.replace("GMT", "UTC");
    element.textContent = `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second} ${zone}`;
    element.dataset.displayTimezone = "Asia/Shanghai";
  });

  const states = [
    "NO_ACTION",
    "WATCH",
    "ENTRY_CANDIDATE",
    "HOLD",
    "REDUCE_CANDIDATE",
    "EXIT_CANDIDATE",
    "BLOCKED_BY_RISK",
    "DATA_STALE",
  ];

  if (chartElement && rows.length && window.echarts) {
    const counts = Object.fromEntries(states.map((state) => [state, 0]));
    rows.forEach((row) => {
      if (Object.hasOwn(counts, row.dataset.state)) counts[row.dataset.state] += 1;
    });
    const chart = window.echarts.init(chartElement, null, { renderer: "svg" });
    chart.setOption({
      animation: false,
      textStyle: { fontFamily: songtiFont },
      aria: { enabled: true, decal: { show: true } },
      color: ["#0c5949"],
      grid: { left: 44, right: 18, top: 18, bottom: 82 },
      tooltip: { trigger: "axis", renderMode: "richText", textStyle: { fontFamily: songtiFont } },
      xAxis: {
        type: "category",
        data: states,
        axisLabel: { interval: 0, rotate: 35, color: "#53635d", fontSize: 9, fontFamily: songtiFont },
        axisLine: { lineStyle: { color: "#bfcac5" } },
      },
      yAxis: {
        type: "value",
        minInterval: 1,
        name: "ETF 数量",
        nameTextStyle: { fontFamily: songtiFont },
        axisLabel: { color: "#53635d", fontFamily: songtiFont, formatter: (value) => new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 0 }).format(value) },
        splitLine: { lineStyle: { color: "#e7ece9" } },
      },
      series: [{ type: "bar", data: states.map((state) => counts[state]), barMaxWidth: 44 }],
    });
    chartElement.dataset.rendered = "true";
    if (chartFallback) chartFallback.hidden = true;
    window.addEventListener("resize", () => chart.resize(), { passive: true });
  }

  const normalize = (value) => value.trim().toLocaleLowerCase("zh-CN");
  const applyFilter = () => {
    const query = normalize(search.value);
    const state = stateFilter.value;
    let visible = 0;

    rows.forEach((row) => {
      const matchesText = !query || normalize(row.dataset.search || "").includes(query);
      const matchesState = !state || row.dataset.state === state;
      const show = matchesText && matchesState;
      row.hidden = !show;
      if (show) visible += 1;
    });

    if (emptyMessage) emptyMessage.hidden = visible !== 0;
  };

  const applySort = () => {
    if (!sort || !rows.length) return;
    const body = rows[0].parentElement;
    const key = sort.value;
    const ordered = [...rows].sort((left, right) => {
      if (key === "score-desc") return Number(right.dataset.score) - Number(left.dataset.score);
      if (key === "scenario-strength-desc") {
        return Number(right.dataset.scenarioStrength) - Number(left.dataset.scenarioStrength);
      }
      if (key === "name-asc") {
        return (left.dataset.name || "").localeCompare(right.dataset.name || "", "zh-CN");
      }
      return Number(left.dataset.defaultOrder) - Number(right.dataset.defaultOrder);
    });
    ordered.forEach((row) => body.appendChild(row));
  };

  if (rows.length && search && stateFilter) {
    search.addEventListener("input", applyFilter);
    stateFilter.addEventListener("change", applyFilter);
    if (sort) sort.addEventListener("change", applySort);
  }

  document.querySelectorAll("[data-acknowledge-alert]").forEach((button) => {
    button.addEventListener("click", async () => {
      const feedback = button.parentElement.querySelector(".action-feedback");
      button.disabled = true;
      if (feedback) feedback.textContent = "正在记录确认…";
      try {
        const response = await fetch(
          `/api/v1/alerts/${encodeURIComponent(button.dataset.acknowledgeAlert)}/acknowledge`,
          { method: "POST", headers: { "X-ETF-Sentinel-Intent": "acknowledge" } },
        );
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        button.textContent = "已确认（不执行交易）";
        if (feedback) feedback.textContent = "审计日志已追加记录。";
      } catch (_error) {
        button.disabled = false;
        if (feedback) feedback.textContent = "确认未写入，请检查服务状态后重试。";
      }
    });
  });
})();
