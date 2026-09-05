from __future__ import annotations

import json
import shutil
import subprocess
from datetime import UTC, datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader, select_autoescape

TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "src/etf_sentinel/templates"


class RenderedText(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self.times: list[dict[str, str | None]] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "time":
            self.times.append(dict(attrs))


def render_macro(name: str, value, *args) -> tuple[str, RenderedText]:
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=select_autoescape())
    output = str(getattr(env.get_template("_macros.html").module, name)(value, *args))
    parsed = RenderedText()
    parsed.feed(output)
    return output, parsed


@pytest.mark.parametrize(
    ("macro", "value", "args", "expected"),
    [
        ("money", 1234567.895, (), "CNY 1,234,567.90"),
        ("number", 12000, (0,), "12,000"),
        ("number", 0.123456, (3,), "0.123"),
        ("number", 0.123456, (4,), "0.1235"),
        ("percent", 0.123456, (), "12.3%"),
        ("percent", 0, (), "0.0%"),
        ("money", 0, (), "CNY 0.00"),
        ("money", -0.00001, (), "CNY 0.00"),
        ("percent", -0.00001, (), "0.0%"),
        ("number", -0.00001, (3,), "0.000"),
        ("money", None, (), "—"),
        ("percent", None, (), "—"),
        ("number", float("nan"), (), "—"),
        ("number", float("inf"), (), "—"),
    ],
)
def test_numeric_display_contract(macro, value, args, expected) -> None:
    output, parsed = render_macro(macro, value, *args)
    assert "".join(parsed.parts).strip() == expected
    assert 'class="numeric"' in output


@pytest.mark.parametrize(
    ("value", "machine", "expected"),
    [
        (
            datetime(2025, 1, 30, 8, tzinfo=UTC),
            "2025-01-30T08:00:00+00:00",
            "2025-01-30 08:00:00 UTC",
        ),
        (datetime(2025, 1, 30, 8), "2025-01-30T08:00:00+00:00", "2025-01-30 08:00:00 UTC"),
        ("2025-01-30T08:00:00Z", "2025-01-30T08:00:00Z", "2025-01-30 08:00:00 UTC"),
        (
            datetime(2025, 1, 30, 16, tzinfo=timezone(timedelta(hours=8))),
            "2025-01-30T16:00:00+08:00",
            "2025-01-30 16:00:00 UTC+08:00",
        ),
    ],
)
def test_time_keeps_machine_value_and_explicit_zone_without_javascript(value, machine, expected):
    _, parsed = render_macro("timestamp", value)
    assert parsed.times[0]["datetime"] == machine
    assert " ".join("".join(parsed.parts).split()) == expected


def test_missing_time_is_not_fabricated() -> None:
    _, parsed = render_macro("timestamp", None)
    assert not parsed.times
    assert "".join(parsed.parts).strip() == "—"


def test_nested_feature_values_are_formatted_and_html_escaped() -> None:
    output, parsed = render_macro("feature_value", {"嵌套": [0.123456789, "<script>风险</script>"]})
    assert "0.123" in "".join(parsed.parts)
    assert "0.123456789</" not in output
    assert "&lt;script&gt;风险&lt;/script&gt;" in output


def test_songti_inheritance_covers_controls_numbers_code_and_print() -> None:
    css = (TEMPLATE_DIR.parent / "static/app.css").read_text()
    js = (TEMPLATE_DIR.parent / "static/app.js").read_text()
    assert '--font-songti: "SimSun", "宋体", "Songti SC", "STSong", serif;' in css
    assert "font-family: var(--font-songti)" in css
    assert "font-family: var(--font-numeric)" in css
    assert "font-variant-numeric: tabular-nums lining-nums" in css
    assert "th.numeric-column, td.numeric-column { text-align: right; }" in css
    assert "button, input, select, textarea, option { font-family: inherit; }" in css
    assert "@media print" in css and "textStyle: { fontFamily: songtiFont }" in js
    assert "fontFamily: songtiFont" in js
    assert 'timeZone: "Asia/Shanghai"' in js


def test_dashboard_zero_counts_do_not_fall_back_to_nonzero_collections() -> None:
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=select_autoescape())
    output = env.get_template("dashboard.html").render(
        url_for=lambda *args, **kwargs: "/static/test",
        summary={"etf_count": 0, "unacknowledged_alerts": 0, "alert_count": 42},
        signals=[{"state": "WATCH", "probability": 0.123456, "composite_score": 0.123456}],
    )
    overview = output.split('id="overview"')[1].split("</section>")[0]
    assert overview.count('data-value="0">0</span>') == 5
    assert 'data-score="0.123456"' in output
    assert 'data-scenario-strength="0.123456"' in output
    assert 'class="numeric-column"><span class="numeric" data-value="0.123456">0.123' in output


def test_browser_time_chart_and_filter_behavior_in_node() -> None:
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js unavailable; browser-script runtime check not verified")
    harness = r"""
const fs = require('fs');
const vm = require('vm');
const assert = require('assert/strict');
const times = ['2025-01-30T08:00:00Z', '2025-03-09T03:30:00-04:00',
  '2025-11-02T01:30:00-05:00', '1988-07-01T00:00:00Z', 'not-a-date', '2025-01-30T08:00:00']
  .map(raw => ({raw, getAttribute() {return this.raw;}, dataset: {}, textContent: 'unchanged'}));
const control = value => ({value, handlers: {},
  addEventListener(name, fn) {this.handlers[name]=fn;}});
const search = control(''), filter = control(''), sort = control('');
const body = {order: [], appendChild(row) {this.order.push(row.dataset.name);}};
const rows = [
  {dataset: {name: '甲', state: 'WATCH', score: '0.123456', scenarioStrength: '0.2',
    search: '甲 股票', defaultOrder: 0}},
  {dataset: {name: '乙', state: 'NO_ACTION', score: '0.123457', scenarioStrength: '0.8',
    search: '乙 债券', defaultOrder: 1}}
].map(row => ({...row, parentElement: body}));
const chart = {dataset: {}}, fallback = {hidden: false}, empty = {hidden: true};
let option;
const lookup = {'#signal-search': search, '#signal-state-filter': filter, '#signal-sort': sort,
  '#signal-filter-empty': empty, '#signal-state-chart': chart, '[data-chart-fallback]': fallback};
const context = {Intl, Date, Object, Number, console,
  document: {querySelector: selector => lookup[selector] || null,
    querySelectorAll: selector => selector === 'time[datetime]' ? times
      : selector === '[data-signal-row]' ? rows : []},
  window: {echarts: {init() {return {setOption(value) {option = value;}, resize() {}};}},
    addEventListener() {}}};
vm.runInNewContext(fs.readFileSync(process.argv[1], 'utf8'), context);
assert.equal(times[0].textContent, '2025-01-30 16:00:00 UTC+08:00');
assert.equal(times[1].textContent, '2025-03-09 15:30:00 UTC+08:00');
assert.equal(times[2].textContent, '2025-11-02 14:30:00 UTC+08:00');
assert.equal(times[3].textContent, '1988-07-01 09:00:00 UTC+09:00');
assert.equal(times[4].textContent, 'unchanged');
assert.equal(times[5].textContent, 'unchanged');
assert.equal(times[0].raw, '2025-01-30T08:00:00Z');
assert.match(option.textStyle.fontFamily, /宋体/);
assert.equal(option.yAxis.nameTextStyle.fontFamily, option.textStyle.fontFamily);
assert.equal(option.yAxis.axisLabel.formatter(12000), '12,000');
assert.equal(fallback.hidden, true);
search.value='债券'; search.handlers.input();
assert.equal(rows[0].hidden, true); assert.equal(rows[1].hidden, false);
filter.value='WATCH'; filter.handlers.change(); assert.equal(empty.hidden, false);
sort.value='score-desc'; sort.handlers.change(); assert.deepEqual(body.order, ['乙','甲']);
console.log(JSON.stringify({result: 'PASS', dates: times.length, filters: 'PASS', chart: 'PASS'}));
"""
    result = subprocess.run(  # noqa: S603 - fixed local script and test harness, no external input
        [node, "-e", harness, str(TEMPLATE_DIR.parent / "static/app.js")],
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    assert json.loads(result.stdout)["result"] == "PASS"
