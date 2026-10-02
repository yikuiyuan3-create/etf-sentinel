# ETF Sentinel · Codex 插件

**v0.2.0 · 中文内部研究与风险监测 · 公开源码仓库**

ETF Sentinel 提供可安装的 Codex 技能、本机只读客户端和独立 Demo 看板。当前数据始终是固定合成的 `DEMO_FIXTURE`，仅支持 `paper` 模式；它适合内部流程演示、研究复核与风险监测，不提供真实行情或交易能力。

公开仓库允许查看和下载代码，使用仍受 [内部授权使用声明](LICENSE) 约束。本次未改为开放源代码许可证，也未上架官方插件商店。

## 下载

- [最新发布页](https://github.com/yikuiyuan3-create/etf-sentinel/releases/latest)
- [v0.2.0 完整插件 ZIP](https://github.com/yikuiyuan3-create/etf-sentinel/releases/download/v0.2.0/etf-sentinel-v0.2.0.zip)
- [v0.2.0 SHA256SUMS](https://github.com/yikuiyuan3-create/etf-sentinel/releases/download/v0.2.0/SHA256SUMS)
- [更新记录](../../CHANGELOG.md) · [升级指南](../../docs/UPGRADE.md) · [验证记录](../../VALIDATION.md)

下载 ZIP 和校验文件到同一目录，在 macOS/Linux 运行 `shasum -a 256 -c SHA256SUMS`。解压后进入发布包根目录，执行 `python3 tools/verify_package.py`，应得到 `VERIFIED`、`0.2.0`。哈希用于检出文件变化，不能替代可信下载来源。

## 本次升级

1. **真正按条数读取候选**：`signals --limit 5` 请求专用接口，服务端最多返回 5 条及 20 个研究字段。详细证据、来源链接和风险后缀不进入该响应。旧服务缺少接口时返回 `UPGRADE_REQUIRED`，不回退读取整批数据。
2. **中文只读诊断**：`doctor.py` 检查 Python、插件元数据、运行时文件、Docker 命令是否存在和服务健康，给出排障提示。
3. **发布可核验**：版本/模式/清单一致性校验、可重复生成的 ZIP、SHA-256 校验文件与 GitHub Actions 自动测试。

## 安装与首次使用

客户端需要 Python 3.11+，只使用标准库。已有服务可直接读取；新建完整 Demo 需要已安装并运行的 Docker Compose，或 Python 3.12/3.13 与 uv。公开仓库的下载无需私有仓库授权。

在本机 Codex CLI 安装：

```sh
codex plugin marketplace add yikuiyuan3-create/etf-sentinel --ref v0.2.0
codex plugin add etf-sentinel --marketplace etf-sentinel-internal
codex plugin list --marketplace etf-sentinel-internal --json
```

市场名 `etf-sentinel-internal` 为兼容旧安装而保留，表示产品用途。安装后在新任务中输入：

> 使用 $etf-sentinel 先检查数据模式、行情截止和调度状态；健康通过后读取最多 5 条 Demo 候选，说明模型限制及待人工复核事项。

这些命令依据本次核验的 Codex CLI 0.144.3 帮助；不同版本请先查本机 `codex plugin --help`。安装操作更新所选插件；运行中的服务和数据库需按升级指南另行维护。

## 只读使用

以下命令均在解压后的发布包根目录运行：

```sh
python3 plugins/etf-sentinel/scripts/doctor.py
python3 plugins/etf-sentinel/scripts/sentinel.py status
python3 plugins/etf-sentinel/scripts/sentinel.py providers
python3 plugins/etf-sentinel/scripts/sentinel.py models
python3 plugins/etf-sentinel/scripts/sentinel.py signals --limit 5
```

默认连接 `http://127.0.0.1:18080`。其他本机端口需把参数放在子命令之前：

```sh
python3 plugins/etf-sentinel/scripts/sentinel.py --base-url http://127.0.0.1:18081 signals --limit 5
python3 plugins/etf-sentinel/scripts/doctor.py --base-url http://127.0.0.1:18081
```

只允许 loopback 地址、不继承代理、不跟随重定向；请求超时默认 5 秒。客户端仅 GET。门禁失败时返回结构化错误，不解释旧候选。数量范围 1–20，默认 10；服务器内部仍会做完整健康检查，此限制指候选选择与网络响应条数，不承诺数据库全部检查只读 N 行。

诊断不会启动 Docker、核验 Docker daemon、替代包哈希校验或证明候选接口兼容；这些状态分别显示。没有 Docker 命令不妨碍连接已有健康服务。

## 启动独立演示

仅在需要新实例时执行。将示例路径换成**发布包之外、父目录已存在、目标尚不存在**的绝对目录；避免覆盖原工作区。新实例示例用 18081 端口：

```sh
python3 plugins/etf-sentinel/scripts/demo.py prepare --workspace /absolute/new/demo-workspace
python3 plugins/etf-sentinel/scripts/demo.py start --workspace /absolute/new/demo-workspace --port 18081 --interval-hours 1
# 核对计划；确认需要启动后执行：
python3 plugins/etf-sentinel/scripts/demo.py start --workspace /absolute/new/demo-workspace --port 18081 --interval-hours 1 --execute
python3 plugins/etf-sentinel/scripts/sentinel.py --base-url http://127.0.0.1:18081 status
```

打开 `http://127.0.0.1:18081/`。首次执行会构建锁定依赖、迁移数据库并初始化合成 Demo。`STARTED` 及分析入队不等于分析完成；须等 Worker 完成后确认 `CURRENT / DEMO_ANALYSIS`。

每 1 或 2 小时任务由本地 Compose 的 Beat/Worker 执行；机器关机、休眠或 Docker 停止时不持续运行。它不会唤醒 Codex，也不采集新市场价格。数据截止时间与检查时间分开显示。

## 应用场景与边界

- **内部晨检**：先核验服务、许可状态和调度，保留检查时间与数据截止。
- **研究流程演示**：观察固定 Demo 候选及风险阻断，练习人工复核和来源追溯。
- **模型复核**：明确展示 `REJECTED / EXPERIMENTAL`，不把失败或实验模型包装为可投产结论。

真实数据仍为 `COMPLIANCE_BLOCKED`。当前没有生产认证/RBAC，不得将本机端口暴露到公网。插件不会读取持仓、账本、审计 payload、新闻正文或账户凭证；无真实订单、外部营销和购买导流。

## 开发与复验

```sh
python3 -m unittest discover -s plugins/etf-sentinel/tests -v
python3 -m unittest discover -s tests -v
python3 tools/verify_package.py
```

应用测试在独立运行目录中按 [运行时说明](runtime/README.md) 执行；JavaScript 监测测试使用 Node.js。依赖版本保存在 `runtime/uv.lock`。发布包 ZIP 的文件内容及元数据可重复生成，Docker 基础镜像仍按版本标签引用，不声明整个容器环境逐字节一致。

维护者修改代码后先显式暂存审阅过的发布文件，再更新清单与构建。构建目录必须位于仓库外且尚不存在：

```sh
python3 tools/build_release.py --write-manifest
python3 tools/build_release.py --output-dir ../etf-sentinel-v0.2.0-dist
```

构建器只接受受 Git 跟踪且通过内容检查的文件。发布包不含数据库、密钥、备份、NAS 资料或原开发历史。第三方依赖保持各自许可证。

## 固定使用提示

本系统仅供本公司授权人员开展内部研究和风险监测。内容由统计模型和人工智能生成，可能错误、遗漏或滞后，不构成证券投资建议、收益承诺或交易指令；历史或回测表现不代表未来。任何投资决定须由授权人员结合独立资料、风险承受能力和持牌机构意见审慎作出，交易仅可在合法持牌券商端人工确认。
