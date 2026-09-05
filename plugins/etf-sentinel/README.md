# ETF Sentinel · Codex 插件

版本 0.1.0。企业内部 ETF 研究与风险监测；`paper` 模式；默认且当前仅提供 `DEMO_FIXTURE`。这是可安装的本地技能插件与随包应用，不是官方插件商店上架，不是证券投资建议或真实交易系统。

## 能做什么

- 从已运行的本地服务读取健康、小时监测、许可状态、Demo 候选与模型摘要。
- 在用户指定的新目录准备并启动独立 FastAPI/PostgreSQL/Redis/Celery/Beat 演示环境。
- 自带中文看板、固定数据夹具、数据库迁移、含防泄漏校验的回测、模拟账本和测试；中文使用宋体字体族，数值统一格式。
- 每 1 或 2 小时检查现有 Demo 数据与研究状态。**没有真实行情小时采集**；不会把演示基金改成真实名称。

## 安装与首次使用

GitHub 私有仓库只对获授权的账号开放；需要现有 GitHub 访问权限，不需要行情 API key。插件本身使用 Python 3.11+ 标准库；随包应用需要 Docker Compose，或 Python 3.12/3.13 + uv。

从仓库根目录直接验证与使用（无需修改 Codex 配置）：

```sh
python3 -m unittest discover -s plugins/etf-sentinel/tests -v
python3 plugins/etf-sentinel/scripts/sentinel.py status
```

安装到 Codex 的操作会修改个人插件配置，由用户自行执行或另行授权。当前 CLI 0.144.3 帮助已核验以下命令；本次发布不自动安装：

```sh
codex plugin marketplace add yikuiyuan3-create/etf-sentinel --ref v0.1.0
codex plugin add etf-sentinel --marketplace etf-sentinel-internal
```

仓库目录文件为 `.agents/plugins/marketplace.json`，使用独立市场名 `etf-sentinel-internal`，避免覆盖已有 `personal` 市场。安装后在新任务中使用 `$etf-sentinel`。当前版本未执行个人环境安装/重启验收。

## 启动独立演示

下列命令在 GitHub 仓库根目录执行。将 `/absolute/new/demo-workspace` 换成已存在父目录下的全新目录；不得指向已有工作区或数据库。现有看板端口 18080 可继续使用，新实例示例用 18081。

```sh
python3 plugins/etf-sentinel/scripts/demo.py prepare --workspace /absolute/new/demo-workspace
python3 plugins/etf-sentinel/scripts/demo.py start --workspace /absolute/new/demo-workspace --port 18081 --interval-hours 1
# 确认计划后才实际启动：
python3 plugins/etf-sentinel/scripts/demo.py start --workspace /absolute/new/demo-workspace --port 18081 --interval-hours 1 --execute
python3 plugins/etf-sentinel/scripts/sentinel.py --base-url http://127.0.0.1:18081 status
```

打开 `http://127.0.0.1:18081/`。默认计划不会启动进程；执行时需要已运行的 Docker，首次拉取锁定镜像和依赖可能耗时。启动后会入队一次幂等的初始健康分析；入队不等于完成，请等 Worker 完成后用 `status` 检查。未完成或失败时维持阻断，不展示旧候选。应用的实际数据截止日期会明确显示，不能用运行时间代替。

小时任务由 Compose 的 Beat/Worker 执行；仅启动 Uvicorn 不包含调度。查看状态不改变任务频率。用户数据写入准备好的独立运行目录/对应 Docker 卷，插件安装目录仅作源文件使用；不存在自动停止、清空或卸载时删除数据的钩子。

## 只读命令

```sh
python3 plugins/etf-sentinel/scripts/sentinel.py status
python3 plugins/etf-sentinel/scripts/sentinel.py providers
python3 plugins/etf-sentinel/scripts/sentinel.py signals --limit 10
python3 plugins/etf-sentinel/scripts/sentinel.py models
```

只允许本机数值 loopback 地址/规范化 localhost 和端口，不跟随重定向，不继承 HTTP 代理。服务中断、结构错误、数据模式变化或门禁失败时返回安全错误，不回退缓存。不会读取 `/portfolio`、`/audit` 或调用写接口；非 Demo 分析默认不进入 Codex。

## 测试与复现

插件测试没有额外 Python 依赖：`python3 -m unittest discover -s plugins/etf-sentinel/tests -v`。

应用测试在准备好的运行目录执行：

```sh
make test
make test-ui
```

版本锁在 `plugins/etf-sentinel/runtime/uv.lock`，所有数据为固定合成夹具，失败模型明确保留。发布目录不带 `.env`、数据库、Parquet、缓存、备份、询价邮件、NAS 内容或原始开发 Git 历史。`PACKAGE_MANIFEST.json` 保存每个发布文件 SHA-256；验证脚本 `python3 tools/verify_package.py` 检查清单、异常文件与包内容。模式扫描不能证明识别所有类型的密钥或私人资料，上传前仍须复核文件清单。

## 安全、数据权利与许可

代码沿用 `Proprietary - internal use only`，未擅自改为开放源代码许可证；第三方依赖许可证不因本声明改变。私有 GitHub 上传不等于批准对外服务、金融资质、商业行情使用或向模型传输真实数据。

真实接入仍须逐用途的数据许可、质量、防泄漏、模型漂移与人工复核。不能用 API key、免费额度或免责声明代替授权。当前没有真实账户/持仓/交易能力，Demo 数据不能用于实际资金决策。

本系统仅供本公司授权人员开展内部研究和风险监测。内容由统计模型和人工智能生成，可能错误、遗漏或滞后，不构成证券投资建议、收益承诺或交易指令；历史或回测表现不代表未来。任何投资决定须由授权人员结合独立资料、风险承受能力和持牌机构意见审慎作出，交易仅可在合法持牌券商端人工确认。

## 格式来源

2026-09-05 核验 [OpenAI 插件打包文档](https://developers.openai.com/plugins/build/plugins)及本机 CLI 帮助。插件清单使用 `.codex-plugin/plugin.json`，技能放在 `skills/`。本项目仅 GitHub 私有分发，不提交公众插件目录。
