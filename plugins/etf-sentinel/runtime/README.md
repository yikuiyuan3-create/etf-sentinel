# ETF Sentinel · 独立 Demo 运行时

仅供企业授权人员内部研究与风险监测，`DEMO_FIXTURE`、`paper`，不含真实下单。固定夹具的数据日期与检查时间分别显示；每 1/2 小时调度不是采集实时行情。

## 启动与验证

推荐从仓库根目录使用 `plugins/etf-sentinel/scripts/demo.py` 准备独立副本并按计划启动，详细步骤见插件 README。直接在本运行目录启动适用于明确隔离的环境：

```sh
APP_PORT=18081 docker compose up --build -d
docker compose ps
docker compose exec -T web python /app/scripts/verify-compose.py
docker compose exec -T web python /app/scripts/verify-hourly-compose.py
```

请先确认端口及 Compose 项目名不与现有实例冲突。不要直接启动多个共用项目名的副本。

本地快速 Demo 使用 Python 3.12/3.13、uv：`make demo`。此路径不含 Celery Beat，完整小时调度请用 Compose。`make test` 执行应用测试；安装 Node.js 后 `make test-ui` 验证监测脚本。

## 模块

- `src/etf_sentinel/providers`：固定合成夹具、许可注册、受限供应商适配器。真实源默认禁用。
- `services`：点时特征、规则/校准基线、失败关闭风控、回测、模拟账本、预警、小时监测。
- `alembic`：PostgreSQL/SQLite 迁移；`templates/static`：中文看板，宋体字体族与规整数值。
- `tests`：防未来数据泄漏、下一 bar 成交、成本敏感性、账本守恒、幂等、故障与展示门禁。

## 恢复与运维

仅在独立固定 Demo 环境运行 `make recovery-drill`；先查看 `scripts/recovery-drill.py --help`。演练使用隔离恢复目标，不覆盖源数据库。备份不是用户真实数据的出境许可，生产备份须另外实施加密、权限、离线保留和恢复验证。

不要上传运行中 `.env`、`var/`、数据库、Parquet、备份、审计业务记录或任何凭证。容器卷是本机状态，不随插件分发。

## 限制

真实数据合同、生产增量摄取、多市场日历/退市全样本、远程 SSO/RBAC 和生产灾备仍未放行。模型失败结果须保留 `REJECTED/EXPERIMENTAL`，不能解释成真实资金推荐；禁止把合成 ETF 改名为真实标的。

本系统仅供本公司授权人员开展内部研究和风险监测。内容由统计模型和人工智能生成，可能错误、遗漏或滞后，不构成证券投资建议、收益承诺或交易指令；历史或回测表现不代表未来。任何投资决定须由授权人员结合独立资料、风险承受能力和持牌机构意见审慎作出，交易仅可在合法持牌券商端人工确认。
