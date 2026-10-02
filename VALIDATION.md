# ETF Sentinel v0.2.0 发布验证

验证日期：2026-10-03。范围：公开源码发布、只读插件、独立固定 Demo；`DEMO_FIXTURE / paper`。

## 已在本次升级中实际执行

| 验证项 | 结果 |
| --- | --- |
| 插件标准库回归 | 75 项通过（客户端 29、启动器 39、诊断 7） |
| 发布工具回归 | 20 项通过，覆盖重复构建、篡改、额外文件、敏感文件、软链接、版本/模式错误 |
| 新候选接口集成 | 14 项通过；真实 SQLite Demo 流水线、SQL limit、HTTP 字段裁剪及门禁 |
| 页面监测 JavaScript | 26 项通过 |
| 修改过的 Python 文件静态检查/格式 | 通过 |
| 旧发布历史与旧 ZIP 复核 | 1 个净化后根提交，95 个跟踪文件；ZIP 与 tag 文件一致 |
| 发布内容检查 | 未发现真实凭据、私人联系方式、内部业务数据或原开发历史 |

新接口测试先在旧实现上观察到预期失败，再验证修复。`signals --limit N` 的限量针对候选选择与网络响应；服务器内部仍执行全局健康检查。

## 自动发布门禁

[GitHub Actions](https://github.com/yikuiyuan3-create/etf-sentinel/actions/workflows/ci.yml) 对提交执行以下检查，具体状态以该提交运行结果为准：

- 75 项插件回归与 20 项发布工具回归。
- 提交中的文件清单原样校验，不在 CI 中自动补写哈希。
- 同一源版本两次生成 ZIP，校验文件必须一致。
- Python 3.12、uv 冻结锁定依赖下执行完整应用回归（247 个用例）。
- Node.js 22 下执行 26 项页面监测测试。

发布资产由 `tools/build_release.py` 生成，上传后应通过匿名下载和 SHA-256 读回核验。对应执行结果在版本发布说明中记录。

## 验证边界

- SQLite Demo 与本地 HTTP/ASGI 契约测试不替代新一轮 PostgreSQL/Redis/Celery 的完整 Compose 演练。本次没有重启现有服务或更新其数据库。
- 源文件与 ZIP 可重复构建；容器镜像使用版本标签，不声明完整镜像环境逐字节一致。
- 保留现有依赖的 Starlette/httpx 与 AnyIO 弃用提示；本次未扩展为整个依赖栈升级。
- 未验证其他操作系统、CPU 架构或真实行情。真实数据许可保持阻断，没有真实交易能力。
- 内容扫描是有限规则与人工复核，不是识别一切未知格式凭据的保证。
- v0.1.0 的 2026-09-05 Compose 验收属于历史证据，不能当成本版重新执行。

## 手工复验

在发布包根目录运行：

```sh
python3 tools/verify_package.py
python3 -m unittest discover -s plugins/etf-sentinel/tests -v
python3 -m unittest discover -s tests -v
```

运行时完整测试按 README 在独立目录中执行；不要在插件缓存中启动应用或存放工作数据。
