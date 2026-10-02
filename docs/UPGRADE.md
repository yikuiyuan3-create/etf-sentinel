# 从 v0.1.0 升级到 v0.2.0

## 升级包含什么

客户端增加专用的有界候选请求，服务端增加 `/api/v1/plugin/signals?limit=N`。原看板接口保持兼容。升级没有新增数据库迁移，也不会改变数据许可或模拟模式。

新版客户端的 `signals` 需要新版服务配合。旧服务缺少此接口时返回 `UPGRADE_REQUIRED`；诊断、状态、供应商和模型命令仍按健康门禁工作。不要为了消除提示绕过客户端调用全量接口。

## 推荐步骤

1. 下载 [v0.2.0 发布包](https://github.com/yikuiyuan3-create/etf-sentinel/releases/tag/v0.2.0)，验证 ZIP 哈希与解压后文件清单。
2. 在仓库根目录运行 `python3 plugins/etf-sentinel/scripts/doctor.py`，记录当前状态；它不会启动或修改服务。
3. 依照主 README 将 Codex marketplace ref 指向 `v0.2.0`，重新安装并用 `codex plugin list` 确认版本。仅刷新一个仍指向 `v0.1.0` 的固定标签不会升级。
4. 使用 `demo.py prepare` 在发布包之外创建新的运行目录，再查看启动计划。选择不占用的端口，例如 18081；核对后加 `--execute` 启动。
5. 对新端口依次运行 `status`、`providers`、`models`、`signals --limit 5`。确认数据仍为 `DEMO_FIXTURE`、`paper`，调度 `CURRENT`，以及真实数据 `COMPLIANCE_BLOCKED`。
6. 验证完成后按实际运维计划切换使用入口。旧实例和数据应保留到确认不再需要时。

旧工作目录保存了运行时内容哈希。将新文件直接覆盖进去会触发 `RUNTIME_MODIFIED`，这是保护行为。已有持久化数据库的原地更新需要单独备份、兼容性和恢复验证，不属于插件重新安装步骤。

## 回退

保留的旧实例可继续使用原端口；Codex 插件可将 ref 重新指向 `v0.1.0` 后安装。v0.1.0 的 `signals --limit` 仅限制输出，会先读取整批接口数据；严格限制读取条数的场景应保持在 v0.2.0 或停止候选读取。

不要把删除数据卷当作升级或回退步骤。

## 故障处理

| 错误或状态 | 处理 |
| --- | --- |
| `UPGRADE_REQUIRED` | 新客户端连接了缺少专用接口的旧服务；准备新版独立 Demo 并连接其端口。 |
| `RESPONSE_LIMIT_EXCEEDED` | 服务返回了超过所请求条数的数据；保持阻断，核对服务版本。 |
| `NETWORK_ERROR` / `TIMEOUT` | 检查本机地址、端口和服务是否运行。 |
| `DATA_HEALTH_BLOCKED` | 检查 Worker/Beat、调度与数据状态；恢复前不解释候选。 |
| `RUNTIME_MODIFIED` / `WORKSPACE_EXISTS` | 保留原目录；使用新的不存在的独立目录。 |
| `PACKAGE_VERIFICATION_FAILED` | 重新从可信发布页下载并核对哈希，不绕过清单检查。 |

原 v0.1.0 Word 说明书中的截图属于历史 Demo；下载权限及候选读取限制以本版 README 和本指南为准。
