# X Monitor 去重发布集成记录（2026-09-24）

本分支从公共仓库 `origin/master` 的 `1b0c439` 创建，随后同步 2026-09-23 BWG `/root/x_monitor` 的运行源码，并应用已验证的去重实现。

## 集成范围

当前运行版本包含 semantic bundle、事件账本、Telegram 锚点、文章队列、学习 feed sidecar，以及以下去重文件：

- `quote_fold.py`：同目标群组和话题内的 confirmed 原文折叠；跨来源信息不足时 fail-open。
- `thread_merge.py`：只对 `dev_release`、`model_api`、`model_launch`、`major_product_launch` 进入 90 秒 idle window；额度和权益事件首条立即处理。
- `reorder_twitter_accounts.py`：预览并安全重排已配置的官方账号顺序，保留账号对象字段和其他账号的位置。
- `event_ledger_review.py`：列出并人工标注 `would_suppress` 候选。

`run.sh` 和 `sync_sent_content_ledger.sh` 保留 BWG 当前 sidecar 语义。sidecar 失败不会覆盖主 monitor 的退出码。

## 分阶段部署

第一阶段只在现有 `config.json` 中加入：

```json
{"translation_reply_enabled": true}
```

先执行 `python3 twitter_monitor.py --dry-run`，确认启动日志中的去重开关和每条 `quote-fold dry-run` 决定。预览路径使用只读 event ledger，不恢复 stale pending，也不写 seen、retry、ledger 或 journal。

账号顺序调整使用：

```bash
python3 reorder_twitter_accounts.py /root/x_monitor/twitter_accounts.json
python3 reorder_twitter_accounts.py /root/x_monitor/twitter_accounts.json --apply --backup
```

默认顺序约束为 `claudeai → ClaudeDevs`、`OpenAI → OpenAIDevs → thsottiaux`。正式应用前备份账号配置，确认计划输出只改变目标账号槽位。

官方圈配置和文本 thread 合并必须在观察两天后分阶段启用。事件账本 enforce 仍需至少 20 条人工复核且误判率不超过 2%。

## 排除项

本分支没有复制 BWG 的 `config.json`、`twitter_accounts.json`、`twitter_ai.json`、cookie、token、状态库、seen 文件、article 缓存、semantic journal、运行日志或 `.monitor.lock`。仓库中原有公开账号列表保持不变；部署时保留目标机实际账号配置，不用仓库示例覆盖。

## 验证

工作副本使用 Python 标准库执行离线完整回归，共 519 项通过，其中包括学习 feed 的 5 项测试。BWG 线上目录未在本次集成中写入、启动或发送消息。
