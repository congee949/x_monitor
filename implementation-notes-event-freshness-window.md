# Implementation Notes — 官方号事件级新鲜度窗口（2026-07-19）

设计出处：chat-daily-tg 仓库 `docs/spark/2026-07-18-official-x-push-policy-design.md`（§freshness 覆盖）。
诱因：cron 30 分钟一轮 + 统一 45 分钟窗口，一轮失败或边缘时刻发推即静默丢
（2026-07-19 实测 ClaudeDevs weekly limits 公告补投时已 39 分钟，差点超窗）。

## Design Decisions

- **窗口合成用 `max(基础窗口, 事件窗口)`，不是直接替换。** 设计文档写"覆盖"，
  但两处基础窗口可能比事件窗口更宽：seen 损坏安全模式的 1440，以及 CLI 显式
  调大的 `--max-push-age-minutes`。取 max 保证事件窗口只放宽、永不反向收紧。
- **`base <= 0`（窗口关闭/不限龄）原样透传**，事件窗口不重新收紧，与
  `is_within_push_window` 的既有语义一致。
- **窗口判定挂在 `_push_event_type` 注解上**，该注解只在 classify 结果为 pass
  时写入——filter 掉的官方推文和普通账号推文都拿不到扩展窗口，维持 45 分钟默认。
  原注解注释"rendering/logging only"已更新（现在还驱动窗口判定）。

## Deviations

- 无。事件→窗口映射逐字来自任务规格（7 个 reset/发布类 → 360，4 个权益类 →
  1440），与设计文档 6h/24h 一致；`classify_official_push` 能发出的 11 种事件
  类型全部有映射。

## Tradeoffs

- 超窗事件仍只记 seen 不推（设计文档验收项 13 原文如此），不进 push_retry；
  push_retry 继续只承接"发送失败"的推文并绕过窗口（行为未动）。

## Open Questions

- 无。

## 验证

- bwg `python3 -m unittest -b test_twitter_monitor`：241 tests OK（新增 8 个：
  `EventPushWindowTest` 6 个单元 + 2 个 `process_user` 集成——20h quota_policy
  与 5h quota_reset 补推、8h quota_reset 超窗只记 seen、普通账号 46 分钟仍 stale）。
- 备份：`twitter_monitor.py.bak-event-freshness-window-20260719` /
  `test_twitter_monitor.py.bak-event-freshness-window-20260719`。
