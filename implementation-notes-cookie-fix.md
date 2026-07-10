# Implementation Notes — X cookie 自动刷新链路修复 + guest 降级看门狗

日期：2026-07-10 · 分支：`fix-cookie-refresh-watchdog`

## 症状
- BWG `/root/x_monitor/.auth_cookies.json` mtime 停在 2026-06-22 18:00 EDT（=06-23 06:00 北京），18 天未刷新。
- 2026-07-05..09 日志 2 次 `[GraphQL] cookie 认证失效，降级 guest 模式`，然后静默。
- 无任何 TG 告警。

## 根因（两个独立故障，叠加成"静默 18 天"）

### 根因 A — macOS 更新重置 TCC，launchd 失去完全磁盘访问
- `/tmp/x-cookie-refresh.log`：`[Errno 1] Operation not permitted: '.../Chrome/Default/Cookies'`。
- `InstallHistory.plist`：macOS 27.0 于 **2026-06-23 06:02:14** 安装完成。
- 本地备份 `~/.local/share/x_cookies_backup.json` 最后成功 mtime = **06-23 06:00**（更新完成前 2 分钟那次 06:00 跑成功，之后每天 06:00 全部被 TCC 拒绝）。
- 沙箱关闭后 `head` 读 Cookies 仍 `Operation not permitted` → 确认是系统级 TCC，不是工具沙箱。
- 结论：OS 更新清空/重排 TCC 授权，launchd 里 `/opt/homebrew/bin/python3` 读 Chrome Cookies 的完全磁盘访问被撤销。

### 根因 B — 失败告警通道早已失效（所以 18 天没人知道）
- `refresh_x_cookies.py._get_tg_token()` 从 `com.chat-daily-tg.agent.plist` 的 EnvironmentVariables 读 `TG_BOT_TOKEN`。
- 该 plist 现在 env 只有 `PATH`——chat-daily-tg 重构后把 token 挪进了 `~/chat-daily/.env`。
- 于是 `token=None` → `notify_tg()` 静默 return。任何失败都发不出告警。

## 部署事实（澄清路径歧义）
- 脚本原本 push 到 `/root/vista8_monitor/.auth_cookies.json`，但 cron 跑的是 `/root/x_monitor/run.sh`（`*/30`）。
- 两处 `.auth_cookies.json` 是**同一 inode（58243）硬链接**，`cat >` 原地截断保留 inode，所以历史上 push vista8 也喂到了 x_monitor。
- 硬链接是隐患：一旦 `_clear_auth_cookies()` 把 x_monitor 侧改名成 `.stale`，push vista8 就再也喂不到 x_monitor（同一静默故障形态）。

## Design Decisions
- **告警 token 源改读 `~/chat-daily/.env`**（与 chat-daily-tg 管线同源），plist 作降级回退，最后回退 `os.environ`。`TG_CHAT_ID` 同样从 env 读，硬编码 8424944105 作兜底（两者一致，已核对）。
- **BWG push 路径改为权威的 `/root/x_monitor/.auth_cookies.json`**，去掉对硬链接的隐式依赖。
- **push 改原子写 + 显式 600 + 落地后校验**（`stat` 回读 `%a`/`%Y`）。原子 `mv` 会断硬链接，故 best-effort `ln -f` 重新把 vista8 链回，保持"两目录同文件"旧不变量的超集，零行为回退。
- **TCC 拒绝单独识别**：`PermissionError`/errno 1 命中 Cookies 路径时，给出可操作告警文案（指向 系统设置>隐私与安全性>完全磁盘访问 重新授权 python3），让下次失败自解释。
- **BWG 侧看门狗**：guest 降级是"静默降级"不是"拉取失败"，现有 `note_account_failure`（按账号拉取失败）抓不到——guest 模式仍能拉公开时间线，账号不算 failure。故新增独立的"连续 N 轮未取得 authed 访问 → 告警一次"看门狗，复用失败态持久化 + 告警/置顶/恢复取消置顶的既有骨架。
- 阈值 `COOKIE_DEGRADE_ALERT_THRESHOLD = 6`（`*/30` cron ≈ 3h）。比按账号的 4 略高，滤掉 X 侧 `Internal server error`/`Timeout` 瞬时抖动导致的误报；持久性 cookie 过期仍会当天早上触发。

## Deviations
- 未用 computer-use/GUI 自动授予完全磁盘访问：TCC 开关受 SIP 保护、需 Touch ID/密码，无法脚本化，属用户手动一步。代码侧全部修好并可端到端自证（push+权限+看门狗），Chrome 读取这一环等授权后一条命令即完成真实刷新。

## Tradeoffs
- 阈值 6 vs 4：宁可晚 ~1h 告警，也不想被 X 侧偶发 5xx 触发 cookie 误报。真实过期是持久的，6 轮内必触发。
- 保留 vista8_monitor 硬链接（best-effort `ln -f`）而非删除：vista8 目前无 cron 读取（已核对 crontab），但保持旧不变量成本近零、消除"我漏看了某处读 vista8"的风险。

## Open Questions
- Jun 22 的 auth_token 是否已真过期？需 FDA 授权后一次真实提取 + 拉取验证才能定论（当前只能证明管道，不能证明 token 新鲜）。
- 是否要把每日 06:00 提取改成对 OS 更新更鲁棒的方案（如 CDP 从活跃 Chrome 读、或授权稳定二进制）？本次不做（简单优先），靠修好的双告警（Mac 端即时 + BWG 端 3h 内）兜住再次断裂。
