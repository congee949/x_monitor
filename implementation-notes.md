# Implementation Notes

## Design Decisions

- Follow-up correction: article images are no longer summarized in a separate 900-token pre-pass. When images exist, the article Markdown and images are sent together in one multimodal summary call so images serve as supporting evidence for understanding the article, not as a standalone output section.

- Article summary output now treats the AI result as the message body only: the Telegram wrapper no longer adds author/title metadata, and the prompt explicitly tells the model not to repeat metadata or links.
- Long article summaries are split into multiple Telegram-safe HTML messages instead of being truncated to one 3300-character body.
- Article images are extracted from Markdown/HTML image tags, downloaded temporarily in memory, described through the configured AI backend vision API, and folded into the text summary; if no image can be read, the article summary still proceeds.

- Article handling uses the queued worker path: detection only enqueues jobs; the automatic queue processor fetches Markdown, summarizes with existing AI backends, sends Telegram, and cleans cached Markdown after send success.
- `article_markdown_cmd` is configured to `/usr/local/bin/x-article-to-markdown`, a wrapper around the installed `baoyu-danger-x-to-markdown` script bundle at `/root/baoyu-danger-x-to-markdown`.
- The worker prefers the source tweet URL when `tweet_id` is available, falling back to the bare article URL only when no tweet id exists.
- Article summary Telegram messages now match normal tweet push style: the message body has no visible source link and sends with no link preview card or inline button.
- AI Markdown output is converted to Telegram HTML before sending, so bold, inline code, bullets and numbered lists render instead of appearing as raw Markdown.
- Failure states are formatted as Telegram-visible status messages so failed article jobs are observable without SSHing into the server.
- Empty or obviously invalid Markdown bodies are treated as fetch failures instead of being sent to AI for misleading summaries.

## Deviations

- The monitor directory and cron path were renamed from the legacy Vista8-specific name to `/root/x_monitor` because the script now monitors multiple X/Twitter accounts.
- `/root/vista8_monitor` remains as a symlink to `/root/x_monitor` for compatibility with old references.

## Tradeoffs

- The worker keeps failed jobs in the queue with attempts and last_error instead of deleting them, trading small state growth for retry/debug visibility.
- Failed article jobs may send Telegram failure notices up to the retry limit so failures are visible without reading logs.
- Fetching via tweet URL is preferred because X sometimes returns `{}` for `ArticleEntityResultByRestId` while the tweet payload contains the full embedded article.
- The Markdown renderer intentionally supports only the Telegram-safe subset currently needed for AI summaries, instead of a full Markdown parser.

## Open Questions

- None for the current implementation. The historical dotey article has been fetched, summarized by Gemini, sent to Telegram without visible link/preview, and cleaned up successfully.

---

## 2026-06-02 — Push reliability + crash-safety pass

### Design Decisions
- **Seen marking is now success-gated** (REL-1/FMT-1): `seen |= (new_ids - push_failed)`. Tweets routed to push are marked seen only if the send returned ok; failed sends stay unseen and retry on the next cron run, bounded by the existing push-age window. Intentionally-skipped tweets (auto_seed / seed / stale / filtered) are still marked seen as before.
- **send_telegram is resilient**: retries 429 (honoring `parameters.retry_after`, capped 30s) and 5xx with bounded backoff (3 attempts); on a 400 (usually an HTML parse error) it degrades ONCE to plain text (`_html_to_plain`) so the message is delivered instead of raising → being dropped. Raises only after retries are exhausted, so the caller leaves the id unseen.
- **State writes are atomic** (STATE-1): `_atomic_write` (tmp + fsync + os.replace) for seen files, article queue, and markdown cache — no more truncate-in-place corruption on crash/overlap.
- **Single-run lock** (LOCK-1): non-blocking `fcntl.flock` on `.monitor.lock` at the top of main(); an overrunning run no longer overlaps the next 30-min cron tick (which caused double-sends + last-writer-wins state clobber). Verified live: lock-held → skip+exit before any fetch; lock-free → proceeds.

### Open Questions / Follow-ups
- ART-1 (article queue persists once per account at loop end) is mitigated by atomic writes but not fully fixed; a mid-account crash can still re-push the in-flight article. Per-entry persistence after each terminal transition is the remaining hardening.
- ESC-1 (TL;DR double-escape) / PRE-1 (code-fence <pre>) / SPLIT-1 (split mid-tag) are cosmetic-to-medium formatting bugs left for a follow-up; none silently drop messages now that send_telegram has a plain-text 400 fallback.
- SEC-3: config.json / twitter_ai.json hold secrets in cleartext on the VPS — confirm chmod 600 and whether the bot token equals the (now-redacted, to-be-rotated) Taoli98Bot token.

---

## 2026-06-03 — Three latent-bug fix pass

Fixed the three priority bugs flagged in the 2026-06-02 multi-agent eval. All were latent (masked by daily cookie refresh / active accounts / slow disk growth), none affected live pushes, but all were real.

### Deviations / Fixes
- **BUG-1 (twitter_graphql.py `get_user_id`)**: `variables` was assigned only inside `if ah:` yet used unconditionally in the URL → guest fallback (`ah == {}`) hit a `NameError`, making the guest path a dead end. Moved the assignment out of the `if` so it is always defined. Verified offline: guest path now returns `None` gracefully instead of raising.
- **BUG-2 (logrotate)**: config still targeted the stale `/var/log/vista8_monitor.log`; the real log `/var/log/x_monitor.log` (≈749K) was never rotated. Replaced `/etc/logrotate.d/vista8_monitor` with `/etc/logrotate.d/x_monitor` pointing at the correct path (kept weekly/rotate 4/maxsize 5M/copytruncate). NOTE: this file lives under /etc, outside this repo, so it is not tracked here. `logrotate -d` dry-run confirms it is now picked up.
- **BUG-3 (twitter_monitor.py idle-clear loop)**: an account idle ≥ `INACTIVE_DAYS` (7) had its seen file wiped, which then triggered `auto_seed` and swallowed the first tweet on revival (recorded, not pushed) — losing the comeback signal (notably Vida_BWE). Removed the idle-clear branch entirely; `save_seen` already caps seen at 500 ids so there is no unbounded growth. `last_post_iso` keeps its loaded value and still feeds `latest_ts` correctly.

### Cleanup (dead code created by BUG-3 fix)
- Removed the now-unreachable `clear_seen()` helper (only caller was the deleted idle branch).
- Removed the now-unused `INACTIVE_DAYS` module constant.

### Verification
- `py_compile` clean on both files; no residual references to `clear_seen` / `INACTIVE_DAYS`.
- Existing unittest suite: 11/11 pass.
- Live `run.sh` end-to-end: normal run, token pool 5/5, GraphQL primary path working.

---

## 2026-06-03 — Medium/low-priority pass (CAT1–CAT4)

Addressed the remaining eval items the user explicitly opted into.

### CAT1 — Security + cleanup
- `chmod 600` on `.auth_cookies.json`, `config.json`, `twitter_ai.json` (were 644; dir is 700 so low risk, done as defense-in-depth).
- Removed dead `vista8_monitor.py` (git-tracked) and untracked clutter (`*.bak*`, `*.before-*`, `vista8_seen_ids.json`). `.gitignore` already excludes secrets/state/backups.

### CAT2 — Cookie SPOF investigation (conclusion)
- **Finding:** `.auth_cookies.json` is **manually maintained**. `twitter_graphql._save_auth_cookies()` exists but has **no caller anywhere** — it is a dead refresh hook, NOT an auto-refresh. There is no cron/timer/script that refreshes cookies; the only cron is the */30 monitor run. When `auth_token`/`ct0` expire, the cookie path fails.
- **Mitigation (not a rewrite):** cookie expiry is no longer silent — see CAT4 guest fallback, which now prints `[GraphQL] cookie 认证失效，降级 guest 模式` (greppable) instead of quietly burning paid 6551.io credits.
- Pre-existing dead `_save_auth_cookies` is intentionally **left in place** (flagged, not deleted) as the hook for a future Set-Cookie-capture auto-refresh. Proper auto-refresh (capturing rotated ct0 from response headers) is deferred.

### CAT3 — Telegram formatting (ESC-1 / PRE-1 / SPLIT-1)
- **ESC-1** (`format_message`): the TL;DR branch escaped `preview` and then `format_message` escaped `body` again → `&`/`<` became literal `&amp;`/`&lt;`. Removed the inner escape so the unified escape runs once.
- **PRE-1** (`markdown_to_telegram_html`): fenced code was converted to `<pre>` first, then the per-line loop re-`html.escape`d it, turning the tags into literal `&lt;pre&gt;`. Now code fences are stashed behind a placeholder before line processing and restored after.
- **SPLIT-1** (`split_telegram_html`): added `_balance_html_chunks` so an inline tag (`<b>/<i>/<code>/<pre>`) spanning a chunk boundary is closed and reopened, making every chunk valid standalone HTML; also guarded the char-level cut so it never lands inside a `<...>` tag.

### CAT4 — Robustness
- **GraphQL dropped tweets** (`twitter_graphql.fetch_tweets`): tweets wrapped in `TweetWithVisibilityResults` carry their `legacy` under `["tweet"]`; the old `tweet_result.get("legacy")` returned None and `continue`d, silently dropping them. Now unwraps the visibility wrapper before reading `legacy`.
- **Guest fallback on auth failure**: when cookies are present but the authed call returns `errors`, the code previously raised → the wrapper fell back to paid 6551.io. Now it retries once with a free guest token first (and logs the degradation), only raising if guest also fails.
- **Zero-tweet alert** (`process_user`): an empty timeline from all sources now prints a greppable `⚠️ WARN: @user 拉到 0 条推文` instead of the indistinguishable-from-normal silent `没拉到推文`.

### Verification
- `py_compile` clean (incl. tests). Full suite **15/15 pass** (11 existing + 4 new regression tests for ESC-1/PRE-1/SPLIT-1/visibility-unwrap).
- Live `run.sh`: all 6 accounts via GraphQL primary path, token pool 5/5.

---

## 2026-06-10 — 调度优化 + 新账号（本轮）

### Design Decisions
- **告警阈值** `FAIL_ALERT_THRESHOLD = 4`（*/30 cron ≈ 2 小时）：单轮抖动不告警；阈值时只发一次 TG（`alerted` 标记），恢复由 `note_account_success` 删除条目实现清零。不发「已恢复」通知（保持最小面）。
- **队列保留期** `ARTICLE_RETENTION_DAYS = 7`：只清 `sent` 与终态 `failed`（attempts 用尽）；无可解析时间戳的条目宁可保留。
- **dry-run 语义**：失败计数不落盘、告警不发送也不标记 `alerted`，与队列「dry-run 不写」保持一致。
- **cron 间隔保持 */30**：加密到 */15 是时效 vs X 风控暴露的取舍，留给用户拍板，本轮不动。

### Deviations
- **article 检测从分类后无条件执行移入「新且非 seed」分支**（评估清单外的必要配套）：原位置会 (a) 新账号首轮 seed 时把历史推文的 article 全部入队灌 TG；(b) 与新增的 7 天队列清理组成「清理 sent → 已 seen 推文重检测 → 重新入队 → 重复推送」循环。副作用：`--test` 模式不再顺带入队 article（原行为是入队），视为可接受。
- **aborninblood 禁用而非删除**：fxtwitter 404；memory.lol 改名史与 Wayback 快照均为零 → 判定录入时即错，保留条目（enabled:false）便于回溯原始出处后改正。

### Open Questions
- aborninblood 的正确 handle 需要从当初的录入来源回溯，网络侧无候选。
- 8BTCNews 确认存在但全站仅 1 条推文（空号），保留监控（无害，每轮 2 次 GraphQL 调用），是否换成真正想监控的账号待用户确认。

### 本轮后半（agents 产出集成）
- **rest_id 持久缓存**（twitter_graphql.py，agent 实现）：`.user_id_cache.json`，命中免 UserByScreenName（768→384 次/天）；失效双触发（suspended/not found 类错误 + 空 user 节点），30 分钟自愈不本轮重试（重试需改公开签名或重建请求，不干净；失败已有 fallback+WARN 可见性）。
- **RT article 修复**（根因 agent 本地复现验证）：壳 URL → 工具退化到按 article_id 直查 → 两端空 `{}` → 116 字节残片 → 判 empty_article_body。修复：归一化解包 `legacy.retweeted_status_result`（article 取原推 + 暴露 retweeted_status），save_article 记原推 id + author，article_fetch_url 走原作者 status URL（与 10 条 sent 成功样本同构）。旧条目无 author 回退 username 不破坏。
- **已知未修**（预存，仅标记）：article_fetch_url 无 tweet_id 的 `/i/article/<id>` 回退分支仍是脆弱路径；get_user_id 缓存 miss 且无 cookie 文件时 `_curl(url, None)` 的 AttributeError 隐患（生产有 cookie 不触发，缓存命中后跳过该路径）。
- 存量修复：dotey 队列 3 条 failed 改原推 id/author 重置 pending 重试（liuren/xiaohu/dongxi_nlp）。

---

## 2026-06-12 — 文章摘要迁移 sendRichMessage（Rich Markdown 模式）

### Design Decisions
- **只迁移文章摘要推送**：普通推文/TL;DR/告警/失败通知保持 sendMessage 旧路径（短消息无收益）。
- **rich-first + 完整回退**：400/404 返回 `rich_fallback` 信号（不抛错、不在 rich 函数内降级），回退到原 HTML 分块路径；429/5xx/网络重试语义与旧函数一致。旧路径整套保留。
- **守卫 30000 字符**（官方上限 32768，留余量 + 规避 "UTF-8 characters" 按字节计的口径歧义）；超长直接走旧分块。
- **`skip_entity_detection: True`**：防止头部 `@X用户名` 被自动链接到 Telegram 同名账号（误导链接）；显式 markdown 链接不受影响。
- AI prompt 放开 `>` 结论引用块 + `###` 三级标题（禁一二级标题和表格）；回退渲染器同步支持（标题整行加粗剥内层、引用斜体、空续行保留段落分隔）。
- 标题清洗：压换行 + 反斜杠转义 GFM/HTML 特殊字符（X 原文标题不可控）。
- 顺手堵预存洞：回退渲染为空时不再假 sent（`empty_rendered_summary` 入 failed）。

### Tradeoffs
- 404 持续探测（方法若未来下线，每篇 candidate 每轮多 1 次调用）：量级个位数/天，验证结论是不值得加状态记忆，不修。
- rich 成功后补 1.2s 间隔，对齐旧路径节奏。

### Open Questions
- `is_bad_tldr` 的 `^[*#>\-\s]+` 会拒绝新 prompt 的 `> ` 开头格式——目前无接线，未来若把文章摘要接入该质量门会全军覆没（future-trap，已记录）。

### Verification
- 3-agent 对抗验证（API 合规对照文档原文 / 回归审查 / 18 项边界探针）全 PASS，6 条 minor 加固全部落地。
- 测试 39→42 全过（Mac 3.14 + 服务器 3.9）；生产 bot 实发 sendRichMessage ok:true（msg 3364，已删）。

---

## 2026-06-12 第三轮 — 拼图/折叠/TLDR/消息闭环（用户拍板清单）

### Design Decisions
- **TL;DR 优先级**：原文自带（`TL;DR|TLDR|太长不看` 行，≥10 字符）> AI 总结 > 短预览；作者写的不过 is_bad_tldr 质量门（信任作者）。
- **长推全文折叠**：`<blockquote expandable>`（老 HTML 路径原生支持，已核实文档），全文截 2800 字符防 4096 超限；仅 note_tweet 分支，普通推文/article 推文不变。
- **配图两级回退**：rich带图 → 被拒去图重 rich → 再败回退分块。理由：图片外链失效不应让整条摘要降级成纯文本。单图用裸 `![](url)`，多图 `<tg-collage>`，沿用 limit=4。
- **告警闭环**：alert_msg_id 存进 .account_failures.json；恢复时原告警改写成 ✅ 已恢复（含原 count/last_error 摘要）+ unpin。编辑/置顶全部走 `_tg_post_quiet`（best-effort，绝不打断监控）。
- **失败通知闭环**：failure_msg_id 存队列 entry；3 次失败发 3 条通知只闭环最后一条（前两条悬空，接受——为闭环全部通知需存数组，不值）。
- **④推文预览修复实为误报**：核实 send_telegram 已有 `url` 钉死 + prefer_large_media，未改动。
- **告警明细折叠未做**：告警本体只有两行，折叠无意义（item 3 的告警部分裁剪）。

### Open Questions
- 状态看板（pinned dashboard）用户询问含义后未拍板，未实施。

### 追加（用户拍板）：置顶状态看板
- `update_status_dashboard()`：`.dashboard.json` 存 message_id + 当日计数（北京时间日界清零）；每轮 editMessageText 原地更新（不触发通知），失败自动重建（静默新发+置顶+清理旧消息）。
- 只在完整 cron 轮更新（dry_run/test/seed/--user 手动运行不碰），避免子集运行污染计数。
- 计数语义：tweets_today 累计各轮 total_push；articles_today 累计 process_article_queue 的 processed（含失败尝试，标签写「文章任务」不写「文章」以保持诚实）。

### 看板对抗审查修复（4 条）
- 幽灵失败记录：main 完整轮修剪不在配置中的 failures 键（--user 子集轮不修剪防误删）+ 看板侧与配置账号求交集双保险。审查发现本地测试污染产生的 "broken" 活体证据。
- 推送计数改为实际送达：process_user 返回 len(to_push)-len(push_failed)；文章 processed 只计 sent/summarized——故障期重试不再双计、看板不虚高。
- _tg_post_quiet 把 "message is not modified" 视为成功，杜绝同内容编辑触发删旧重建链。
- main 的看板调用包 try/except + 计数字段类型自愈（脏状态文件不再让 cron 尾部整轮崩）。
- 看板日界改为北京时间 06:00 起始（用户指定）：逻辑日期 = now_cn - 6h；06:00 前计入前一天。

### 看板规避聊天 auto-delete（用户开了 1 天自动删除）
- 问题：auto-delete 按消息「发送时刻」计时，editMessageText 不重置 → 看板必在 24h 后被删。
- 方案：每轮 getChat 读 message_auto_delete_time（实测私聊可读，=86400）存 state；看板存活到 TTL 的 85%（DASHBOARD_REBUILD_FRACTION）时主动重建新看板+置顶，旧的留给 auto-delete 清理。自适应：用户改 TTL 自动跟随；无 TTL 则不主动重建只靠自愈。
- created_at 取 sendMessage 返回的 result.date（Telegram UTC epoch），与 time.time()(UTC epoch) 同基准。
- 审查修复(major)：created_at 非数值（脏状态文件）时 float() 容错视作 0，与计数字段同款自愈，防每轮崩在 _atomic_write 前导致永久瘫痪+丢计数。
- 实测：首轮升级把旧看板(无 created_at)重建为 3415 并置顶，state 含 ttl/created_at，getChat 确认 pinned==3415。

## 2026-07-02 — TG 发送幂等语义修复（重复推送根因）

### 根因
- 7-01 16:30 UTC 轮 philschmid 推文（id 2072357009022943258）在 X 话题落了两条（16:30:12 / 16:30:29，间隔 17s = 15s 读超时 + 2s 退避）：sendRichMessage 带图时 Telegram 服务端先拉图再回响应，`_tg_post` 15s 读超时把「已送达但响应慢」误判为失败，重试循环盲目重发。经典 at-most-once / at-least-once 混淆；Bot API 无幂等 token，重发必重复。

### Design Decisions
- **异常分相分类**（依据 bwg /usr/bin/python3 3.9.25 的 `AbstractHTTPHandler.do_open` 实测源码）：连接/TLS/发送请求体阶段的 OSError 被 urllib 包成 `URLError` → 未送达，重试安全；`getresponse()`/读响应体阶段异常裸抛 → 请求已完整送出，归类为新异常 `TgAmbiguousDelivery`，禁止重发。
- **歧义即视为已送达**：send_telegram / send_telegram_rich 捕获 TgAmbiguousDelivery 后打 ⚠ 日志并返回 `{"ok": True, "assumed_delivered": True}`——调用方正常标 seen/标 sent，不进 push_retry，rich 也不再走 HTML 回退（回退同样会产生第二条）。取舍：宁可极小概率漏一条推送，也不重复刷屏；漏推概率 ≈ P(响应丢失 × 实际未处理)，远小于重复概率。
- **超时 15s → 60s**：缩小歧义窗口本身。rich 带图响应可超 15s；60s 后仍歧义的才走 assumed_delivered。
- **TgAmbiguousDelivery 子类 OSError**：macrumors_daily 直接调 `tm._tg_post`，其 `except (URLError, TimeoutError, OSError)` 捕获面不变（行为等同旧裸 socket.timeout），不在本次范围内改其语义（P2 记录）。

### Tradeoffs
- 429/5xx 保留原重试：状态码表明请求被拒/内部错误，未产生消息；504 理论上有歧义但 Telegram sendMessage 实际罕见，不为它加分支。
- assumed_delivered 响应无 result.message_id：文章失败通知的 failure_msg_id 闭环、看板 new_mid 在该罕见路径下跳过（下轮自愈/重建），接受。

### Open Questions / P2
- macrumors_daily send_html/send_card 的读超时仍会重试（重复卡片风险同款机制），待单独修复。

### 两轮对抗式审查结论（v1 → v2 → v3）
**第一轮（5 视角 41 agent，10 条确认 / 8 条否决）**，确认项已修：
- P1 `except Exception` 过宽：token 脏字符（InvalidURL/UnicodeEncodeError，发生在联网前）被误归歧义 → 全量静默丢推。修复：catch-all 收窄为 `(OSError, HTTPException)` + InvalidURL 显式放行。
- P1 大面积故障批量 assumed → 加连续歧义熔断 `_AMBIGUOUS_STREAK`（第 2 条起按失败进 push_retry）。
- P1 只有 ⚠ print 不可观测 → `.assumed_delivered.json` 留痕 + 轮首汇总 DM（`_flush_assumed_delivery_notice`）。
- P1 告警路径 alerted=True 被 assumed 锁定 → `ok and not assumed_delivered` 才落定（宁重勿漏）。
- P1/P2 测试空洞：timeout=60 锚定、HTTPError 放行锚定、ambiguous 测试补 patch time.sleep。
- P2 article sent 落盘顺序 → send 成功即刻落盘。
否决项（预先存在/不可触发，记录不修）：504 走 5xx 盲重试（同构风险但预先存在）、推送循环无时间预算 + 25m kill 窗口、macrumors body 读失败重试重复、_tg_post_quiet 60s 拖慢看板路径。

**第二轮（对 v2 增量 3 视角 23 agent，10 条确认 / 0 否决）**，全部已修：
- P1 flush 自身歧义会自记账本（自指噪音挤掉真实痕迹）+ 占用熔断额度 → flush 改直发 `_tg_post`，不留痕不占额度。
- P1 任意 HTTPError 清零熔断（502/504 是边缘 nginx 后端挂死的产物）→ 只有 `e.code < 500` 清零；同理 2xx+垃圾体不清零（`_note_definite_response` 移到 json.loads 成功后）。
- P1 mid-save 抛 OSError 会把已送达 sent 翻成 failed（磁盘满 → 每轮重发）→ mid-save 包 try/except OSError 忽略。
- P2 `_record` 缺 makedirs（全新部署丢痕迹）、`_flush` 缺非 list 守卫（损坏账本永不清理）、flush 未跟随「完整轮」门槛（--test/--chat-id 调试会把核对 DM 发进调试目标并删账本）→ 全部修复。
- P2 三处测试空洞 → 补 FlushAssumedDeliveryNoticeTest（5 用例）、5xx/4xx/垃圾 2xx streak 锚定（3 用例）、ArticleSentPersistBeforeQuietEditTest。

### 最终交付语义
- 发出前失败（URLError/InvalidURL/本地错误）：响亮失败，重试安全，走 push_retry。
- 发出后响应缺失（读超时/连接中断/垃圾响应）：首条按已送达（防重复）+ 留痕 + 下轮汇总 DM；连续第 2 条起熔断按失败（防批量丢失）。
- 4xx/可解析 2xx 回执：清零熔断计数；5xx/垃圾 2xx：不清零。
- 告警通道（note_account_failure）宁重勿漏；内容通道宁漏勿重。
- 测试 173 个（新增 25），本地 3.14 与 bwg 3.9.25 双绿；已部署 bwg（备份 twitter_monitor.py.bak-20260702-idempotency）。

## 2026-07-02 — 第三批：三个案底 P2 落地（504 / macrumors / 推送预算）

### Design Decisions
- **504 归歧义**：`_tg_post` 对 `e.code == 504` 抛 TgAmbiguousDelivery（网关超时=上游可能已处理，与读超时同构），502/503/500 保留盲重试（未达后端，重试安全）。关闭 5xx 家族里唯一的「已处理但无响应」重复路径。
- **macrumors 同款语义**：send_card 歧义按已送达返回（防 main 的「回落到文字」对已送达卡片重发）+ 留痕带卡片 link；send_html 歧义首条按已送达 + 留痕带 `digest i/N` 段落标识，连续第 2 条熔断抛出走 main 既有「未标 seen 次日重试」。卡片不熔断（其失败路径是换格式重发）但计入连续计数，让后续 send_html 第一条即可熔断。留痕文件与 x_monitor 进程 8:00 重叠时有读改写竞态，最坏丢一条痕迹，已接受。
- **推送预算两层门槛**：硬不变量在逐次层——`SEND_ATTEMPT_MIN_REMAINING_SECONDS=65`（60s socket 超时+余量），send_telegram(_rich) 每次尝试发起前检查，发起了的请求必然在 SIGALRM 前收到结果；粗门槛 `PUSH_MIN_REMAINING_SECONDS=240` 在推送循环层避免白撞。直调/测试/macrumors 下 `_ARTICLE_QUEUE_RUN_START=None → remaining=inf`，门槛不触发。
- **送达即刻 checkpoint**：每条推文送达后立即 save_seen（先）+ save_push_retry 摘除（后）——顺序不可换（中间被杀留孤儿 retry 条目会被 seen 短路无重复；反序被杀则下轮当新推文重发）。载入时清理 `push_retry ∩ seen` 孤儿。**checkpoint 加 `not args.test` 守卫**（审查抓的 P1：--test 推送发调试目标，写生产 seen 会让生产群永久漏推）。

### 第三批对抗审查（3 视角 15 agent，5 条确认 / 1 条否决）
确认并已修：--test 污染生产 seen（P1）；240s 门槛低估 rich→HTML 复合最坏 318–510s（P1+P2 同根，治本改逐次门槛）；macrumors send_html 无熔断批量假送达（P2）；send_html 留痕无段落标识（P2）。
否决：重试梯子末次 sleep-then-raise 死等（预先存在，未触碰）。

### 测试
- 180→184：--test 守卫、逐次预算门槛、macrumors html 熔断、卡片计数联动。本地与 bwg 3.9 双绿。

## 2026-07-10 — 统一主题分流：F2 账号级话题路由 + 话题失效自愈 + 部署

### Design Decisions
- **解析链** `account.topic → telegram_topic_threads → 默认 telegram_twitter_thread_id(19)`：EN 账号不写 topic 字段，靠回退落「X·AI 前沿」（19 改名），新增账号默认同路——map 只维护例外（ai_cn=1145: vista8/dotey；biz=1146: dontbesilent/Vida_BWE/MacroMargin）。
- **路由表整表并入 overlay**：`apply_route_overlay` 把路由表 `topics` 全量 merge 进 `cfg["telegram_topic_threads"]`（路由表键优先，config 同名键回落）——话题名跨 fleet 同名（ai_cn/biz），x_monitor 无需再造第二份映射事实源。config.json 里的 map 是表缺失时的回落副本。
- **thread_map 建于 --user 过滤之前**：article 队列按文件遍历不受 --user 限制，子集轮里其他账号的文章也要解析到各自话题。
- **话题失效自愈**：send_telegram(_rich) 的确定性 400 命中 "message thread not found" → `_swap_thread_on_not_found` 换 `_THREAD_FALLBACK_ID` 重试一次（无回退话题则落 General），事件轮末 `_alert_thread_fallback` 汇总一条 DM。占用既有 3 次重试预算之一；歧义（TgAmbiguousDelivery）不触发。send_telegram 里该检查置于 parse_mode 剥离**之前**，否则先剥格式重进死话题后直接 raise 进 push_retry 死循环。
- **测试防宿主污染**：`MainContentRoutingTest._run_main` 必须 patch `ROUTE_TABLE_PATH` 到不存在路径——Mac/服务器都有真实路由表，overlay 会把真实群 id 覆进 fake config（部署当天就在本机复现 2 个失败）。

### Deviations
- **growth(497) 未并入商业·投资**：spec 原计划 497 改名复用；登 bwg 核实 pool.json 后发现 growth 是 JamesClear 式个人成长语录（782 条），非商业内容——另建「商业·投资」=1146，growth 保持独占 497。market_recap 路由表 17→1146。
- 话题改名用 Bot API `editForumTopic`（bot 有 can_manage_topics，实测 19/41 直接改成功），未动用 telethon。

### 部署与验证（2026-07-10）
- 话题：新建 X·中文 AI=1145、商业·投资=1146；改名 19→X·AI 前沿、41→科技·资讯。
- 路由表 +ai_cn/biz、market_recap→1146，sync 到 r4s/bwg 回读一致；chat-daily config.yaml 删美女鉴赏社/哀酱/Gary Playa 三频道、投机之路 topic:biz（10:24 实跑：被删频道不再抓取、biz sender 正确指向 1146、科技圈照常 41）。
- bwg 解析链活体验证 13 账号 map 正确；端到端探针 send→thread 1145 送达后删除。
- 测试 184→195（MainContentRoutingTest+2 / ResolveTopicThreadTest+2 / ThreadNotFoundFallbackTest+5 / ApplyRouteOverlayTest+2，含 overlay 整表合并断言）；本地与 bwg 3.9 双绿。**服务器跑测试要避开整/半点**：与在跑的 cron 轮竞态会闪现假失败（本次 ff 后首跑撞上 10:00 轮）。

## 2026-07-10 — F1 跨账号去重（纯转发 + Article，config 键 cross_account_dedup）

### Design Decisions
- **抑制面收窄到纯转发**：canonical = `t:原推id`（仅 `retweeted_status.id` 存在时可被抑制）；原创/引用壳登记 `t:自身id`、引用**不穿透**被引原推（带评论=新内容，Grok/GLM 双评审点名的价值损失点）。article 用 `a:rest_id` 跨账号。
- **索引 `twitter_seen/.pushed_index.json`**：TTL 14 天 + 4000 条容量 GC（45min 推送窗口已挡旧推，索引只防迟到 RT 波）；单进程内存缓存即同轮跨账号共享；IO 全旁路容错（OSError 只打日志）。
- **写点与 seen checkpoint 同点位**（send-then-mark）：`elif not args.test` 块内 save_seen 之后——dry-run/--test/--seed 天然不写；失败/tombstone/guest 残缺轮不落库；崩溃窗口=他号最多重复 1 条（重复优于丢失）。
- **抑制点在 push_retry 判断之前**：上轮失败进 retry 的 RT 若期间已由他号送达，本轮抑制而非重发；tid 已在 new_ids → 轮末进 seen，retry 孤儿走既有 `push_retry∩seen` 清理，零新增状态机。
- **article 双闸**：save_article 入队闸（挡后来者）+ 队列取件二闸（挡同轮双账号已各自入队）；`skipped` 纳入终态 7 天清理。sent 即刻落盘点登记 `a:` 指纹。

### Deviations
- 计划拆「RT 提交 + article 提交」两个提交；实际共享索引模块导致切分要拆碎公共代码，合为单提交（整体可回滚：删 config 键全短路）。

### 测试
- 195→206（CrossAccountDedupTest ×11：抑制/首推登记/同轮双账号/quote 不穿透/原创不抑制/dry-run 与 --test 不写/默认关/TTL+容量 GC/article 双闸+skipped 终态）。

## 2026-07-10 — F3 rich 可播视频内嵌（config 键 rich_video_embed）

### Design Decisions
- **选流 HEAD 优先**：探针实测 GraphQL `bitrate` 是峰值声明，bitrate×duration 估算偏大 3-5 倍（10.9MB 估→3.7MB 实、49MB 估→9.4MB 实），纯估算会误杀大量可嵌视频；bwg 对 video.twimg.com 的 HEAD Content-Length 与 Telegram 物化 file_size 一字不差 → `_pick_embeddable_mp4` 逐档 HEAD（≤19MB 取最大档），HEAD 失败回退估算（≤18MB 更保守）。gif/无码率档估算为 0 视为可嵌。
- **渲染**：可嵌 → `<video src>`（去 ▶️ hint 去封面）；不可嵌/全超限/m3u8-only → 维持封面 `<img>`+▶️+时长（不算失败）；photo+video 混媒体 `<tg-collage>` 混排（官方支持），上限 4 保持。HTML 回退路径不动。
- **降级梯**（send_tweet，仿文章「带图被拒→去图重试」）：rich 含 `<video>` 被**确定性 400** 拒 → `format_message(embed_video=False)` 剥视频换封面重发 rich → 再拒才落 HTML；歧义（TgAmbiguousDelivery）在 send 内部已按已送达返回，走不到降级梯——无视频+封面双发。
- 探针（2026-07-10，msg 4192/4193 留 DM 供肉眼核）：Telegram 服务端可拉 video.twimg.com 并物化原生 Video 对象（含 file_id/宽高/缩略图），49MB 峰值档实际 9.4MB 也成功——20MB 上限针对实际字节数。

### 测试
- 206→216（RichVideoEmbedTest ×10：开关开时嵌 video/HEAD 选档/估算回退/全超限回封面/gif/混排 collage/剥视频重试不落 HTML/两级拒后落 HTML/歧义恰一发/默认关封面行为）。

## 2026-07-22 — 碎碎念（musing）过滤，全账号

### Context
- 触发样例：`@vista8`「把pocket3充满电，准备去钓鱼。」+ Photo 被推到 TG（`📢 @vista8`）。
- 既有 `classify()` 只挡过短/推广 hashtag/联盟链/商业词/纯链壳；18 字生活状态全过。

### Design Decisions
- **规则优先 + AI 复核**，与推广过滤同构：`classify` 标 `suspicious`，`process_user` 按 reason 前缀分流。
  - `commercial*` / `self_disclose*` → 既有 `confirm_promo`
  - `musing*` → 新 `confirm_musing`
- **不新增 status 枚举**，用 `REASON_MUSING_PREFIX = "musing"` 字符串协议，少改 `process_user` 分支面。
- **启发式（零 API）**：`musing_short_photo`（有图且 body≤40）/ `musing_life_kw`（生活词）/ `musing_status_photo`（有图+状态句式且 body<60）；实质信号（非媒体 URL / 技术词 / 长 note≥120 / article）短路不做 musing。
- **AI**：`MUSING_SYSTEM_PROMPT` + `AIBackend.classify_musing` / `AIClassifier.confirm_musing`；`_parse_result` 泛化为 `flag_key`（`promo`/`musing`）。
- **全账号生效**，不改 `twitter_accounts.json`；阈值/词表先做模块常量。

### Tradeoffs
- **无 AI 时 musing fail-closed（filter），promo fail-open（放行）**：兴趣门控优先安静；商业讨论避免无 AI 时误杀。注释与测试对照写死。
- **AI 全失败同样 filter**（与 promo P0-4 一致）。
- `MUSING_SHORT_MAX=40`：覆盖样例 18 字及稍长状态句；过宽靠 AI 复核/实质信号兜。
- 首版不做视觉看图判碎碎念（成本高；媒体类型+正文足够）。

### 测试
- 216→234（MusingClassifyTest ×9 + MusingProcessUserTest ×5 + confirm_musing fail-closed + FakeAI 补 confirm_*）。
- 本地 3.14 全绿。部署 bwg 后观察 log 中 `musing_*` / `ai:` 过滤原因。
