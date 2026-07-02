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
