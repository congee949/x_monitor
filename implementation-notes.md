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
- SEC-3: cc98_config.json / twitter_ai.json hold secrets in cleartext on the VPS — confirm chmod 600 and whether the bot token equals the (now-redacted, to-be-rotated) Taoli98Bot token.

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
