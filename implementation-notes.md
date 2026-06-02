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
