#!/usr/bin/env python3
"""Twitter 多账号监控 → Telegram 推送

数据源：Twitter GraphQL API（免费）/ ai.6551.io API（fallback）
配置：
  twitter_accounts.json  — 监控的账号列表
  twitter_tokens.json    — API token 池（多 token 轮换）
  twitter_ai.json        — AI 推广识别配置（可选，支持多后端）

用法：
  python3 twitter_monitor.py              # 常规模式：只推送新推文
  python3 twitter_monitor.py --test       # 测试模式：推送过滤后最新 N 条
  python3 twitter_monitor.py --dry-run    # 只打印不推送
  python3 twitter_monitor.py --seed       # 只记录已见 ID，不推送（首次用）
  python3 twitter_monitor.py --user vista8  # 只处理指定用户
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import html
import shlex
import subprocess
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

try:
    import fcntl  # POSIX file locking; used for the single-run guard (LOCK-1)
except ImportError:
    fcntl = None


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "cc98_config.json")
ACCOUNTS_PATH = os.path.join(SCRIPT_DIR, "twitter_accounts.json")
TOKENS_PATH = os.path.join(SCRIPT_DIR, "twitter_tokens.json")
AI_CONFIG_PATH = os.path.join(SCRIPT_DIR, "twitter_ai.json")
SEEN_DIR = os.path.join(SCRIPT_DIR, "twitter_seen")

# GraphQL data source (free, no API key)
try:
    sys.path.insert(0, SCRIPT_DIR)
    import twitter_graphql
    HAS_GRAPHQL = True
except ImportError:
    HAS_GRAPHQL = False

API_BASE = "https://ai.6551.io"
API_ENDPOINT = f"{API_BASE}/open/twitter_user_tweets"


def _atomic_write(path: str, data: str) -> None:
    """Write text atomically: tmp in same dir, fsync, then os.replace (POSIX-atomic).

    Prevents the truncate-in-place corruption (STATE-1) where a crash or an
    overlapping run leaves an empty/partial JSON that load_* silently resets.
    """
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

# ── Article 处理 ─────────────────────────────────────

ARTICLE_QUEUE_DIR = os.path.join(SCRIPT_DIR, "twitter_articles")
ARTICLE_CACHE_DIR = os.path.join(ARTICLE_QUEUE_DIR, "cache")
ARTICLE_MAX_ATTEMPTS = 3
ARTICLE_RETENTION_DAYS = 7  # sent/终态 failed 条目保留天数，到期从队列清除（防无限累积）
RICH_MESSAGE_MAX_CHARS = 30000  # Rich message 上限 32768，留余量；超出回退旧分块路径
FAILURES_PATH = os.path.join(SCRIPT_DIR, ".account_failures.json")
FAIL_ALERT_THRESHOLD = 4  # 账号连续失败轮数达到阈值（*/30 cron ≈ 2 小时）发一次 TG 告警
ARTICLE_MARKDOWN_CMD = os.environ.get("X_ARTICLE_MARKDOWN_CMD", "").strip()
ARTICLE_URL_RE = re.compile(
    r"https?://(?:x\.com|twitter\.com)/(?:i/article|([a-zA-Z0-9_]+)/articles)/(\d+)",
    re.IGNORECASE,
)
ARTICLE_API_ENDPOINT = f"{API_BASE}/open/twitter_article_by_id"


def detect_article(tweet: dict) -> str | None:
    """从推文数据中检测 Article，返回 article_id 或 None。"""
    text = tweet.get("text") or ""
    m = ARTICLE_URL_RE.search(text)
    if m:
        return m.group(2)
    entities = tweet.get("entities") or {}
    for url_obj in entities.get("urls", []):
        expanded = url_obj.get("expanded_url") or url_obj.get("url") or ""
        m = ARTICLE_URL_RE.search(expanded)
        if m:
            return m.group(2)
    note = tweet.get("note_tweet") or {}
    if note.get("is_expandable"):
        for url_obj in note.get("entities", {}).get("urls", []):
            expanded = url_obj.get("expanded_url") or url_obj.get("url") or ""
            m = ARTICLE_URL_RE.search(expanded)
            if m:
                return m.group(2)
    return None


def save_article(username: str, article_id: str, tweet: dict) -> None:
    """保存检测到的 Article 到队列文件。"""
    os.makedirs(ARTICLE_QUEUE_DIR, exist_ok=True)
    queue_path = os.path.join(ARTICLE_QUEUE_DIR, f"{username}_queue.json")
    queue = []
    if os.path.exists(queue_path):
        try:
            with open(queue_path) as f:
                queue = json.load(f)
        except Exception:
            queue = []
    if any(a.get("article_id") == article_id for a in queue):
        return
    # Store note_tweet text and article data from GraphQL
    note = tweet.get("note_tweet") or {}
    note_text = note.get("text", "").strip()
    article_data = tweet.get("article") or {}
    # RT 时记原推 id + 原作者：抓取必须走原作者 status URL（壳 URL 无 article
    # 节点，工具会退化到按 article_id 直查的空 {} 路径 → empty_article_body）
    rt = tweet.get("retweeted_status") or {}
    entry = {
        "article_id": article_id,
        "tweet_id": rt.get("id") or tweet.get("id"),
        "author": rt.get("screen_name") or username,
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "tweet_text": (tweet.get("text") or "")[:200],
        "note_tweet_text": note_text,
        "article_title": article_data.get("title", ""),
        "article_preview": article_data.get("preview_text", ""),
        "status": "pending",
        "content": None,
    }
    queue.append(entry)
    _atomic_write(queue_path, json.dumps(queue, ensure_ascii=False, indent=2))
    print(f"    Article detected: {article_id} (queued)")


def fetch_article_content(token: str, article_id: str) -> dict | None:
    """调 6551 API 拉取 Article 全文。消耗 1 次额度。"""
    body = json.dumps({"id": article_id}).encode("utf-8")
    req = urllib.request.Request(
        ARTICLE_API_ENDPOINT,
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read().decode("utf-8"))
        if resp.get("success") is False:
            print(f"    Article API error: {resp.get('error')}")
            return None
        return resp.get("data")
    except Exception as e:
        print(f"    Article API exception: {e}")
        return None


def load_article_markdown_cmd() -> str:
    """Load the external X article-to-Markdown command."""
    if ARTICLE_MARKDOWN_CMD:
        return ARTICLE_MARKDOWN_CMD
    if os.path.exists(AI_CONFIG_PATH):
        try:
            with open(AI_CONFIG_PATH) as f:
                cfg = json.load(f)
            return (cfg.get("article_markdown_cmd") or "").strip()
        except Exception:
            return ""
    return ""


def article_url(article_id: str) -> str:
    return f"https://x.com/i/article/{article_id}"


def article_fetch_url(username: str, entry: dict) -> str:
    tweet_id = entry.get("tweet_id")
    if tweet_id:
        # RT 条目带 author（原作者）；旧条目无该键时回退监控账号名
        author = entry.get("author") or username
        return f"https://x.com/{author}/status/{tweet_id}"
    return article_url(entry["article_id"])


def cache_article_markdown(article_id: str, markdown: str) -> str:
    os.makedirs(ARTICLE_CACHE_DIR, exist_ok=True)
    path = os.path.join(ARTICLE_CACHE_DIR, f"{article_id}.md")
    _atomic_write(path, markdown)
    return path


def cleanup_old_article_cache(max_age_hours: int = 24) -> None:
    if not os.path.exists(ARTICLE_CACHE_DIR):
        return
    cutoff = time.time() - max_age_hours * 3600
    for fname in os.listdir(ARTICLE_CACHE_DIR):
        path = os.path.join(ARTICLE_CACHE_DIR, fname)
        try:
            if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
                os.remove(path)
        except Exception:
            pass


def delete_article_cache(entry: dict) -> None:
    for key in ("markdown_path", "summary_path"):
        path = entry.get(key)
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except Exception as e:
                print(f"    cache cleanup failed: {path}: {e}")
        entry.pop(key, None)


def fetch_article_markdown(username: str, entry: dict) -> tuple[str | None, str | None]:
    """Fetch article Markdown through baoyu-danger-x-to-markdown-compatible command."""
    cmd_template = load_article_markdown_cmd()
    if not cmd_template:
        return None, "markdown_fetch_command_missing"
    url = article_fetch_url(username, entry)
    try:
        cmd = shlex.split(cmd_template) + [url]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    except Exception as e:
        return None, f"markdown_fetch_exception:{e}"
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip().replace("\n", " ")[:300]
        return None, f"markdown_fetch_failed:{err}"

    stdout = result.stdout.strip()
    markdown = ""
    if stdout.startswith("{"):
        try:
            payload = json.loads(stdout)
            markdown_path = payload.get("markdownPath")
            if markdown_path and os.path.exists(markdown_path):
                with open(markdown_path) as f:
                    markdown = f.read().strip()
        except Exception as e:
            return None, f"markdown_fetch_json_parse_failed:{e}"
    elif os.path.exists(stdout):
        with open(stdout) as f:
            markdown = f.read().strip()
    else:
        markdown = stdout

    if not markdown:
        return None, "markdown_fetch_empty"
    body = re.sub(r"^---\n[\s\S]*?\n---\n*", "", markdown).strip()
    if len(body) < 200 or body in {"```json\n{}\n```", "{}"}:
        return None, "markdown_fetch_empty_article_body"
    return markdown, None


ARTICLE_SUMMARY_PROMPT = """请把下面这篇文章总结成适合 Telegram 推送的中文摘要。

先给一句话结论（用引用块 > 开头），然后用三级标题（###）分节说明核心观点、论证链条和关键细节。
不要输出链接，不要复述作者和标题，不要编造原文没有的信息。
可以使用 Markdown 三级标题、粗体、列表、编号和引用块；不要用一级/二级标题和表格。
"""


def extract_article_image_urls(markdown: str, limit: int = 4) -> list[str]:
    urls: list[str] = []
    for pattern in (r"!\[[^\]]*\]\((https?://[^\s)]+)\)", r'<img[^>]+src=["\'](https?://[^"\']+)["\']'):
        for url in re.findall(pattern, markdown, re.IGNORECASE):
            clean = html.unescape(url).strip()
            if clean and clean not in urls:
                urls.append(clean)
            if len(urls) >= limit:
                return urls
    return urls


def fetch_article_images(image_urls: list[str], max_bytes: int = 4_000_000) -> list[dict]:
    images: list[dict] = []
    for idx, url in enumerate(image_urls, 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                content_type = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
                data = r.read(max_bytes + 1)
            if len(data) > max_bytes or not content_type.startswith("image/"):
                continue
            images.append({"index": idx, "url": url, "content_type": content_type, "data": data})
        except Exception as e:
            print(f"    Article image fetch failed: {url}: {e}")
    return images


def summarize_article(ai: "AIClassifier", username: str, entry: dict, markdown: str) -> tuple[str | None, str | None]:
    if not ai.is_available():
        return None, "ai_unavailable"
    image_urls = extract_article_image_urls(markdown)
    images = fetch_article_images(image_urls) if image_urls else []
    source_url = article_url(entry["article_id"])
    prompt = (
        f"{ARTICLE_SUMMARY_PROMPT}\n\n"
        f"以下元信息仅供理解，摘要中不要复述：作者 @{username}；标题 {entry.get('article_title') or '未知'}；原文 {source_url}\n\n"
        f"文章 Markdown：\n{markdown[:30000]}"
    )
    if images:
        summary, backend_name = ai.complete_with_images(prompt, images, max_tokens=4000, temperature=0.2)
        if not summary:
            print(f"    Article image summary failed ({backend_name}); retrying Gemini text-only")
            summary, backend_name = ai.complete(prompt, max_tokens=4000, temperature=0.2)
    else:
        summary, backend_name = ai.complete(prompt, max_tokens=4000, temperature=0.2)
    if not summary:
        return None, "ai_summary_empty"
    return summary.strip(), backend_name


def markdown_to_telegram_html(text: str) -> str:
    # Protect fenced code blocks: escape once and stash behind a placeholder so the
    # per-line escaping below does not re-escape the <pre> tags into literal &lt;pre&gt;.
    pre_blocks: list[str] = []

    def _stash_pre(m: "re.Match") -> str:
        pre_blocks.append(f"<pre>{html.escape(m.group(1).strip())}</pre>")
        return f"\x00PRE{len(pre_blocks) - 1}\x00"

    text = re.sub(r"```(?:\w+)?\n([\s\S]*?)```", _stash_pre, text)
    out = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            out.append("")
            continue
        if re.fullmatch(r"\x00PRE\d+\x00", line):
            out.append(line)
            continue
        heading = bool(re.match(r"^#{1,6}\s+", line))
        line = re.sub(r"^#{1,6}\s+", "", line)
        quote = bool(re.match(r"^>\s*", line))
        line = re.sub(r"^>\s*", "", line)
        bullet = re.match(r"^[-*]\s+(.+)$", line)
        numbered = re.match(r"^(\d+)\.\s+(.+)$", line)
        prefix = ""
        if bullet:
            prefix = "• "
            line = bullet.group(1)
        elif numbered:
            prefix = f"{numbered.group(1)}. "
            line = numbered.group(2)
        escaped = html.escape(line)
        escaped = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped)
        escaped = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", escaped)
        escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
        escaped = re.sub(r"!\[([^\]]*)\]\((https?://[^\s)]+)\)", r"\1", escaped)
        escaped = re.sub(r"\[([^\]]+)\]\((https?://[^\s)]+)\)", r"\1", escaped)
        escaped = re.sub(r"https?://\S+", "", escaped).strip()
        if escaped:
            # 回退渲染（rich 不可用时）：标题行加粗、引用行斜体，
            # 与 ARTICLE_SUMMARY_PROMPT 要求的 ###/> 结构对应。
            # 整行包裹前剥掉行内同名标签（标题整体加粗后内部粗体冗余）。
            if heading:
                escaped = "<b>" + escaped.replace("<b>", "").replace("</b>", "") + "</b>"
            elif quote:
                escaped = "<i>" + escaped.replace("<i>", "").replace("</i>", "") + "</i>"
            out.append(prefix + escaped)
        elif quote:
            out.append("")  # 多段引用的 '>' 空续行保留段落分隔
    result = "\n".join(out).strip()
    for i, block in enumerate(pre_blocks):
        result = result.replace(f"\x00PRE{i}\x00", block)
    return result


def _balance_html_chunks(chunks: list[str]) -> list[str]:
    """Make each chunk valid standalone Telegram HTML: close inline tags left open
    at a chunk boundary and reopen them at the start of the next chunk, so a split
    never produces an unbalanced <b>/<i>/<code> that Telegram rejects with HTTP 400."""
    inline = {"b", "strong", "i", "em", "u", "s", "code", "pre"}
    carry: list[str] = []
    balanced: list[str] = []
    for chunk in chunks:
        body = "".join(f"<{t}>" for t in carry) + chunk
        stack: list[str] = []
        for m in re.finditer(r"<(/?)([a-zA-Z]+)[^>]*>", body):
            closing, name = m.group(1), m.group(2).lower()
            if name not in inline:
                continue
            if closing:
                if stack and stack[-1] == name:
                    stack.pop()
            else:
                stack.append(name)
        body += "".join(f"</{t}>" for t in reversed(stack))
        balanced.append(body)
        carry = stack
    return balanced


def split_telegram_html(text: str, limit: int = 3500) -> list[str]:
    text = text.strip()
    if not text:
        return []
    parts: list[str] = []
    current = ""
    for block in re.split(r"(\n\n+)", text):
        if not block:
            continue
        candidate = current + block
        if len(candidate) <= limit:
            current = candidate
            continue
        if current.strip():
            parts.append(current.strip())
            current = ""
        if len(block) <= limit:
            current = block
            continue
        lines = block.splitlines(keepends=True) or [block]
        for line in lines:
            if len(current) + len(line) <= limit:
                current += line
                continue
            if current.strip():
                parts.append(current.strip())
                current = ""
            while len(line) > limit:
                cut = line.rfind("。", 0, limit)
                if cut < limit // 2:
                    cut = line.rfind("，", 0, limit)
                if cut < limit // 2:
                    cut = limit
                # Never cut inside an HTML tag (would emit a broken "<b" fragment).
                lt = line.rfind("<", 0, cut)
                gt = line.rfind(">", 0, cut)
                if lt > gt and lt > 0:
                    cut = lt
                parts.append(line[:cut].strip())
                line = line[cut:].lstrip()
            current = line
    if current.strip():
        parts.append(current.strip())
    return _balance_html_chunks(parts)


def format_article_summary_messages(username: str, entry: dict, summary: str) -> list[str]:
    rendered_summary = markdown_to_telegram_html(summary)
    chunks = split_telegram_html(rendered_summary)
    if len(chunks) <= 1:
        return chunks
    total = len(chunks)
    return [f"<b>X Article 摘要 {idx}/{total}</b>\n\n{chunk}" for idx, chunk in enumerate(chunks, 1)]


def _fold_summary_details(summary: str) -> str:
    """### 分节的摘要只露「结论 + 首节」，其余折叠进 details（点开展开）。

    只在 rich 路径调用；400 回退时用原始 summary 走旧分块渲染，互不污染。
    """
    parts = re.split(r"(?m)^(?=### )", summary)
    if len(parts) <= 2:  # 没有或只有一个分节，不折叠
        return summary
    visible = (parts[0] + parts[1]).rstrip()
    rest = "".join(parts[2:]).strip()
    return (f"{visible}\n\n<details><summary>展开论证与细节</summary>\n\n"
            f"{rest}\n\n</details>")


def format_article_summary_rich(username: str, entry: dict, summary: str,
                                image_urls: list[str] | None = None) -> str:
    """组装 Rich Markdown 摘要（sendRichMessage 用）：克制的头部 + AI 摘要原文。

    AI 输出本来就是 Markdown，rich 模式原生渲染标题/列表/引用块，
    不再经过 markdown_to_telegram_html 转换和 3500 字符分块。
    image_urls：文章配图外链（Telegram 服务端拉取），多图拼 tg-collage。
    """
    title = (entry.get("article_title") or "").strip() or "X Article"
    # 标题来自 X 原文不可控：压掉换行，转义会被 rich markdown 解析的特殊字符
    title = re.sub(r"\s+", " ", title)
    title = re.sub(r"([\[\]()*_#`<>|~])", r"\\\1", title)
    link = article_url(entry["article_id"])
    header = (f"## \U0001f4c4 {title}\n"
              f"**@{username}** · [原文]({link})\n\n---\n\n")
    body = _fold_summary_details(summary.strip())
    collage = ""
    if image_urls:
        urls = image_urls[:4]
        if len(urls) == 1:
            collage = f"\n\n![]({urls[0]})"
        else:
            blocks = "\n".join(f"![]({u})" for u in urls)
            collage = f"\n\n<tg-collage>\n\n{blocks}\n\n</tg-collage>"
    return header + body + collage


def format_article_summary_message(username: str, entry: dict, summary: str) -> tuple[str, str]:
    messages = format_article_summary_messages(username, entry, summary)
    return (messages[0] if messages else "", "")


def format_article_failure_message(username: str, entry: dict, reason: str) -> tuple[str, str]:
    link = article_url(entry["article_id"])
    title = entry.get("article_title") or "X Article"
    attempts = entry.get("attempts", 0)
    msg = (
        f"⚠️ <b>X Article 处理失败</b>\n\n"
        f"作者：@{html.escape(username)}\n"
        f"主题：<b>{html.escape(title)}</b>\n"
        f"链接：{html.escape(link)}\n"
        f"阶段：{html.escape(entry.get('failed_stage', 'unknown'))}\n"
        f"尝试：{attempts}/{ARTICLE_MAX_ATTEMPTS}\n"
        f"原因：{html.escape(reason[:500])}"
    )
    return msg, link


def format_article_message(username: str, tweet: dict, article_id: str, content: dict | None) -> tuple[str, str]:
    """Legacy Article message formatter."""
    link = article_url(article_id)
    if content and content.get("text"):
        title = content.get("title") or "untitled"
        body_text = content["text"][:800]
        if len(content["text"]) > 800:
            body_text += "..."
        hidden = f'<a href="{link}">​</a>'
        msg = f"📄 <b>@{html.escape(username)}</b> published Article{hidden}\n\n<b>{html.escape(title)}</b>\n\n{html.escape(body_text)}"
    else:
        msg = f"📄 <b>@{html.escape(username)}</b> published Article\n\n{html.escape(link)}"
    return msg, link




# ── Token 池 ───────────────────────────────────────

COOLDOWN_SECONDS = 300


class TokenPool:
    def __init__(self, tokens: list[dict]):
        self._tokens = tokens
        self._cooldowns: dict[int, float] = {}
        self._current = 0

    @classmethod
    def load(cls) -> "TokenPool":
        if os.path.exists(TOKENS_PATH):
            with open(TOKENS_PATH) as f:
                data = json.load(f)
            if isinstance(data, list) and data:
                tokens = []
                for item in data:
                    if isinstance(item, str):
                        tokens.append({"label": "", "token": item})
                    elif isinstance(item, dict) and item.get("token"):
                        tokens.append(item)
                if tokens:
                    print(f"  加载 {len(tokens)} 个 token（twitter_tokens.json）")
                    return cls(tokens)
        single = _load_single_token_legacy()
        if single:
            print("  加载 1 个 token（兼容模式）")
            return cls([{"label": "legacy", "token": single}])
        sys.exit("找不到任何 token。配置 twitter_tokens.json 或设置 TWITTER_TOKEN")

    def get_token(self) -> tuple[str, str]:
        now = time.time()
        n = len(self._tokens)
        for i in range(n):
            idx = (self._current + i) % n
            until = self._cooldowns.get(idx, 0)
            if now >= until:
                self._current = idx
                t = self._tokens[idx]
                return t["token"], t.get("label", f"token-{idx}")
        earliest_idx = min(self._cooldowns, key=self._cooldowns.get)
        self._current = earliest_idx
        t = self._tokens[earliest_idx]
        return t["token"], t.get("label", f"token-{earliest_idx}")

    def mark_failed(self, label: str) -> None:
        for i, t in enumerate(self._tokens):
            if t.get("label") == label or f"token-{i}" == label:
                self._cooldowns[i] = time.time() + COOLDOWN_SECONDS
                remaining = len(self._tokens) - sum(
                    1 for v in self._cooldowns.values() if v > time.time()
                )
                print(f"  {label} 进入冷却 {COOLDOWN_SECONDS}s（剩余可用: {remaining}）")
                return

    def mark_success(self, label: str) -> None:
        for i, t in enumerate(self._tokens):
            if t.get("label") == label or f"token-{i}" == label:
                self._cooldowns.pop(i, None)
                return

    @property
    def available_count(self) -> int:
        now = time.time()
        return sum(1 for i in range(len(self._tokens)) if now >= self._cooldowns.get(i, 0))


def _load_single_token_legacy() -> str | None:
    token = os.environ.get("TWITTER_TOKEN", "").strip()
    if token:
        return token
    claude_json = os.path.expanduser("~/.claude.json")
    try:
        with open(claude_json) as f:
            data = json.load(f)
        env = data.get("mcpServers", {}).get("twitter", {}).get("env", {})
        token = env.get("TWITTER_TOKEN", "").strip()
        if token:
            return token
    except Exception:
        pass
    return None


# ── AI 推广识别（多后端）────────────────────────────

PROMO_SYSTEM_PROMPT = """你是一个推文内容审核员。判断以下推文是否为推广/营销/广告内容。

推广特征包括：
- 推销产品、服务、API、平台
- 包含邀请码、返佣链接、affiliate 链接
- 为品牌/公司做软广
- 推荐特定工具并附带推广链接
- 要求关注、转发、加群等引流行为

非推广特征：
- 分享个人见解、技术讨论、行业观点
- 讨论产品但无利益关系
- 纯粹的技术教程或经验分享

请只回复 JSON：{"promo": true/false, "reason": "简短理由"}"""


class AIBackend:
    """单个 AI 后端。"""

    def __init__(self, name: str, api_base: str, api_key: str, model: str,
                 backend_type: str = "openai", timeout: int = 15):
        self.name = name
        self.api_base = api_base.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.backend_type = backend_type  # "openai" or "gemini"
        self.timeout = timeout
        self._available = bool(api_key)

    def _openai_chat_url(self) -> str:
        if self.api_base.endswith("/v1"):
            return f"{self.api_base}/chat/completions"
        return f"{self.api_base}/v1/chat/completions"

    def classify(self, username: str, text: str) -> tuple[bool, str]:
        """返回 (is_promo, reason)。失败抛异常。"""
        if not self._available:
            raise RuntimeError("no api_key")
        if self.backend_type == "gemini":
            return self._call_gemini(username, text)
        return self._call_openai(username, text)

    def _call_openai(self, username: str, text: str) -> tuple[bool, str]:
        user_msg = f"@{username} 发的推文：\n\n{text[:500]}"
        body = json.dumps({
            "model": self.model,
            "messages": [
                {"role": "system", "content": PROMO_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            "temperature": 0.1,
            "max_tokens": 100,
        }).encode("utf-8")
        req = urllib.request.Request(
            self._openai_chat_url(),
            data=body,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            resp = json.loads(r.read().decode("utf-8"))
        content = resp["choices"][0]["message"]["content"].strip()
        return self._parse_result(content)

    def _call_gemini(self, username: str, text: str) -> tuple[bool, str]:
        user_msg = f"@{username} 发的推文：\n\n{text[:500]}"
        prompt = f"{PROMO_SYSTEM_PROMPT}\n\n{user_msg}"
        body = json.dumps({
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 1000,
            },
        }).encode("utf-8")
        url = f"{self.api_base}/models/{self.model}:generateContent?key={self.api_key}"
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            resp = json.loads(r.read().decode("utf-8"))
        content = resp["candidates"][0]["content"]["parts"][0]["text"].strip()
        return self._parse_result(content)

    def complete(self, prompt: str, max_tokens: int = 1200, temperature: float = 0.2) -> str:
        if not self._available:
            raise RuntimeError("no api_key")
        if self.backend_type == "gemini":
            return self._complete_gemini(prompt, max_tokens=max_tokens, temperature=temperature)
        return self._complete_openai(prompt, max_tokens=max_tokens, temperature=temperature)

    def complete_with_images(self, prompt: str, images: list[dict], max_tokens: int = 1200, temperature: float = 0.2) -> str:
        if not self._available:
            raise RuntimeError("no api_key")
        if self.backend_type == "gemini":
            return self._complete_gemini_with_images(prompt, images, max_tokens=max_tokens, temperature=temperature)
        return self._complete_openai_with_images(prompt, images, max_tokens=max_tokens, temperature=temperature)

    def _complete_openai_with_images(self, prompt: str, images: list[dict], max_tokens: int = 1200, temperature: float = 0.2) -> str:
        content = [{"type": "text", "text": prompt}]
        for image in images:
            b64 = base64.b64encode(image["data"]).decode("ascii")
            content.append({
                "type": "image_url",
                "image_url": {"url": f"data:{image['content_type']};base64,{b64}"},
            })
        body = json.dumps({
            "model": self.model,
            "messages": [{"role": "user", "content": content}],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }).encode("utf-8")
        req = urllib.request.Request(
            self._openai_chat_url(),
            data=body,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=max(self.timeout, 60)) as r:
            resp = json.loads(r.read().decode("utf-8"))
        return resp["choices"][0]["message"]["content"].strip()

    def _complete_gemini_with_images(self, prompt: str, images: list[dict], max_tokens: int = 1200, temperature: float = 0.2) -> str:
        parts = [{"text": prompt}]
        for image in images:
            parts.append({
                "inline_data": {
                    "mime_type": image["content_type"],
                    "data": base64.b64encode(image["data"]).decode("ascii"),
                }
            })
        body = json.dumps({
            "contents": [{"parts": parts}],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_tokens,
            },
        }).encode("utf-8")
        url = f"{self.api_base}/models/{self.model}:generateContent?key={self.api_key}"
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=max(self.timeout, 60)) as r:
            resp = json.loads(r.read().decode("utf-8"))
        return resp["candidates"][0]["content"]["parts"][0]["text"].strip()

    def _complete_openai(self, prompt: str, max_tokens: int = 1200, temperature: float = 0.2) -> str:
        body = json.dumps({
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }).encode("utf-8")
        req = urllib.request.Request(
            self._openai_chat_url(),
            data=body,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=max(self.timeout, 45)) as r:
            resp = json.loads(r.read().decode("utf-8"))
        return resp["choices"][0]["message"]["content"].strip()

    def _complete_gemini(self, prompt: str, max_tokens: int = 1200, temperature: float = 0.2) -> str:
        body = json.dumps({
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_tokens,
            },
        }).encode("utf-8")
        url = f"{self.api_base}/models/{self.model}:generateContent?key={self.api_key}"
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=max(self.timeout, 45)) as r:
            resp = json.loads(r.read().decode("utf-8"))
        return resp["candidates"][0]["content"]["parts"][0]["text"].strip()

    def _parse_result(self, content: str) -> tuple[bool, str]:
        # 去掉 markdown 代码块包裹
        cleaned = re.sub(r'```(?:json)?\s*', '', content).strip().rstrip('`').strip()
        m = re.search(r'\{.*\}', cleaned, re.DOTALL)
        if m:
            try:
                result = json.loads(m.group())
                return bool(result.get("promo", False)), result.get("reason", "")
            except json.JSONDecodeError:
                pass
        return False, f"parse_error:{content[:60]}"


class AIClassifier:
    """多后端 AI 分类器，按顺序尝试，自动 fallback。"""

    def __init__(self, backends: list[AIBackend]):
        self._backends = backends

    @classmethod
    def load(cls) -> "AIClassifier":
        if not os.path.exists(AI_CONFIG_PATH):
            return cls([])
        with open(AI_CONFIG_PATH) as f:
            cfg = json.load(f)

        backends: list[AIBackend] = []

        # 新格式：{"backends": [...]}
        if "backends" in cfg:
            for b in cfg["backends"]:
                if not b.get("api_key"):
                    continue
                backends.append(AIBackend(
                    name=b.get("name", "unknown"),
                    api_base=b.get("api_base", ""),
                    api_key=b["api_key"],
                    model=b.get("model", ""),
                    backend_type=b.get("type", "openai"),
                    timeout=b.get("timeout", 15),
                ))
        # 旧格式：单个 {"api_base": ..., "api_key": ...}
        elif cfg.get("enabled") and cfg.get("api_key"):
            backends.append(AIBackend(
                name="default",
                api_base=cfg.get("api_base", "https://api.deepseek.com"),
                api_key=cfg["api_key"],
                model=cfg.get("model", "deepseek-chat"),
                backend_type="openai",
                timeout=cfg.get("timeout", 15),
            ))

        if backends:
            names = ", ".join(b.name for b in backends)
            print(f"  AI 推广识别已启用（{names}）")
        return cls(backends)

    def is_available(self) -> bool:
        return bool(self._backends)

    def confirm_promo(self, username: str, text: str) -> tuple[bool, str]:
        """按顺序尝试各后端，第一个成功的结果返回。全部失败则放行。"""
        for backend in self._backends:
            try:
                is_promo, reason = backend.classify(username, text)
                return is_promo, f"{backend.name}:{reason}"
            except Exception as e:
                print(f"    AI [{backend.name}] 失败: {e}")
                continue
        return False, "all_ai_failed"

    def complete_with_images(self, prompt: str, images: list[dict], max_tokens: int = 1200, temperature: float = 0.2) -> tuple[str | None, str]:
        for backend in self._backends:
            try:
                return backend.complete_with_images(prompt, images, max_tokens=max_tokens, temperature=temperature), backend.name
            except Exception as e:
                print(f"    AI [{backend.name}] 图片理解失败: {e}")
                continue
        return None, "all_image_ai_failed"

    def complete(self, prompt: str, max_tokens: int = 1200, temperature: float = 0.2) -> tuple[str | None, str]:
        """按顺序尝试各后端生成文本。"""
        for backend in self._backends:
            try:
                return backend.complete(prompt, max_tokens=max_tokens, temperature=temperature), backend.name
            except Exception as e:
                print(f"    AI [{backend.name}] 摘要失败: {e}")
                continue
        return None, "all_ai_failed"


# ── 账号配置 ───────────────────────────────────────

def load_accounts() -> list[dict]:
    if not os.path.exists(ACCOUNTS_PATH):
        print(f"配置文件不存在: {ACCOUNTS_PATH}", file=sys.stderr)
        sys.exit(1)
    with open(ACCOUNTS_PATH) as f:
        accounts = json.load(f)
    return [a for a in accounts if a.get("enabled", True)]


# ── 过滤规则 ───────────────────────────────────────
MIN_LEN = 18
SKIP_HASHTAGS = {"#byteplus", "#seedance", "#seedance_2", "#dreamina"}
COMMERCIAL_KEYWORDS = [
    "@bytepluseglobal", "@bytepluseglobal",
    "byteplus", "seedance 2.0 api", "seedance 2.0",
    "api 文档", "api文档",
    "访问体验", "开通模型",
    "冲 200", "冲200", "200块", "200 块",
    "立即体验", "方舟平台",
]
COMMERCIAL_HIT_THRESHOLD = 2
DEFAULT_MAX_PUSH_AGE_MINUTES = 45
URL_RE = re.compile(r"https?://\S+")
AFFILIATE_URL_RE = re.compile(
    r"/invite/|/referral/|[?&](ref|aff|affiliate|inviter|invitecode|promo)=",
    re.IGNORECASE,
)
COMMERCIAL_SELF_DISCLOSE = [
    "赚个佣金", "赚点佣金", "返佣", "邀请码", "邀请链接",
    "扫码体验", "立即开通", "限时优惠",
]


def classify(tweet: dict) -> tuple[str, str]:
    """返回 (status, reason)。status: pass / suspicious / filter"""
    text = (tweet.get("text") or "").strip()
    low = text.lower()

    if len(text) < MIN_LEN:
        return "filter", f"too_short({len(text)}字)"

    for tag in SKIP_HASHTAGS:
        if tag in low:
            return "filter", f"skip_tag:{tag}"

    if AFFILIATE_URL_RE.search(text):
        return "filter", "affiliate_link"

    hits = [kw for kw in COMMERCIAL_KEYWORDS if kw in low]
    if len(hits) >= COMMERCIAL_HIT_THRESHOLD:
        return "suspicious", f"commercial({','.join(hits[:3])})"

    for kw in COMMERCIAL_SELF_DISCLOSE:
        if kw in low:
            return "suspicious", f"self_disclose:{kw}"

    stripped = URL_RE.sub("", text).strip()
    if len(stripped) < 10:
        return "filter", f"link_only({len(stripped)}字)"

    return "pass", "ok"


# ── API（带 token 轮换）────────────────────────────

class TokenExhausted(Exception):
    pass


def fetch_tweets(pool: TokenPool, username: str, limit: int = 20) -> list[dict]:
    """拉取用户推文。优先用 GraphQL（免费），fallback 到 6551.io。"""
    # Try GraphQL first (free, no API key)
    if HAS_GRAPHQL:
        try:
            tweets = twitter_graphql.fetch_tweets(username, limit=limit)
            if tweets:
                print(f"  [GraphQL] 拉取 {len(tweets)} 条推文")
                return tweets
        except Exception as e:
            print(f"  [GraphQL] 失败: {e}，回退到 6551.io")

    # Fallback: 6551.io API (requires token)
    if pool is None:
        raise TokenExhausted("GraphQL 失败且无 6551.io token")

    body = json.dumps({
        "username": username,
        "maxResults": limit,
        "product": "Latest",
        "includeReplies": False,
        "includeRetweets": False,
    }).encode("utf-8")

    attempts = len(pool._tokens)
    last_error = None

    for _ in range(attempts):
        token, label = pool.get_token()
        req = urllib.request.Request(
            API_ENDPOINT,
            data=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "User-Agent": "twitter-monitor/2.0",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                resp = json.loads(r.read().decode("utf-8"))
            pool.mark_success(label)
            return resp.get("data") or []
        except urllib.error.HTTPError as e:
            code_err = e.code
            try:
                err_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = ""
            last_error = f"HTTP {code_err}: {err_body[:100]}"
            if code_err in (402, 429, 401, 403, 500, 502, 503):
                pool.mark_failed(label)
                continue
            raise
        except Exception as e:
            last_error = str(e)
            pool.mark_failed(label)
            continue

    raise TokenExhausted(f"所有数据源均不可用，最后错误: {last_error}")


# ── Seen IDs ───────────────────────────────────────

def get_seen_path(username: str) -> str:
    os.makedirs(SEEN_DIR, exist_ok=True)
    return os.path.join(SEEN_DIR, f"{username}.json")


def load_seen(username: str) -> tuple[set[str], str | None]:
    path = get_seen_path(username)
    if not os.path.exists(path):
        return set(), None
    try:
        with open(path) as f:
            data = json.load(f)
        return set(data.get("ids", [])), data.get("last_post_ts")
    except Exception:
        return set(), None


def save_seen(username: str, seen: set[str], last_post_ts: str | None = None) -> None:
    path = get_seen_path(username)
    kept = sorted(seen, reverse=True)[:500]
    _atomic_write(path, json.dumps({"ids": kept, "updated": datetime.now().isoformat(),
                                    "last_post_ts": last_post_ts},
                                   ensure_ascii=False, indent=2))


def load_account_failures() -> dict:
    """读取账号连续失败状态 {username: {count, alerted, last_error, last_failed_at}}。"""
    try:
        with open(FAILURES_PATH) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_account_failures(failures: dict) -> None:
    _atomic_write(FAILURES_PATH, json.dumps(failures, ensure_ascii=False, indent=2))


def note_account_failure(failures: dict, username: str, error: str,
                         bot_token: str, chat_id: str, dry_run: bool = False) -> None:
    """记一次账号级失败；连续达到 FAIL_ALERT_THRESHOLD 轮只发一次 TG 告警。

    背景：aborninblood 曾静默失败 54 轮无人知晓（TokenExhausted 只进 stderr）。
    恢复成功由 note_account_success 清零，下次再连续失败会重新告警。
    """
    rec = failures.get(username) or {"count": 0, "alerted": False}
    rec["count"] = int(rec.get("count", 0)) + 1
    rec["last_error"] = str(error)[:300]
    rec["last_failed_at"] = datetime.now(timezone.utc).isoformat()
    if rec["count"] >= FAIL_ALERT_THRESHOLD and not rec.get("alerted"):
        text = (f"⚠️ <b>X 监控告警</b>：@{username} 已连续 {rec['count']} 轮拉取失败\n"
                f"最近错误：{html.escape(rec['last_error'])}")
        if dry_run:
            print(f"  DRY RUN 失败告警: @{username} 连续 {rec['count']} 轮")
        else:
            try:
                r = send_telegram(bot_token, chat_id, text)
                print(f"  失败告警推送 {'OK' if r.get('ok') else 'FAIL'}: @{username}")
                if r.get("ok"):
                    rec["alerted"] = True
                    mid = (r.get("result") or {}).get("message_id")
                    if mid:
                        # 故障期间置顶常驻可见（私聊置顶天然静默），恢复时取消
                        rec["alert_msg_id"] = mid
                        _tg_post_quiet(bot_token, {"chat_id": chat_id, "message_id": mid},
                                       "pinChatMessage")
            except Exception as e:
                print(f"  失败告警推送异常: {e}")
    failures[username] = rec


def note_account_success(failures: dict, username: str,
                         bot_token: str = "", chat_id: str = "",
                         dry_run: bool = False) -> None:
    """账号本轮成功，清除失败计数；曾告警过的把原告警原地改成已恢复并取消置顶。"""
    rec = failures.pop(username, None)
    if not rec or not rec.get("alerted") or dry_run:
        return
    mid = rec.get("alert_msg_id")
    if not (mid and bot_token):
        return
    text = (f"✅ <b>已恢复</b>：@{username} 拉取恢复正常\n"
            f"此前连续 {rec.get('count', '?')} 轮失败："
            f"{html.escape(str(rec.get('last_error', ''))[:200])}")
    _tg_post_quiet(bot_token, {"chat_id": chat_id, "message_id": mid,
                               "text": text, "parse_mode": "HTML"}, "editMessageText")
    _tg_post_quiet(bot_token, {"chat_id": chat_id, "message_id": mid}, "unpinChatMessage")
    print(f"  告警闭环: @{username} 已恢复，原告警已更新并取消置顶")


def parse_tweet_datetime(t: dict) -> datetime | None:
    ts_str = t.get("createdAt") or t.get("created_at") or ""
    if not ts_str:
        return None
    try:
        return datetime.strptime(ts_str, "%a %b %d %H:%M:%S %z %Y")
    except Exception:
        return None


def is_within_push_window(t: dict, max_age_minutes: int) -> bool:
    if max_age_minutes <= 0:
        return True
    dt = parse_tweet_datetime(t)
    if dt is None:
        return True
    return datetime.now(timezone.utc) - dt <= timedelta(minutes=max_age_minutes)


# ── Telegram ───────────────────────────────────────

TWEET_PREVIEW_LIMIT = 140
TLDR_PREVIEW_LIMIT = 140


def collapse_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def short_preview(text: str, limit: int = TWEET_PREVIEW_LIMIT) -> str:
    text = collapse_text(text)
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "…"


def article_preview_text(t: dict) -> str:
    article = t.get("article") or {}
    title = collapse_text(article.get("title", ""))
    preview = short_preview(article.get("preview_text", ""), TLDR_PREVIEW_LIMIT)
    if title and preview:
        return f"X Article：{title}\n{preview}"
    if title:
        return f"X Article：{title}"
    if preview:
        return f"X Article：{preview}"
    return "X Article 已加入摘要队列"


def is_bad_tldr(summary: str, source_text: str) -> bool:
    if len(summary) < 20 or summary.endswith(("，", "、", ",", "(", "'", "&")):
        return True
    if re.search(r"&[#a-zA-Z0-9]+;", summary):
        return True
    if re.match(r"^[*#>\-\s]+", summary):
        return True
    source_has_cjk = bool(re.search(r"[一-鿿]", source_text))
    if source_has_cjk:
        cjk_count = len(re.findall(r"[一-鿿]", summary))
        if cjk_count / max(len(summary), 1) < 0.25:
            return True
    return False


def extract_author_tldr(text: str) -> str | None:
    """原文自带 TL;DR 行则直接采用（有就用原文的，没有再 AI 总结）。"""
    m = re.search(r"(?:^|\n)\s*(?:TL;?DR|太长不看)\s*[:：]\s*(.+)", text, re.IGNORECASE)
    if not m:
        return None
    line = collapse_text(m.group(1).strip())
    if len(line) < 10:
        return None
    return short_preview(line, TLDR_PREVIEW_LIMIT)


def summarize_note_tweet(ai: "AIClassifier", username: str, note_text: str) -> str | None:
    if not ai.is_available():
        return None
    prompt = (
        "请把下面这条 X/Twitter 长推压缩成一行中文 TL;DR。"
        "只输出摘要正文，不要加 TL;DR 前缀，不要加项目符号，不要编造原文没有的信息。"
        "摘要必须是完整的一句话，长度控制在 60-100 个中文字符；如果原文是中文，必须用中文摘要。\n\n"
        f"作者：@{username}\n"
        f"长推正文：\n{note_text[:4000]}"
    )
    summary, backend = ai.complete(prompt, max_tokens=220, temperature=0.2)
    if not summary:
        return None
    summary = collapse_text(re.sub(r"^TL;?DR[:：]\s*", "", summary, flags=re.IGNORECASE))
    if is_bad_tldr(summary, note_text):
        print(f"    AI [{backend}] TL;DR 质量不足，使用短预览")
        return None
    return short_preview(summary, TLDR_PREVIEW_LIMIT)


def format_message(username: str, t: dict, ai: "AIClassifier | None" = None) -> tuple[str, str]:
    tid = t.get("id") or t.get("conversation_id_str") or ""
    link = f"https://x.com/{username}/status/{tid}" if tid else ""
    hidden = f'<a href="{link}">​</a>' if link else ""
    note = t.get("note_tweet") or {}
    note_text = note.get("text", "").strip()
    folded_full = ""
    if note_text:
        # TL;DR 优先级：原文自带 > AI 总结 > 短预览
        preview = extract_author_tldr(note_text)
        if not preview and ai:
            preview = summarize_note_tweet(ai, username, note_text)
        if preview:
            # Do NOT escape here: format_message escapes `body` once below. Escaping
            # twice turned "&"/"<" in the TL;DR into literal &amp;/&lt; in Telegram.
            body = f"TL;DR：{preview}"
        else:
            body = short_preview(note_text)
        # 长推全文折叠在 expandable blockquote 里：列表只占几行，点开看全文
        folded_full = note_text if len(note_text) <= 2800 else note_text[:2800] + "…"
        # Telegram 按 UTF-16 计长（astral 表情每个 2 单位）：表情密集长推按字符
        # 截断仍可能超 4096 → HTML 与 plain 降级双双 400 → 推文静默丢失。按单位收缩。
        while len(folded_full.encode("utf-16-le")) // 2 > 3000 and len(folded_full) > 100:
            folded_full = folded_full[: int(len(folded_full) * 0.9)] + "…"
    elif t.get("article"):
        body = article_preview_text(t)
    else:
        body = short_preview(t.get("text", ""))
    if body:
        text = f'📢 @{username}{hidden}\n\n{html.escape(body)}'
    else:
        text = f'📢 @{username}{hidden}'
    if folded_full:
        text += f"\n\n<blockquote expandable>{html.escape(folded_full)}</blockquote>"
    return text, link


def _tg_post(token: str, payload: dict, method: str = "sendMessage") -> dict:
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode("utf-8"))


def _tg_post_quiet(token: str, payload: dict, method: str) -> dict:
    """编辑/置顶类锦上添花调用：失败只打日志，绝不打断本轮监控。"""
    try:
        return _tg_post(token, payload, method=method)
    except Exception as e:
        print(f"  {method} 失败（忽略）: {e}")
        return {"ok": False}


def _html_to_plain(text: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", text))


def send_telegram_rich(token: str, chat_id: str, markdown: str, link: str = "") -> dict:
    """sendRichMessage（Bot API Rich Markdown 模式，上限 32768 字符）。

    重试语义与 send_telegram 一致（429 按 retry_after、5xx/网络退避重试）。
    与 send_telegram 的关键差异：400/404 不在本函数内降级，而是返回
    {"ok": False, "rich_fallback": True, ...} 让调用方回退到旧的
    parse_mode=HTML 分块路径（那条路径自带完整的转义/分块/降级逻辑）。
    """
    payload: dict = {
        "chat_id": chat_id,
        # skip_entity_detection：不关的话头部 @X用户名 会被自动链接到
        # Telegram 同名账号（误导）；显式 [文字](url) 链接不受影响
        "rich_message": {"markdown": markdown, "skip_entity_detection": True},
    }
    if link:
        payload["reply_markup"] = {
            "inline_keyboard": [[{"text": "\U0001f517 打开原文", "url": link}]]
        }

    last_err = None
    for attempt in range(3):
        try:
            return _tg_post(token, payload, method="sendRichMessage")
        except urllib.error.HTTPError as e:
            last_err = e
            try:
                body = e.read().decode("utf-8", "replace")
            except Exception:
                body = ""
            if e.code == 429:
                retry_after = 3
                try:
                    retry_after = int(json.loads(body)["parameters"]["retry_after"])
                except Exception:
                    pass
                time.sleep(min(max(retry_after, 1), 30))
                continue
            if e.code >= 500:
                time.sleep(2 * (attempt + 1))
                continue
            if e.code in (400, 404):
                # 400=内容被拒（标记/嵌套超限等），404=方法未对该 bot 开放
                desc = ""
                try:
                    desc = json.loads(body).get("description", "")
                except Exception:
                    desc = body[:200]
                return {"ok": False, "rich_fallback": True,
                        "error_code": e.code, "description": desc}
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
            continue
    if last_err:
        raise last_err
    raise RuntimeError("send_telegram_rich: exhausted retries")


def send_telegram(token: str, chat_id: str, text: str, link: str = "") -> dict:
    payload: dict = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }
    if link:
        payload["link_preview_options"] = {
            "url": link,
            "is_disabled": False,
            "prefer_large_media": True,
        }
        payload["reply_markup"] = {
            "inline_keyboard": [[{"text": "\U0001f517 打开原文", "url": link}]]
        }
    else:
        payload["link_preview_options"] = {"is_disabled": True}

    # Resilient send (REL-1/FMT-1): retry 429 honoring retry_after and 5xx with bounded
    # backoff; on a 400 (usually an HTML parse error) degrade once to plain text so the
    # message is still delivered instead of raising and being dropped/marked-seen.
    last_err = None
    for attempt in range(3):
        try:
            return _tg_post(token, payload)
        except urllib.error.HTTPError as e:
            last_err = e
            try:
                body = e.read().decode("utf-8", "replace")
            except Exception:
                body = ""
            if e.code == 429:
                retry_after = 3
                try:
                    retry_after = int(json.loads(body)["parameters"]["retry_after"])
                except Exception:
                    pass
                time.sleep(min(max(retry_after, 1), 30))
                continue
            if e.code >= 500:
                time.sleep(2 * (attempt + 1))
                continue
            if e.code == 400 and payload.get("parse_mode"):
                payload = dict(payload)
                payload["text"] = _html_to_plain(text)
                payload.pop("parse_mode", None)
                continue
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last_err = e
            time.sleep(2 * (attempt + 1))
            continue
    if last_err:
        raise last_err
    raise RuntimeError("send_telegram: exhausted retries")


# ── 单用户处理 ──────────────────────────────────────

def process_user(
    pool: TokenPool,
    ai: AIClassifier,
    username: str,
    bot_token: str,
    chat_id: str,
    args: argparse.Namespace,
) -> tuple[int, int, int, int]:
    """返回 (new_count, push_count, filter_count, ai_overridden)。"""
    print(f"\n{'='*40}")
    print(f"  @{username}")
    print(f"{'='*40}")

    tweets = fetch_tweets(pool, username, limit=args.limit)
    if not tweets:
        # Every data source returned an empty timeline — anomalous (auth break,
        # query-id drift, or account issue). Emit a greppable WARN marker so this
        # never stays silent the way a normal "0 new tweets" run does.
        print(f"  ⚠️ WARN: @{username} 拉到 0 条推文（疑似数据源异常/认证失效）")
        return 0, 0, 0, 0

    seen, last_post_iso = load_seen(username)

    new_ids: set[str] = set()
    to_push: list[tuple[dict, str]] = []
    filtered: list[tuple[dict, str]] = []
    ai_overridden = 0

    auto_seed = not seen and not args.test and not args.seed
    if auto_seed:
        print("  seen 为空，自动 seed（只记录，不推送）")

    for t in tweets:
        tid = str(t.get("id") or "")
        if not tid:
            continue

        status, reason = classify(t)
        text = (t.get("text") or "").strip()

        if status == "suspicious" and ai.is_available():
            is_promo, ai_reason = ai.confirm_promo(username, text)
            if is_promo:
                status = "filter"
                reason = f"{reason}|ai:{ai_reason}"
                print(f"    AI 确认推广 [{reason}] {text[:50]}")
            else:
                status = "pass"
                ai_overridden += 1
                print(f"    AI 否决 [{reason} -> {ai_reason}] {text[:50]}")

        if args.test:
            if status == "pass":
                to_push.append((t, reason))
            elif status == "filter":
                filtered.append((t, reason))
            else:
                filtered.append((t, reason))
        else:
            if tid in seen:
                continue
            new_ids.add(tid)
            if auto_seed or args.seed:
                continue
            # Article detection（零 API 成本）。只对「新且非 seed」的推文入队：
            # 放在 seen 判断之前会让 seed/新账号首轮灌入历史文章，且已 seen 推文
            # 会把被 7 天清理删掉的 sent 条目重新入队造成重复推送。
            article_id = detect_article(t)
            if article_id:
                save_article(username, article_id, t)
            if not is_within_push_window(t, args.max_push_age_minutes):
                print(f"    skip stale: {tid}")
                continue
            if status == "pass":
                to_push.append((t, reason))
            elif status == "filter":
                filtered.append((t, reason))
            else:
                if ai.is_available():
                    filtered.append((t, reason))
                else:
                    to_push.append((t, reason))
                    print(f"    无 AI，suspicious 放行 [{reason}]")

    if args.test:
        to_push = to_push[: args.test_count]

    mode = "seed" if (auto_seed or args.seed) else "normal"
    print(f"  拉取 {len(tweets)} 条，新推 {len(new_ids)} 条 [{mode}]")
    print(f"  推送: {len(to_push)}  过滤: {len(filtered)}  AI 否决: {ai_overridden}")
    for t, reason in filtered:
        text = (t.get("text") or "").replace("\n", " ")[:50]
        print(f"    [{reason}] {text}")

    if args.seed:
        print("    seed 模式：跳过推送")
        to_push = []

    push_failed: set[str] = set()
    for t, _reason in to_push:
        msg, link = format_message(username, t, ai)
        tid = str(t.get("id") or "")
        if args.dry_run:
            print("----- DRY RUN -----")
            print(msg)
            print(f"link: {link}")
            print()
        else:
            try:
                r = send_telegram(bot_token, chat_id, msg, link)
                ok = r.get("ok", False)
                print(f"    推送 {'OK' if ok else 'FAIL'}: {t.get('id')}")
                if not ok:
                    print(f"        resp: {r}")
                    push_failed.add(tid)
                time.sleep(1.2)
            except Exception as e:
                print(f"    推送异常: {e}")
                push_failed.add(tid)

    if args.seed:
        seen |= {str(t.get("id")) for t in tweets if t.get("id")}
    else:
        # REL-1/FMT-1: only mark a tweet seen if its push did NOT fail. Failed sends
        # stay unseen so the next cron run retries them (bounded by the push-age
        # window) instead of being silently dropped forever.
        seen |= (new_ids - push_failed)

    latest_ts = last_post_iso
    for t in tweets:
        dt = parse_tweet_datetime(t)
        if dt:
            iso = dt.isoformat()
            if not latest_ts or iso > latest_ts:
                latest_ts = iso

    save_seen(username, seen, latest_ts)
    print(f"  已记录 seen_ids 共 {len(seen)} 条")

    return len(new_ids), len(to_push), len(filtered), ai_overridden


# ── 主流程 ──────────────────────────────────────────


def _article_entry_expired(entry: dict, now: datetime | None = None) -> bool:
    """sent / 终态 failed（attempts 用尽）条目超过保留期后从队列清除。

    没有可解析时间戳的条目一律保留（宁可不删）。
    """
    status = entry.get("status")
    terminal = status == "sent" or (
        status == "failed" and int(entry.get("attempts", 0)) >= ARTICLE_MAX_ATTEMPTS)
    if not terminal:
        return False
    ts_str = entry.get("updated_at") or entry.get("sent_at") or entry.get("detected_at") or ""
    try:
        ts = datetime.fromisoformat(ts_str)
    except (ValueError, TypeError):
        return False
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return (now - ts) > timedelta(days=ARTICLE_RETENTION_DAYS)


def process_article_queue(ai: AIClassifier, bot_token: str, chat_id: str, dry_run: bool = False) -> int:
    """处理 Article 队列：抓 Markdown、AI 摘要、推送，成功后删除缓存。"""
    if not os.path.exists(ARTICLE_QUEUE_DIR):
        print("No article queue directory")
        return 0
    cleanup_old_article_cache()
    processed = 0
    for fname in sorted(os.listdir(ARTICLE_QUEUE_DIR)):
        if not fname.endswith("_queue.json"):
            continue
        queue_path = os.path.join(ARTICLE_QUEUE_DIR, fname)
        try:
            with open(queue_path) as f:
                queue = json.load(f)
        except Exception as e:
            print(f"  article queue load failed: {queue_path}: {e}")
            continue

        username = fname.replace("_queue.json", "")
        before = len(queue)
        queue = [a for a in queue if not _article_entry_expired(a)]
        changed = len(queue) != before
        if changed:
            print(f"  @{username}: 清理 {before - len(queue)} 条过期文章记录")

        candidates = [a for a in queue if a.get("status") in ("pending", "failed", "fetched") and a.get("attempts", 0) < ARTICLE_MAX_ATTEMPTS]
        if not candidates:
            if changed and not dry_run:
                _atomic_write(queue_path, json.dumps(queue, ensure_ascii=False, indent=2))
            continue
        print(f"  @{username}: {len(candidates)} article jobs")

        for entry in candidates:
            aid = entry["article_id"]
            entry["status"] = "processing"
            entry["attempts"] = int(entry.get("attempts", 0)) + 1
            entry["updated_at"] = datetime.now(timezone.utc).isoformat()
            changed = True

            markdown, err = fetch_article_markdown(username, entry)
            if err:
                entry["status"] = "failed"
                entry["failed_stage"] = "fetch_markdown"
                entry["last_error"] = err
                entry["updated_at"] = datetime.now(timezone.utc).isoformat()
                msg, link = format_article_failure_message(username, entry, err)
                if dry_run:
                    print(f"    DRY RUN failure notice: {err}")
                else:
                    try:
                        r = send_telegram(bot_token, chat_id, msg, link)
                        print(f"    Failure notice push {'OK' if r.get('ok') else 'FAIL'}")
                        _mid = (r.get("result") or {}).get("message_id")
                        if _mid:
                            entry["failure_msg_id"] = _mid  # 重试成功后原地改写闭环
                        time.sleep(1.2)
                    except Exception as e:
                        print(f"    failure notice push error: {e}")
                continue

            md_path = cache_article_markdown(aid, markdown)
            entry["markdown_path"] = md_path
            entry["fetched_at"] = datetime.now(timezone.utc).isoformat()
            print(f"    Article {aid}: markdown fetched ({len(markdown)} chars)")

            summary, backend = summarize_article(ai, username, entry, markdown)
            if not summary:
                err = backend
                entry["status"] = "failed"
                entry["failed_stage"] = "ai_summary"
                entry["last_error"] = err
                entry["updated_at"] = datetime.now(timezone.utc).isoformat()
                msg, link = format_article_failure_message(username, entry, err)
                if dry_run:
                    print(f"    DRY RUN summary failure notice: {err}")
                else:
                    try:
                        r = send_telegram(bot_token, chat_id, msg, link)
                        print(f"    Failure notice push {'OK' if r.get('ok') else 'FAIL'}")
                        _mid = (r.get("result") or {}).get("message_id")
                        if _mid:
                            entry["failure_msg_id"] = _mid  # 重试成功后原地改写闭环
                        time.sleep(1.2)
                    except Exception as e:
                        print(f"    failure notice push error: {e}")
                continue

            entry["summary_backend"] = backend
            entry["summary_at"] = datetime.now(timezone.utc).isoformat()
            messages = format_article_summary_messages(username, entry, summary)
            img_urls = extract_article_image_urls(markdown)
            rich_md = format_article_summary_rich(username, entry, summary, image_urls=img_urls)
            if dry_run:
                print(f"    DRY RUN rich markdown {len(rich_md)} chars; fallback parts: {len(messages)}")
                for idx, part in enumerate(messages, 1):
                    print(f"      part {idx}: {part[:160]}...")
                entry["status"] = "summarized"
            else:
                try:
                    ok = False
                    last_resp = {}
                    article_link = article_url(entry["article_id"])
                    # 优先 sendRichMessage（单条 32k、原生渲染 Markdown）；
                    # 被拒/未开放/超长时回退旧的 HTML 分块多条路径。
                    if len(rich_md) <= RICH_MESSAGE_MAX_CHARS:
                        r = send_telegram_rich(bot_token, chat_id, rich_md, article_link)
                        last_resp = r
                        ok = r.get("ok", False)
                        if not ok and img_urls and r.get("rich_fallback"):
                            # 配图外链可能是被拒原因：去图重试一次 rich，再不行才回退分块
                            print(f"    rich 带图被拒({str(r.get('description', ''))[:60]})，去图重试")
                            r = send_telegram_rich(
                                bot_token, chat_id,
                                format_article_summary_rich(username, entry, summary),
                                article_link)
                            last_resp = r
                            ok = r.get("ok", False)
                        if ok:
                            print("    Article summary rich push OK")
                            time.sleep(1.2)
                        else:
                            print(f"    rich 推送被拒({str(r.get('description', ''))[:80]})，回退分块 HTML")
                            time.sleep(1.2)
                    if not ok:
                        # 渲染为空（极端：摘要只剩 URL 被剥光）不能假装 sent
                        ok = bool(messages)
                        if not messages:
                            last_resp = {"ok": False, "description": "empty_rendered_summary"}
                        for idx, part in enumerate(messages, 1):
                            r = send_telegram(bot_token, chat_id, part, article_link)
                            last_resp = r
                            part_ok = r.get("ok", False)
                            print(f"    Article summary part {idx}/{len(messages)} push {'OK' if part_ok else 'FAIL'}")
                            ok = ok and part_ok
                            time.sleep(1.2)
                            if not part_ok:
                                break
                    if ok:
                        entry["status"] = "sent"
                        entry["sent_at"] = datetime.now(timezone.utc).isoformat()
                        delete_article_cache(entry)
                        _fmid = entry.pop("failure_msg_id", None)
                        if _fmid:
                            # 把此前的失败通知原地改写，不留悬空故障消息。
                            # editMessageText 不传 reply_markup 会移除原按钮，显式带上。
                            _tg_post_quiet(bot_token, {
                                "chat_id": chat_id, "message_id": _fmid,
                                "text": (f"✅ <b>X Article 重试成功</b>：@{html.escape(username)} "
                                         f"摘要已推送（此前失败 {max(int(entry.get('attempts', 1)) - 1, 1)} 次）"),
                                "parse_mode": "HTML",
                                "reply_markup": {"inline_keyboard": [[
                                    {"text": "\U0001f517 打开原文", "url": article_link}]]},
                            }, "editMessageText")
                    else:
                        entry["status"] = "failed"
                        entry["failed_stage"] = "telegram_send"
                        entry["last_error"] = str(last_resp)[:500]
                except Exception as e:
                    entry["status"] = "failed"
                    entry["failed_stage"] = "telegram_send"
                    entry["last_error"] = str(e)[:500]
                    print(f"    Article summary push error: {e}")
            entry["updated_at"] = datetime.now(timezone.utc).isoformat()
            processed += 1

        if changed and not dry_run:
            _atomic_write(queue_path, json.dumps(queue, ensure_ascii=False, indent=2))
    return processed


def main() -> int:
    ap = argparse.ArgumentParser(description="Twitter 多账号监控 → Telegram 推送")
    ap.add_argument("--test", action="store_true", help="测试模式")
    ap.add_argument("--seed", action="store_true", help="只记录已见，不推送")
    ap.add_argument("--dry-run", action="store_true", help="只打印，不推送")
    ap.add_argument("--limit", type=int, default=20, help="拉取条数")
    ap.add_argument("--test-count", type=int, default=3, help="--test 推送条数")
    ap.add_argument("--max-push-age-minutes", type=int, default=DEFAULT_MAX_PUSH_AGE_MINUTES)
    ap.add_argument("--chat-id", default=None)
    ap.add_argument("--bot-token", default=None)
    ap.add_argument("--user", default=None)
    ap.add_argument("--fetch-articles", action="store_true", help="Process article queue now (auto runs after polling too)")
    args = ap.parse_args()

    # LOCK-1: prevent an overrunning run from overlapping the next cron tick (which
    # causes double-sends + last-writer-wins state clobber). Non-blocking; skip if held.
    if fcntl is not None:
        _lock_fp = open(os.path.join(SCRIPT_DIR, ".monitor.lock"), "w")
        try:
            fcntl.flock(_lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("上一轮 monitor 仍在运行，跳过本次", file=sys.stderr)
            return 0

    # 每轮起止时间戳：日志此前无任何时间标记，无法事后审计运行时长/定位轮次
    run_started = time.monotonic()
    print(f"\n==== monitor run {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %z')} ====")

    with open(CONFIG_PATH) as f:
        cfg = json.load(f)
    bot_token = args.bot_token or cfg["telegram_bot_token"]
    chat_id = args.chat_id or cfg["telegram_chat_id"]

    # Load token pool for 6551.io fallback (optional if GraphQL works)
    try:
        pool = TokenPool.load()
    except SystemExit:
        if HAS_GRAPHQL:
            print("  TokenPool 不可用，仅使用 GraphQL 数据源")
            pool = None
        else:
            raise
    ai = AIClassifier.load()

    accounts = load_accounts()
    if args.user:
        accounts = [a for a in accounts if a["username"] == args.user]
        if not accounts:
            print(f"用户 {args.user} 不在配置中或未启用", file=sys.stderr)
            return 1

    if not accounts:
        print("没有启用的账号", file=sys.stderr)
        return 1

    print(f"Twitter 监控：{len(accounts)} 个账号")

    total_new = 0
    total_push = 0
    total_filter = 0
    total_ai_override = 0
    failures = load_account_failures()

    for account in accounts:
        username = account["username"]
        try:
            new, pushed, filtered, ai_ov = process_user(
                pool=pool, ai=ai, username=username,
                bot_token=bot_token, chat_id=chat_id, args=args,
            )
        except TokenExhausted as e:
            print(f"  @{username}: {e}", file=sys.stderr)
            note_account_failure(failures, username, str(e), bot_token, chat_id, args.dry_run)
            continue
        except Exception as e:
            print(f"  @{username} failed: {e}", file=sys.stderr)
            note_account_failure(failures, username, str(e), bot_token, chat_id, args.dry_run)
            continue
        note_account_success(failures, username, bot_token, chat_id, args.dry_run)
        total_new += new
        total_push += pushed
        total_filter += filtered
        total_ai_override += ai_ov

    print(f"\n{'='*40}")
    print(f"  汇总：新推 {total_new} | 推送 {total_push} | 过滤 {total_filter} | AI 否决 {total_ai_override}")
    # Process article queue (auto, uses GraphQL note_tweet — free)
    try:
        article_count = process_article_queue(ai, bot_token, chat_id, args.dry_run)
        if article_count:
            print(f"  Articles processed: {article_count}")
    except Exception as e:
        print(f"  Article queue error: {e}")
    if pool is not None:
        print(f"  token 池：{pool.available_count}/{len(pool._tokens)} 可用")
    if not args.dry_run:
        save_account_failures(failures)
    print(f"  耗时 {time.monotonic() - run_started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
