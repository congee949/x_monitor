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
import http.client
import json
import os
import html
import shlex
import subprocess
import re
import signal
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
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
ACCOUNTS_PATH = os.path.join(SCRIPT_DIR, "twitter_accounts.json")

ROUTE_TABLE_PATH = os.path.expanduser("~/qwenproxy/.tg-notify-targets.json")
# config.json 的 telegram_*_thread_id / telegram_group_chat_id → 话题 key 映射。
_ROUTE_THREAD_KEYS = {
    "telegram_twitter_thread_id": "twitter",
    "telegram_macrumors_thread_id": "macrumors",
    "telegram_growth_thread_id": "growth",
}


def apply_route_overlay(cfg):
    """把 fleet 统一路由表的群 chat 与话题 thread 覆盖到 cfg，让 config.json 里的
    telegram_group_chat_id / telegram_*_thread_id 退居回落安全网、路由表成为事实源
    （Spec: 统一路由表 2026-07-09）。表缺失/损坏时保持 cfg 原值不变。就地修改并返回 cfg。"""
    try:
        with open(ROUTE_TABLE_PATH) as f:
            t = json.load(f)
    except Exception as e:
        print(f"[warn] 路由表读取失败，沿用 config.json: {type(e).__name__}: {e}", file=sys.stderr)
        return cfg
    gid = t.get("chat_id")
    topics = t.get("topics") or {}
    if gid:
        cfg["telegram_group_chat_id"] = str(gid)
    for cfg_key, topic_key in _ROUTE_THREAD_KEYS.items():
        tid = topics.get(topic_key)
        if tid:
            cfg[cfg_key] = tid
    # 话题名→thread 整表映射（账号级主题路由 topic 字段用）：路由表键优先，
    # config.json 的 telegram_topic_threads 同名键退居回落。
    merged = dict(cfg.get("telegram_topic_threads") or {})
    for name, tid in topics.items():
        if tid:
            merged[name] = tid
    if merged:
        cfg["telegram_topic_threads"] = merged
    return cfg
TOKENS_PATH = os.path.join(SCRIPT_DIR, "twitter_tokens.json")
AI_CONFIG_PATH = os.path.join(SCRIPT_DIR, "twitter_ai.json")
SEEN_DIR = os.path.join(SCRIPT_DIR, "twitter_seen")
# 跨账号去重索引（纯转发原推 id / article rest_id → 首推记录）
PUSHED_INDEX_PATH = os.path.join(SEEN_DIR, ".pushed_index.json")
PUSHED_INDEX_TTL_DAYS = 14      # 45min 推送窗口已挡旧推，索引只防迟到的 RT 波
PUSHED_INDEX_MAX_ENTRIES = 4000
# 歧义按已送达处理的发送痕迹（下一轮汇总 DM 核对后清除）
ASSUMED_DELIVERY_PATH = os.path.join(SEEN_DIR, ".assumed_delivered.json")

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
DASHBOARD_PATH = os.path.join(SCRIPT_DIR, ".dashboard.json")
DASHBOARD_REBUILD_FRACTION = 0.85  # 消息存活到 TTL 的 85% 时主动重建，避开 auto-delete
FAIL_ALERT_THRESHOLD = 4  # 账号连续失败轮数达到阈值（*/30 cron ≈ 2 小时）发一次 TG 告警

COOKIE_HEALTH_PATH = os.path.join(SCRIPT_DIR, ".cookie_health.json")
# 连续多少轮"整轮未取得 authed 访问"（静默降级 guest）后告警一次。*/30 cron 每轮
# ≈ 30min，6 轮 ≈ 3h：够滤掉 X 侧偶发 5xx/超时导致的单轮降级误报，而 cookie 真过期
# 是持久的，必在当天早上触发。独立于按账号的 FAIL_ALERT_THRESHOLD（guest 仍能拉公开
# 推文，账号不算 failure，故账号失败告警抓不到这种"认证整体失效"）。
COOKIE_DEGRADE_ALERT_THRESHOLD = 6
ARTICLE_MARKDOWN_CMD = os.environ.get("X_ARTICLE_MARKDOWN_CMD", "").strip()
ARTICLE_URL_RE = re.compile(
    r"https?://(?:x\.com|twitter\.com)/(?:i/article|([a-zA-Z0-9_]+)/articles)/(\d+)",
    re.IGNORECASE,
)
ARTICLE_API_ENDPOINT = f"{API_BASE}/open/twitter_article_by_id"

# Article queue crash-safety / run-overrun guards
ARTICLE_MARKDOWN_TIMEOUT = 30  # seconds; reduced from 90 to avoid cron overruns
MAX_ARTICLES_PER_RUN = 5
ARTICLE_QUEUE_TIME_BUDGET_SECONDS = 25 * 60  # align with SIGALRM global timeout
ARTICLE_QUEUE_MIN_REMAINING_SECONDS = 5 * 60
# 防「kill 砸在发送在途窗口」的两层预算门槛（kill 落在已送达未落盘 = 重复推送入口）：
# 1) 硬不变量在逐次层：send_telegram(_rich) 每次尝试发起前须剩 ≥65s（60s socket
#    超时 + 余量）——发起了的请求必然在 SIGALRM 前收到结果并 checkpoint，
#    复合最坏（rich 慢退化 429/超时 → HTML 回退再烧一梯子，可达 300s+）不再依赖
#    循环外的粗粒度估算。
# 2) 粗门槛在推送循环层：剩余 <240s 不再开始新推文，避免明知发不完还逐条撞逐次
#    门槛（每条白等一次失败路径）。
SEND_ATTEMPT_MIN_REMAINING_SECONDS = 65
PUSH_MIN_REMAINING_SECONDS = 4 * 60
ARTICLE_PROCESSING_STALL_MINUTES = 30
_ARTICLE_QUEUE_RUN_START: float | None = None


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


def _quote_comment_text(tweet: dict) -> str:
    """引用推文中博主自己的评论：note_tweet 优先否则壳 text，去尾部 t.co 短链。

    保留原文换行（引子按原文分行显示，不再折成一行 / 砍到 200 字）：只把每行内的
    连续空白压成单空格、3+ 连续空行收敛为一个，末尾大上限 2000 兜住 rich 预算。
    """
    text = (tweet.get("note_tweet") or {}).get("text") or tweet.get("text") or ""
    text = re.sub(r"\s*https?://t\.co/\w+\s*$", "", text)  # 去尾部 t.co 短链
    text = re.sub(r"[ \t]+", " ", text)          # 行内连续空白 → 单空格（不动换行）
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:2000]


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
    if _CROSS_DEDUP_ENABLED and ("a:" + str(article_id)) in load_pushed_index():
        by = (load_pushed_index().get("a:" + str(article_id)) or {}).get("by")
        print(f"    skip cross-dup article: {article_id}（已由 @{by} 推送摘要，不入队）")
        return
    # Store note_tweet text and article data from GraphQL
    note = tweet.get("note_tweet") or {}
    note_text = note.get("text", "").strip()
    article_data = tweet.get("article") or {}
    # RT/引用时记原推 id + 原作者：抓取必须走原作者 status URL（壳 URL 无 article
    # 节点，工具会退化到按 article_id 直查的空 {} 路径 → empty_article_body）。
    # 优先级转推 > 引用（与解析器 article 取值一致）。
    rt = tweet.get("retweeted_status") or {}
    quoted = tweet.get("quoted_status") or {}
    origin = rt or quoted
    # quote_comment 仅引用（quoted 有、rt 无）时设：博主自己的评论作摘要引子。
    quote_comment = _quote_comment_text(tweet) if (quoted and not rt) else ""
    entry = {
        "article_id": article_id,
        "tweet_id": origin.get("id") or tweet.get("id"),
        "author": origin.get("screen_name") or username,
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "tweet_text": (tweet.get("text") or "")[:200],
        "note_tweet_text": note_text,
        "article_title": article_data.get("title", ""),
        "article_preview": article_data.get("preview_text", ""),
        "quote_comment": quote_comment,
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
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=ARTICLE_MARKDOWN_TIMEOUT)
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


def extract_article_cover(markdown: str) -> "str | None":
    """封面图：baoyu markdown front matter 的 coverImage 字段（正文里通常没有 ![]()）。"""
    m = re.search(r'(?im)^coverImage\s*:\s*["\']?(https?://[^"\'\s]+)', markdown)
    return html.unescape(m.group(1)).strip() if m else None


def extract_article_body_images(markdown: str, limit: int = 4) -> list[str]:
    """正文内嵌图：markdown body 的 ![]() 与 <img src>（不含 front matter 封面）。"""
    urls: list[str] = []
    for pattern in (r"!\[[^\]]*\]\((https?://[^\s)]+)\)", r'<img[^>]+src=["\'](https?://[^"\']+)["\']'):
        for url in re.findall(pattern, markdown, re.IGNORECASE):
            clean = html.unescape(url).strip()
            if clean and clean not in urls:
                urls.append(clean)
            if len(urls) >= limit:
                return urls
    return urls


def extract_article_image_urls(markdown: str, limit: int = 4) -> list[str]:
    """封面 + 正文内嵌图合并去重（供 AI 视觉理解用；展示层封面/正文图分开放置）。"""
    cover = extract_article_cover(markdown)
    urls: list[str] = [cover] if cover else []
    for u in extract_article_body_images(markdown, limit):
        if u not in urls:
            urls.append(u)
        if len(urls) >= limit:
            break
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
    author = entry.get("author") or username  # RT 的 article 归原作者，不是转推本博主
    prompt = (
        f"{ARTICLE_SUMMARY_PROMPT}\n\n"
        f"以下元信息仅供理解，摘要中不要复述：作者 @{author}；标题 {entry.get('article_title') or '未知'}；原文 {source_url}\n\n"
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
        return None, backend_name or "ai_summary_empty"
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
    if len(chunks) > 1:
        total = len(chunks)
        chunks = [f"<b>X Article 摘要 {idx}/{total}</b>\n\n{chunk}" for idx, chunk in enumerate(chunks, 1)]
    # 引用文章：博主评论作引子（与 rich 摘要一致），HTML 转义，仅引用且渲染非空时加。
    # chunks 为空（摘要被剥光）不加引子，保留 process_article_queue「渲染为空判 failed
    # 不假 sent」的防线（否则空摘要会被引子撑成非空、误判已送达）。
    comment = (entry.get("quote_comment") or "").strip()
    if comment and chunks:
        # 保留原文分行，@user 独占首行，用 Telegram HTML 原生 <blockquote>；作独立首块，
        # 避免更长的引子拼进 chunks[0] 顶破 4096（HTML 回退单条上限）。
        lead_in = f"<blockquote>@{html.escape(username)} 引用：\n{html.escape(comment)}</blockquote>"
        chunks = [lead_in] + chunks
    return chunks


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


def _inject_detail_images(body: str, image_urls: list) -> str:
    """把文章正文内嵌图插进「展开论证与细节」折叠区（</details> 之前）；

    无折叠区（摘要没分节）时附在正文末尾。单图裸 ![]()，多图 <tg-collage>。
    """
    urls = [u for u in (image_urls or []) if u][:4]
    if not urls:
        return body
    if len(urls) == 1:
        block = f"![]({urls[0]})"
    else:
        block = "<tg-collage>\n\n" + "\n".join(f"![]({u})" for u in urls) + "\n\n</tg-collage>"
    if "</details>" in body:
        return body.replace("</details>", f"\n\n{block}\n\n</details>", 1)
    return f"{body}\n\n{block}"


def format_article_summary_rich(username: str, entry: dict, summary: str,
                                image_urls: list[str] | None = None,
                                detail_image_urls: list[str] | None = None) -> str:
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
    # RT 的 article 归原作者（entry["author"]，save_article 已存），不是转推的本博主
    # （username = 队列归属账号）。旧条目无 author 键时回退 username。
    author = entry.get("author") or username
    # 引用文章：博主评论作引子，单条消息顶部一行 rich blockquote（username = 引用者）。
    lead_in = ""
    comment = (entry.get("quote_comment") or "").strip()
    if comment:
        # 保留原文分行：@user 独占首行，每行加 blockquote 前缀，逐行转义 markdown 特殊
        # 字符（含行首「1.」→字面，避免被当有序列表重排）；空行用 `>` 维持引用块连续。
        esc_lines = []
        for ln in comment.split("\n"):
            ln = re.sub(r"([\[\]()*_#`<>|~])", r"\\\1", ln)
            ln = re.sub(r"^(\s*\d+)\.", r"\1\\.", ln)
            esc_lines.append(f"> {ln}" if ln.strip() else ">")
        quoted_body = "\n".join(esc_lines)
        lead_in = f"> @{username} 引用：\n{quoted_body}\n\n"
    title_line = (f"## \U0001f4c4 {title}\n"
                  f"**@{author}** · [原文]({link})")
    body = _fold_summary_details(summary.strip())
    # 正文内嵌图插进「展开论证与细节」折叠区（封面仍走顶部 image_urls）。
    body = _inject_detail_images(body, detail_image_urls)
    collage = ""
    if image_urls:
        urls = image_urls[:4]
        if len(urls) == 1:
            collage = f"![]({urls[0]})"
        else:
            blocks = "\n".join(f"![]({u})" for u in urls)
            collage = f"<tg-collage>\n\n{blocks}\n\n</tg-collage>"
    # 封面从消息末尾移到正文之前（用户 2026-07-01）：有引用引子 → 引子紧下方
    # （引子 → 封面 → 标题 → 正文）；无引子 → 标题下、正文上（标题 → 封面 → 正文）。
    if lead_in:
        mid = (collage + "\n\n") if collage else ""
        return f"{lead_in}{mid}{title_line}\n\n---\n\n{body}"
    mid = (collage + "\n\n") if collage else ""
    return f"{title_line}\n\n{mid}---\n\n{body}"


def format_article_summary_message(username: str, entry: dict, summary: str) -> tuple[str, str]:
    messages = format_article_summary_messages(username, entry, summary)
    return (messages[0] if messages else "", "")


def format_article_failure_message(username: str, entry: dict, reason: str) -> tuple[str, str]:
    link = article_url(entry["article_id"])
    title = entry.get("article_title") or "X Article"
    attempts = entry.get("attempts", 0)
    author = entry.get("author") or username  # RT 的 article 归原作者，不是转推本博主
    lead_in = ""  # 引用文章：博主评论作引子（HTML 转义），与 rich 摘要保持一致
    comment = (entry.get("quote_comment") or "").strip()
    if comment:
        comment = re.sub(r"\s+", " ", comment)
        lead_in = f"> @{html.escape(username)} 引用：{html.escape(comment)}\n\n"
    msg = (
        f"{lead_in}"
        f"⚠️ <b>X Article 处理失败</b>\n\n"
        f"作者：@{html.escape(author)}\n"
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

MUSING_SYSTEM_PROMPT = """你是推文内容审核员。判断该推文对「AI/科技/商业信息订阅者」是否为无信息量的生活碎碎念。

碎碎念特征：
- 个人生活状态、出行/饮食/天气/心情、晒图说明
- 无观点、无数据、无产品/行业结论
- 纯打卡、行程准备、设备充电等日常琐事

非碎碎念：
- 技术讨论、产品/行业见解、工具评测
- 带实质信息的分享（即便口语化）
- 对订阅者有信息增量的内容

请只回复 JSON：{"musing": true/false, "reason": "简短理由"}"""


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
            return self._call_gemini(username, text, PROMO_SYSTEM_PROMPT, "promo")
        return self._call_openai(username, text, PROMO_SYSTEM_PROMPT, "promo")

    def classify_musing(self, username: str, text: str) -> tuple[bool, str]:
        """返回 (is_musing, reason)。失败抛异常。"""
        if not self._available:
            raise RuntimeError("no api_key")
        if self.backend_type == "gemini":
            return self._call_gemini(username, text, MUSING_SYSTEM_PROMPT, "musing")
        return self._call_openai(username, text, MUSING_SYSTEM_PROMPT, "musing")

    def _call_openai(self, username: str, text: str, system_prompt: str,
                     flag_key: str) -> tuple[bool, str]:
        user_msg = f"@{username} 发的推文：\n\n{text[:500]}"
        body = json.dumps({
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
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
        return self._parse_result(content, flag_key)

    def _call_gemini(self, username: str, text: str, system_prompt: str,
                     flag_key: str) -> tuple[bool, str]:
        user_msg = f"@{username} 发的推文：\n\n{text[:500]}"
        prompt = f"{system_prompt}\n\n{user_msg}"
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
        return self._parse_result(content, flag_key)

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

    def _parse_result(self, content: str, flag_key: str = "promo") -> tuple[bool, str]:
        # 去掉 markdown 代码块包裹
        cleaned = re.sub(r'```(?:json)?\s*', '', content).strip().rstrip('`').strip()
        m = re.search(r'\{.*\}', cleaned, re.DOTALL)
        if m:
            try:
                result = json.loads(m.group())
                return bool(result.get(flag_key, False)), result.get("reason", "")
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
        """按顺序尝试各后端，第一个成功的结果返回。全部失败则 (False, all_ai_failed)。"""
        for backend in self._backends:
            try:
                is_promo, reason = backend.classify(username, text)
                return is_promo, f"{backend.name}:{reason}"
            except Exception as e:
                print(f"    AI [{backend.name}] 失败: {e}")
                continue
        return False, "all_ai_failed"

    def confirm_musing(self, username: str, text: str) -> tuple[bool, str]:
        """碎碎念 AI 复核。全部失败则 (False, all_ai_failed)；调用方 fail-closed。"""
        for backend in self._backends:
            try:
                is_musing, reason = backend.classify_musing(username, text)
                return is_musing, f"{backend.name}:{reason}"
            except Exception as e:
                print(f"    AI [{backend.name}] 碎碎念识别失败: {e}")
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
        """按顺序尝试各后端生成文本。

        空结果视为失败、继续下一后端：推理模型 token 预算不够时
        会把额度全花在隐藏推理上、content 返回空串（不抛异常）。旧逻辑把第一个
        不抛异常的后端结果直接返回，空串也算成功 → 永远轮不到 gemini 兜底。
        """
        for backend in self._backends:
            try:
                result = backend.complete(prompt, max_tokens=max_tokens, temperature=temperature)
            except Exception as e:
                print(f"    AI [{backend.name}] 摘要失败: {e}")
                continue
            if result and result.strip():
                return result, backend.name
            print(f"    AI [{backend.name}] 返回空内容，尝试下一后端")
        return None, "all_ai_failed"


# ── 账号配置 ───────────────────────────────────────

def load_accounts() -> list[dict]:
    if not os.path.exists(ACCOUNTS_PATH):
        print(f"配置文件不存在: {ACCOUNTS_PATH}", file=sys.stderr)
        sys.exit(1)
    with open(ACCOUNTS_PATH) as f:
        accounts = json.load(f)
    return [a for a in accounts if a.get("enabled", True)]


# 未知 topic 每轮只告警一次（进程即轮次，无需跨轮持久化）
_UNKNOWN_TOPIC_WARNED: set = set()


def _resolve_topic_thread(account: dict, topic_threads: dict,
                          default_thread_id: "int | None") -> "int | None":
    """账号级主题路由：account["topic"] → topic_threads[topic] → 默认 thread。

    未配置 topic 的账号（含未来新增账号）落默认话题；topic 配了但映射表里
    没有（话题被删/拼写错/路由表未同步）时回退默认并告警一次，绝不静默丢。"""
    topic = (account.get("topic") or "").strip()
    if not topic:
        return default_thread_id
    tid = (topic_threads or {}).get(topic)
    if tid:
        return tid
    if topic not in _UNKNOWN_TOPIC_WARNED:
        _UNKNOWN_TOPIC_WARNED.add(topic)
        print(f"  ⚠️ WARN: 未知 topic '{topic}'（@{account.get('username')}），回退默认 thread")
    return default_thread_id


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

# 碎碎念（musing）启发式：reason 以 REASON_MUSING_PREFIX 开头，process_user 据此分流 AI。
# 与 promo 不对称：无 AI 时 musing 默认 filter（兴趣门控优先安静），promo 默认放行。
REASON_MUSING_PREFIX = "musing"
MUSING_SHORT_MAX = 40
MUSING_STATUS_MAX = 60
MUSING_NOTE_LONG_MIN = 120
# 生活场景词（子串匹配，小写后）；按日志可增补。
MUSING_LIFE_KEYWORDS = [
    "钓鱼", "充电", "充满电", "出门", "散步", "跑步", "健身",
    "午饭", "晚饭", "早餐", "外卖", "睡觉", "起床", "下班", "通勤",
    "下雨", "晒太阳", "遛狗", "看电影", "追剧", "打卡", "周末", "宅家",
    "口袋机", "pocket3", "pocket 3", "gopro", "相机充满",
    "去玩", "晒图", "自拍", "好累", "好困", "摸鱼中",
]
# 实质信号：命中则不做 musing 可疑（避免口语化技术帖被 life_kw 误伤）。
SUBSTANTIVE_KEYWORDS = [
    "模型", "api", "发布", "开源", "论文", "评测", "对比", "价格", "额度",
    "bug", "更新", "版本", "融资", "gpt", "claude", "gemini", "agent",
    "prompt", "llm", "开源", "benchmark", "推理", "训练", "微调",
    "token", "上下文", "多模态", "开源模型", "权重", "sota",
    "产品", "上线", "changelog", "release", "sdk", "文档",
]
MUSING_STATUS_RE = re.compile(
    r"(准备去|准备|要去|先.{0,6}再|出门了|到了|回来了)",
)


def _tweet_body_text(tweet: dict) -> str:
    """优先 note_tweet 全文，否则 text；用于长度/关键词启发式。"""
    note = tweet.get("note_tweet") or {}
    note_text = (note.get("text") or "").strip()
    if note_text:
        return note_text
    return (tweet.get("text") or "").strip()


def _has_photo_media(tweet: dict) -> bool:
    for m in tweet.get("media") or []:
        if not isinstance(m, dict):
            continue
        if (m.get("type") or "") in ("photo", "animated_gif"):
            return True
    # GraphQL 归一化前的 fallback
    for m in ((tweet.get("extended_entities") or {}).get("media") or []):
        if isinstance(m, dict) and (m.get("type") or "") in ("photo", "animated_gif"):
            return True
    return False


def _has_non_media_url(tweet: dict, body: str) -> bool:
    """正文里是否有「非媒体 t.co」的实质外链。"""
    media_tcos: set[str] = set()
    for m in ((tweet.get("extended_entities") or {}).get("media") or []):
        if isinstance(m, dict) and m.get("url"):
            media_tcos.add(m["url"])
    for m in ((tweet.get("entities") or {}).get("media") or []):
        if isinstance(m, dict) and m.get("url"):
            media_tcos.add(m["url"])
    # 归一化 media 列表里通常是 pbs 直链，不含 t.co；entities 更可靠
    urls = URL_RE.findall(body)
    for u in urls:
        if u not in media_tcos:
            return True
    # entities.urls 里的 expanded/display 也算实质链接
    for ent in ((tweet.get("entities") or {}).get("urls") or []):
        if not isinstance(ent, dict):
            continue
        expanded = (ent.get("expanded_url") or ent.get("url") or "").strip()
        if not expanded:
            continue
        # 媒体 pic.twitter / pbs 不算
        if "pbs.twimg.com" in expanded or "pic.twitter.com" in expanded:
            continue
        if "twitter.com" in expanded and "/status/" in expanded and "/photo/" in expanded:
            continue
        return True
    return False


def _has_substantive_signal(tweet: dict, body: str) -> bool:
    """任一实质信号 → 不做 musing 可疑。"""
    if tweet.get("article"):
        return True
    note = tweet.get("note_tweet") or {}
    note_text = (note.get("text") or "").strip()
    if len(note_text) >= MUSING_NOTE_LONG_MIN:
        return True
    if _has_non_media_url(tweet, body):
        return True
    low = body.lower()
    for kw in SUBSTANTIVE_KEYWORDS:
        if kw.lower() in low:
            return True
    return False


def _musing_reason(tweet: dict, body: str) -> str | None:
    """若像碎碎念，返回 reason（以 musing 开头）；否则 None。"""
    if _has_substantive_signal(tweet, body):
        return None
    low = body.lower()
    has_photo = _has_photo_media(tweet)
    life_hits = [kw for kw in MUSING_LIFE_KEYWORDS if kw.lower() in low]

    if has_photo and len(body) <= MUSING_SHORT_MAX:
        return f"{REASON_MUSING_PREFIX}_short_photo({len(body)}字)"

    if life_hits:
        return f"{REASON_MUSING_PREFIX}_life_kw({','.join(life_hits[:3])})"

    if has_photo and len(body) < MUSING_STATUS_MAX and MUSING_STATUS_RE.search(body):
        return f"{REASON_MUSING_PREFIX}_status_photo({len(body)}字)"

    return None


def classify(tweet: dict) -> tuple[str, str]:
    """返回 (status, reason)。status: pass / suspicious / filter

    suspicious 的 reason 前缀分流 AI：
      commercial* / self_disclose* → confirm_promo
      musing*                      → confirm_musing
    """
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

    # 碎碎念启发式（promo 之后、pass 之前）：用全文 body（含 note_tweet）
    body = _tweet_body_text(tweet)
    musing = _musing_reason(tweet, body)
    if musing:
        return "suspicious", musing

    return "pass", "ok"


# ── API（带 token 轮换）────────────────────────────

class TokenExhausted(Exception):
    pass


def fetch_tweets(pool: TokenPool, username: str, limit: int = 20) -> list[dict]:
    """拉取用户推文。优先用 GraphQL（免费），fallback 到 6551.io。"""
    # Try GraphQL first (free, no API key)
    graphql_failed = False
    if HAS_GRAPHQL:
        try:
            tweets = twitter_graphql.fetch_tweets(username, limit=limit)
            print(f"  [GraphQL] 拉取 {len(tweets)} 条推文")
            return tweets
        except Exception as e:
            print(f"  [GraphQL] 失败: {e}，回退到 6551.io")
            graphql_failed = True

    # Fallback: 6551.io API (requires token)
    if pool is None:
        if graphql_failed:
            raise TokenExhausted("GraphQL 失败且无 6551.io token")
        return []

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


SEEN_RECOVERY_DIR = os.path.join(SEEN_DIR, ".seen_recovery")


def get_seen_backup_path(username: str) -> str:
    os.makedirs(SEEN_RECOVERY_DIR, exist_ok=True)
    return os.path.join(SEEN_RECOVERY_DIR, f"{username}.json")


def _read_seen_file(path: str) -> tuple[set[str], str | None]:
    with open(path) as f:
        data = json.load(f)
    return set(data.get("ids", [])), data.get("last_post_ts")


def load_seen(username: str) -> tuple[set[str], str | None]:
    path = get_seen_path(username)
    backup_path = get_seen_backup_path(username)

    # 1. 若存在 recovery 备份，合并/恢复主文件（P0-3 save_seen 写盘失败恢复）。
    if os.path.exists(backup_path):
        try:
            backup_ids, backup_ts = _read_seen_file(backup_path)
        except Exception:
            backup_ids, backup_ts = set(), None

        main_ids, main_ts = set(), None
        main_ok = False
        if os.path.exists(path):
            try:
                main_ids, main_ts = _read_seen_file(path)
                main_ok = True
            except Exception:
                main_ok = False

        if main_ok:
            merged_ids = main_ids | backup_ids
            merged_ts = main_ts
            if backup_ts and (not merged_ts or backup_ts > merged_ts):
                merged_ts = backup_ts
            try:
                save_seen(username, merged_ids, merged_ts)
                os.remove(backup_path)
            except Exception:
                # 落盘/删除失败时保留备份，下次继续恢复；内存中仍返回合并结果
                pass
            return merged_ids, merged_ts
        else:
            # 主文件缺失或损坏：用备份重建主文件
            try:
                save_seen(username, backup_ids, backup_ts)
                os.remove(backup_path)
            except Exception:
                pass
            return backup_ids, backup_ts

    # 2. 首次运行
    if not os.path.exists(path):
        return set(), None

    # 3. 主文件损坏且无备份：返回 corrupted 标记，让 process_user 进入安全推送模式（P0-5）
    try:
        return _read_seen_file(path)
    except Exception:
        return set(), "corrupted"


def save_seen(username: str, seen: set[str], last_post_ts: str | None = None) -> None:
    path = get_seen_path(username)
    kept = sorted(seen, reverse=True)[:500]
    payload = json.dumps({"ids": kept, "updated": datetime.now().isoformat(),
                          "last_post_ts": last_post_ts},
                         ensure_ascii=False, indent=2)
    try:
        _atomic_write(path, payload)
    except OSError as e:
        # 写主文件失败时先把当前内存状态写入 recovery 备份，防止已送达推文因超窗丢失（P0-3）
        backup_path = get_seen_backup_path(username)
        try:
            _atomic_write(backup_path, payload)
            print(f"  seen 写盘失败，已写入恢复备份 {backup_path}: {e}")
        except Exception as be:
            print(f"  seen 恢复备份也失败: {be}")
        raise


def get_push_retry_path(username: str) -> str:
    os.makedirs(SEEN_DIR, exist_ok=True)
    return os.path.join(SEEN_DIR, f"{username}_retry.json")


def load_push_retry(username: str) -> set[str]:
    path = get_push_retry_path(username)
    if not os.path.exists(path):
        return set()
    try:
        with open(path) as f:
            data = json.load(f)
        if isinstance(data, list):
            return {str(x) for x in data}
        if isinstance(data, dict):
            return {str(x) for x in data.get("ids", [])}
    except Exception:
        pass
    return set()


def save_push_retry(username: str, retry: set[str]) -> None:
    path = get_push_retry_path(username)
    if not retry:
        if os.path.exists(path):
            os.remove(path)
        return
    _atomic_write(path, json.dumps(sorted(retry), ensure_ascii=False, indent=2))


# ── 跨账号去重索引（纯转发 + Article；config 键 cross_account_dedup 开关）──
_CROSS_DEDUP_ENABLED = False        # main 从 cfg 置位；默认关 = 行为与现状一致
_PUSHED_INDEX_CACHE: "dict | None" = None   # 单进程内存缓存 = 同轮跨账号共享


def load_pushed_index() -> dict:
    """惰性读全局已推索引 {canonical_key: {ts, by}}；损坏/缺失回空（旁路容错）。"""
    global _PUSHED_INDEX_CACHE
    if _PUSHED_INDEX_CACHE is not None:
        return _PUSHED_INDEX_CACHE
    data: dict = {}
    try:
        with open(PUSHED_INDEX_PATH) as f:
            raw = json.load(f)
        entries = raw.get("entries")
        if isinstance(entries, dict):
            data = {str(k): v for k, v in entries.items() if isinstance(v, dict)}
    except Exception:
        data = {}
    _PUSHED_INDEX_CACHE = data
    return data


def save_pushed_index() -> None:
    """TTL + 容量 GC 后原子落盘。索引是旁路：任何失败都不该打断发送主线，
    调用方需捕获 OSError 只打日志。"""
    idx = load_pushed_index()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=PUSHED_INDEX_TTL_DAYS)).isoformat()
    for k in [k for k, v in idx.items() if str(v.get("ts") or "") < cutoff]:
        del idx[k]
    if len(idx) > PUSHED_INDEX_MAX_ENTRIES:
        overflow = sorted(idx, key=lambda k: str(idx[k].get("ts") or ""))
        for k in overflow[: len(idx) - PUSHED_INDEX_MAX_ENTRIES]:
            del idx[k]
    _atomic_write(PUSHED_INDEX_PATH,
                  json.dumps({"version": 1, "entries": idx}, ensure_ascii=False, indent=1))


def _canonical_key(t: dict) -> str:
    """推文的跨账号规范 id：纯转发 → 原推 id；原创/引用壳 → 自身 id。
    引用是新内容（带评论），只登记自身、不穿透到被引原推。"""
    rt = t.get("retweeted_status") or {}
    if rt.get("id"):
        return "t:" + str(rt["id"])
    return "t:" + str(t.get("id"))


def _cross_dup_hit(t: dict) -> "dict | None":
    """仅纯转发可被抑制：原推 canonical 已在索引中则返回命中条目，否则 None。"""
    rt = t.get("retweeted_status") or {}
    if not rt.get("id"):
        return None
    return load_pushed_index().get("t:" + str(rt["id"]))


def _record_pushed(t: dict, username: str) -> None:
    """送达 checkpoint 同点位登记 canonical（send-then-mark：崩溃窗口最多重复一条，
    重复优于丢失；失败/tombstone/降级轮不会走到这里 → 残缺快照不落库）。"""
    idx = load_pushed_index()
    idx[_canonical_key(t)] = {"ts": datetime.now(timezone.utc).isoformat(), "by": username}
    save_pushed_index()


def _record_pushed_article(article_id: str, username: str) -> None:
    idx = load_pushed_index()
    idx["a:" + str(article_id)] = {"ts": datetime.now(timezone.utc).isoformat(), "by": username}
    save_pushed_index()


def _alert_seen_save_failure(bot_token: str, chat_id: str, username: str, error: Exception) -> None:
    """seen 写盘失败时发 TG 告警，避免推送已送达但状态未落盘时无人知晓。"""
    if not (bot_token and chat_id):
        return
    text = (f"🚨 <b>seen 写盘失败</b>：@{html.escape(username)}\n"
            f"推送可能已送达但下轮会重复推。请检查磁盘空间。\n"
            f"<code>{html.escape(str(error)[:300])}</code>")
    _tg_post_quiet(bot_token, {"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                   "sendMessage")


# 话题失效自愈：thread not found 时把消息回退到默认话题（main 从配置置位），
# 事件累积到轮末汇总一条 DM 告警——话题被删/关闭不能变成静默断流。
_THREAD_FALLBACK_ID: "int | None" = None
_THREAD_FALLBACK_EVENTS: list = []


def _swap_thread_on_not_found(payload: dict, desc: str) -> "dict | None":
    """400 描述命中 thread not found 时返回换好话题的新 payload，否则 None。

    回退目标是 _THREAD_FALLBACK_ID；已在回退话题（或未配置回退）则直接摘掉
    message_thread_id 落 General——比静默丢消息好。"""
    if payload.get("message_thread_id") is None:
        return None
    if "message thread not found" not in (desc or "").lower():
        return None
    bad = payload.get("message_thread_id")
    _THREAD_FALLBACK_EVENTS.append(bad)
    fixed = dict(payload)
    if _THREAD_FALLBACK_ID and bad != _THREAD_FALLBACK_ID:
        fixed["message_thread_id"] = _THREAD_FALLBACK_ID
        print(f"  ⚠️ thread {bad} 不存在（话题被删/关闭？），回退默认话题 {_THREAD_FALLBACK_ID}")
    else:
        fixed.pop("message_thread_id", None)
        print(f"  ⚠️ thread {bad} 不存在且无可用回退话题，落 General")
    return fixed


def _alert_thread_fallback(bot_token: str, chat_id: str) -> None:
    """轮末把本轮全部 thread-not-found 回退汇总成一条 DM 告警并清空事件。"""
    if not _THREAD_FALLBACK_EVENTS:
        return
    bad = ", ".join(sorted({str(x) for x in _THREAD_FALLBACK_EVENTS}))
    _THREAD_FALLBACK_EVENTS.clear()
    if not (bot_token and chat_id):
        return
    text = ("⚠️ <b>X Monitor 话题路由回退</b>\n"
            f"以下 message_thread_id 报 thread not found，消息已回退默认话题：<code>{html.escape(bad)}</code>\n"
            "请检查话题是否被删除/关闭，并修正路由表或 config。")
    _tg_post_quiet(bot_token, {"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                   "sendMessage")


def _alert_ai_all_failed(bot_token: str, chat_id: str, username: str) -> None:
    """AI 推广识别全部后端失败时告警，避免 suspicious 推文被错误放行。"""
    if not (bot_token and chat_id):
        return
    text = (f"⚠️ <b>AI 推广识别全部后端失败</b>：@{html.escape(username)}\n"
            f"本轮 suspicious 推文已降级为 filter，避免推广内容漏推。")
    _tg_post_quiet(bot_token, {"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                   "sendMessage")


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


def load_cookie_health() -> dict:
    """读取 cookie 认证健康状态 {consecutive_degraded, alerted, alert_msg_id, ...}。"""
    try:
        with open(COOKIE_HEALTH_PATH) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_cookie_health(state: dict) -> None:
    _atomic_write(COOKIE_HEALTH_PATH, json.dumps(state, ensure_ascii=False, indent=2))


def check_cookie_health(bot_token: str, chat_id: str, dry_run: bool = False) -> None:
    """轮末看门狗：本进程是否"整轮未取得 authed 访问"（cookie 失效→静默降级 guest）。

    连续达到 COOKIE_DEGRADE_ALERT_THRESHOLD 轮只告警一次；authed 恢复后清零并取消置顶。

    背景：Mac 端 refresh_x_cookies 因 macOS 更新重置完全磁盘访问而静默断了 18 天，
    cookie 过期后监控整轮降级 guest 仍能拉公开推文 → 按账号的 note_account_failure
    抓不到（guest 拉取不算 failure）。此看门狗独立盯"认证整体失效"，宁重勿漏。
    """
    try:
        import twitter_graphql as tg
        health = tg.auth_health_summary()
    except Exception as e:
        print(f"  cookie 健康检查跳过（读 auth 状态失败）: {e}", file=sys.stderr)
        return

    state = load_cookie_health()

    # 恢复轮：本轮拿到了 authed 访问 → 清零；曾告警过则改文案 + 取消置顶。
    if not health.get("degraded"):
        if state.get("alerted") and not dry_run:
            mid = state.get("alert_msg_id")
            if mid:
                _tg_post_quiet(bot_token, {"chat_id": chat_id, "message_id": mid},
                               "unpinChatMessage")
                _tg_post_quiet(bot_token,
                               {"chat_id": chat_id, "message_id": mid,
                                "text": "✅ X cookie 认证已恢复（authed 访问成功）。",
                                "parse_mode": "HTML"}, "editMessageText")
        save_cookie_health({"consecutive_degraded": 0, "alerted": False})
        return

    # 降级轮：累加连续计数。
    count = int(state.get("consecutive_degraded", 0)) + 1
    now_iso = datetime.now(timezone.utc).isoformat()
    state["consecutive_degraded"] = count
    state["last_degraded_at"] = now_iso
    state.setdefault("first_degraded_at", now_iso)

    if count >= COOKIE_DEGRADE_ALERT_THRESHOLD and not state.get("alerted"):
        reason = ("cookie 文件缺失或被改名 .stale"
                  if not health.get("cookies_loaded")
                  else f"authed 请求全被拒（本轮降级 {health.get('degrade_events', 0)} 次）")
        text = (f"🍪 <b>X cookie 认证告警</b>：已连续 {count} 轮未取得 authed 访问（降级 guest）。\n"
                f"原因：{reason}。\n"
                f"多半是 Mac 端 refresh_x_cookies 断了（常见：系统更新重置完全磁盘访问）。\n"
                f"排查：launchd <code>com.apple.x-cookie-refresh</code> + "
                f"<code>/tmp/x-cookie-refresh.log</code>。")
        if dry_run:
            print(f"  DRY RUN cookie 降级告警: 连续 {count} 轮")
        else:
            try:
                r = send_telegram(bot_token, chat_id, text)
                print(f"  cookie 降级告警推送 {'OK' if r.get('ok') else 'FAIL'}")
                # 告警宁重勿漏：assumed_delivered（疑似送达）不落定 alerted，下轮重发。
                if r.get("ok") and not r.get("assumed_delivered"):
                    state["alerted"] = True
                    mid = (r.get("result") or {}).get("message_id")
                    if mid:
                        state["alert_msg_id"] = mid
                        _tg_post_quiet(bot_token, {"chat_id": chat_id, "message_id": mid},
                                       "pinChatMessage")
            except Exception as e:
                print(f"  cookie 降级告警推送异常: {e}")

    save_cookie_health(state)


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
                # 告警通道方向与内容推送相反：宁重勿漏。assumed_delivered（疑似
                # 送达）不落定 alerted，下轮重发；重复一条告警 << 告警静默丢失。
                if r.get("ok") and not r.get("assumed_delivered"):
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


def update_status_dashboard(bot_token: str, chat_id: str, accounts: list[dict],
                            failures: dict, pushed: int, articles: int,
                            elapsed: float) -> None:
    """置顶状态看板：一条置顶消息每轮原地编辑（编辑不触发通知，零打扰）。

    message_id / 当日计数 / 看板创建时刻 / 聊天 TTL 存 .dashboard.json。
    若聊天开了自动删除（auto-delete），看板消息会按"发送时刻"被删——editMessageText
    不重置该计时器，故每轮读 TTL，在消息存活到 TTL 的 85% 时主动重建一条新看板
    并置顶（旧的留给 auto-delete 自然清理）；编辑失败（被手动删等）也走重建。
    全程 best-effort。
    """
    try:
        with open(DASHBOARD_PATH) as f:
            state = json.load(f)
        if not isinstance(state, dict):
            state = {}
    except Exception:
        state = {}

    now_cn = datetime.now(timezone(timedelta(hours=8)))  # 北京时间（无夏令时）
    # 计数日界 = 北京时间每天 06:00（用户指定起始点）：06:00 前计入前一天
    today = (now_cn - timedelta(hours=6)).strftime("%Y-%m-%d")
    if state.get("date") != today:
        state["date"] = today
        state["tweets_today"] = 0
        state["articles_today"] = 0
    for k in ("tweets_today", "articles_today"):
        if not isinstance(state.get(k), int):  # 脏状态自愈，不让看板崩整轮
            state[k] = 0
    state["tweets_today"] += pushed
    state["articles_today"] += articles

    # 只统计仍在配置中的账号：被移除/禁用账号的幽灵失败记录不污染看板
    known = {a.get("username") for a in accounts}
    bad = {u: r for u, r in failures.items()
           if u in known and int(r.get("count", 0)) > 0}
    lines = [
        "📊 <b>X 监控状态</b>",
        f"🕒 上轮 {now_cn.strftime('%m-%d %H:%M')}（北京时间）· {elapsed:.0f}s",
        f"📤 本轮推送 {pushed} · 今日 {state['tweets_today']} 条 · 文章任务 {state['articles_today']}",
        f"👀 账号 {len(accounts) - len(bad)}/{len(accounts)} 正常",
    ]
    for u, r in sorted(bad.items()):
        lines.append(f"⚠️ @{u} 连续 {r.get('count')} 轮失败："
                     f"{html.escape(str(r.get('last_error', ''))[:60])}")
    text = "\n".join(lines)

    # 读聊天 auto-delete TTL（秒）；成功则更新缓存，失败沿用上次缓存值
    info = _tg_post_quiet(bot_token, {"chat_id": chat_id}, "getChat")
    if info.get("ok"):
        state["ttl"] = int((info.get("result") or {}).get("message_auto_delete_time") or 0)
    ttl = int(state.get("ttl") or 0)

    mid = state.get("message_id")
    try:
        created_at = float(state.get("created_at") or 0)
    except (TypeError, ValueError):
        created_at = 0.0  # 脏 created_at 视作 0：触发一次重建后写回干净值自愈
    age = time.time() - created_at
    # TTL 启用且看板将近到期 → 主动重建，避免被 auto-delete 删后出现置顶空窗
    stale = bool(mid and ttl and age > ttl * DASHBOARD_REBUILD_FRACTION)
    if stale:
        print(f"  看板将近 auto-delete（存活 {age:.0f}s / TTL {ttl}s），主动重建")

    if mid and not stale:
        r = _tg_post_quiet(bot_token, {"chat_id": chat_id, "message_id": mid,
                                       "text": text, "parse_mode": "HTML"},
                           "editMessageText")
        if r.get("ok"):
            _atomic_write(DASHBOARD_PATH, json.dumps(state, ensure_ascii=False, indent=2))
            return
        print("  看板编辑失败，重建")
    r = _tg_post_quiet(bot_token, {"chat_id": chat_id, "text": text,
                                   "parse_mode": "HTML",
                                   "disable_notification": True,
                                   "link_preview_options": {"is_disabled": True}},
                       "sendMessage")
    new_mid = (r.get("result") or {}).get("message_id") if r.get("ok") else None
    if new_mid:
        _tg_post_quiet(bot_token, {"chat_id": chat_id, "message_id": new_mid},
                       "pinChatMessage")
        if mid:
            _tg_post_quiet(bot_token, {"chat_id": chat_id, "message_id": mid},
                           "unpinChatMessage")
            _tg_post_quiet(bot_token, {"chat_id": chat_id, "message_id": mid},
                           "deleteMessage")
        state["message_id"] = new_mid
        # 用 Telegram 返回的消息发送时刻（服务器时钟）作为 TTL 计时基准
        state["created_at"] = (r.get("result") or {}).get("date") or time.time()
    _atomic_write(DASHBOARD_PATH, json.dumps(state, ensure_ascii=False, indent=2))


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
    if re.fullmatch(r"^[*#>\-\s]+$", summary):
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
    # 2000 而非 220：推理模型（如 gemini-3.5-flash）220 token 会被隐藏推理吃掉
    # 推理吃光（finish_reason=length，content 空 / 截断片段）→ TL;DR 退化成截断。
    # 实测 2000 下两个后端都能产出完整一行 TL;DR（推理 ~150-300 + 正文 ~50-100）。
    summary, backend = ai.complete(prompt, max_tokens=2000, temperature=0.2)
    if not summary:
        return None
    summary = collapse_text(re.sub(r"^TL;?DR[:：]\s*", "", summary, flags=re.IGNORECASE))
    if is_bad_tldr(summary, note_text):
        print(f"    AI [{backend}] TL;DR 质量不足，使用短预览")
        return None
    return short_preview(summary, TLDR_PREVIEW_LIMIT)


RT_PREFIX_RE = re.compile(r"^(RT @[A-Za-z0-9_]+:)[ \t]*")


def _break_rt_prefix(text: str) -> str:
    """转推正文 'RT @用户名: 正文' → 'RT @用户名:\\n\\n正文'：转推归属与正文分两段。"""
    return RT_PREFIX_RE.sub(r"\1\n\n", text, count=1)


def _rich_preserve(text: str) -> str:
    """Rich 消息 html 字段保留原推结构：转义后换行转 <br>、连续空格转 &nbsp;。

    Rich HTML 默认把源码里的换行和连续空格压扁成一行（官方文档原话
    'all the text above was on the same line'）——这就是「全文」变一坨的原因。
    <br> 与 &nbsp; 都在 Rich HTML 支持的标签/命名实体范围内。先 escape 再加标签，
    顺序保证加进去的 <br>/&nbsp; 不被二次转义。
    """
    esc = html.escape(text)
    esc = esc.replace("\n", "<br>")
    esc = re.sub(r" {2,}", lambda m: "&nbsp;" * len(m.group()), esc)
    return esc


def _fmt_duration(ms: "int | None") -> str:
    s = int((ms or 0) / 1000)
    return f"{s // 60}:{s % 60:02d}"


# rich 外链媒体拉取上限 20MB（照片 5MB）。HEAD 拿到精确字节数时留 1MB 余量；
# HEAD 失败回退 bitrate×时长估算时再收紧（bitrate 是峰值声明，实测估算偏大 3-5 倍）。
RICH_VIDEO_HEAD_MAX_BYTES = 19 * 1024 * 1024
RICH_VIDEO_EST_MAX_BYTES = 18 * 1024 * 1024
_RICH_VIDEO_ENABLED = False   # main 从 cfg["rich_video_embed"] 置位；默认关 = 封面行为


def _head_content_length(url: str) -> "int | None":
    """HEAD 取 Content-Length（bwg 实测与 Telegram 拉取到的 file_size 一字不差）；
    任何失败回 None（回退估算，绝不阻塞格式化主线超过超时）。"""
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=8) as r:
            cl = r.headers.get("Content-Length")
            return int(cl) if cl else None
    except Exception:
        return None


def _pick_embeddable_mp4(m: dict) -> "str | None":
    """从 variants（bitrate 降序）选「能塞进外链上限的最大清晰度」mp4。

    HEAD 精确判断优先；HEAD 不可用退回 bitrate/8×duration 估算（更保守上限）。
    gif/无 bitrate 档估算为 0 视为可嵌。全部超限/无档回 None → 维持封面行为。"""
    variants = m.get("variants") or []
    if not variants and m.get("video_url"):
        variants = [{"url": m["video_url"], "bitrate": m.get("bitrate") or 0}]
    dur_s = (m.get("duration_ms") or 0) / 1000
    for v in variants:
        url = v.get("url")
        if not url or not url.startswith("https://") or '"' in url or "<" in url:
            continue
        size = _head_content_length(url)
        if size is not None:
            if size <= RICH_VIDEO_HEAD_MAX_BYTES:
                return url
            continue  # 精确超限：试下一档
        est = (v.get("bitrate") or 0) / 8 * dur_s
        if est <= RICH_VIDEO_EST_MAX_BYTES:
            return url
    return None


def _rich_media_block(t: dict, embed_video: bool = True) -> str:
    """普通推文媒体块（rich html 字段）：照片嵌 <img>；视频/GIF 在 rich_video_embed
    开启且体积能塞进外链上限时嵌可播放 <video>（2026-07-10 探针验证 Telegram
    服务端可拉 video.twimg.com 并物化原生 Video），否则退回封面 <img> + ▶️/时长；
    多媒体拼 <tg-collage>（官方支持 img/video 混排），上限 4。
    语法是官方 Rich HTML（不是文章摘要用的 Markdown ![]()），媒体只能作独立块。
    URL 来自 GraphQL 已提取的 t['media']（转推媒体由转推全文重建回填进来）。
    embed_video=False 供发送降级梯剥视频重试（视频被 Telegram 拒时换封面重发）。
    """
    parts = []
    hint = ""
    for m in t.get("media") or []:
        url = m.get("url")  # photo: media_url_https；video/gif: 封面缩略图
        if not url or not url.startswith("https://") or '"' in url or "<" in url:
            continue
        mp4 = None
        if (m.get("type") in ("video", "animated_gif")
                and embed_video and _RICH_VIDEO_ENABLED):
            mp4 = _pick_embeddable_mp4(m)
        if mp4:
            parts.append(f'<video src="{mp4}"/>')
        else:
            parts.append(f'<img src="{url}"/>')
            if m.get("type") in ("video", "animated_gif") and not hint:
                hint = (f"▶️ 视频 · {_fmt_duration(m.get('duration_ms'))}"
                        if m.get("type") == "video" else "▶️ GIF")
    if not parts:
        return ""
    parts = parts[:4]
    if len(parts) == 1:
        media_html = parts[0]
    else:
        media_html = "<tg-collage>" + "".join(parts) + "</tg-collage>"
    block = f"<br><br>{hint}" if hint else ""
    return block + f"<br><br>{media_html}"


def _strip_media_tco(text: str, t: dict) -> str:
    """去掉 full_text 里「媒体对应」的 t.co 短链（图片已作为媒体块内嵌，裸链冗余）。

    用 entities/extended_entities 的 media[].url（媒体专属 t.co）精确匹配并移除，
    不碰用户正文里主动分享的其它链接——避免「末尾正则」误删真实分享链接。原推（截图）
    里本就不显示这个媒体短链。
    """
    media = ((t.get("extended_entities") or {}).get("media") or []) + \
            ((t.get("entities") or {}).get("media") or [])
    urls = {m.get("url") for m in media if isinstance(m, dict) and m.get("url")}
    for u in urls:
        text = text.replace(u, "")
    return text.strip()


def format_message(
    username: str, t: dict, ai: "AIClassifier | None" = None,
    *, embed_video: bool = True,
) -> tuple[str, str, str]:
    """Build both the HTML fallback message and the rich-message variant.

    Returns (html_text, rich_html, link). The HTML `text` is unchanged from the
    legacy path (folded_full capped at ~2800 chars / 3000 UTF-16 units to stay
    under the 4096 sendMessage limit) and feeds send_telegram on rich fallback.
    rich_html targets sendRichMessage's html field (RICH_MESSAGE_MAX_CHARS budget)
    and folds the full note text with a much larger cap so long tweets show in full.
    """
    tid = t.get("id") or t.get("conversation_id_str") or ""
    link = f"https://x.com/{username}/status/{tid}" if tid else ""
    hidden = f'<a href="{link}">​</a>' if link else ""
    note = t.get("note_tweet") or {}
    # 长推(note_tweet)与普通推文都平铺全文（用户指定 2026-07-01：不再 TL;DR/折叠/140 截断）；
    # 带 article 节点且无长文的推走文章预览（标题），不裸推 t.co 链接。
    note_text = _break_rt_prefix(note.get("text", "").strip())  # RT 归属换行
    full_text = note_text
    if not full_text and not t.get("article"):
        full_text = _break_rt_prefix(t.get("text", "").strip())
    if full_text and any((m or {}).get("type") == "photo" for m in (t.get("media") or [])):
        # 图片已作为 rich 媒体块内嵌，正文里对应的 t.co 短链冗余，剥掉（不碰其它真实分享链接）。
        full_text = _strip_media_tco(full_text, t)
    rich_body = ""
    if full_text:
        # HTML 回退受 sendMessage 4096 限制：Telegram 按 UTF-16 计长（astral 表情每个
        # 2 单位），按单位收缩到 3000 以内，否则截断仍可能超 4096 致整条静默丢失。
        body = full_text if len(full_text) <= 2800 else full_text[:2800] + "…"
        while len(body.encode("utf-16-le")) // 2 > 3000 and len(body) > 100:
            body = body[: int(len(body) * 0.9)] + "…"
        # Rich 正文上限远大（sendRichMessage 单条 32768）：约 28000 字符起步，但
        # _rich_preserve 的 <br>/&nbsp; 会让长度膨胀，故按「渲染后」UTF-16 长度收缩到
        # 27000 单位以内，留 header/标签余量保持在 RICH_MESSAGE_MAX_CHARS 下。
        rich_src = full_text if len(full_text) <= 28000 else full_text[:28000] + "…"
        rich_body = _rich_preserve(rich_src)
        while len(rich_body.encode("utf-16-le")) // 2 > 27000 and len(rich_src) > 100:
            rich_src = rich_src[: int(len(rich_src) * 0.9)] + "…"
            rich_body = _rich_preserve(rich_src)
    elif t.get("article"):
        body = article_preview_text(t)
    else:
        body = ""
    if body:
        text = f'📢 @{username}{hidden}\n\n{html.escape(body)}'
    else:
        text = f'📢 @{username}{hidden}'
    # Rich HTML variant: tweet body/note is raw user content → _rich_preserve it
    # (escape + <br> + &nbsp;) and send via the rich `html` field (NOT markdown),
    # so < > * _ # | $ [ ] etc. can't be parsed as rich syntax / inject nested
    # blocks，同时保留原推的换行与空格。长推 rich_body 已是平铺全文。
    if not rich_body:
        rich_body = _rich_preserve(body) if body else ""
    if rich_body:
        # 头部独占一行：rich html 把裸 \n\n 折叠成同一行，必须用 <br><br>
        # （HTML 回退路径用原生 \n\n，那条路径换行不折叠）。
        rich_html = f'📢 @{username}<br><br>{rich_body}'
    else:
        rich_html = f'📢 @{username}'
    # 照片 / 视频封面缩略图嵌进 rich 末尾（文章自带配图走另一路径，不在此处理）。
    if not t.get("article"):
        rich_html += _rich_media_block(t, embed_video)
    return text, rich_html, link


class TgAmbiguousDelivery(OSError):
    """请求已完整送出但响应缺失/不可读：Telegram 可能已处理，不可盲目重发。

    2026-07-02 X 话题重复推送根因：sendRichMessage 带图时 Telegram 服务端先拉图
    再回响应，15s 读超时被当作"发送失败"重发，实际第一次已入群。
    子类 OSError 是为了不缩小 macrumors_daily 等既有 except (URLError, OSError)
    调用方的捕获面（行为等同旧的裸 socket.timeout）；twitter_monitor 自己的发送
    函数则显式识别本异常并按已送达处理。
    """


# 连续歧义熔断：单条慢响应（本次事故形态）按已送达防重复；但连续多条 60s 读超时
# 的先验解释是「Telegram 没在处理」（边缘 LB 活着、后端挂死的大面积故障形态），
# 继续按已送达会把整轮推文批量标 seen 静默丢弃。故本进程内第 2 条起改按失败处理
# （进 push_retry，恢复后重发；最坏重复 1 条 << 批量永久丢失）。只有确由 Bot API
# 后端产生的响应（可解析的 2xx 回执、4xx 含 429）才清零计数；5xx/垃圾 2xx 可能
# 是边缘 nginx 在后端挂死时生成的，不清零。
_AMBIGUOUS_STREAK = 0


def _note_definite_response() -> None:
    global _AMBIGUOUS_STREAK
    _AMBIGUOUS_STREAK = 0


def _register_ambiguous_send() -> bool:
    """记一次歧义发送；返回 True=按已送达处理，False=连续歧义应按失败处理。"""
    global _AMBIGUOUS_STREAK
    _AMBIGUOUS_STREAK += 1
    return _AMBIGUOUS_STREAK < 2


def _record_assumed_delivery(method: str, link: str) -> None:
    """按已送达处理的持久痕迹：下一轮开头汇总 DM 提醒人工核对（真丢推可发现可补救）。

    任何失败只打日志：留痕是发送路径的旁路，绝不能让它打断 assumed_delivered 返回。
    """
    entries = []
    try:
        with open(ASSUMED_DELIVERY_PATH, encoding="utf-8") as f:
            entries = json.load(f)
        if not isinstance(entries, list):
            entries = []
    except FileNotFoundError:
        pass
    except Exception:
        entries = []
    entries.append({"ts": datetime.now(timezone.utc).isoformat(),
                    "method": method, "link": link or ""})
    try:
        # 全新部署/目录被清时 SEEN_DIR 可能尚未创建（load_seen 是唯一其他来源）
        os.makedirs(os.path.dirname(ASSUMED_DELIVERY_PATH) or ".", exist_ok=True)
        _atomic_write(ASSUMED_DELIVERY_PATH,
                      json.dumps(entries[-50:], ensure_ascii=False, indent=2))
    except OSError as e:
        print(f"  assumed-delivery 痕迹写盘失败（忽略）: {e}")


def _flush_assumed_delivery_notice(bot_token: str, chat_id: str) -> None:
    """上轮有歧义按已送达的发送时，发汇总 DM 供人工核对，送达确认后清痕迹。

    直发 _tg_post 而不走 send_telegram：通知自身再遇歧义时绝不能写回它正在
    汇报的账本（自指条目会挤掉真实痕迹），也不占用本进程"首条歧义按已送达"
    的熔断额度（否则内容通道阈值实际从 2 降到 1）。失败/歧义都只保留文件，
    下轮重试——告警自身绝不静默丢失。
    """
    entries = None
    try:
        with open(ASSUMED_DELIVERY_PATH, encoding="utf-8") as f:
            entries = json.load(f)
    except FileNotFoundError:
        return
    except Exception:
        pass
    if not isinstance(entries, list) or not entries:
        # 空/损坏痕迹无法报告：清掉避免每轮空转噪音（内容已不可恢复）
        try:
            os.remove(ASSUMED_DELIVERY_PATH)
        except OSError:
            pass
        return
    lines = [f"⚠️ <b>歧义送达核对</b>：此前 {len(entries)} 条消息因响应缺失按已送达处理，"
             f"请核对是否真的入群："]
    for e in entries[-10:]:
        ts = str(e.get("ts", ""))[:16].replace("T", " ")
        lines.append(html.escape(f"· {ts} {e.get('method', '')} {e.get('link') or '(无链接)'}"))
    if len(entries) > 10:
        lines.append(f"…另有 {len(entries) - 10} 条更早的略")
    payload = {"chat_id": chat_id, "text": "\n".join(lines), "parse_mode": "HTML",
               "link_preview_options": {"is_disabled": True}}
    try:
        r = _tg_post(bot_token, payload)
    except Exception as e:
        print(f"  歧义送达汇总告警发送失败，保留痕迹下轮再试: {e}")
        return
    if r.get("ok"):
        try:
            os.remove(ASSUMED_DELIVERY_PATH)
        except OSError:
            pass


def _tg_post(token: str, payload: dict, method: str = "sendMessage") -> dict:
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    # urllib 的异常分相（以 bwg /usr/bin/python3 3.9 的 do_open 实现为准）：
    # 连接/TLS/发送请求体阶段的 OSError 会被包成 URLError —— 请求未送达，重试安全；
    # getresponse()/读响应体阶段的异常（socket.timeout/ConnectionReset/
    # RemoteDisconnected/BadStatusLine/IncompleteRead…）裸抛 —— 请求已完整送出，
    # Telegram 可能已处理，归类为 TgAmbiguousDelivery 禁止盲目重发。
    # 收窄到 (OSError, HTTPException)：InvalidURL（token 脏字符，发生在联网前）及
    # ValueError/UnicodeEncodeError 等本地确定性错误必须裸抛响亮失败——误归歧义
    # 会把持久性配置错误变成「全部标 seen 的静默丢推」。
    try:
        resp = urllib.request.urlopen(req, timeout=60)
    except urllib.error.HTTPError as e:
        if e.code == 504:
            # 网关超时：上游已收到请求但未及时响应——与读超时同构的歧义
            # （请求可能已被处理），归入 TgAmbiguousDelivery，绝不能走
            # 5xx 盲重试路径重发。502/503 表示未到达后端，保留重试。
            raise TgAmbiguousDelivery(f"HTTP 504: {e.reason}") from e
        # 4xx（含 429）由 Bot API 后端产生，证明链路在处理请求 → 清零熔断计数；
        # 其余 5xx 多为边缘 nginx 在后端不可达时直接生成，不证明任何事，不清零——
        # 否则「一半 502 一半挂死」的大面积故障会让熔断永不触发、批量丢推。
        if e.code < 500:
            _note_definite_response()
        raise
    except urllib.error.URLError:
        raise  # 发出前失败（连接/TLS/发送阶段），可安全重试
    except http.client.InvalidURL:
        raise  # URL 本地校验失败，未联网，绝非歧义
    except (OSError, http.client.HTTPException) as e:
        raise TgAmbiguousDelivery(f"{type(e).__name__}: {e}") from e
    try:
        with resp:
            body = resp.read()
    except (OSError, http.client.HTTPException) as e:
        # 状态行已收到但响应体读取失败：消息几乎必定已发出
        raise TgAmbiguousDelivery(f"{type(e).__name__}: {e}") from e
    try:
        result = json.loads(body.decode("utf-8"))
    except ValueError as e:
        # 2xx 已确认但响应体不是合法 JSON（网关/代理异常页）：已送达按歧义处理，
        # 且不清零熔断计数——连续垃圾 2xx 同样是「后端没在处理」的故障形态
        raise TgAmbiguousDelivery(f"{type(e).__name__}: {e}") from e
    _note_definite_response()  # 可解析的 Bot API 回执才算确定性响应
    return result


def _tg_post_quiet(token: str, payload: dict, method: str) -> dict:
    """编辑/置顶类锦上添花调用：失败只打日志，绝不打断本轮监控。

    "message is not modified" 视为成功：内容没变不该触发看板的删旧重建。
    """
    try:
        return _tg_post(token, payload, method=method)
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", "replace")
        except Exception:
            body = ""
        if "message is not modified" in body:
            return {"ok": True, "not_modified": True}
        print(f"  {method} 失败（忽略）: {e} {body[:120]}")
        return {"ok": False}
    except Exception as e:
        print(f"  {method} 失败（忽略）: {e}")
        return {"ok": False}


def _html_to_plain(text: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", text))


def send_telegram_rich(token: str, chat_id: str, markdown: str = "", link: str = "",
                       *, html: str = "", thread_id: "str | int | None" = None) -> dict:
    """sendRichMessage（Bot API Rich Message，上限 32768 字符）。

    传 markdown 走 Rich Markdown 字段；传 html=… 走 Rich HTML 字段（恰传其一）。
    原始用户内容（推文正文 / AI 标题）走 html 字段 + html.escape 更安全，避免
    < > * _ # | $ [ ] 等被当 markdown 语法解析或注入嵌套块；文章摘要仍用 markdown。
    重试语义与 send_telegram 一致（429 按 retry_after、5xx/网络退避重试）。
    与 send_telegram 的关键差异：400/404 不在本函数内降级，而是返回
    {"ok": False, "rich_fallback": True, ...} 让调用方回退到旧的
    parse_mode=HTML 分块路径（那条路径自带完整的转义/分块/降级逻辑）。
    """
    # skip_entity_detection：不关的话头部 @X用户名 会被自动链接到 Telegram
    # 同名账号（误导）；显式链接（markdown [文字](url) / html <a href>）不受影响。
    rich: dict = {"skip_entity_detection": True}
    if html:
        rich["html"] = html
    else:
        rich["markdown"] = markdown
    payload: dict = {"chat_id": chat_id, "rich_message": rich}
    if thread_id is not None:
        payload["message_thread_id"] = thread_id
    if link:
        payload["reply_markup"] = {
            "inline_keyboard": [[{"text": "\U0001f517 打开原文", "url": link}]]
        }

    last_err = None
    for attempt in range(3):
        # 逐次预算门槛：单次尝试最坏挂 60s（socket 超时），剩余预算不足时绝不
        # 发起请求——被放弃的尝试从未发出，抛给调用方走 push_failed/push_retry
        # 无重复风险；发起了的请求必然在 SIGALRM 前收到结果（checkpoint 可落盘）。
        # 直调/测试/macrumors 下 _ARTICLE_QUEUE_RUN_START 为 None → inf，不触发。
        if _article_queue_time_remaining() < SEND_ATTEMPT_MIN_REMAINING_SECONDS:
            raise last_err or RuntimeError(
                "send_telegram_rich: 剩余预算不足以发起尝试，本轮放弃（进 push_retry）")
        try:
            return _tg_post(token, payload, method="sendRichMessage")
        except TgAmbiguousDelivery as e:
            # 请求已送出、响应缺失：大概率已入群，重发必产生重复消息（Bot API
            # 无幂等 token）。按已送达返回成功，调用方正常标 seen / 标 sent，
            # 宁可极小概率漏推也不重复推。连续歧义则熔断改按失败（防大面积故障
            # 时批量静默丢推），痕迹落盘供下轮汇总核对。
            if not _register_ambiguous_send():
                print(f"  ⚠ sendRichMessage 连续歧义（疑似 Telegram 故障），按失败进重试: {e}")
                raise
            _record_assumed_delivery("sendRichMessage", link)
            print(f"  ⚠ sendRichMessage 响应缺失，按已送达处理（防重复）: {e}")
            return {"ok": True, "assumed_delivered": True}
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
                if e.code == 400:
                    fixed = _swap_thread_on_not_found(payload, desc)
                    if fixed is not None:
                        payload = fixed  # 话题失效：换回退话题占用一次重试，400 即时无预算压力
                        continue
                return {"ok": False, "rich_fallback": True,
                        "error_code": e.code, "description": desc}
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            # 只有发出前的失败会走到这（_tg_post 已把发出后的失败归为
            # TgAmbiguousDelivery 并在上方分支返回）：请求未送达，重试安全。
            last_err = e
            time.sleep(2 * (attempt + 1))
            continue
    if last_err:
        raise last_err
    raise RuntimeError("send_telegram_rich: exhausted retries")


def send_telegram(token: str, chat_id: str, text: str, link: str = "",
                  *, thread_id: "str | int | None" = None) -> dict:
    payload: dict = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }
    if thread_id is not None:
        payload["message_thread_id"] = thread_id
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
        # 逐次预算门槛：同 send_telegram_rich（未发出=未送达，放弃无重复风险）
        if _article_queue_time_remaining() < SEND_ATTEMPT_MIN_REMAINING_SECONDS:
            raise last_err or RuntimeError(
                "send_telegram: 剩余预算不足以发起尝试，本轮放弃（进 push_retry）")
        try:
            return _tg_post(token, payload)
        except TgAmbiguousDelivery as e:
            # 同 send_telegram_rich：请求已送出、响应缺失，重发必重复，按已送达处理；
            # 连续歧义熔断按失败，痕迹落盘供下轮汇总核对。
            if not _register_ambiguous_send():
                print(f"  ⚠ sendMessage 连续歧义（疑似 Telegram 故障），按失败进重试: {e}")
                raise
            _record_assumed_delivery("sendMessage", link)
            print(f"  ⚠ sendMessage 响应缺失，按已送达处理（防重复）: {e}")
            return {"ok": True, "assumed_delivered": True}
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
            if e.code == 400:
                # 话题失效先于 parse_mode 剥离：否则先剥格式重进同一个死话题，
                # 再 400 时 parse_mode 已不在，直接 raise 进 push_retry 永久循环。
                desc = ""
                try:
                    desc = json.loads(body).get("description", "")
                except Exception:
                    desc = body[:200]
                fixed = _swap_thread_on_not_found(payload, desc)
                if fixed is not None:
                    payload = fixed
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


def send_tweet(
    token: str, chat_id: str, username: str, t: dict, ai: "AIClassifier | None" = None,
    *, thread_id: "str | int | None" = None,
) -> dict:
    """统一推文推送入口：rich-first → HTML fallback。

    username/t/ai 与原 format_message 调用点（process_user 循环）的实参一致。
    返回发送响应 dict，调用方仍用 r.get("ok") 做 push_failed/seen 判定。
    - rich 优先：rich_html 不超 RICH_MESSAGE_MAX_CHARS 时走 send_telegram_rich
      的 html 字段；成功直接返回；非 rich_fallback 的失败（如 429 已重试穷尽）原样返回。
    - rich 被拒（rich_fallback）或超长 → 回退现有 HTML 路径 send_telegram。
    """
    html_text, rich_html, link = format_message(username, t, ai)
    if len(rich_html) <= RICH_MESSAGE_MAX_CHARS:
        r = send_telegram_rich(token, chat_id, link=link, html=rich_html, thread_id=thread_id)
        if r.get("ok"):
            return r
        if not r.get("rich_fallback"):
            return r
        if "<video" in rich_html:
            # 降级梯（仿文章「带图被拒→去图重试」）：rich 含可播视频被确定性 400 拒
            # （拉取失败/超限误估）→ 剥视频换封面重发 rich，再拒才落 HTML。
            # 歧义（TgAmbiguousDelivery）在 send 内部已按已送达返回，走不到这里，
            # 不存在视频+封面双发。
            _h2, rich_nv, _l2 = format_message(username, t, ai, embed_video=False)
            if rich_nv != rich_html and len(rich_nv) <= RICH_MESSAGE_MAX_CHARS:
                print("    rich 含视频被拒，剥 video 换封面重试")
                r = send_telegram_rich(token, chat_id, link=link, html=rich_nv,
                                       thread_id=thread_id)
                if r.get("ok"):
                    return r
                if not r.get("rich_fallback"):
                    return r
    return send_telegram(token, chat_id, html_text, link, thread_id=thread_id)


# ── 单用户处理 ──────────────────────────────────────

def process_user(
    pool: TokenPool,
    ai: AIClassifier,
    username: str,
    bot_token: str,
    chat_id: str,
    args: argparse.Namespace,
    *,
    content_chat_id: "str | None" = None,
    content_thread_id: "int | None" = None,
) -> tuple[int, int, int, int]:
    """返回 (new_count, push_count, filter_count, ai_overridden)。

    content_chat_id/content_thread_id 未传时回落 chat_id（行为不变）：账号级
    失败告警（_alert_seen_save_failure/_alert_ai_all_failed）仍固定用 chat_id，
    只有推文推送（send_tweet）走 content 目标——告警/看板与内容分流。
    """
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
    push_retry = load_push_retry(username)
    stale_retry = push_retry & seen
    if stale_retry:
        # 送达 checkpoint（先 seen 后 retry）中间被杀的孤儿：已送达已 seen，
        # 只清记录不重推（seen 检查会短路，留着只是脏状态）
        push_retry -= stale_retry
        print(f"  清理 {len(stale_retry)} 条已 seen 的孤儿 push_retry 记录")

    # P0-5：seen 文件损坏且无备份时进入安全推送模式
    seen_corrupted = (last_post_iso == "corrupted")
    if seen_corrupted:
        print("  seen 文件损坏且无备份，进入安全推送模式（本轮新推文可推送，放宽时间窗）")
        last_post_iso = None

    new_ids: set[str] = set()
    to_push: list[tuple[dict, str]] = []
    filtered: list[tuple[dict, str]] = []
    ai_overridden = 0
    ai_all_failed_alerted = False

    push_age_minutes = args.max_push_age_minutes
    auto_seed = not seen and not args.test and not args.seed and not seen_corrupted
    if seen_corrupted:
        push_age_minutes = max(args.max_push_age_minutes, 1440)
    if auto_seed:
        print("  seen 为空，自动 seed（只记录，不推送）")

    for t in tweets:
        tid = str(t.get("id") or "")
        if not tid:
            continue

        status, reason = classify(t)
        text = (t.get("text") or "").strip()
        is_musing_suspect = (
            status == "suspicious" and reason.startswith(REASON_MUSING_PREFIX)
        )

        if status == "suspicious" and is_musing_suspect:
            # 碎碎念：有 AI 则复核；无 AI / AI 全失败 → fail-closed filter。
            # 与 promo 不对称：promo 无 AI 时放行（避免误杀商业讨论），
            # musing 无 AI 时过滤（兴趣门控优先安静）。
            if ai.is_available():
                is_musing, ai_reason = ai.confirm_musing(username, text)
                if is_musing:
                    status = "filter"
                    reason = f"{reason}|ai:{ai_reason}"
                    print(f"    AI 确认碎碎念 [{reason}] {text[:50]}")
                elif ai_reason == "all_ai_failed":
                    status = "filter"
                    reason = f"{reason}|ai:{ai_reason}"
                    print(f"    AI 全部失败，碎碎念可疑推文降级为 filter: {text[:50]}")
                    if not ai_all_failed_alerted:
                        ai_all_failed_alerted = True
                        _alert_ai_all_failed(bot_token, chat_id, username)
                else:
                    status = "pass"
                    ai_overridden += 1
                    print(f"    AI 否决碎碎念 [{reason} -> {ai_reason}] {text[:50]}")
            else:
                status = "filter"
                reason = f"{reason}|no_ai"
                print(f"    无 AI，碎碎念可疑直接 filter [{reason}] {text[:50]}")
        elif status == "suspicious" and ai.is_available():
            is_promo, ai_reason = ai.confirm_promo(username, text)
            if is_promo:
                status = "filter"
                reason = f"{reason}|ai:{ai_reason}"
                print(f"    AI 确认推广 [{reason}] {text[:50]}")
            elif ai_reason == "all_ai_failed":
                # P0-4：AI 全部失败时 fail-closed，按 filter 处理
                status = "filter"
                reason = f"{reason}|ai:{ai_reason}"
                print(f"    AI 全部失败，suspicious 推文降级为 filter: {text[:50]}")
                if not ai_all_failed_alerted:
                    ai_all_failed_alerted = True
                    _alert_ai_all_failed(bot_token, chat_id, username)
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
            if not article_id:
                # 节点兜底：引用文章的壳推 entities.urls 为空，detect_article 必漏，
                # 但归一化后挂了 article 节点 → 用其 rest_id 入队，避免裸推漏掉。
                art = t.get("article") or {}
                if art.get("rest_id"):
                    article_id = art["rest_id"]
            if article_id:
                save_article(username, article_id, t)
                # DEDUP-1：带 article 的推文只走摘要队列，不再作为普通/长推重复推送。
                # 否则博主转推他人 article 时，转推壳会以本博主名义再推一条长推
                # （misattributed），形成「长推 + 摘要」两条消息（见 Issue 4）。
                # tid 已加入 new_ids → 仍会被标记 seen，下轮不重复检测。
                continue
            if _CROSS_DEDUP_ENABLED:
                hit = _cross_dup_hit(t)
                if hit:
                    # 跨账号去重：同一原推的纯转发只推首见一条。置于 push_retry
                    # 判断之前——上轮失败进 retry 的 RT 若期间已由他号送达，本轮
                    # 应抑制而非重发；tid 在 new_ids → 轮末进 seen，其 retry 孤儿
                    # 由下轮 push_retry∩seen 清理兜走，零新增状态机。
                    print(f"    skip cross-dup: {tid} ← 原推已由 @{hit.get('by')} 推送")
                    continue
            if tid in push_retry:
                # 上轮 TG 推送失败：绕过 push-age 窗口重试，避免超龄后静默标 seen 丢推。
                to_push.append((t, "push_retry"))
                continue
            if not is_within_push_window(t, push_age_minutes):
                print(f"    skip stale: {tid}")
                continue
            if status == "pass":
                to_push.append((t, reason))
            elif status == "filter":
                filtered.append((t, reason))
            else:
                # 残留 suspicious：仅 promo 路径在无 AI 时走到这里 → 放行
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
    push_deferred = False
    for t, _reason in to_push:
        tid = str(t.get("id") or "")
        if args.dry_run:
            html_text, rich_html, link = format_message(username, t, ai)
            print("----- DRY RUN -----")
            print("[rich_html]")
            print(rich_html)
            print("[html_text]")
            print(html_text)
            print(f"link: {link}")
            print()
        else:
            # 剩余时间预算（与 article 队列同一时钟）：临近 25m SIGALRM 时不再
            # 发起新发送，未发推文进 push_retry（绝不标 seen）下轮绕过 push-age
            # 继续——kill 落在发送在途窗口时无法得知送达与否，防重复优先。
            if push_deferred or _article_queue_time_remaining() < PUSH_MIN_REMAINING_SECONDS:
                if not push_deferred:
                    push_deferred = True
                    print(f"    剩余时间不足 {PUSH_MIN_REMAINING_SECONDS}s，"
                          f"本轮停止推送，余量进 push_retry 下轮继续")
                push_failed.add(tid)
                continue
            try:
                r = send_tweet(bot_token, content_chat_id or chat_id, username, t, ai,
                               thread_id=content_thread_id if content_chat_id else None)
                ok = r.get("ok", False)
                print(f"    推送 {'OK' if ok else 'FAIL'}: {t.get('id')}")
                if not ok:
                    print(f"        resp: {r}")
                    push_failed.add(tid)
                elif not args.test:
                    # 送达即刻 checkpoint（先 seen 后 push_retry，顺序不可换：
                    # 中间被杀留下的孤儿 retry 条目会被 seen 短路，不产生重复；
                    # 反序被杀则推文既不 seen 也不 retry → 下轮当新推文重发）。
                    # 失败只打日志：末尾统一落盘 + _alert 兜底。
                    # --test 不 checkpoint：测试推送发往调试目标，写生产 seen /
                    # 摘 push_retry 会让生产群永久漏掉这些推文（基线语义如此）。
                    seen.add(tid)
                    try:
                        save_seen(username, seen, last_post_iso)
                        if tid in push_retry:
                            push_retry.discard(tid)
                            save_push_retry(username, push_retry)
                    except OSError as e:
                        print(f"    checkpoint 落盘失败（忽略，末尾统一落盘）: {e}")
                    if _CROSS_DEDUP_ENABLED:
                        try:
                            _record_pushed(t, username)
                        except OSError as e:
                            print(f"    pushed_index 落盘失败（忽略）: {e}")
                time.sleep(1.2)
            except Exception as e:
                print(f"    推送异常: {e}")
                push_failed.add(tid)

    if args.seed:
        seen |= {str(t.get("id")) for t in tweets if t.get("id")}
    else:
        # REL-1/FMT-1: only mark a tweet seen if its push did NOT fail. Failed sends
        # stay in push_retry and bypass push-age on the next run instead of being
        # silently dropped when they go stale.
        seen |= (new_ids - push_failed)

    pushed_ok = {str(t.get("id") or "") for t, _ in to_push} - push_failed
    push_retry = (push_retry | push_failed) - pushed_ok
    if not args.dry_run:
        try:
            save_push_retry(username, push_retry)
        except OSError as e:
            _alert_seen_save_failure(bot_token, chat_id, username, e)
            raise

    latest_ts = last_post_iso
    for t in tweets:
        dt = parse_tweet_datetime(t)
        if dt:
            iso = dt.isoformat()
            if not latest_ts or iso > latest_ts:
                latest_ts = iso

    try:
        save_seen(username, seen, latest_ts)
    except OSError as e:
        _alert_seen_save_failure(bot_token, chat_id, username, e)
        raise
    print(f"  已记录 seen_ids 共 {len(seen)} 条")

    # 推送计数 = 实际送达（尝试数会让失败重试双重计入、故障期看板虚高）
    return len(new_ids), len(to_push) - len(push_failed), len(filtered), ai_overridden


# ── 主流程 ──────────────────────────────────────────


def _article_entry_expired(entry: dict, now: datetime | None = None) -> bool:
    """sent / 终态 failed（attempts 用尽）条目超过保留期后从队列清除。

    没有可解析时间戳的条目一律保留（宁可不删）。
    """
    status = entry.get("status")
    terminal = status in ("sent", "skipped") or (
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


def _save_article_queue(queue_path: str, queue: list, dry_run: bool = False) -> None:
    if not dry_run:
        _atomic_write(queue_path, json.dumps(queue, ensure_ascii=False, indent=2))


def _revert_stalled_processing(queue: list, username: str, now: datetime | None = None) -> bool:
    """Crash recovery: entries stuck in 'processing' for too long are retried."""
    now = now or datetime.now(timezone.utc)
    stall = timedelta(minutes=ARTICLE_PROCESSING_STALL_MINUTES)
    changed = False
    for entry in queue:
        if entry.get("status") != "processing":
            continue
        ts_str = entry.get("updated_at") or entry.get("detected_at") or ""
        try:
            ts = datetime.fromisoformat(ts_str)
        except (ValueError, TypeError):
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if (now - ts) > stall:
            entry["status"] = "pending"
            entry["last_error"] = "stalled_processing_reverted"
            entry["updated_at"] = now.isoformat()
            changed = True
            print(f"  @{username}: article {entry.get('article_id')} processing 超时 {ARTICLE_PROCESSING_STALL_MINUTES}min，回退为 pending")
    return changed


def _article_queue_time_remaining() -> float:
    """Seconds left in the current monitor run; infinity when not tracking."""
    start = _ARTICLE_QUEUE_RUN_START
    if start is None:
        return float("inf")
    elapsed = time.monotonic() - start
    return ARTICLE_QUEUE_TIME_BUDGET_SECONDS - elapsed


def process_article_queue(ai: AIClassifier, bot_token: str, chat_id: str, dry_run: bool = False,
                          *, thread_id: "int | None" = None,
                          thread_map: "dict | None" = None) -> int:
    """处理 Article 队列：抓 Markdown、AI 摘要、推送，成功后删除缓存。

    chat_id 在 main() 已解析为 content 目标（配置了群组则为群组，否则回落 DM）；
    本函数发的都是文章相关内容（含失败通知），不区分 alert，全部带 thread_id。
    thread_map（监控账号→thread）存在时按队列文件对应账号解析话题，缺席回落 thread_id。
    """
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
        file_thread = (thread_map or {}).get(username, thread_id)
        before = len(queue)
        queue = [a for a in queue if not _article_entry_expired(a)]
        changed = len(queue) != before
        if changed:
            print(f"  @{username}: 清理 {before - len(queue)} 条过期文章记录")

        if _revert_stalled_processing(queue, username):
            changed = True

        candidates = [a for a in queue if a.get("status") in ("pending", "failed", "fetched", "processing") and a.get("attempts", 0) < ARTICLE_MAX_ATTEMPTS]
        # Prefer fresh, oldest-detected entries; cap per run to avoid cron overruns.
        candidates.sort(key=lambda a: (int(a.get("attempts", 0)), a.get("detected_at") or a.get("updated_at") or ""))
        candidates = candidates[:MAX_ARTICLES_PER_RUN]
        if not candidates:
            if changed:
                _save_article_queue(queue_path, queue, dry_run)
            continue
        print(f"  @{username}: {len(candidates)} article jobs")

        for entry in candidates:
            if _article_queue_time_remaining() < ARTICLE_QUEUE_MIN_REMAINING_SECONDS:
                print(f"  @{username}: 剩余时间不足 {ARTICLE_QUEUE_MIN_REMAINING_SECONDS}s，停止处理新 article，留到下一轮")
                break

            aid = entry["article_id"]
            if _CROSS_DEDUP_ENABLED and ("a:" + str(aid)) in load_pushed_index():
                # 二闸：同轮两账号已各自入队（入队闸只能挡后来者）——他号先送达
                # 后本队列同 article 直接终态 skipped，纳入 7 天清理。
                by = (load_pushed_index().get("a:" + str(aid)) or {}).get("by")
                entry["status"] = "skipped"
                entry["skip_reason"] = "cross_dup"
                entry["updated_at"] = datetime.now(timezone.utc).isoformat()
                changed = True
                _save_article_queue(queue_path, queue, dry_run)
                print(f"  @{username}: article {aid} 跨账号去重（已由 @{by} 推送），跳过")
                continue
            entry["status"] = "processing"
            entry["attempts"] = int(entry.get("attempts", 0)) + 1
            entry["updated_at"] = datetime.now(timezone.utc).isoformat()
            changed = True
            _save_article_queue(queue_path, queue, dry_run)

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
                        r = send_telegram(bot_token, chat_id, msg, link, thread_id=file_thread)
                        print(f"    Failure notice push {'OK' if r.get('ok') else 'FAIL'}")
                        _mid = (r.get("result") or {}).get("message_id")
                        if _mid:
                            entry["failure_msg_id"] = _mid  # 重试成功后原地改写闭环
                        time.sleep(1.2)
                    except Exception as e:
                        print(f"    failure notice push error: {e}")
                _save_article_queue(queue_path, queue, dry_run)
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
                        r = send_telegram(bot_token, chat_id, msg, link, thread_id=file_thread)
                        print(f"    Failure notice push {'OK' if r.get('ok') else 'FAIL'}")
                        _mid = (r.get("result") or {}).get("message_id")
                        if _mid:
                            entry["failure_msg_id"] = _mid  # 重试成功后原地改写闭环
                        time.sleep(1.2)
                    except Exception as e:
                        print(f"    failure notice push error: {e}")
                _save_article_queue(queue_path, queue, dry_run)
                continue

            entry["summary_backend"] = backend
            entry["summary_at"] = datetime.now(timezone.utc).isoformat()
            messages = format_article_summary_messages(username, entry, summary)
            cover = extract_article_cover(markdown)
            cover_urls = [cover] if cover else []
            body_imgs = extract_article_body_images(markdown)
            img_urls = cover_urls + body_imgs  # 去图重试的判定用
            rich_md = format_article_summary_rich(
                username, entry, summary,
                image_urls=cover_urls, detail_image_urls=body_imgs)
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
                        r = send_telegram_rich(bot_token, chat_id, rich_md, article_link, thread_id=file_thread)
                        last_resp = r
                        ok = r.get("ok", False)
                        if not ok and img_urls and r.get("rich_fallback"):
                            # 配图外链可能是被拒原因：去图重试一次 rich，再不行才回退分块
                            print(f"    rich 带图被拒({str(r.get('description', ''))[:60]})，去图重试")
                            r = send_telegram_rich(
                                bot_token, chat_id,
                                format_article_summary_rich(username, entry, summary),
                                article_link, thread_id=file_thread)
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
                            r = send_telegram(bot_token, chat_id, part, article_link, thread_id=file_thread)
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
                        # 已送达即刻落盘：下面的 quiet 编辑可挂 60s，进程在窗口内
                        # 被杀会让磁盘停留在 processing → 下轮整篇摘要重发。
                        # best-effort：落盘失败（磁盘满等）绝不能抛进外层 except
                        # 把已送达的 sent 翻成 failed（那会重造重复推送），退回
                        # 函数末尾的统一落盘即可。
                        try:
                            _save_article_queue(queue_path, queue, dry_run)
                        except OSError as e:
                            print(f"    sent 状态即刻落盘失败（忽略，末尾统一落盘）: {e}")
                        if _CROSS_DEDUP_ENABLED and not dry_run:
                            try:
                                _record_pushed_article(aid, username)
                            except OSError as e:
                                print(f"    pushed_index 落盘失败（忽略）: {e}")
                        delete_article_cache(entry)
                        _fmid = entry.pop("failure_msg_id", None)
                        if _fmid:
                            # 把此前的失败通知原地改写，不留悬空故障消息。
                            # editMessageText 不传 reply_markup 会移除原按钮，显式带上。
                            _tg_post_quiet(bot_token, {
                                "chat_id": chat_id, "message_id": _fmid,
                                "text": (f"✅ <b>X Article 重试成功</b>：@{html.escape(entry.get('author') or username)} "
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
                # Persist terminal state as soon as the send attempt finishes.
                _save_article_queue(queue_path, queue, dry_run)
            entry["updated_at"] = datetime.now(timezone.utc).isoformat()
            if entry["status"] in ("sent", "summarized"):  # 只计送达，不计失败尝试
                processed += 1
            _save_article_queue(queue_path, queue, dry_run)
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
    _lock_fp = None
    if fcntl is not None:
        _lock_fp = open(os.path.join(SCRIPT_DIR, ".monitor.lock"), "w")
        try:
            fcntl.flock(_lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("上一轮 monitor 仍在运行，跳过本次", file=sys.stderr)
            _lock_fp.close()
            return 0

    # P0-1: global wall-clock timeout so a hung task cannot hold the flock forever
    # and starve subsequent cron ticks.
    def _timeout_handler(signum, frame):
        print("ERROR: monitor 运行超过 25 分钟全局超时，强制退出", file=sys.stderr)
        if _lock_fp is not None:
            try:
                fcntl.flock(_lock_fp, fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                _lock_fp.close()
            except Exception:
                pass
        sys.exit(1)

    if hasattr(signal, "SIGALRM"):
        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(25 * 60)

    try:
        # 每轮起止时间戳：日志此前无任何时间标记，无法事后审计运行时长/定位轮次
        run_started = time.monotonic()
        global _ARTICLE_QUEUE_RUN_START, _THREAD_FALLBACK_ID, _CROSS_DEDUP_ENABLED
        _ARTICLE_QUEUE_RUN_START = run_started
        print(f"\n==== monitor run {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %z')} ====")

        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
        apply_route_overlay(cfg)  # 路由表优先，config.json 作回落
        bot_token = args.bot_token or cfg["telegram_bot_token"]
        chat_id = args.chat_id or cfg["telegram_chat_id"]
        # X 内容（推文+article 摘要）路由到通知群「X」话题；--chat-id 手动覆盖时
        # 视为整体调试目标，群组路由让位（content_thread_id 同时清空）。账号级
        # 失败告警/状态看板不受影响，仍用上面的 chat_id（DM）。
        group_chat_id = None if args.chat_id else cfg.get("telegram_group_chat_id")
        content_chat_id = group_chat_id or chat_id
        content_thread_id = cfg.get("telegram_twitter_thread_id") if group_chat_id else None
        # 账号级主题路由映射（accounts.json 的 topic 字段 → 话题 thread）；
        # --chat-id 覆盖时随群组路由一起让位（全部落覆盖目标、无 thread）。
        topic_threads = (cfg.get("telegram_topic_threads") or {}) if group_chat_id else {}
        # 话题失效自愈的回退目标（默认 X 话题）；无群组路由时无话题可回退
        _THREAD_FALLBACK_ID = content_thread_id
        # 跨账号去重（纯转发 + Article）：config 键开关，默认关 = 行为与现状一致
        _CROSS_DEDUP_ENABLED = bool(cfg.get("cross_account_dedup"))
        if _CROSS_DEDUP_ENABLED:
            print("  跨账号去重已启用（纯转发 + Article）")
        # rich 可播视频内嵌：config 键开关，默认关 = 封面缩略图行为
        global _RICH_VIDEO_ENABLED
        _RICH_VIDEO_ENABLED = bool(cfg.get("rich_video_embed"))
        if _RICH_VIDEO_ENABLED:
            print("  rich 视频内嵌已启用（≤20MB 档，超限回退封面）")

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
        # thread 映射建于 --user 过滤之前：article 队列按文件遍历、不受 --user 限制，
        # 子集轮里其他账号的文章也要能解析到各自话题。
        account_thread_map = {
            a["username"]: _resolve_topic_thread(a, topic_threads, content_thread_id)
            for a in accounts
        }
        if args.user:
            accounts = [a for a in accounts if a["username"] == args.user]
            if not accounts:
                print(f"用户 {args.user} 不在配置中或未启用", file=sys.stderr)
                return 1

        if not accounts:
            print("没有启用的账号", file=sys.stderr)
            return 1

        print(f"Twitter 监控：{len(accounts)} 个账号")

        if not (args.dry_run or args.test or args.seed or args.user):
            # 上轮存在「歧义按已送达」的发送时，先发汇总 DM 供人工核对（真丢推可发现）。
            # 完整 cron 轮才执行（与看板同门槛）：--test/--seed/--user 调试运行常带
            # --chat-id 覆盖，核对 DM 发进调试目标并删账本会让生产告警永久丢失。
            try:
                _flush_assumed_delivery_notice(bot_token, chat_id)
            except Exception as e:
                print(f"  歧义送达汇总告警异常（忽略）: {e}")

        total_new = 0
        total_push = 0
        total_filter = 0
        total_ai_override = 0
        failures = load_account_failures()
        if not args.user:
            # 完整轮才修剪：--user 子集运行下修剪会误删其他账号的失败状态
            known = {a["username"] for a in accounts}
            for stale in [u for u in failures if u not in known]:
                print(f"  清理幽灵失败记录: @{stale}（已不在配置中）")
                del failures[stale]

        for account in accounts:
            username = account["username"]
            try:
                new, pushed, filtered, ai_ov = process_user(
                    pool=pool, ai=ai, username=username,
                    bot_token=bot_token, chat_id=chat_id, args=args,
                    content_chat_id=content_chat_id,
                    content_thread_id=account_thread_map.get(username, content_thread_id),
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
        article_count = 0
        try:
            article_count = process_article_queue(ai, bot_token, content_chat_id, args.dry_run,
                                                  thread_id=content_thread_id,
                                                  thread_map=account_thread_map)
            if article_count:
                print(f"  Articles processed: {article_count}")
        except Exception as e:
            print(f"  Article queue error: {e}")
        if not args.dry_run:
            try:
                _alert_thread_fallback(bot_token, chat_id)
            except Exception as e:
                print(f"  话题回退告警异常（忽略）: {e}")
        if pool is not None:
            print(f"  token 池：{pool.available_count}/{len(pool._tokens)} 可用")
        if not args.dry_run:
            try:
                save_account_failures(failures)
            except OSError as e:
                print(f"  保存账号失败状态失败（忽略）: {e}", file=sys.stderr)
        # cookie 认证看门狗 + 状态看板：都只在完整 cron 轮跑（seed/test/单账号手动不算）
        if not (args.dry_run or args.test or args.seed or args.user):
            try:
                check_cookie_health(bot_token, chat_id, args.dry_run)
            except Exception as e:
                print(f"  cookie 健康检查失败（忽略）: {e}", file=sys.stderr)
            try:
                update_status_dashboard(bot_token, chat_id, accounts, failures,
                                        pushed=total_push, articles=article_count,
                                        elapsed=time.monotonic() - run_started)
            except Exception as e:
                print(f"  看板更新失败（忽略）: {e}")
        print(f"  耗时 {time.monotonic() - run_started:.1f}s")
        return 0
    finally:
        # P0-1: always cancel the global timeout and release the flock lock so a
        # hung/hard-killed predecessor cannot starve subsequent cron ticks.
        if hasattr(signal, "SIGALRM"):
            signal.alarm(0)
        if _lock_fp is not None:
            try:
                fcntl.flock(_lock_fp, fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                _lock_fp.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
