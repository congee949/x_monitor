#!/usr/bin/env python3
"""vista8 (向阳乔木) Twitter 监控 → Telegram 推送

数据源：opentwitter-mcp 背后的 ai.6551.io API（需要 TWITTER_TOKEN）
目标通道：CC98 Python Bot（复用 cc98_config.json 里的 telegram_bot_token/chat_id）

用法：
  python3 vista8_monitor.py              # 常规模式：只推送新推文
  python3 vista8_monitor.py --test       # 测试模式：推送过滤后最新 N 条
  python3 vista8_monitor.py --dry-run    # 只打印不推送
  python3 vista8_monitor.py --seed       # 只记录已见 ID，不推送（首次用）
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "cc98_config.json")
SEEN_PATH = os.path.join(SCRIPT_DIR, "vista8_seen_ids.json")

USERNAME = "vista8"
API_BASE = "https://ai.6551.io"
API_ENDPOINT = f"{API_BASE}/open/twitter_user_tweets"

# TWITTER_TOKEN 优先读 env，再 fallback 到 ~/.claude.json 的 twitter MCP env
def load_twitter_token() -> str:
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
    sys.exit("❌ 找不到 TWITTER_TOKEN。设置环境变量或确认 ~/.claude.json 里 twitter MCP 有配置")

# ── 过滤规则 ───────────────────────────────────────
MIN_LEN = 18                                    # 过滤短推（"测试啊"这种占位贴）
SKIP_HASHTAGS = {"#byteplus", "#seedance", "#seedance_2", "#dreamina"}
COMMERCIAL_KEYWORDS = [
    "@bytepluseglobal", "@bytepluseglobal",    # typo-proof
    "@bytepluseglobal",
    "byteplus", "seedance 2.0 api", "seedance 2.0",
    "api 文档", "api文档",
    "访问体验", "开通模型",
    "冲 200", "冲200", "200块", "200 块",
    "立即体验", "方舟平台",
]
# 命中 ≥2 个商业关键词 → 判为恰饭
COMMERCIAL_HIT_THRESHOLD = 2
# 纯链接贴：文本去除 URL/空白后 < 10 字
URL_RE = re.compile(r"https?://\S+")

# 返佣/邀请链接（商单硬特征，命中即过滤）
AFFILIATE_URL_RE = re.compile(
    r"/invite/|/referral/|[?&](ref|aff|affiliate|inviter|invitecode|promo)=",
    re.IGNORECASE,
)
# 自认返佣/推销套话（命中任一即过滤）
COMMERCIAL_SELF_DISCLOSE = [
    "赚个佣金", "赚点佣金", "返佣", "邀请码", "邀请链接",
    "扫码体验", "立即开通", "限时优惠",
]


def classify(tweet: dict) -> tuple[bool, str]:
    """返回 (should_push, reason)。should_push=False 表示被过滤。"""
    text = (tweet.get("text") or "").strip()
    low = text.lower()

    # 1. 短推
    if len(text) < MIN_LEN:
        return False, f"too_short({len(text)}字)"

    # 2. 带商务 hashtag
    for tag in SKIP_HASHTAGS:
        if tag in low:
            return False, f"skip_tag:{tag}"

    # 3. 商业关键词命中阈值
    hits = [kw for kw in COMMERCIAL_KEYWORDS if kw in low]
    if len(hits) >= COMMERCIAL_HIT_THRESHOLD:
        return False, f"commercial({','.join(hits[:3])})"

    # 3a. 返佣/邀请链接（硬特征）
    if AFFILIATE_URL_RE.search(text):
        return False, "affiliate_link"

    # 3b. 自认返佣/推销套话
    for kw in COMMERCIAL_SELF_DISCLOSE:
        if kw in low:
            return False, f"self_disclose:{kw}"

    # 4. 纯链接贴（文本扣掉 URL 后内容 <10 字）
    stripped = URL_RE.sub("", text).strip()
    if len(stripped) < 10:
        return False, f"link_only({len(stripped)}字)"

    return True, "ok"


def fetch_tweets(token: str, limit: int = 20) -> list[dict]:
    body = json.dumps({
        "username": USERNAME,
        "maxResults": limit,
        "product": "Latest",
        "includeReplies": False,
        "includeRetweets": False,
    }).encode("utf-8")
    req = urllib.request.Request(
        API_ENDPOINT,
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "vista8-monitor/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        resp = json.loads(r.read().decode("utf-8"))
    return resp.get("data") or []


def load_seen() -> set[str]:
    if not os.path.exists(SEEN_PATH):
        return set()
    try:
        with open(SEEN_PATH) as f:
            return set(json.load(f).get("ids", []))
    except Exception:
        return set()


def save_seen(seen: set[str]) -> None:
    kept = sorted(seen, reverse=True)[:500]
    with open(SEEN_PATH, "w") as f:
        json.dump({"ids": kept, "updated": datetime.now().isoformat()}, f,
                  ensure_ascii=False, indent=2)


def format_message(t: dict) -> tuple[str, str]:
    """Returns (text, link). Link is embedded invisibly so TG renders tweet preview."""
    tid = t.get("id") or t.get("conversation_id_str") or ""
    link = f"https://x.com/{USERNAME}/status/{tid}" if tid else ""
    # zero-width hidden anchor so TG generates preview without visible junk
    hidden = f'<a href="{link}">​</a>' if link else ""
    text = f'📢 @{USERNAME}{hidden}'
    return text, link


def offset_hours(h: int):
    from datetime import timedelta
    return timedelta(hours=h)


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
            "inline_keyboard": [[{"text": "🔗 打开推文", "url": link}]]
        }
    else:
        payload["link_preview_options"] = {"is_disabled": True}
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode("utf-8"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true", help="测试模式：推送过滤后最新 N 条")
    ap.add_argument("--seed", action="store_true", help="只记录已见，不推送（首次用）")
    ap.add_argument("--dry-run", action="store_true", help="只打印，不推送")
    ap.add_argument("--limit", type=int, default=20, help="拉取条数（默认 20）")
    ap.add_argument("--test-count", type=int, default=3, help="--test 模式推送条数")
    ap.add_argument("--chat-id", default=None, help="覆盖目标 chat_id")
    ap.add_argument("--bot-token", default=None, help="覆盖 Telegram bot token")
    args = ap.parse_args()

    with open(CONFIG_PATH) as f:
        cfg = json.load(f)

    bot_token = args.bot_token or cfg["telegram_bot_token"]
    chat_id = args.chat_id or cfg["telegram_chat_id"]
    tw_token = load_twitter_token()

    tweets = fetch_tweets(tw_token, limit=args.limit)
    if not tweets:
        print("❌ 没拉到推文", file=sys.stderr)
        return 1

    seen = load_seen()
    new_ids: set[str] = set()
    to_push: list[tuple[dict, str]] = []   # (tweet, reason)
    filtered: list[tuple[dict, str]] = []

    for t in tweets:
        tid = str(t.get("id") or "")
        if not tid:
            continue
        ok, reason = classify(t)
        if args.test:
            if ok:
                to_push.append((t, reason))
            else:
                filtered.append((t, reason))
        else:
            if tid in seen:
                continue
            new_ids.add(tid)
            if ok:
                to_push.append((t, reason))
            else:
                filtered.append((t, reason))

    if args.test:
        to_push = to_push[: args.test_count]

    # 日志
    print(f"拉取 {len(tweets)} 条，新推 {len(new_ids)} 条")
    print(f"  推送: {len(to_push)}  过滤: {len(filtered)}")
    for t, reason in filtered:
        text = (t.get("text") or "").replace("\n", " ")[:50]
        print(f"    ⏭  [{reason}] {text}")

    # 推送（seed 模式只记录不推送）
    if args.seed:
        print("    🌱 seed 模式：跳过推送")
        to_push = []

    for t, _reason in to_push:
        msg, link = format_message(t)
        if args.dry_run:
            print("----- DRY RUN -----")
            print(msg)
            print(f"link: {link}")
            print()
        else:
            try:
                r = send_telegram(bot_token, chat_id, msg, link)
                ok = r.get("ok", False)
                print(f"    ✉️  推送 {'OK' if ok else 'FAIL'}: {t.get('id')}")
                if not ok:
                    print(f"        resp: {r}")
                time.sleep(1.2)
            except Exception as e:
                print(f"    ❌ 推送异常: {e}")

    # 保存 seen（seed 模式把全部最新都记下）
    if args.seed:
        seen |= {str(t.get("id")) for t in tweets if t.get("id")}
    else:
        seen |= new_ids
    save_seen(seen)
    print(f"已记录 seen_ids 共 {len(seen)} 条")
    return 0


if __name__ == "__main__":
    sys.exit(main())
