#!/usr/bin/env python3
"""MacRumors 每日资讯 → Telegram 中文摘要日报（头条图文卡片 + 可折叠汇总）。

数据源是 MacRumors 官方 RSS（每小时更新，自带 guid 去重锚点），无需爬取。
复用同目录 twitter_monitor 的 AIClassifier 与官方 Bot API。

输出（纯 Bot API，不依赖外部托管）：
  1) 前 CARD_MAX 条有配图的头条 —— 每条一张 sendPhoto 卡片：
     图 + 粗体中文标题 + 一句话摘要 + 「📖 原文」按钮（图文贴合）；
  2) 其余条目 —— 一条 sendMessage(HTML)，按设备分类包成 <blockquote expandable> 可折叠。
卡片与汇总合起来覆盖全集、互不重复；某张卡片发送失败时该条自动回落到文字汇总。

设计要点：Python 控制抓取、去重、分组、抽图、拼链接，保证完整不丢条、链接不错配；
AI 只对每条做「中文标题 + 一句话中文摘要」并返回 JSON，缺失则回退英文原标题。
"""
import html as _html
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

try:
    import fcntl
except ImportError:
    fcntl = None

import twitter_monitor as tm

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SEEN_PATH = os.path.join(SCRIPT_DIR, ".macrumors_seen.json")

FEED_URL = "https://feeds.macrumors.com/MacRumors-All"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 macrumors-daily/1.0"
FIRST_RUN_WINDOW_H = 24      # 首次运行只推过去 24h，更早的标记已读不补推
MAX_ITEMS = 60               # 单期上限，超出取最新 N 条并在页脚注明
CARD_MAX = 5                 # 头条图文卡片数上限
TG_LIMIT = 3800              # 单条文字消息字符上限（留余量，官方 4096）
BJT = timezone(timedelta(hours=8))

# 主题分桶：按优先级顺序匹配 title+categories，命中第一个即归类
BUCKETS = [
    ("📱 iPhone",       ["iphone"]),
    ("📟 iPad",         ["ipad"]),
    ("💻 Mac",          ["macbook", "imac", "mac mini", "mac studio", "mac pro", " mac ", "macos", "m5", "m4 ", "m3 "]),
    ("⌚ Apple Watch",  ["apple watch", "watchos"]),
    ("🎧 AirPods",      ["airpods"]),
    ("🥽 Vision Pro",   ["vision pro", "visionos"]),
    ("🔄 系统更新",      ["ios ", "ipados", "beta", "update", "watchos", "tvos"]),
    ("📺 服务/其他硬件", ["apple tv", "homepod", "apple music", "icloud", "app store", "apple intelligence", "siri"]),
    ("💰 优惠",         ["deal", "deals", "discount", "% off", "save $"]),
]
OTHER = "🔖 其他"


def fetch_feed() -> bytes:
    req = urllib.request.Request(FEED_URL, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def strip_html(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s or "")
    s = _html.unescape(s)
    return re.sub(r"\s+", " ", s).strip()


def first_image(content_html: str) -> str:
    if not content_html:
        return ""
    m = re.search(r'<img[^>]+src="([^"]+)"', content_html)
    return m.group(1) if m else ""


def parse_items(xml: bytes) -> list[dict]:
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml)
    out = []
    for it in root.iter("item"):
        def t(tag):
            el = it.find(tag)
            return el.text.strip() if el is not None and el.text else ""
        raw_desc = t("description")  # CDATA：含 <img> 与正文 HTML
        guid = t("guid") or t("link")
        cats = [el.text.strip() for el in it.findall("category") if el.text]
        pub = t("pubDate")
        try:
            dt = parsedate_to_datetime(pub)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            dt = None
        out.append({
            "guid": guid,
            "title": strip_html(t("title")),
            "link": t("link"),
            "cats": cats,
            "desc": strip_html(raw_desc)[:280],
            "image": first_image(raw_desc),
            "dt": dt,
        })
    return out


def bucket_of(it: dict) -> str:
    hay = (it["title"] + " " + " ".join(it["cats"])).lower()
    for name, kws in BUCKETS:
        if any(k in hay for k in kws):
            return name
    return OTHER


def _guid_ts(guid: str) -> "datetime | None":
    """尝试从 MacRumors URL/permalink 中提取发布日期；失败返回 None。

    MacRumors RSS 的 guid 通常是带日期的 permalink，例如：
    https://www.macrumors.com/2026/06/30/some-article-title/
    """
    if not guid:
        return None
    m = re.search(r"/(\d{4})/(\d{1,2})/(\d{1,2})/", guid)
    if m:
        try:
            return datetime(
                int(m.group(1)), int(m.group(2)), int(m.group(3)),
                tzinfo=timezone.utc,
            )
        except ValueError:
            pass
    return None


def load_seen() -> tuple[set, bool]:
    """返回 (seen_set, seen_existed)。

    文件损坏时视为「seen 存在但为空」，避免进入 first-run 模式把 24h 前新闻永久吞掉。
    """
    if not os.path.exists(SEEN_PATH):
        return set(), False
    try:
        with open(SEEN_PATH) as f:
            return set(json.load(f)), True
    except Exception:
        return set(), True


def save_seen(seen: set) -> None:
    """保留最近 600 个 GUID。按 GUID 内嵌时间戳排序；无法提取时间戳的按已有文件顺序追加在后。"""
    existing_order: dict[str, int] = {}
    if os.path.exists(SEEN_PATH):
        try:
            with open(SEEN_PATH) as f:
                for i, g in enumerate(json.load(f)):
                    existing_order[str(g)] = i
        except Exception:
            pass

    def sort_key(guid: str) -> tuple[float, float]:
        ts = _guid_ts(guid)
        # 有时间戳的按时间戳升序；无时间戳的按文件顺序排在最后，新条目追加在末尾
        return (
            ts.timestamp() if ts is not None else float("inf"),
            existing_order.get(guid, float("inf")),
        )

    sorted_guids = sorted(seen, key=sort_key)
    keep = sorted_guids[-600:]
    tm._atomic_write(SEEN_PATH, json.dumps(keep, ensure_ascii=False))


def mark_guids_seen(seen: set, guids: list[str], dry: bool = False) -> None:
    """每成功送达一批条目后立即落盘，避免中途崩溃重复推送。"""
    if dry or not guids:
        return
    for g in guids:
        if g:
            seen.add(g)
    save_seen(seen)


def build_prompt(items: list[dict]) -> str:
    lines = []
    for i, it in enumerate(items):
        lines.append(f"[{i}] EN_TITLE: {it['title']}\n    EXCERPT: {it['desc']}")
    body = "\n".join(lines)
    return (
        "你是科技资讯编辑。下面是 MacRumors 的若干条英文新闻，每条有编号、英文标题和摘录。\n"
        "请为每一条输出：准确的中文标题（不夸张、不加感叹号、不臆造未提及的信息）和一句话中文摘要"
        "（客观陈述要点，≤40字）。\n"
        "严格只返回一个 JSON 数组，每个元素形如 {\"i\": 0, \"zh_title\": \"...\", \"zh_summary\": \"...\"}，"
        "必须覆盖全部编号，不要遗漏，不要输出 JSON 以外的任何文字。\n\n"
        f"{body}"
    )


def parse_ai_json(text: str) -> dict:
    if not text:
        return {}
    m = re.search(r"\[.*\]", text, re.S)
    if not m:
        return {}
    try:
        arr = json.loads(m.group())
    except Exception:
        return {}
    out = {}
    for e in arr:
        try:
            out[int(e["i"])] = (str(e.get("zh_title", "")).strip(),
                                str(e.get("zh_summary", "")).strip())
        except Exception:
            continue
    return out


def translate(ai, items: list[dict], batch: int = 8) -> None:
    """分批翻译并直接写回每条的 zh_title/zh_summary，缺失回退英文原标题。

    分批是因为推理模型在大请求（如 16 条）上会偶发返回不可解析内容；每批从 0 编号、重试 3 次。
    """
    for it in items:
        it["zh_title"], it["zh_summary"] = it["title"], ""
    for start in range(0, len(items), batch):
        chunk = items[start:start + batch]
        part: dict = {}
        for _ in range(3):
            txt, _backend = ai.complete(build_prompt(chunk), max_tokens=2500)
            part = parse_ai_json(txt or "")
            if len(part) >= len(chunk):
                break
            time.sleep(1)
        for local, (zt, zs) in part.items():
            if 0 <= local < len(chunk):
                chunk[local]["zh_title"] = zt or chunk[local]["title"]
                chunk[local]["zh_summary"] = zs


def build_merge_prompt(items: list[dict]) -> str:
    lines = []
    for i, it in enumerate(items):
        lines.append(f"[{i}] {it['zh_title']}")
    body = "\n".join(lines)
    return (
        "你是科技资讯编辑。下面是今天的若干条新闻标题。\n"
        "请将报道同一事件或同一主题的条目合并为一组（同一产品发布/更新/事件才合并，"
        "不要仅因设备品类相同就合并，如「iPhone 折扣」和「iPhone 新功能」不应合并）。\n"
        "每组输出合并后的中文标题和≤50字的合并摘要。只含单条的组也要列出。\n"
        "返回 JSON 数组：[{\"indices\": [0,2,5], \"zh_title\": \"...\", \"zh_summary\": \"...\"}]\n"
        "每个编号必须恰好出现一次。只返回 JSON。\n\n"
        f"{body}"
    )


def parse_merge_json(text, n):
    if not text:
        return None
    m = re.search(r"\[.*\]", text, re.S)
    if not m:
        return None
    try:
        arr = json.loads(m.group())
    except Exception:
        return None
    seen_indices = set()
    groups = []
    for e in arr:
        indices = e.get("indices", [])
        if not isinstance(indices, list) or not indices:
            continue
        indices = [int(i) for i in indices if isinstance(i, (int, float)) and 0 <= int(i) < n]
        if not indices:
            continue
        groups.append({
            "indices": indices,
            "zh_title": str(e.get("zh_title", "")).strip(),
            "zh_summary": str(e.get("zh_summary", "")).strip(),
        })
        seen_indices.update(indices)
    if not groups:
        return None
    for i in range(n):
        if i not in seen_indices:
            groups.append({"indices": [i], "zh_title": "", "zh_summary": ""})
    return groups


def merge_similar(ai, items: list[dict]) -> list[dict]:
    if len(items) < 4:
        return items
    merged_result = None
    for attempt in range(3):
        txt, backend = ai.complete(build_merge_prompt(items), max_tokens=4000)
        merged_result = parse_merge_json(txt or "", len(items))
        if merged_result is not None:
            break
        print(f"  合并第 {attempt+1} 次失败（{backend}）: {repr((txt or '')[:200])}", file=sys.stderr)
        time.sleep(1)
    if merged_result is None:
        print("  合并聚类失败，跳过合并", file=sys.stderr)
        return items
    out = []
    n_merged = 0
    used: set = set()
    for g in merged_result:
        # AI 可能把同一编号放进多个组或组内重复；去重保证每条恰好用一次，
        # 与 parse_merge_json 的「漏号补单条」配合 → 全覆盖且不重复（防重复推送）。
        indices = [i for i in dict.fromkeys(g["indices"]) if i not in used]
        if not indices:
            continue
        used.update(indices)
        sub = [items[i] for i in indices]
        if len(sub) == 1:
            out.append(sub[0])
            continue
        n_merged += len(sub)
        first = sub[0]
        image = next((s["image"] for s in sub if s["image"]), "")
        out.append({
            "guid": first["guid"],
            "title": first["title"],
            "link": first["link"],
            "cats": first["cats"],
            "desc": first["desc"],
            "image": image,
            "dt": first["dt"],
            "zh_title": g["zh_title"] or first["zh_title"],
            "zh_summary": g["zh_summary"] or first["zh_summary"],
            "sub_items": sub,
        })
    if n_merged:
        print(f"  合并：{len(items)} 条 → {len(out)} 条（{n_merged} 条被合并）")
    return out


def all_guids(items: list[dict]) -> list[str]:
    out = []
    for it in items:
        if "sub_items" in it:
            out.extend(s["guid"] for s in it["sub_items"])
        else:
            out.append(it["guid"])
    return out


def esc(s: str) -> str:
    return _html.escape(s or "", quote=False)


def esc_attr(s: str) -> str:
    return _html.escape(s or "", quote=True)


def select_cards(items: list[dict]) -> list[dict]:
    cards = []
    for it in items:
        if it["image"]:
            cards.append(it)
        if len(cards) >= CARD_MAX:
            break
    return cards


def send_card(token: str, chat_id: str, it: dict, header: str = "") -> None:
    sub = it.get("sub_items")
    if sub:
        cap = f"<b>{esc(it['zh_title'])}（{len(sub)} 篇）</b>"
    else:
        cap = f"<b>{esc(it['zh_title'])}</b>"
    if it["zh_summary"]:
        cap += f"\n{esc(it['zh_summary'])}"
    if header:
        cap = f"{header}\n\n{cap}"
    payload = {
        "chat_id": chat_id, "photo": it["image"],
        "caption": cap[:1024], "parse_mode": "HTML",
        "reply_markup": {"inline_keyboard": [[{"text": "📖 原文", "url": it["link"]}]]},
    }
    try:
        tm._tg_post(token, payload, "sendPhoto")
    except tm.TgAmbiguousDelivery as e:
        # 请求已送出、响应缺失：卡片大概率已入群。抛出去会走 main 的「回落到
        # 文字」——对已送达的卡片再发一遍文字 = 重复。按已送达返回（调用方正常
        # 标 seen），留痕供 x_monitor 下轮汇总 DM 核对。卡片自身不熔断（其失败
        # 路径是换格式重发而非干净的下轮重试，按失败处理只会制造重复），但计入
        # 连续歧义计数：大面积故障时后续 send_html 第一条即可熔断，整批留到次日。
        # （留痕文件与 x_monitor 进程有读改写竞态，最坏丢一条痕迹，可接受。）
        tm._register_ambiguous_send()
        tm._record_assumed_delivery("sendPhoto(macrumors)", it["link"])
        print(f"卡片响应缺失，按已送达处理（防重复）: {e}", file=sys.stderr)


def _format_item_line(it: dict) -> str:
    sub = it.get("sub_items")
    if not sub:
        line = f'• <a href="{esc_attr(it["link"])}">{esc(it["zh_title"])}</a>'
        if it["zh_summary"]:
            line += f" — {esc(it['zh_summary'])}"
        return line
    line = f'• <b>{esc(it["zh_title"])}（{len(sub)} 篇）</b>'
    if it["zh_summary"]:
        line += f" — {esc(it['zh_summary'])}"
    for s in sub:
        line += f'\n  ↳ <a href="{esc_attr(s["link"])}">{esc(s["zh_title"])}</a>'
    return line


def build_html_messages(items: list[dict], header: str, truncated: int) -> list[str]:
    """折叠汇总：每个设备分类一个 <blockquote expandable>，按长度切分成多条。"""
    groups: dict[str, list[str]] = {}
    for it in items:
        groups.setdefault(bucket_of(it), []).append(_format_item_line(it))

    blocks = []
    order = [b[0] for b in BUCKETS] + [OTHER]
    for name in order:
        if name in groups:
            lines = "\n".join(groups[name])
            blocks.append(f"<b>{esc(name)} ({len(groups[name])})</b>\n"
                          f"<blockquote expandable>{lines}</blockquote>")
    if truncated:
        blocks.append(f"<i>（今日条目较多，已展示最新条目，另有 {truncated} 条更早条目略过）</i>")

    messages, cur = [], header
    for blk in blocks:
        if len(cur) + len(blk) + 2 > TG_LIMIT:
            messages.append(cur)
            cur = blk
        else:
            cur += "\n\n" + blk
    if cur:
        messages.append(cur)
    return messages


def send_html(token: str, chat_id: str, text: str, trace_id: str = "") -> None:
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
               "link_preview_options": {"is_disabled": True}}
    for attempt in range(3):
        try:
            tm._tg_post(token, payload, "sendMessage")
            return
        except tm.TgAmbiguousDelivery as e:
            # 请求已送出、响应缺失：重试必产生重复消息，首条按已送达返回（调用方
            # 标 seen），留痕（带段落标识）供 x_monitor 汇总 DM 核对。连续歧义
            # 熔断抛出：send_html 的失败路径是 main 的「未标 seen 次日重试」，
            # 是干净重试，按 x_monitor 同款权衡防整期 digest 批量假送达丢失
            # （最坏次日重发一批 << 全量永久丢失）。
            if not tm._register_ambiguous_send():
                print(f"sendMessage 连续歧义（疑似 Telegram 故障），按失败处理: {e}",
                      file=sys.stderr)
                raise
            tm._record_assumed_delivery("sendMessage(macrumors)", trace_id)
            print(f"sendMessage 响应缺失，按已送达处理（防重复）: {e}", file=sys.stderr)
            return
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace") if hasattr(e, "read") else ""
            if e.code == 429:
                try:
                    time.sleep(min(int(json.loads(body)["parameters"]["retry_after"]), 30))
                except Exception:
                    time.sleep(3)
                continue
            raise RuntimeError(f"sendMessage 失败 {e.code}: {body[:200]}")
        except (urllib.error.URLError, TimeoutError, OSError):
            time.sleep(2 * (attempt + 1))
    raise RuntimeError("sendMessage 重试耗尽")


def main() -> int:
    dry = "--dry-run" in sys.argv

    if fcntl is not None and not dry:
        lock_fp = open(os.path.join(SCRIPT_DIR, ".macrumors.lock"), "w")
        try:
            fcntl.flock(lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("上一轮 macrumors 仍在运行，跳过本次", file=sys.stderr)
            return 0

    # 复用 twitter_monitor 的配置路径（项目真源），避免文件被改名后再次断裂
    with open(tm.CONFIG_PATH) as f:
        cfg = json.load(f)
    token, chat_id = cfg["telegram_bot_token"], cfg["telegram_chat_id"]

    items = parse_items(fetch_feed())
    seen, seen_existed = load_seen()
    now = datetime.now(timezone.utc)

    unseen = [it for it in items if it["guid"] and it["guid"] not in seen]
    if not seen_existed:
        def age_ok(it):
            return it["dt"] is not None and (now - it["dt"]) <= timedelta(hours=FIRST_RUN_WINDOW_H)
        push = [it for it in unseen if age_ok(it)]
        for it in items:
            seen.add(it["guid"])
    else:
        push = unseen

    push.sort(key=lambda it: it["dt"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)

    truncated = 0
    if len(push) > MAX_ITEMS:
        truncated = len(push) - MAX_ITEMS
        push = push[:MAX_ITEMS]

    if not push:
        print("无新增条目，跳过")
        if not dry:
            save_seen(seen)
        return 0

    ai = tm.AIClassifier.load()
    n_original = len(push)
    translate(ai, push)
    n_zh = sum(1 for it in push if it["zh_title"] != it["title"])
    print(f"AI 翻译 {n_zh}/{len(push)} 条（缺失回退英文原标题）")

    push = merge_similar(ai, push)

    today = datetime.now(BJT).strftime("%-m月%-d日")
    if len(push) < n_original:
        header = f"🍎 <b>MacRumors 每日资讯</b> · {today}（{n_original} 条 → {len(push)} 个主题）"
    else:
        header = f"🍎 <b>MacRumors 每日资讯</b> · {today}（共 {len(push)} 条）"
    cards = select_cards(push)
    card_guids = {it["guid"] for it in cards}

    if dry:
        print("=== DRY RUN ===")
        print(f"[头条卡片] {len(cards)} 张：")
        for it in cards:
            n_sub = len(it["sub_items"]) if "sub_items" in it else 1
            label = f"（{n_sub} 篇合并）" if n_sub > 1 else ""
            print(f"  - {it['zh_title']}{label}  | {it['image']}")
        rest = [it for it in push if it["guid"] not in card_guids]
        rest_header = f"📋 <b>其余资讯（{len(rest)} 条）</b>" if cards else header
        for msg in build_html_messages(rest, rest_header, truncated):
            print("-" * 40)
            print(msg)
        return 0

    sent_cards = set()
    for idx, it in enumerate(cards):
        try:
            send_card(token, chat_id, it, header if idx == 0 else "")
            sent_cards.add(it["guid"])
            mark_guids_seen(seen, all_guids([it]), dry=dry)
            time.sleep(1)
        except Exception as e:
            print(f"卡片发送失败（回落到文字）: {it['link']} -> {e}", file=sys.stderr)

    rest = [it for it in push if it["guid"] not in sent_cards]
    rest_sent = False
    if rest:
        rest_header = f"📋 <b>其余资讯（{len(rest)} 条）</b>" if sent_cards else header
        try:
            messages = build_html_messages(rest, rest_header, truncated)
            for i, msg in enumerate(messages, 1):
                send_html(token, chat_id, msg, trace_id=f"digest {i}/{len(messages)}")
                time.sleep(1)
            rest_sent = True
            mark_guids_seen(seen, all_guids(rest), dry=dry)
        except Exception as e:
            print(f"文字汇总发送失败，未标 seen（下轮重试）: {e}", file=sys.stderr)

    print(f"已推送 {len(push)} 条（卡片 {len(sent_cards)} + 文字汇总 {len(rest) if rest_sent else 0} 条）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
