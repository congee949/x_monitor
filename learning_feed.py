#!/usr/bin/env python3
"""Publish selected English X Articles as Biguo-compatible learning material.

The exporter is a sidecar: it never changes X Monitor delivery, seen state, or
article queue state.  A successfully delivered X Article is evaluated before
its temporary Markdown is removed.  Eligible long-form English articles are
rendered as static reading pages and exposed through RSS 2.0.
"""

from __future__ import annotations

import html
import json
import os
import re
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote


DEFAULT_OUTPUT_DIR = "/var/www/x-learning"
MIN_WORDS = 450
MIN_ENGLISH_RATIO = 0.78
MIN_SCORE = 4
MAX_ITEMS = 50

_WORD_RE = re.compile(r"[A-Za-z]+(?:['’][A-Za-z]+)?")
_URL_RE = re.compile(r"https?://\S+")
_IMAGE_RE = re.compile(r"!\[([^\]]*)\]\([^)]*\)")
_LINK_RE = re.compile(r"\[([^\]]+)\]\([^)]*\)")
_PROMO_RE = re.compile(
    r"\b(giveaway|coupon|discount code|affiliate|referral|limited[- ]time offer|"
    r"buy now|price prediction|airdrop)\b",
    re.IGNORECASE,
)
_DOMAIN_GROUPS = {
    "health/science": re.compile(
        r"\b(health|medicine|medical|clinical|patient|biology|biological|"
        r"neuroscience|psychology|disease|public health|epidemiology|"
        r"research|scientific|experiment|evidence)\b",
        re.IGNORECASE,
    ),
    "society/ethics": re.compile(
        r"\b(ethics|ethical|society|social|policy|education|climate|history|"
        r"philosophy|justice|inequality|culture|democracy|regulation)\b",
        re.IGNORECASE,
    ),
    "technology": re.compile(
        r"\b(artificial intelligence|machine learning|technology|software|"
        r"internet|algorithm|automation)\b",
        re.IGNORECASE,
    ),
}
_ARGUMENT_RE = re.compile(
    r"\b(however|therefore|because|although|whereas|in contrast|evidence|"
    r"study|research|argue|argument|suggests|consequently|nevertheless|"
    r"on the other hand|assumption|counterargument)\b",
    re.IGNORECASE,
)


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
        os.chmod(path, 0o644)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def markdown_to_plain(markdown: str) -> str:
    text = _IMAGE_RE.sub(r"\1", markdown or "")
    text = _LINK_RE.sub(r"\1", text)
    text = _URL_RE.sub(" ", text)
    text = re.sub(r"^---\s*$.*?^---\s*$", " ", text, count=1, flags=re.M | re.S)
    text = re.sub(r"[`*_>#|~-]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def evaluate_article(title: str, markdown: str) -> dict[str, Any]:
    plain = markdown_to_plain(markdown)
    words = _WORD_RE.findall(plain)
    letters = [char for char in plain if char.isalpha()]
    ascii_letters = [char for char in letters if "a" <= char.lower() <= "z"]
    english_ratio = len(ascii_letters) / max(len(letters), 1)
    combined = f"{title}\n{plain}"
    reasons: list[str] = []
    score = 0

    if len(words) < MIN_WORDS:
        return {
            "eligible": False,
            "reason": "too_short",
            "word_count": len(words),
            "english_ratio": round(english_ratio, 3),
            "score": 0,
            "reasons": [],
        }
    if english_ratio < MIN_ENGLISH_RATIO:
        return {
            "eligible": False,
            "reason": "not_english_dominant",
            "word_count": len(words),
            "english_ratio": round(english_ratio, 3),
            "score": 0,
            "reasons": [],
        }
    if _PROMO_RE.search(combined):
        return {
            "eligible": False,
            "reason": "promotional",
            "word_count": len(words),
            "english_ratio": round(english_ratio, 3),
            "score": 0,
            "reasons": [],
        }

    if len(words) >= 1500:
        score += 3
        reasons.append("deep long-form")
    elif len(words) >= 800:
        score += 2
        reasons.append("long-form")
    else:
        score += 1
        reasons.append("substantial length")

    argument_markers = len(_ARGUMENT_RE.findall(combined))
    if argument_markers >= 5:
        score += 3
        reasons.append("strong argument structure")
    elif argument_markers >= 2:
        score += 2
        reasons.append("argument structure")

    for label, pattern in _DOMAIN_GROUPS.items():
        if pattern.search(combined):
            weight = 3 if label in {"health/science", "society/ethics"} else 1
            score += weight
            reasons.append(label)

    return {
        "eligible": score >= MIN_SCORE,
        "reason": "eligible" if score >= MIN_SCORE else "low_learning_value",
        "word_count": len(words),
        "english_ratio": round(english_ratio, 3),
        "score": score,
        "reasons": reasons,
    }


def _parse_time(value: str | None) -> datetime:
    try:
        parsed = datetime.fromisoformat(value or "")
    except (TypeError, ValueError):
        parsed = datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _source_url(username: str, entry: dict[str, Any]) -> str:
    author = entry.get("author") or username
    tweet_id = entry.get("tweet_id")
    if tweet_id:
        return f"https://x.com/{quote(str(author))}/status/{quote(str(tweet_id))}"
    return f"https://x.com/i/article/{quote(str(entry.get('article_id') or ''))}"


def _render_markdown_body(markdown: str) -> str:
    blocks: list[str] = []
    for raw in re.split(r"\n\s*\n", markdown or ""):
        part = raw.strip()
        if not part or part == "---":
            continue
        part = _IMAGE_RE.sub(r"\1", part)
        part = _LINK_RE.sub(r"\1", part)
        part = _URL_RE.sub("", part)
        part = re.sub(r"^[#>*+\-\d.\s]+", "", part).strip()
        if not part:
            continue
        blocks.append(f"<p>{html.escape(part).replace(chr(10), '<br>')}</p>")
    return "\n".join(blocks)


def _render_article_page(item: dict[str, Any], markdown: str) -> str:
    title = html.escape(item["title"])
    source = html.escape(item["source_url"], quote=True)
    reasons = " · ".join(html.escape(x) for x in item.get("reasons") or [])
    body = _render_markdown_body(markdown)
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title><style>
body{{margin:0;background:#f7f4ec;color:#20201d;font:18px/1.72 Georgia,serif}}
main{{max-width:780px;margin:auto;padding:48px 24px 96px}}h1{{font-size:2.1rem;line-height:1.2}}
.meta,.prompt{{font:14px/1.5 -apple-system,BlinkMacSystemFont,sans-serif;color:#666}}
.prompt{{background:#fff8db;border-left:4px solid #e0ad20;padding:14px 16px;margin:28px 0}}
a{{color:#7a4b00}}p{{margin:1.15em 0}}
</style></head><body><main><h1>{title}</h1>
<p class="meta">@{html.escape(item['author'])} · {item['word_count']} words · score {item['score']}<br>{reasons}</p>
<div class="prompt"><strong>Learning task</strong><br>Identify the claim, two supporting pieces of evidence, and one hidden assumption. Then give a 90-second English teach-back and write a 150-word summary or rebuttal.</div>
<article>{body}</article><p><a href="{source}">Open the original X Article</a></p></main></body></html>"""


def _load_items(path: Path) -> list[dict[str, Any]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return []
    return data if isinstance(data, list) else []


def _render_rss(items: list[dict[str, Any]], base_url: str) -> str:
    root = ET.Element("rss", {"version": "2.0"})
    channel = ET.SubElement(root, "channel")
    ET.SubElement(channel, "title").text = "BWG X Articles · English Learning"
    ET.SubElement(channel, "link").text = base_url.rstrip("/") + "/"
    ET.SubElement(channel, "description").text = (
        "English-dominant long-form X Articles selected for PTE and GAMSAT transfer practice."
    )
    ET.SubElement(channel, "language").text = "en"
    ET.SubElement(channel, "lastBuildDate").text = format_datetime(datetime.now(timezone.utc))
    for item in items:
        node = ET.SubElement(channel, "item")
        ET.SubElement(node, "title").text = item["title"]
        ET.SubElement(node, "link").text = item["learning_url"]
        ET.SubElement(node, "guid", {"isPermaLink": "false"}).text = "x-article:" + item["article_id"]
        ET.SubElement(node, "pubDate").text = format_datetime(_parse_time(item.get("published_at")))
        ET.SubElement(node, "author").text = "@" + item["author"]
        ET.SubElement(node, "description").text = (
            f"{item['word_count']} words; learning score {item['score']}; "
            + ", ".join(item.get("reasons") or [])
        )
    return '<?xml version="1.0" encoding="utf-8"?>\n' + ET.tostring(root, encoding="unicode")


def ensure_feed(
    *,
    output_dir: str | Path | None = None,
    base_url: str | None = None,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    """Create a valid feed even when no retained article passes the filter."""
    if output_dir is None or base_url is None:
        try:
            cfg = json.loads(Path(config_path or Path(__file__).with_name("config.json")).read_text())
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {"ready": False, "reason": "config_unavailable"}
        if not cfg.get("learning_feed_enabled"):
            return {"ready": False, "reason": "disabled"}
        output_dir = output_dir or cfg.get("learning_feed_output_dir") or DEFAULT_OUTPUT_DIR
        base_url = base_url or cfg.get("learning_feed_base_url")
    if not base_url:
        return {"ready": False, "reason": "base_url_missing"}

    out = Path(output_dir)
    index_path = out / "articles.json"
    items = _load_items(index_path)
    if not index_path.exists():
        _atomic_write(index_path, "[]\n")
    _atomic_write(out / "feed.xml", _render_rss(items, base_url))
    return {"ready": True, "item_count": len(items)}


def publish_article(
    username: str,
    entry: dict[str, Any],
    markdown: str,
    *,
    output_dir: str | Path | None = None,
    base_url: str | None = None,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    """Evaluate and publish one delivered X Article.

    Production settings come from the non-secret public keys in config.json:
    learning_feed_enabled, learning_feed_output_dir, learning_feed_base_url.
    Tests can pass output_dir/base_url directly.
    """
    if output_dir is None or base_url is None:
        try:
            cfg = json.loads(Path(config_path or Path(__file__).with_name("config.json")).read_text())
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {"published": False, "reason": "config_unavailable"}
        if not cfg.get("learning_feed_enabled"):
            return {"published": False, "reason": "disabled"}
        output_dir = output_dir or cfg.get("learning_feed_output_dir") or DEFAULT_OUTPUT_DIR
        base_url = base_url or cfg.get("learning_feed_base_url")
    if not base_url:
        return {"published": False, "reason": "base_url_missing"}

    title = (entry.get("article_title") or "").strip() or "Untitled X Article"
    verdict = evaluate_article(title, markdown)
    if not verdict["eligible"]:
        return {"published": False, **verdict}

    article_id = str(entry.get("article_id") or "").strip()
    if not article_id:
        return {"published": False, "reason": "article_id_missing"}
    out = Path(output_dir)
    page_rel = f"articles/{article_id}.html"
    item = {
        "article_id": article_id,
        "tweet_id": str(entry.get("tweet_id") or ""),
        "author": str(entry.get("author") or username),
        "title": title,
        "source_url": _source_url(username, entry),
        "learning_url": base_url.rstrip("/") + "/" + page_rel,
        "published_at": entry.get("sent_at") or entry.get("detected_at") or datetime.now(timezone.utc).isoformat(),
        "word_count": verdict["word_count"],
        "english_ratio": verdict["english_ratio"],
        "score": verdict["score"],
        "reasons": verdict["reasons"],
    }
    _atomic_write(out / page_rel, _render_article_page(item, markdown))

    index_path = out / "articles.json"
    items = [x for x in _load_items(index_path) if str(x.get("article_id")) != article_id]
    items.append(item)
    items.sort(key=lambda x: str(x.get("published_at") or ""), reverse=True)
    items = items[:MAX_ITEMS]
    _atomic_write(index_path, json.dumps(items, ensure_ascii=False, indent=2))
    _atomic_write(out / "feed.xml", _render_rss(items, base_url))

    keep_ids = {str(x.get("article_id")) for x in items}
    for page in (out / "articles").glob("*.html"):
        if page.stem not in keep_ids:
            try:
                page.unlink()
            except OSError:
                pass
    return {"published": True, **item}
