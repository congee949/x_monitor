#!/usr/bin/env python3
"""Render fixed X review pairs with native rich media and stable callback bindings.

Rendering uses local snapshots only. Video size checks are opt-in; otherwise the
monitor's bitrate/duration estimate selects a suitable MP4. No monitor is run.
"""
from __future__ import annotations

import copy
from contextlib import closing
import hashlib
import html
import re
import sqlite3
from pathlib import Path
import urllib.parse

import twitter_monitor as monitor
from x_review_bot import VERDICTS, json_text, positive_id


def bindings(packet: dict, receipt: dict, owner: int) -> dict:
    if str(receipt.get("chat_id")) != str(owner):
        raise ValueError("receipt owner mismatch")
    cases = packet.get("cases", [])
    ids = [c.get("case_id") for c in cases]
    if not ids or len(set(ids)) != len(ids) or any(not re.fullmatch(r"R[0-9]{2,}", str(x)) for x in ids):
        raise ValueError("invalid case ids")
    mapping = {}
    for row in receipt.get("receipts", []):
        if "case_id" not in row:
            continue
        cid = row["case_id"]
        if row.get("ok") is not True or cid not in ids or cid in mapping:
            raise ValueError("invalid case receipt")
        mid = positive_id(row["message_id"])
        if mid in mapping.values():
            raise ValueError("duplicate message id")
        mapping[cid] = mid
    if set(mapping) != set(ids):
        raise ValueError("missing case receipt")
    return mapping


def read_selections(state: Path, packet: dict, mapping: dict, owner: int, *, require_drained=False) -> dict:
    """Read the existing review sidecar without migrating or writing it."""
    uri = "file:" + urllib.parse.quote(str(Path(state).resolve())) + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as db:
        if require_drained and db.execute("SELECT count(*) FROM updates WHERE kind='callback_query' AND processed=0").fetchone()[0]:
            raise ValueError("pending callback edits; drain the processor before converting cards")
        meta = dict(db.execute("SELECT key,value FROM meta"))
        fingerprint = hashlib.sha256(json_text([packet, mapping]).encode()).hexdigest()
        if meta.get("owner") != str(owner) or meta.get("packet_fingerprint") != fingerprint:
            raise ValueError("state packet/owner mismatch")
        rows = db.execute("""SELECT c.case_id,c.verdict FROM callbacks c JOIN
            (SELECT case_id,max(seq) seq FROM callbacks WHERE accepted=1 GROUP BY case_id) x
            ON c.seq=x.seq""").fetchall()
        return {cid: verdict for cid, verdict in rows if cid in mapping and verdict in VERDICTS}


def progress(packet: dict, selections: dict) -> str:
    cases = {c["case_id"]: c for c in packet["cases"]}
    selected = {cid: v for cid, v in selections.items() if cid in cases and v in VERDICTS}
    eligible = sum(c.get("gate_eligible") is True for c in cases.values())
    decisive = sum(cases[cid].get("gate_eligible") is True and v != "uncertain"
                   for cid, v in selected.items())
    return f"已选择 {len(selected)}/{len(cases)}；门槛有效标注 {decisive}/{eligible}"


def keyboard(case: dict, verdict=None) -> dict:
    links = []
    for side, label in (("prior", "原消息"), ("candidate", "后续消息")):
        url = case[side].get("url") or ""
        if urllib.parse.urlsplit(url).hostname in ("x.com", "twitter.com"):
            links.append({"text": label, "url": url})
    choices = [{"text": ("✓ " if verdict == key else "") + label,
                "callback_data": f"xreview:{case['case_id']}:{key}"}
               for key, label in VERDICTS.items()]
    return {"inline_keyboard": ([links] if links else []) + [choices]}


def x_media_block(items: list, *, probe_video=False) -> str:
    """Reuse the monitor's size-aware renderer, scoped to this one-shot command."""
    media = []
    for item in items:
        if not isinstance(item, dict):
            continue
        item = copy.deepcopy(item)
        item["url"] = monitor._safe_x_media_url(item.get("url"))
        item["video_url"] = monitor._safe_x_media_url(item.get("video_url"))
        item["variants"] = [v for v in item.get("variants", [])
                            if monitor._safe_x_media_url(v.get("url"))]
        if item["url"]:
            media.append(item)
    enabled, head = monitor._RICH_VIDEO_ENABLED, monitor._head_content_length
    try:
        monitor._RICH_VIDEO_ENABLED = True
        if not probe_video:
            monitor._head_content_length = lambda _url: None
        return monitor._rich_media_block({"media": media}, embed_video=True)
    finally:
        monitor._RICH_VIDEO_ENABLED, monitor._head_content_length = enabled, head


def node_media(node: dict, refs: list, attachments: list, *, probe_video=False) -> str:
    # References must come from this same bot's original Telegram message.
    reusable = [r for r in refs if str(r.get("owner_tweet_id")) == str(node.get("tweet_id"))
                and r.get("type") in ("photo", "video", "animation")
                and isinstance(r.get("file_id"), str) and r["file_id"]]
    if not reusable:
        return x_media_block(node.get("media") or [], probe_video=probe_video)
    tags = []
    for item in reusable[:4]:
        ident = "review_media_" + str(len(attachments))
        kind = "photo" if item["type"] == "photo" else "video"
        attachments.append({"id": ident, "media": {"type": item["type"], "media": item["file_id"]}})
        tag = "img" if kind == "photo" else "video"
        tags.append(f'<{tag} src="tg://{kind}?id={ident}"/>')
    block = tags[0] if len(tags) == 1 else "<tg-collage>" + "".join(tags) + "</tg-collage>"
    return "<br><br>" + block


def node_text(node: dict) -> str:
    view = dict(node, note_tweet=node.get("note"))
    body = ((node.get("note") or {}).get("text") or node.get("text") or "")
    body = monitor._strip_media_tco(body, view)
    return monitor._render_tweet_urls(body, view, rich=True)


def side_html(side: dict, label: str, entry: dict, attachments: list, *, probe_video=False) -> str:
    name = html.escape(str(side.get("username") or ""))
    result = f"<b>{label} · @{name}</b><br>"
    bundle = entry.get("bundle") or {}
    expected_id = str(side.get("tweet_id"))
    observation_id = str((bundle.get("observation") or {}).get("outer_id") or "")
    if bundle and expected_id != observation_id:
        raise ValueError("media tweet identity mismatch")
    anchor = bundle.get("anchor") or {}
    refs = entry.get("telegram_media") or []
    if anchor:
        if bundle.get("repost_path"):
            result += "<i>转推 @" + html.escape(anchor.get("author") or "") + "</i><br>"
        result += node_text(anchor) + node_media(anchor, refs, attachments, probe_video=probe_video)
        seen = {str(anchor.get("tweet_id"))}
        for node in bundle.get("context_nodes") or []:
            if str(node.get("tweet_id")) in seen:
                continue
            seen.add(str(node.get("tweet_id")))
            result += "<br><br><b>引用 · @" + html.escape(node.get("author") or "") + "</b><br>"
            result += node_text(node) + node_media(node, refs, attachments, probe_video=probe_video)
        reasons = (bundle.get("resolution") or {}).get("reasons") or []
        if any("missing_" in reason for reason in reasons):
            result += "<br><i>部分引用内容暂未取回，请打开原文核对。</i>"
    else:
        result += monitor._rich_preserve(side.get("caption") or "正文暂未取回，请打开原文核对。")
        result += "<br><i>当前无法读取 X 详情，显示已送达的文本快照；媒体尚未恢复。</i>"
    url = side.get("url") or ""
    if urllib.parse.urlsplit(url).hostname in ("x.com", "twitter.com"):
        result += f'<br><a href="{html.escape(url, quote=True)}">打开{label}原文</a>'
    return result


def render_card(case: dict, media: dict, *, total: int, probe_video=False) -> dict:
    attachments = []
    label = "线上候选" if case.get("gate_eligible") is True else "历史对照，不计入门槛"
    text = (f'<b>{html.escape(case["case_id"])} / {total} · X 事件人工复核</b><br>'
            f'<i>{label} · 机器记录：{html.escape(str(case.get("machine_decision") or ""))}</i>')
    for side, title in (("prior", "此前"), ("candidate", "当前")):
        value = case[side]
        text += "<br><br>" + side_html(value, title, media.get("entries", {}).get(str(value["tweet_id"]), {}),
                                          attachments, probe_video=probe_video)
    text += "<br><br>点击下方按钮选择：保留两条 / 可以合并 / 不确定。勾选按钮显示最新选择，可再次点击改判。"
    if len(text) > monitor.RICH_MESSAGE_MAX_CHARS or len(attachments) > 50:
        raise ValueError("review card exceeds rich message budget")
    result = {"html": text}
    if attachments:
        result["media"] = attachments
    return result


def build_payloads(packet: dict, receipt: dict, media: dict, owner: int, selections=None, *, probe_video=False) -> list:
    mapping = bindings(packet, receipt, owner)
    selections = selections or {}
    return [{"case_id": c["case_id"], "payload": {
                "chat_id": owner, "message_id": mapping[c["case_id"]],
                "rich_message": render_card(c, media, total=len(packet["cases"]), probe_video=probe_video),
                "reply_markup": keyboard(c, selections.get(c["case_id"]))}}
            for c in packet["cases"]]

