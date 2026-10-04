#!/usr/bin/env python3
"""Durable handoff from the CC98 Telegram poller to the X review consumer."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any, Mapping

CALLBACK_RE = re.compile(r"^xreview:(R\d{2,}):(keep|merge|uncertain)$")
CONTENT_FIELDS = frozenset({
    "text", "caption", "photo", "document", "audio", "video", "voice",
    "video_note", "sticker", "animation", "contact", "location", "venue", "poll",
    "dice", "game",
})


def _positive_int(value: Any) -> bool:
    return type(value) is int and value > 0


def _owner_id(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("invalid X review owner id")
    text = str(value).strip()
    if not text.isascii() or not text.isdigit() or int(text) <= 0:
        raise ValueError("invalid X review owner id")
    return int(text)


def bridge_settings(config: Mapping[str, Any]) -> tuple[Path, int] | None:
    raw_path = os.environ.get("CC98_XREVIEW_OUTBOX") or config.get("x_review_outbox")
    raw_owner = os.environ.get("CC98_XREVIEW_OWNER_ID") or config.get("x_review_owner_id")
    if not raw_path and not raw_owner:
        return None
    if not raw_path or not raw_owner:
        raise ValueError("X review bridge needs both outbox path and owner id")
    path = Path(str(raw_path)).expanduser()
    if not path.is_absolute():
        raise ValueError("X review outbox path must be absolute")
    return path, _owner_id(raw_owner)


def allowed_updates(config: Mapping[str, Any]) -> list[str]:
    return ["message", "callback_query"] if bridge_settings(config) else ["message"]


def _owned_private_message(message: Any, owner: int, *, require_actor: bool) -> bool:
    if not isinstance(message, dict) or not _positive_int(message.get("message_id")):
        return False
    chat = message.get("chat")
    if not isinstance(chat, dict) or chat.get("type") != "private" or type(chat.get("id")) is not int or chat["id"] != owner:
        return False
    if require_actor:
        actor = message.get("from")
        if not isinstance(actor, dict) or type(actor.get("id")) is not int or actor["id"] != owner or actor.get("is_bot") is not False:
            return False
    return True


def update_kind(update: Any, owner: int) -> str | None:
    if not isinstance(update, dict) or type(update.get("update_id")) is not int or update["update_id"] < 0:
        return None
    message = update.get("message")
    if _owned_private_message(message, owner, require_actor=True) and any(message.get(key) for key in CONTENT_FIELDS):
        return "message"
    callback = update.get("callback_query")
    if not isinstance(callback, dict):
        return None
    actor = callback.get("from")
    data = callback.get("data")
    if (not isinstance(actor, dict) or type(actor.get("id")) is not int
            or actor["id"] != owner or actor.get("is_bot") is not False
            or not isinstance(callback.get("id"), str) or not callback["id"]
            or not isinstance(data, str) or CALLBACK_RE.fullmatch(data) is None
            or not _owned_private_message(callback.get("message"), owner, require_actor=False)):
        return None
    return "xreview"


def _canonical(update: Mapping[str, Any]) -> str:
    return json.dumps(update, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _open_writer(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        pass
    else:
        os.close(fd)
    db = sqlite3.connect(path, timeout=10, isolation_level=None)
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA busy_timeout=10000")
    db.execute("CREATE TABLE IF NOT EXISTS x_review_outbox (update_id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL)")
    return db


def persist_batch(updates: list[dict[str, Any]], *, path: Path, owner: int) -> set[int]:
    """Commit all matching updates before the caller advances Telegram offset.

    Returning xreview IDs lets the old CC98 command handler skip these callbacks.
    Identical retries are no-ops; changed payload/owner for an existing update fails
    the complete transaction. No queue entries are deleted or acknowledged here.
    """
    if not isinstance(updates, list):
        raise ValueError("Telegram update batch must be an array")
    owner = _owner_id(owner)
    rows = []
    callbacks: set[int] = set()
    for update in updates:
        kind = update_kind(update, owner)
        if kind:
            rows.append((update["update_id"], owner, kind, _canonical(update)))
            if kind == "xreview":
                callbacks.add(update["update_id"])
    db = _open_writer(Path(path))
    try:
        db.execute("BEGIN IMMEDIATE")
        for update_id, owner_id, kind, payload in rows:
            prior = db.execute("SELECT owner_id, kind, payload FROM x_review_outbox WHERE update_id=?", (update_id,)).fetchone()
            if prior is not None:
                if prior != (owner_id, kind, payload):
                    raise ValueError(f"conflicting Telegram update payload: {update_id}")
            else:
                db.execute("INSERT INTO x_review_outbox VALUES (?, ?, ?, ?)", (update_id, owner_id, kind, payload))
        db.execute("COMMIT")
    except BaseException:
        if db.in_transaction:
            db.execute("ROLLBACK")
        raise
    finally:
        db.close()
    return callbacks


def persist_configured_batch(config: Mapping[str, Any], updates: list[dict[str, Any]]) -> set[int]:
    settings = bridge_settings(config)
    if settings is None:
        return set()
    path, owner = settings
    return persist_batch(updates, path=path, owner=owner)


def export_updates(*, path: Path, after: int, owner: int, limit: int = 100) -> list[dict[str, Any]]:
    """Read the queue without changing a cursor, queue row, or database schema."""
    if type(after) is not int or after < -1:
        raise ValueError("after must be an integer >= -1")
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("limit must be in 1..100")
    owner = _owner_id(owner)
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    db = sqlite3.connect(uri, uri=True, timeout=10)
    try:
        db.execute("PRAGMA query_only=ON")
        rows = db.execute("SELECT update_id, kind, payload FROM x_review_outbox WHERE owner_id=? AND update_id>? ORDER BY update_id LIMIT ?", (owner, after, limit)).fetchall()
        updates = []
        for update_id, kind, payload in rows:
            update = json.loads(payload)
            if update.get("update_id") != update_id or update_kind(update, owner) != kind:
                raise ValueError("invalid persisted X review update")
            updates.append(update)
        return updates
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=os.environ.get("CC98_XREVIEW_OUTBOX"))
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    export = sub.add_parser("export")
    export.add_argument("--after", type=int, required=True)
    export.add_argument("--owner", type=int, required=True)
    export.add_argument("--limit", type=int, default=100)
    args = parser.parse_args(argv)
    if not args.state:
        parser.error("provide --state or CC98_XREVIEW_OUTBOX")
    if args.command == "init":
        _open_writer(Path(args.state)).close()
        print(json.dumps({"initialized": True}))
        return 0
    print(json.dumps(export_updates(path=Path(args.state), after=args.after, owner=args.owner, limit=args.limit), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
