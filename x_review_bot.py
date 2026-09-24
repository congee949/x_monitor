#!/usr/bin/env python3
"""Single Telegram update consumer for human X-event review and durable DM relay."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

VERDICTS = {"keep": "保留两条", "merge": "可以合并", "uncertain": "不确定"}
CALLBACK_RE = re.compile(r"^xreview:(R\d{2,}):(keep|merge|uncertain)$")
ROOT = Path(__file__).resolve().parent


class BotError(Exception):
    def __init__(self, code: int, description: str = ""):
        self.code = code
        self.description = description
        # Never include a request URL: it contains the bot token.
        super().__init__(f"Telegram API error {code}")


class ReviewRejected(Exception):
    pass


class Telegram:
    def __init__(self, token: str):
        if not token or not isinstance(token, str):
            raise ValueError("missing telegram_bot_token")
        self._token = token

    def call(self, method: str, payload: dict):
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{self._token}/{method}",
            data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=25) as response:
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            try:
                error = json.loads(exc.read())
                description = str(error.get("description") or "")
            except Exception:
                description = ""
            raise BotError(exc.code, description) from None
        except (OSError, ValueError, urllib.error.URLError):
            raise BotError(0, "transport failure") from None
        if not result.get("ok"):
            raise BotError(int(result.get("error_code") or 0), str(result.get("description") or ""))
        return result.get("result")


def json_text(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def positive_id(value) -> int:
    if isinstance(value, bool):
        raise ValueError("invalid id")
    number = int(value)
    if number <= 0:
        raise ValueError("invalid id")
    return number


class ReviewStore:
    def __init__(self, path: Path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=15)
        os.chmod(path, 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS updates(update_id INTEGER PRIMARY KEY,kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,processed INTEGER NOT NULL DEFAULT 0);
          CREATE TABLE IF NOT EXISTS messages(update_id INTEGER PRIMARY KEY,message_id INTEGER NOT NULL,
            from_id TEXT NOT NULL,chat_id TEXT NOT NULL,chat_type TEXT NOT NULL,date INTEGER NOT NULL,
            text TEXT NOT NULL,payload_json TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS callbacks(seq INTEGER PRIMARY KEY AUTOINCREMENT,
            callback_id TEXT UNIQUE NOT NULL,update_id INTEGER NOT NULL,case_id TEXT,verdict TEXT,
            actor_id TEXT NOT NULL,chat_id TEXT NOT NULL,message_id TEXT NOT NULL,
            accepted INTEGER NOT NULL,reason TEXT NOT NULL,clicked_at TEXT NOT NULL,
            base_text TEXT,entities_json TEXT);
        """)
        self.db.commit()

    def close(self):
        self.db.close()

    def offset(self) -> int:
        row = self.db.execute("SELECT value FROM meta WHERE key='next_offset'").fetchone()
        return int(row[0]) if row else 0

    def bind(self, fingerprint: str, owner: int):
        with self.db:
            for key, value in (("packet_fingerprint", fingerprint), ("owner", str(owner))):
                old = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
                if old and old[0] != value:
                    raise ValueError("review state belongs to another packet/owner; use a new state database")
                self.db.execute("INSERT OR IGNORE INTO meta VALUES (?,?)", (key, value))

    def done(self, update_id: int):
        with self.db:
            self.db.execute("UPDATE updates SET processed=1 WHERE update_id=?", (update_id,))
            self.db.execute("INSERT INTO meta VALUES ('next_offset',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                            (str(max(self.offset(), update_id + 1)),))


def relay(path: Path, after: int, owner: int) -> list[dict]:
    """Read only; the caller checkpoints update_id after accepting each message."""
    uri = "file:" + urllib.parse.quote(str(Path(path).resolve())) + "?mode=ro"
    db = sqlite3.connect(uri, uri=True)
    db.row_factory = sqlite3.Row
    try:
        rows = db.execute("""SELECT message_id AS id,update_id,date,text FROM messages
            WHERE update_id>? AND from_id=? AND chat_id=? AND chat_type='private'
            ORDER BY update_id""", (after, str(owner), str(owner))).fetchall()
        return [dict(row) for row in rows]
    finally:
        db.close()


class ReviewBot:
    def __init__(self, api, store: ReviewStore, packet: dict, receipt: dict,
                 owner: int, ledger: Path):
        self.api, self.store = api, store
        self.owner, self.ledger = positive_id(owner), Path(ledger).resolve()
        if str(receipt.get("chat_id")) != str(self.owner):
            raise ValueError("receipt owner does not match config telegram_chat_id")
        self.cases, self.mapping = {}, {}
        for case in packet.get("cases", []):
            case_id = case.get("case_id")
            if not re.fullmatch(r"R\d{2,}", str(case_id)) or case_id in self.cases:
                raise ValueError("invalid or duplicate case id")
            self.cases[case_id] = case
        if not self.cases:
            raise ValueError("empty review packet")
        for row in receipt.get("receipts", []):
            if "case_id" not in row:
                continue
            case_id = row["case_id"]
            if row.get("ok") is not True or case_id not in self.cases or case_id in self.mapping:
                raise ValueError("invalid case receipt")
            message_id = positive_id(row["message_id"])
            if message_id in self.mapping.values():
                raise ValueError("duplicate receipt message id")
            self.mapping[case_id] = message_id
        if set(self.cases) != set(self.mapping):
            raise ValueError("receipt must map every review case")
        fingerprint = hashlib.sha256(json_text([packet, self.mapping]).encode()).hexdigest()
        store.bind(fingerprint, self.owner)

    def validate_callback(self, callback: dict):
        match = CALLBACK_RE.fullmatch(str(callback.get("data") or ""))
        message = callback.get("message") or {}
        chat = message.get("chat") or {}
        actor = callback.get("from") or {}
        if (str(actor.get("id")) != str(self.owner) or actor.get("is_bot") is True
                or str(chat.get("id")) != str(self.owner) or chat.get("type") != "private"):
            raise ReviewRejected("unauthorized_sender_or_chat")
        if not match:
            raise ReviewRejected("unknown_callback")
        case_id, verdict = match.groups()
        if case_id not in self.cases or str(message.get("message_id")) != str(self.mapping[case_id]):
            raise ReviewRejected("case_message_mismatch")
        if not isinstance(message.get("text"), str) or not message["text"]:
            raise ReviewRejected("inaccessible_message")
        return case_id, verdict

    def apply_gate(self, case_id: str, verdict: str, callback_id: str):
        case = self.cases[case_id]
        if case.get("gate_eligible") is not True:
            return
        if (case.get("source_kind") != "online_observation"
                or case.get("machine_decision") != "would_suppress"):
            raise ReviewRejected("invalid_gate_provenance")
        try:
            observation = positive_id(case["observation_id"])
            candidate = case["candidate"]
            expected_id = str(candidate["tweet_id"])
            expected_name = str(candidate["username"])
            expected_key = str(case["event_key"])
        except (KeyError, TypeError, ValueError):
            raise ReviewRejected("invalid_gate_binding") from None
        uri = "file:" + urllib.parse.quote(str(self.ledger)) + "?mode=rw"
        db = sqlite3.connect(uri, uri=True, timeout=15)
        db.row_factory = sqlite3.Row
        try:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM event_observations WHERE id=?", (observation,)).fetchone()
            if (row is None or row["decision"] != "would_suppress"
                    or row["event_key"] != expected_key
                    or row["candidate_tweet_id"] != expected_id
                    or row["candidate_username"] != expected_name):
                raise ReviewRejected("observation_identity_mismatch")
            reviewed, fp = (0, None) if verdict == "uncertain" else (1, 1 if verdict == "keep" else 0)
            note = f"human telegram review {case_id}: {verdict}; callback={callback_id}"
            db.execute("UPDATE event_observations SET reviewed=?,false_positive=?,note=? WHERE id=?",
                       (reviewed, fp, note, observation))
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _answer(self, callback_id: str, text: str, rejected=False):
        try:
            self.api.call("answerCallbackQuery", {"callback_query_id": callback_id,
                          "text": text, "show_alert": rejected})
        except BotError as exc:
            # Telegram expires callback answers, but an already saved human verdict
            # must still finish delivery/checkpoint after a service restart.
            if exc.code == 400 and any(value in exc.description.lower() for value in
                                      ("query is too old", "query id is invalid", "query_id_invalid")):
                return
            raise

    def render(self, callback_id: str, row):
        case_id, verdict = row["case_id"], row["verdict"]
        text = row["base_text"] + "\n\n复核状态：" + VERDICTS[verdict] + "（可再次点击改判）"
        buttons = [{"text": ("✓ " if key == verdict else "") + label,
                    "callback_data": f"xreview:{case_id}:{key}"} for key, label in VERDICTS.items()]
        urls = []
        for side, label in (("prior", "原消息"), ("candidate", "后续消息")):
            url = (self.cases[case_id].get(side) or {}).get("url")
            if isinstance(url, str) and url.startswith(("https://x.com/", "https://twitter.com/")):
                urls.append({"text": label, "url": url})
        keyboard = ([urls] if urls else []) + [buttons]
        try:
            self.api.call("editMessageText", {"chat_id": self.owner,
                "message_id": self.mapping[case_id], "text": text,
                "entities": json.loads(row["entities_json"] or "[]"),
                "link_preview_options": {"is_disabled": True},
                "reply_markup": {"inline_keyboard": keyboard}})
        except BotError as exc:
            if not (exc.code == 400 and "message is not modified" in exc.description.lower()):
                raise
        self._answer(callback_id, "已记录：" + VERDICTS[verdict])

    def persist(self, update: dict):
        """Retain the full fetched batch before a failing callback can stop processing."""
        update_id = positive_id(update["update_id"])
        kind = "callback_query" if "callback_query" in update else "message" if "message" in update else "other"
        db = self.store.db
        with db:
            db.execute("INSERT OR IGNORE INTO updates(update_id,kind,payload_json) VALUES (?,?,?)",
                       (update_id, kind, json_text(update)))
            if kind == "message":
                message = update["message"]
                chat, actor = message.get("chat") or {}, message.get("from") or {}
                db.execute("INSERT OR IGNORE INTO messages VALUES (?,?,?,?,?,?,?,?)",
                    (update_id, int(message.get("message_id") or 0), str(actor.get("id") or ""),
                     str(chat.get("id") or ""), str(chat.get("type") or ""),
                     int(message.get("date") or 0), str(message.get("text") or message.get("caption") or ""),
                     json_text(message)))
        return kind

    def handle(self, update: dict):
        update_id = positive_id(update["update_id"])
        db = self.store.db
        prior = db.execute("SELECT * FROM updates WHERE update_id=?", (update_id,)).fetchone()
        if prior and prior["processed"]:
            return
        if prior:
            update = json.loads(prior["payload_json"])
        kind = self.persist(update)
        if kind == "message":
            self.store.done(update_id)
            return
        if kind != "callback_query":
            self.store.done(update_id)
            return
        callback = update["callback_query"]
        callback_id = str(callback.get("id") or "")
        if not callback_id:
            raise ValueError("missing callback id")
        saved = db.execute("SELECT * FROM callbacks WHERE callback_id=?", (callback_id,)).fetchone()
        if saved is None:
            case_id = verdict = None
            reason, accepted = "", 1
            try:
                case_id, verdict = self.validate_callback(callback)
            except ReviewRejected as exc:
                reason, accepted = str(exc), 0
            message = callback.get("message") or {}
            original = db.execute("SELECT base_text,entities_json FROM callbacks WHERE case_id=? AND accepted=1 ORDER BY seq LIMIT 1",
                                  (case_id,)).fetchone()
            base = original["base_text"] if original else str(message.get("text") or "")
            entities = original["entities_json"] if original else json_text(message.get("entities") or [])
            with db:
                db.execute("""INSERT INTO callbacks(callback_id,update_id,case_id,verdict,actor_id,chat_id,
                  message_id,accepted,reason,clicked_at,base_text,entities_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (callback_id, update_id, case_id, verdict, str((callback.get("from") or {}).get("id") or ""),
                   str((message.get("chat") or {}).get("id") or ""), str(message.get("message_id") or ""),
                   accepted, reason, datetime.now(timezone.utc).isoformat(), base, entities))
            saved = db.execute("SELECT * FROM callbacks WHERE callback_id=?", (callback_id,)).fetchone()
        if saved["update_id"] != update_id:
            # The same callback delivered under another update is not a new click.
            # Never replay an older verdict over a later intentional change.
            self._answer(callback_id, "该点击已记录。")
            self.store.done(update_id)
            return
        if saved["accepted"]:
            # Retry the idempotent ledger write before acknowledgement. An external
            # ledger failure must never acknowledge or advance the update offset.
            try:
                self.apply_gate(saved["case_id"], saved["verdict"], callback_id)
            except ReviewRejected as exc:
                with db:
                    db.execute("UPDATE callbacks SET accepted=0,reason=? WHERE callback_id=?",
                               (str(exc), callback_id))
                self._answer(callback_id, "复核绑定已变化，请重新核对。", rejected=True)
            else:
                self.render(callback_id, saved)
        else:
            self._answer(callback_id, "此按钮无法用于当前复核。", rejected=True)
        self.store.done(update_id)

    def poll(self):
        updates = self.api.call("getUpdates", {"offset": self.store.offset(), "timeout": 15,
                                "allowed_updates": ["callback_query", "message"], "limit": 100})
        if not isinstance(updates, list):
            raise ValueError("invalid getUpdates result")
        for update in updates:
            self.persist(update)
        # Recovery includes durable records from a previous response even if
        # Telegram no longer returns them (for example after another consumer).
        pending = self.store.db.execute(
            "SELECT payload_json FROM updates WHERE processed=0 ORDER BY update_id").fetchall()
        for row in pending:
            self.handle(json.loads(row["payload_json"]))


@contextmanager
def consumer_lock(state: Path):
    lock_path = Path(str(state) + ".consumer.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another local review consumer owns the lock") from None
        yield


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, default=ROOT / "state/x-review.sqlite3")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--config", type=Path, default=ROOT / "config.json")
    run.add_argument("--packet", type=Path, required=True)
    run.add_argument("--receipt", type=Path, required=True)
    run.add_argument("--ledger", type=Path, default=ROOT / "twitter_seen/.event_ledger.sqlite3")
    run.add_argument("--once", action="store_true")
    relay_cmd = sub.add_parser("relay")
    relay_cmd.add_argument("--after", type=int, default=0)
    relay_cmd.add_argument("--owner", type=int, required=True)
    args = parser.parse_args(argv)
    if args.command == "relay":
        print(json.dumps(relay(args.state, args.after, positive_id(args.owner)), ensure_ascii=False))
        return 0
    os.umask(0o077)
    config = json.loads(args.config.read_text())
    owner = positive_id(config["telegram_chat_id"])
    api = Telegram(config["telegram_bot_token"])
    with consumer_lock(args.state):
        store = ReviewStore(args.state)
        try:
            bot = ReviewBot(api, store, json.loads(args.packet.read_text()),
                            json.loads(args.receipt.read_text()), owner, args.ledger)
            while True:
                try:
                    bot.poll()
                except BotError as exc:
                    print(f"review consumer Telegram error code={exc.code}", file=sys.stderr, flush=True)
                    if exc.code == 409:
                        return 75  # operator must resolve the competing consumer
                    if args.once:
                        return 1
                    time.sleep(3)
                except Exception as exc:
                    print(f"review consumer failed type={type(exc).__name__}", file=sys.stderr, flush=True)
                    if args.once:
                        return 1
                    time.sleep(3)
                if args.once:
                    return 0
        finally:
            store.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, RuntimeError, KeyError) as exc:
        print(f"review consumer startup failed type={type(exc).__name__}", file=sys.stderr)
        raise SystemExit(1)
