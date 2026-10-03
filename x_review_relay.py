#!/usr/bin/env python3
"""Consume an owner-filtered R4S outbox without calling Telegram getUpdates."""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time

from x_review_bot import BotError, ReviewBot, ReviewStore, Telegram, consumer_lock, json_text, positive_id

ROOT = Path(__file__).resolve().parent


def fetch_updates(host, script, source_state, owner, after, runner=subprocess.run):
    if not re.fullmatch(r"(?:[A-Za-z0-9_][A-Za-z0-9_.-]*@)?[A-Za-z0-9][A-Za-z0-9_.-]*", host):
        raise ValueError("invalid source host")
    for path in (script, source_state):
        if not path.startswith("/") or any(ord(c) < 32 for c in path):
            raise ValueError("invalid absolute source path")
    command = shlex.join(["python3", script, "--state", source_state, "export",
                         "--after", str(max(0, after)), "--owner", str(positive_id(owner)),
                         "--limit", "100"])
    result = runner(["ssh", "-o", "BatchMode=yes", "-o", "ClearAllForwardings=yes",
                     "-o", "ConnectTimeout=8", host, command],
                    capture_output=True, text=True, check=True, timeout=20)
    if len(result.stdout) > 8_000_000:
        raise ValueError("source batch too large")
    rows = json.loads(result.stdout)
    if not isinstance(rows, list) or len(rows) > 100:
        raise ValueError("invalid source batch")
    previous = after
    for row in rows:
        if not isinstance(row, dict) or type(row.get("update_id")) is not int or row["update_id"] <= previous:
            raise ValueError("source updates must be ordered and newer than cursor")
        previous = row["update_id"]
    return rows


def ingest(bot, updates):
    # Check the complete batch before writes. Identical updates replay safely;
    # a changed payload under an existing id is a source integrity failure.
    for update in updates:
        old = bot.store.db.execute("SELECT payload_json FROM updates WHERE update_id=?",
                                   (positive_id(update["update_id"]),)).fetchone()
        if old and json.loads(old[0]) != update:
            raise ValueError("conflicting update payload")
    for update in updates:
        bot.persist(update)
    pending = bot.store.db.execute(
        "SELECT payload_json FROM updates WHERE processed=0 ORDER BY update_id").fetchall()
    for row in pending:
        bot.handle(json.loads(row[0]))
    return len(pending)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config.json")
    parser.add_argument("--state", type=Path, default=ROOT / "state/x-review.sqlite3")
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, default=ROOT / "twitter_seen/.event_ledger.sqlite3")
    parser.add_argument("--host", default="r4s")
    parser.add_argument("--source-script", default="/opt/r4sbot/x_review_bridge.py")
    parser.add_argument("--source-state", default="/opt/r4sbot/state/x-review-outbox.sqlite3")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    os.umask(0o077)
    config = json.loads(args.config.read_text())
    owner = positive_id(config["telegram_chat_id"])
    with consumer_lock(args.state):
        store = ReviewStore(args.state)
        try:
            bot = ReviewBot(Telegram(config["telegram_bot_token"]), store,
                            json.loads(args.packet.read_text()), json.loads(args.receipt.read_text()),
                            owner, args.ledger)
            while True:
                try:
                    # Finish durable local work even if the source is offline.
                    ingest(bot, [])
                    updates = fetch_updates(args.host, args.source_script, args.source_state,
                                            owner, max(0, store.offset() - 1))
                    count = ingest(bot, updates)
                    if count:
                        print(f"review relay processed={count} next_offset={store.offset()}", flush=True)
                except Exception as exc:
                    print(f"review relay failed type={type(exc).__name__}", file=sys.stderr, flush=True)
                    if args.once:
                        return 1
                    time.sleep(3)
                if args.once:
                    return 0
                time.sleep(2)
        finally:
            store.close()


if __name__ == "__main__":
    raise SystemExit(main())
