#!/bin/sh
# Best-effort append-ledger replica: BWG X Monitor -> r4s Hermes input.
# The caller deliberately ignores this script's rc; this script itself never
# edits twitter_seen or chat-daily's authoritative media_sent_ledger.jsonl.
set -eu

SOURCE=/root/x_monitor/state/x_monitor_sent_content_ledger.jsonl
REMOTE_HOST=r4s
REMOTE_PATH=/root/chat-daily/state/x_monitor_sent_content_ledger.jsonl
SSH_OPTS="-o BatchMode=yes -o ClearAllForwardings=yes -o ConnectTimeout=12 -o ServerAliveInterval=10 -o ServerAliveCountMax=2"
REMOTE_TMP="${REMOTE_PATH}.tmp.x-monitor.$$"

validate_jsonl() {
    /usr/bin/python3 - "$1" <<'PY'
import datetime
import hashlib
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
required = {
    "schema", "chat_id", "thread_id", "message_id", "producer",
    "source_kind", "source_ref", "source_message_ids", "url", "content",
    "content_hash", "delivery_state", "sent_at", "content_id",
}
rows = []
with path.open("r", encoding="utf-8") as fh:
    for number, raw in enumerate(fh, 1):
        if not raw.endswith("\n"):
            raise SystemExit(f"{path}: line {number}: missing newline")
        try:
            row = json.loads(raw)
        except Exception as exc:
            raise SystemExit(f"{path}: line {number}: invalid JSON: {exc}")
        extra = set(row) - required if isinstance(row, dict) else set()
        if (not isinstance(row, dict) or not required.issubset(row)
                or extra - {"links"}):
            raise SystemExit(f"{path}: line {number}: schema fields mismatch")
        if "links" in row:
            links = row["links"]
            if (not isinstance(links, list) or len(links) > 20
                    or len(set(links)) != len(links)
                    or any(not isinstance(v, str) or not v or len(v) > 2048
                           or not (v.startswith(("https://", "http://"))
                                   or v.startswith("x.com/"))
                           for v in links)):
                raise SystemExit(f"{path}: line {number}: invalid links")
        if row["schema"] != "sent-content.v1" or row["producer"] != "x_monitor":
            raise SystemExit(f"{path}: line {number}: wrong schema/producer")
        if row["source_kind"] not in {"x_tweet", "x_article"}:
            raise SystemExit(f"{path}: line {number}: wrong source_kind")
        if row["delivery_state"] != "confirmed":
            raise SystemExit(f"{path}: line {number}: non-confirmed delivery")
        if (isinstance(row["chat_id"], bool) or not isinstance(row["chat_id"], int)
                or isinstance(row["message_id"], bool) or not isinstance(row["message_id"], int)
                or row["message_id"] <= 0):
            raise SystemExit(f"{path}: line {number}: invalid Telegram ids")
        if row["thread_id"] is not None and (
                isinstance(row["thread_id"], bool) or not isinstance(row["thread_id"], int)
                or row["thread_id"] <= 0):
            raise SystemExit(f"{path}: line {number}: invalid thread_id")
        source_ids = row["source_message_ids"]
        if (not isinstance(source_ids, list) or not source_ids
                or any(isinstance(v, bool) or not isinstance(v, int) or v <= 0
                       for v in source_ids)):
            raise SystemExit(f"{path}: line {number}: invalid source_message_ids")
        for field in ("source_ref", "url"):
            value = row[field]
            if (not isinstance(value, str)
                    or not value.startswith(("https://x.com/", "https://twitter.com/"))):
                raise SystemExit(f"{path}: line {number}: invalid {field}")
        if (not isinstance(row["content"], str) or not row["content"]
                or len(row["content"]) > 12000):
            raise SystemExit(f"{path}: line {number}: invalid content length")
        digest = hashlib.sha256(row["content"].encode("utf-8")).hexdigest()
        if row["content_hash"] != digest:
            raise SystemExit(f"{path}: line {number}: content_hash mismatch")
        if not isinstance(row["content_id"], str) or not row["content_id"]:
            raise SystemExit(f"{path}: line {number}: invalid content_id")
        try:
            datetime.datetime.fromisoformat(row["sent_at"])
        except Exception:
            raise SystemExit(f"{path}: line {number}: invalid sent_at")
        rows.append(row)
print(len(rows))
PY
}

if [ ! -f "$SOURCE" ]; then
    echo "sent-content sync skip: source missing" >&2
    exit 0
fi

source_rows=$(validate_jsonl "$SOURCE")

# scp writes only a unique temporary name; validation and rename happen on r4s.
# ClearAllForwardings prevents an unrelated configured reverse-forward failure
# from confusing this one-shot ledger transport.
if ! /usr/bin/scp $SSH_OPTS -- "$SOURCE" "${REMOTE_HOST}:${REMOTE_TMP}"; then
    /usr/bin/ssh $SSH_OPTS "$REMOTE_HOST" "rm -f '$REMOTE_TMP'" >/dev/null 2>&1 || true
    echo "sent-content sync failed: scp" >&2
    exit 1
fi

if ! /usr/bin/ssh $SSH_OPTS "$REMOTE_HOST" /bin/sh -s -- "$REMOTE_TMP" "$REMOTE_PATH" <<'REMOTE'
set -eu
tmp=$1
dest=$2
cleanup() { rm -f "$tmp"; }
trap cleanup EXIT HUP INT TERM

/usr/bin/python3 - "$tmp" "$dest" <<'PY'
import datetime
import hashlib
import json
import pathlib
import sys

tmp, dest = map(pathlib.Path, sys.argv[1:])
required = {
    "schema", "chat_id", "thread_id", "message_id", "producer",
    "source_kind", "source_ref", "source_message_ids", "url", "content",
    "content_hash", "delivery_state", "sent_at", "content_id",
}

def load(path):
    rows = []
    with path.open("r", encoding="utf-8") as fh:
        for number, raw in enumerate(fh, 1):
            if not raw.endswith("\n"):
                raise SystemExit(f"{path}: line {number}: missing newline")
            try:
                row = json.loads(raw)
            except Exception as exc:
                raise SystemExit(f"{path}: line {number}: invalid JSON: {exc}")
            extra = set(row) - required if isinstance(row, dict) else set()
            if (not isinstance(row, dict) or not required.issubset(row)
                    or extra - {"links"}):
                raise SystemExit(f"{path}: line {number}: schema fields mismatch")
            if "links" in row:
                links = row["links"]
                if (not isinstance(links, list) or len(links) > 20
                        or len(set(links)) != len(links)
                        or any(not isinstance(v, str) or not v or len(v) > 2048
                               or not (v.startswith(("https://", "http://"))
                                       or v.startswith("x.com/"))
                               for v in links)):
                    raise SystemExit(f"{path}: line {number}: invalid links")
            if (row["schema"] != "sent-content.v1" or row["producer"] != "x_monitor"
                    or row["source_kind"] not in {"x_tweet", "x_article"}
                    or row["delivery_state"] != "confirmed"):
                raise SystemExit(f"{path}: line {number}: invalid constants")
            if (isinstance(row["chat_id"], bool) or not isinstance(row["chat_id"], int)
                    or isinstance(row["message_id"], bool) or not isinstance(row["message_id"], int)
                    or row["message_id"] <= 0):
                raise SystemExit(f"{path}: line {number}: invalid Telegram ids")
            if row["thread_id"] is not None and (
                    isinstance(row["thread_id"], bool) or not isinstance(row["thread_id"], int)
                    or row["thread_id"] <= 0):
                raise SystemExit(f"{path}: line {number}: invalid thread_id")
            ids = row["source_message_ids"]
            if (not isinstance(ids, list) or not ids
                    or any(isinstance(v, bool) or not isinstance(v, int) or v <= 0 for v in ids)):
                raise SystemExit(f"{path}: line {number}: invalid source ids")
            if any(not isinstance(row[field], str)
                   or not row[field].startswith(("https://x.com/", "https://twitter.com/"))
                   for field in ("source_ref", "url")):
                raise SystemExit(f"{path}: line {number}: invalid refs")
            content = row["content"]
            if not isinstance(content, str) or not content or len(content) > 12000:
                raise SystemExit(f"{path}: line {number}: invalid content")
            if row["content_hash"] != hashlib.sha256(content.encode("utf-8")).hexdigest():
                raise SystemExit(f"{path}: line {number}: hash mismatch")
            if not isinstance(row["content_id"], str) or not row["content_id"]:
                raise SystemExit(f"{path}: line {number}: invalid content_id")
            try:
                datetime.datetime.fromisoformat(row["sent_at"])
            except Exception:
                raise SystemExit(f"{path}: line {number}: invalid sent_at")
            rows.append(row)
    return rows

incoming = load(tmp)
current = load(dest) if dest.exists() else []
if len(incoming) < len(current):
    raise SystemExit(
        f"shrink protection: incoming {len(incoming)} rows < current {len(current)}")
if incoming[:len(current)] != current:
    raise SystemExit("append-only protection: incoming ledger does not preserve current prefix")
print(f"validated incoming={len(incoming)} current={len(current)}")
PY

chmod 600 "$tmp"
mv -f "$tmp" "$dest"
chmod 600 "$dest"
trap - EXIT HUP INT TERM
REMOTE
then
    echo "sent-content sync failed: remote validation/atomic replace" >&2
    exit 1
fi

echo "sent-content sync OK: ${source_rows} rows"
