#!/usr/bin/env python3
"""Read-only backfill of retained sent X Articles into the learning feed.

Article queues are never modified.  The script re-fetches Markdown using the
existing X Article command and publishes only items accepted by learning_feed.
"""

from __future__ import annotations

import glob
import json
import os

import learning_feed
import twitter_monitor as monitor


def main() -> int:
    published = skipped = failed = 0
    feed = learning_feed.ensure_feed()
    if not feed.get("ready"):
        print(f"feed initialization failed: {feed.get('reason')}")
        return 2
    for queue_path in sorted(glob.glob(os.path.join(monitor.ARTICLE_QUEUE_DIR, "*_queue.json"))):
        username = os.path.basename(queue_path).removesuffix("_queue.json")
        try:
            with open(queue_path, encoding="utf-8") as handle:
                queue = json.load(handle)
        except Exception as exc:
            print(f"{username}: queue read failed: {type(exc).__name__}")
            failed += 1
            continue
        for entry in queue:
            if entry.get("status") != "sent":
                continue
            markdown, error = monitor.fetch_article_markdown(username, entry)
            if error or not markdown:
                print(f"{entry.get('article_id')}: fetch skipped ({error or 'empty'})")
                failed += 1
                continue
            result = learning_feed.publish_article(username, entry, markdown)
            if result.get("published"):
                print(
                    f"{entry.get('article_id')}: published "
                    f"({result.get('word_count')} words, score {result.get('score')})"
                )
                published += 1
            else:
                print(f"{entry.get('article_id')}: skipped ({result.get('reason')})")
                skipped += 1
    print(f"backfill complete: published={published} skipped={skipped} failed={failed}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
