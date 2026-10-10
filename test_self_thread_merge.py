import argparse
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import thread_merge
import twitter_monitor as tm
from test_twitter_monitor import FakeAI

STAMP = datetime.now(timezone.utc).isoformat()


def post(tid, text, *, parent=None, author="vista8", conversation="100", **extra):
    tweet = {"id": tid, "text": text, "created_at": STAMP, "conversation_id_str": conversation}
    if parent:
        tweet["in_reply_to_status"] = {"id": parent, "screen_name": author}
    tweet.update(extra)
    return tweet


PHOTO = [{"type": "photo", "url": "https://pbs.twimg.com/media/demo.jpg"}]


class MergeSelfRepliesTest(unittest.TestCase):
    def merge(self, *tweets, enabled=True, username="vista8"):
        return thread_merge.merge_self_replies([(t, "r") for t in tweets], username, enabled=enabled)

    def test_text_follow_ups_join_a_media_parent(self):
        result = self.merge(post("100", "开发了一个 Chrome 插件，演示见视频。开源地址见评论区", media=PHOTO),
                            post("101", "开源地址：github.com/joeseesun/qiao", parent="100"),
                            post("102", "补充：也支持 B 站", parent="101"))
        self.assertEqual(len(result), 1)
        merged = result[0][0]
        self.assertEqual([t["id"] for t in merged["_official_thread_members"]], ["100", "101", "102"])
        self.assertIn("开源地址：github.com", merged["note_tweet"]["text"])
        self.assertEqual(merged["media"], PHOTO)
        self.assertEqual(merged["id"], "100")

    def test_follow_up_with_its_own_content_keeps_its_card(self):
        for extra in ({"media": PHOTO}, {"quoted_status": {"id": "9", "screen_name": "x"}}):
            with self.subTest(extra=extra):
                self.assertEqual(len(self.merge(post("100", "主帖"),
                                                post("101", "补充", parent="100", **extra))), 2)

    def test_quote_parent_and_foreign_replies_are_not_merged(self):
        self.assertEqual(len(self.merge(post("100", "主帖", quoted_status={"id": "9"}),
                                        post("101", "补充", parent="100"))), 2)
        self.assertEqual(len(self.merge(post("100", "主帖"),
                                        post("101", "回复别人", parent="100", author="other"))), 2)
        self.assertEqual(len(self.merge(post("100", "主帖"),
                                        post("101", "另一串", parent="99", conversation="99"))), 2)

    def test_disabled_and_length_limit(self):
        items = (post("100", "主帖"), post("101", "补充", parent="100"))
        self.assertEqual(len(self.merge(*items, enabled=False)), 2)
        long_items = (post("100", "长" * 3000), post("101", "长" * 1500, parent="100"))
        self.assertEqual(len(self.merge(*long_items)), 2)


class CuratorDeliveryTest(unittest.TestCase):
    def process(self, tweets, *, outcome=None):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            stack.enter_context(patch.object(tm, "SEEN_DIR", directory))
            stack.enter_context(patch.object(tm, "EVENT_LEDGER_PATH", str(Path(directory) / "ledger.db")))
            stack.enter_context(patch.object(tm, "SEMANTIC_DECISION_JOURNAL", str(Path(directory) / "journal")))
            stack.enter_context(patch.object(tm, "_ACCOUNT_CONFIG_BY_USERNAME", {"vista8": {}}))
            stack.enter_context(patch.object(tm, "_SELF_THREAD_MERGE_ENABLED", True))
            stack.enter_context(patch.object(tm, "_SENT_CONTENT_LEDGER_ENABLED", False))
            stack.enter_context(patch.object(tm, "_SEMANTIC_BUNDLE_ENABLED", False))
            stack.enter_context(patch.object(tm, "_SEMANTIC_BUNDLE_SHADOW", False))
            stack.enter_context(patch.object(tm, "_EVENT_DEDUP_EFFECTIVE_MODE", "off"))
            stack.enter_context(patch.object(tm, "_CROSS_DEDUP_ENABLED", False))
            stack.enter_context(patch.object(tm, "fetch_tweets", return_value=tweets))
            stack.enter_context(patch.object(tm, "classify", return_value=("pass", "test")))
            stack.enter_context(patch.object(tm, "_verify_configured_account_identity"))
            stack.enter_context(patch.object(tm.time, "sleep"))
            stack.enter_context(patch.object(tm, "_article_queue_time_remaining", return_value=10000))
            stack.enter_context(patch.object(tm, "lookup_tweet_anchor", return_value=None))
            stack.enter_context(patch.object(tm, "record_tweet_anchor"))
            tm.save_seen("vista8", {"old"})
            send = stack.enter_context(patch.object(
                tm, "send_tweet", return_value=outcome or {"ok": True, "result": {"message_id": 50}}))
            tm.process_user(None, FakeAI(False), "vista8", "test", "group",
                            argparse.Namespace(test=False, seed=False, dry_run=False, limit=20,
                                               max_push_age_minutes=60))
            return send.call_args_list, tm.load_seen("vista8")[0], tm.load_push_retry("vista8")

    def thread(self):
        return [post("101", "开源地址：github.com/joeseesun/qiao", parent="100"),
                post("100", "开发了一个 Chrome 插件。开源地址见评论区")]

    def test_one_card_and_all_members_seen(self):
        calls, seen, retry = self.process(self.thread())
        self.assertEqual(len(calls), 1)
        self.assertIn("开源地址：github.com", calls[0].args[3]["note_tweet"]["text"])
        self.assertTrue({"100", "101"} <= seen)
        self.assertEqual(retry, set())

    def test_failed_card_keeps_every_member_retryable(self):
        calls, seen, retry = self.process(self.thread(), outcome={"ok": False})
        self.assertEqual(len(calls), 1)
        self.assertFalse({"100", "101"} & seen)
        self.assertEqual(retry, {"100", "101"})


if __name__ == "__main__":
    unittest.main()
