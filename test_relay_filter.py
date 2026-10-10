import copy
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import relay_filter as rf
import twitter_monitor as tm

NOW = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)

HAIKU_EN = ("Introducing Claude Haiku 5.5: the cheapest, fastest, and most capable small model "
            "we've ever released. On average, it costs around 75% less to run than Claude Haiku 4.5.")
HAIKU_CN = ("Anthropic 发布 Claude Haiku 5.5：小模型价格降到上一代的十分之一。Haiku 是 Claude 家族里"
            "最小、最便宜的一档，排在 Sonnet 和 Opus 之下，主要用来跑量大、对成本敏感的任务。"
            "平均运行成本比 Haiku 4.5 低约 75%。")
OWN_TOOL_CN = ("刚开发了一个 Obsidian 插件，专门用来学习提升审美，也支持一键复制提示词，生成类似网站，"
               "一会提交官方审核，明天应该能在插件库搜到。")


def row(author, tweet_id, content, *, hours_ago=2, links=()):
    return {"schema": "sent-content.v1", "producer": "x_monitor", "delivery_state": "confirmed",
            "source_ref": f"https://x.com/{author}/status/{tweet_id}",
            "source_message_ids": [int(tweet_id)], "message_id": int(tweet_id) % 100000,
            "content_id": f"x-tweet:{tweet_id}", "links": list(links),
            "content": f"📢 @{author}​\n\n{content}",
            "sent_at": (NOW - timedelta(hours=hours_ago)).isoformat()}


def fake_ai(answer):
    ai = Mock()
    ai.is_available.return_value = True
    ai.complete.return_value = (json.dumps(answer, ensure_ascii=False), "fake")
    return ai


class TokenTest(unittest.TestCase):
    def test_versioned_names_and_numbers_are_tokens(self):
        toks = rf.tokens("Claude Haiku 5.5 costs 75% less than GPT-6")
        self.assertIn("v:haiku5.5", toks)
        self.assertIn("n:75%", toks)
        self.assertNotIn("w:claude", toks)

    def test_chinese_post_threshold(self):
        self.assertTrue(rf.is_chinese_post(HAIKU_CN))
        self.assertFalse(rf.is_chinese_post(HAIKU_EN))
        self.assertFalse(rf.is_chinese_post("太强了"))


class RetrieveTest(unittest.TestCase):
    def setUp(self):
        self.rows = [row("claudeai", "2107896388163000001", HAIKU_EN),
                     row("OpenAI", "2107896388163000002",
                         "Ultrafast is available today for GPT-6 Astra in Codex and the API.")]

    def test_cross_language_relay_matches_english_source(self):
        found = rf.retrieve(HAIKU_CN, set(), self.rows, author="dotey")
        self.assertEqual([rf.row_author(c["row"]) for c in found], ["claudeai"])

    def test_direct_reference_always_qualifies(self):
        found = rf.retrieve("引用一下这条，" + OWN_TOOL_CN, {"2107896388163000002"},
                            self.rows, author="dotey")
        self.assertTrue(found[0]["direct"])
        self.assertEqual(rf.row_author(found[0]["row"]), "OpenAI")

    def test_same_author_and_unrelated_posts_are_excluded(self):
        self.assertEqual(rf.retrieve(HAIKU_CN, set(), self.rows, author="claudeai"), [])
        self.assertEqual(rf.retrieve(OWN_TOOL_CN, set(), self.rows, author="vista8"), [])

    def test_load_recent_keeps_confirmed_rows_inside_window(self):
        rows = [row("a", "1000001", "x", hours_ago=1), row("b", "1000002", "y", hours_ago=80),
                dict(row("c", "1000003", "z"), delivery_state="ambiguous")]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ledger.jsonl")
            with open(path, "w", encoding="utf-8") as stream:
                stream.write("not json\n")
                for item in rows:
                    stream.write(json.dumps(item) + "\n")
            self.assertEqual([rf.row_author(r) for r in rf.load_recent(path, now=NOW)], ["a"])


class DecideTest(unittest.TestCase):
    candidates = [{"row": row("claudeai", "1000001", HAIKU_EN)}]

    def verdict(self, **overrides):
        base = {"same_event": True, "matched": 1, "added": "background", "confidence": 0.9}
        base.update(overrides)
        return rf.decide(base, self.candidates)[0]

    def test_mapping(self):
        self.assertEqual(self.verdict(), "drop")
        self.assertEqual(self.verdict(added="none"), "drop")
        self.assertEqual(self.verdict(added="substantive"), "fold")
        self.assertEqual(self.verdict(same_event=False), "keep")

    def test_malformed_or_uncertain_keeps(self):
        self.assertEqual(self.verdict(confidence=0.6), "keep")
        self.assertEqual(self.verdict(confidence="0.9"), "keep")
        self.assertEqual(self.verdict(matched=2), "keep")
        self.assertEqual(self.verdict(matched=True), "keep")
        self.assertEqual(self.verdict(added="other"), "keep")


class EvaluateTest(unittest.TestCase):
    def setUp(self):
        rf.reset_run()
        self.rows = [row("claudeai", "1000001", HAIKU_EN)]

    def test_drop_record(self):
        ai = fake_ai({"same_event": True, "matched": 1, "added": "background",
                      "delta": "", "confidence": 0.93, "reason": "转述发布"})
        record = rf.evaluate(text=HAIKU_CN, quoted_text="", referenced_ids=set(),
                             author="dotey", rows=self.rows, ai=ai)
        self.assertEqual(record["decision"], "drop")
        self.assertEqual(record["matched"]["author"], "claudeai")
        prompt = ai.complete.call_args.args[0]
        self.assertIn(HAIKU_EN[:40], prompt)

    def test_no_candidate_skips_ai(self):
        ai = fake_ai({})
        record = rf.evaluate(text=OWN_TOOL_CN, quoted_text="", referenced_ids=set(),
                             author="vista8", rows=self.rows, ai=ai)
        self.assertEqual(record["reason"], "no_candidate")
        ai.complete.assert_not_called()

    def test_ai_failure_and_budget_keep(self):
        ai = Mock()
        ai.is_available.return_value = True
        ai.complete.return_value = ("not json", "fake")
        record = rf.evaluate(text=HAIKU_CN, quoted_text="", referenced_ids=set(),
                             author="dotey", rows=self.rows, ai=ai)
        self.assertEqual((record["decision"], record["reason"]), ("keep", "ai_failed:JSONDecodeError"))
        with patch.object(rf, "MAX_AI_CALLS_PER_RUN", 1):
            record = rf.evaluate(text=HAIKU_CN, quoted_text="", referenced_ids=set(),
                                 author="dotey", rows=self.rows, ai=ai)
        self.assertEqual(record["reason"], "ai_budget")


class MonitorObserveTest(unittest.TestCase):
    def test_observe_writes_audit_without_touching_tweet(self):
        rf.reset_run()
        tweet = {"id": "2107896388163999999", "text": HAIKU_CN, "entities": {}}
        before = copy.deepcopy(tweet)
        ai = fake_ai({"same_event": True, "matched": 1, "added": "none",
                      "delta": "", "confidence": 0.97, "reason": "翻译"})
        with tempfile.TemporaryDirectory() as tmp:
            ledger = os.path.join(tmp, "ledger.jsonl")
            audit = os.path.join(tmp, "state", "relay-observe.jsonl")
            fresh = dict(row("claudeai", "1000001", HAIKU_EN),
                         sent_at=datetime.now(timezone.utc).isoformat())
            with open(ledger, "w", encoding="utf-8") as stream:
                stream.write(json.dumps(fresh) + "\n")
            with patch.object(tm, "SENT_CONTENT_LEDGER_PATH", ledger), \
                    patch.object(tm, "RELAY_OBSERVE_PATH", audit):
                record = tm._relay_observe(tweet, "dotey", ai)
            with open(audit, encoding="utf-8") as stream:
                saved = [json.loads(line) for line in stream]
        self.assertEqual(tweet, before)
        self.assertEqual(record["decision"], "drop")
        self.assertEqual(saved[0]["tweet_id"], "2107896388163999999")
        self.assertEqual(saved[0]["url"], "https://x.com/dotey/status/2107896388163999999")
        self.assertEqual(saved[0]["mode"], "observe")

    def test_ineligible_post_is_not_recorded(self):
        with tempfile.TemporaryDirectory() as tmp:
            audit = os.path.join(tmp, "relay-observe.jsonl")
            with patch.object(tm, "RELAY_OBSERVE_PATH", audit):
                record = tm._relay_observe({"id": "1", "text": HAIKU_EN}, "claudeai", fake_ai({}))
            self.assertEqual(record["reason"], "not_eligible")
            self.assertFalse(os.path.exists(audit))

    def test_mode_normalization(self):
        self.assertEqual(rf.normalize_mode("Observe"), "observe")
        self.assertEqual(rf.normalize_mode("enforce"), "off")
        self.assertEqual(rf.normalize_mode(None), "off")


if __name__ == "__main__":
    unittest.main()


class ShortCommentTest(unittest.TestCase):
    def setUp(self):
        rf.reset_run()
        self.rows = [row("thsottiaux", "2108000000000000001", "Codex resets for everyone tonight.")]

    def test_short_reaction_to_delivered_post_drops_without_ai(self):
        ai = fake_ai({})
        record = rf.evaluate(text="这是一道送分题 x.com/thsottiaux/status/2108000000000000001",
                             quoted_text="Codex resets for everyone tonight.",
                             referenced_ids={"2108000000000000001"}, author="dotey",
                             rows=self.rows, ai=ai)
        self.assertEqual((record["decision"], record["reason"]),
                         ("drop", "short_comment_on_delivered"))
        self.assertEqual(record["matched"]["author"], "thsottiaux")
        ai.complete.assert_not_called()

    def test_short_reaction_to_new_post_is_not_recorded(self):
        record = rf.evaluate(text="这条也挺酷", quoted_text="A new demo",
                             referenced_ids={"2109000000000000009"}, author="dotey",
                             rows=self.rows, ai=fake_ai({}))
        self.assertEqual(record["reason"], "not_eligible")

    def test_links_do_not_count_toward_length(self):
        self.assertTrue(rf.short_comment("开源的：https://x.com/blended_jpeg/status/123456789"))
        self.assertFalse(rf.short_comment("　 "))
        self.assertFalse(rf.short_comment("这是一段明显超过四十个字的评论，" * 3))
