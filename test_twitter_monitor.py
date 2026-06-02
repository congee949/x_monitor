import argparse
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import twitter_monitor


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        current = cls(2026, 5, 12, 0, 30, 0, tzinfo=timezone.utc)
        return current if tz else current.replace(tzinfo=None)


class FakeAI:
    def __init__(self, available=False, summary=None):
        self.available = available
        self.summary = summary

    def is_available(self):
        return self.available

    def complete(self, prompt, max_tokens=1200, temperature=0.2):
        return self.summary, "fake"


class BacklogGuardTest(unittest.TestCase):
    def test_skips_stale_unseen_tweets_after_api_outage(self):
        sent = []
        saved = {}
        tweets = [
            {
                "id": "old-tweet",
                "text": "This is an old tweet accumulated during an API outage.",
                "createdAt": "Mon May 11 15:55:43 +0000 2026",
            },
            {
                "id": "recent-tweet",
                "text": "This recent tweet should still be pushed normally.",
                "createdAt": "Tue May 12 00:20:00 +0000 2026",
            },
        ]
        args = argparse.Namespace(
            test=False,
            seed=False,
            dry_run=False,
            limit=20,
            max_push_age_minutes=45,
        )

        def fake_save_seen(username, seen, last_post_ts=None):
            saved["seen"] = seen
            saved["last_post_ts"] = last_post_ts

        def fake_send_telegram(token, chat_id, text, link=""):
            sent.append(link)
            return {"ok": True}

        with patch.object(twitter_monitor, "datetime", FixedDatetime):
            with patch.object(twitter_monitor, "fetch_tweets", return_value=tweets):
                with patch.object(twitter_monitor, "load_seen", return_value=({"already-seen"}, "2026-05-11T15:00:00+00:00")):
                    with patch.object(twitter_monitor, "save_seen", side_effect=fake_save_seen):
                        with patch.object(twitter_monitor, "send_telegram", side_effect=fake_send_telegram):
                            with patch.object(twitter_monitor.time, "sleep", return_value=None):
                                new_count, push_count, filter_count, ai_overridden = twitter_monitor.process_user(
                                    pool=None,
                                    ai=FakeAI(False),
                                    username="vista8",
                                    bot_token="bot-token",
                                    chat_id="chat-id",
                                    args=args,
                                )

        self.assertEqual(new_count, 2)
        self.assertEqual(push_count, 1)
        self.assertEqual(filter_count, 0)
        self.assertEqual(ai_overridden, 0)
        self.assertEqual(sent, ["https://x.com/vista8/status/recent-tweet"])
        self.assertEqual(saved["seen"], {"already-seen", "old-tweet", "recent-tweet"})


class AccountIsolationTest(unittest.TestCase):
    def test_main_continues_when_one_account_fetch_fails(self):
        processed = []
        config_path = Path("test_config.json")
        config_path.write_text('{"telegram_bot_token": "bot", "telegram_chat_id": "chat"}')

        def fake_process_user(pool, ai, username, bot_token, chat_id, args):
            processed.append(username)
            if username == "broken":
                raise RuntimeError("api payment required")
            return 1, 1, 0, 0

        try:
            with patch.object(twitter_monitor, "CONFIG_PATH", str(config_path)):
                with patch.object(twitter_monitor.TokenPool, "load", return_value=None):
                    with patch.object(twitter_monitor.AIClassifier, "load", return_value=FakeAI(False)):
                        with patch.object(twitter_monitor, "load_accounts", return_value=[{"username": "ok1"}, {"username": "broken"}, {"username": "ok2"}]):
                            with patch.object(twitter_monitor, "process_user", side_effect=fake_process_user):
                                with patch.object(sys, "argv", ["twitter_monitor.py"]):
                                    result = twitter_monitor.main()
        finally:
            config_path.unlink(missing_ok=True)

        self.assertEqual(result, 0)
        self.assertEqual(processed, ["ok1", "broken", "ok2"])


class ArticleFormattingTest(unittest.TestCase):
    def test_article_fetch_url_prefers_tweet_url(self):
        entry = {"article_id": "2057247064115838976", "tweet_id": "2057250417638035555"}
        self.assertEqual(twitter_monitor.article_fetch_url("dotey", entry), "https://x.com/dotey/status/2057250417638035555")

    def test_article_fetch_url_falls_back_to_article_url(self):
        entry = {"article_id": "2057247064115838976"}
        self.assertEqual(twitter_monitor.article_fetch_url("dotey", entry), "https://x.com/i/article/2057247064115838976")

    def test_markdown_to_telegram_html_renders_basic_markdown(self):
        rendered = twitter_monitor.markdown_to_telegram_html("**结论**\n- [链接文本](https://example.com)\n1. `code`")
        self.assertIn("<b>结论</b>", rendered)
        self.assertIn("• 链接文本", rendered)
        self.assertIn("<code>code</code>", rendered)
        self.assertNotIn("https://example.com", rendered)

    def test_article_summary_message_hides_link_and_renders_markdown(self):
        msg, link = twitter_monitor.format_article_summary_message(
            "dotey",
            {"article_id": "2057247064115838976", "article_title": "测试标题"},
            "**一句话结论**\n- 要点 [原文](https://x.com/i/article/2057247064115838976)",
        )
        self.assertEqual(link, "")
        self.assertNotIn("链接：", msg)
        self.assertNotIn("https://x.com", msg)
        self.assertIn("<b>一句话结论</b>", msg)
        self.assertIn("• 要点 原文", msg)


class MessageFormattingTest(unittest.TestCase):
    def test_format_message_includes_normal_tweet_preview(self):
        msg, link = twitter_monitor.format_message(
            "vista8",
            {"id": "1", "text": "这是一条普通推文，用来验证 iOS 通知栏里能直接看到内容，而不是只看到账号名。"},
        )
        self.assertEqual(link, "https://x.com/vista8/status/1")
        self.assertIn("📢 @vista8", msg)
        self.assertIn("这是一条普通推文", msg)

    def test_format_message_uses_tldr_for_note_tweet(self):
        long_text = "Stack Overflow 因为大家都用 AI 导致发帖量下降，但公司靠企业知识库和数据授权收入增长。" * 20
        msg, link = twitter_monitor.format_message(
            "dotey",
            {"id": "2", "text": long_text[:200], "note_tweet": {"text": long_text}},
            FakeAI(True, "Stack Overflow 社区提问减少，但公司靠企业知识库和数据授权从 AI 浪潮中赚钱。"),
        )
        self.assertEqual(link, "https://x.com/dotey/status/2")
        self.assertIn("TL;DR：Stack Overflow 社区提问减少", msg)
        self.assertLess(len(msg), 300)

    def test_format_message_falls_back_to_short_preview_when_tldr_unavailable(self):
        long_text = "Agent 应用和传统 App + AI 的最大差别，在于执行的主体不同。" * 20
        msg, _ = twitter_monitor.format_message(
            "dotey",
            {"id": "3", "text": long_text[:200], "note_tweet": {"text": long_text}},
            FakeAI(False),
        )
        self.assertNotIn("TL;DR", msg)
        self.assertIn("Agent 应用和传统 App", msg)
        self.assertLess(len(msg), 260)

    def test_format_message_falls_back_when_tldr_quality_is_bad(self):
        long_text = "Stack Overflow 因为大家都用 AI 导致发帖量下降，但公司靠企业知识库和数据授权收入增长。" * 20
        msg, _ = twitter_monitor.format_message(
            "dotey",
            {"id": "5", "text": long_text[:200], "note_tweet": {"text": long_text}},
            FakeAI(True, "* However, Stack Overflow&#x27;"),
        )
        self.assertNotIn("TL;DR", msg)
        self.assertIn("Stack Overflow 因为大家都用 AI", msg)
        self.assertLess(len(msg), 260)

    def test_format_message_article_keeps_short_article_hint(self):
        msg, link = twitter_monitor.format_message(
            "dotey",
            {
                "id": "4",
                "text": "https://t.co/example",
                "article": {
                    "title": "DeepSeek 的 10 万亿美元大战略【译】",
                    "preview_text": "作者讨论 DeepSeek 如何通过模型能力、生态和低成本推理建立长期战略优势。",
                },
            },
        )
        self.assertEqual(link, "https://x.com/dotey/status/4")
        self.assertIn("X Article：DeepSeek 的 10 万亿美元大战略【译】", msg)
        self.assertIn("作者讨论 DeepSeek", msg)


class LatentFixRegressionTest(unittest.TestCase):
    """Regression guards for the 2026-06-03 latent-bug fixes."""

    def test_esc1_tldr_not_double_escaped(self):
        # ESC-1: a TL;DR containing '&' must render as a single &amp;, not &amp;amp;.
        ai = FakeAI(available=True,
                    summary="这是一个包含 A & B 与符号的中文摘要内容长度足够通过质量检查测试")
        msg, _ = twitter_monitor.format_message("dotey", {"id": "1", "note_tweet": {"text": "x" * 80}}, ai)
        self.assertNotIn("&amp;amp;", msg)
        self.assertIn("&amp;", msg)

    def test_pre1_code_fence_renders_real_pre(self):
        # PRE-1: fenced code must produce real <pre>, not literal &lt;pre&gt;.
        out = twitter_monitor.markdown_to_telegram_html("看代码：\n```python\nprint('a < b & c')\n```\n完")
        self.assertIn("<pre>", out)
        self.assertNotIn("&lt;pre&gt;", out)
        self.assertIn("&lt; b &amp; c", out)  # content escaped once, inside <pre>

    def test_split1_balances_inline_tags_across_chunks(self):
        # SPLIT-1: a <b> spanning a chunk boundary must be closed/reopened.
        chunks = twitter_monitor._balance_html_chunks(["前段 <b>加粗开始", "加粗结束</b> 后段"])
        self.assertTrue(chunks[0].endswith("</b>"))
        self.assertTrue(chunks[1].startswith("<b>"))

    def test_cat4_unwraps_tweet_with_visibility_results(self):
        # CAT4: TweetWithVisibilityResults-wrapped tweets must not be dropped.
        import json as _json
        import twitter_graphql as tg
        legacy = lambda i, t: {"id_str": i, "full_text": t, "created_at": "Mon Jun 02 10:00:00 +0000 2026"}
        synthetic = {"data": {"user": {"result": {"timeline_v2": {"timeline": {"instructions": [
            {"entries": [
                {"content": {"itemContent": {"tweet_results": {"result": {
                    "__typename": "Tweet", "legacy": legacy("100", "plain")}}}}},
                {"content": {"itemContent": {"tweet_results": {"result": {
                    "__typename": "TweetWithVisibilityResults",
                    "tweet": {"legacy": legacy("200", "wrapped")}}}}}},
            ]}]}}}}}}
        with patch.object(tg, "_auth_headers", lambda: None), \
             patch.object(tg, "_get_guest_token", lambda: "gt"), \
             patch.object(tg, "get_user_id", lambda u: "123"), \
             patch.object(tg, "_curl", lambda *a, **k: _json.dumps(synthetic)):
            ids = [t["id"] for t in tg.fetch_tweets("dotey", limit=20)]
        self.assertIn("100", ids)
        self.assertIn("200", ids)


if __name__ == "__main__":
    unittest.main()
