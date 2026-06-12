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
        failures_path = Path("test_failures.json")

        def fake_process_user(pool, ai, username, bot_token, chat_id, args):
            processed.append(username)
            if username == "broken":
                raise RuntimeError("api payment required")
            return 1, 1, 0, 0

        try:
            with patch.object(twitter_monitor, "CONFIG_PATH", str(config_path)):
                with patch.object(twitter_monitor, "FAILURES_PATH", str(failures_path)):
                    with patch.object(twitter_monitor.TokenPool, "load", return_value=None):
                        with patch.object(twitter_monitor.AIClassifier, "load", return_value=FakeAI(False)):
                            with patch.object(twitter_monitor, "load_accounts", return_value=[{"username": "ok1"}, {"username": "broken"}, {"username": "ok2"}]):
                                with patch.object(twitter_monitor, "process_user", side_effect=fake_process_user):
                                    with patch.object(sys, "argv", ["twitter_monitor.py"]):
                                        result = twitter_monitor.main()
        finally:
            config_path.unlink(missing_ok=True)
            failures_path.unlink(missing_ok=True)

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


class ArticleQueuePruneTest(unittest.TestCase):
    """_article_entry_expired：只清理超过保留期的 sent / 终态 failed 条目。"""

    NOW = datetime(2026, 6, 10, 0, 0, 0, tzinfo=timezone.utc)

    def test_old_sent_and_terminal_failed_expire(self):
        old_sent = {"status": "sent", "updated_at": "2026-06-01T00:00:00+00:00"}
        old_failed_terminal = {"status": "failed", "attempts": 3,
                               "updated_at": "2026-06-01T00:00:00+00:00"}
        self.assertTrue(twitter_monitor._article_entry_expired(old_sent, now=self.NOW))
        self.assertTrue(twitter_monitor._article_entry_expired(old_failed_terminal, now=self.NOW))

    def test_fresh_retryable_pending_and_unstamped_are_kept(self):
        fresh_sent = {"status": "sent", "updated_at": "2026-06-08T00:00:00+00:00"}
        old_failed_retryable = {"status": "failed", "attempts": 1,
                                "updated_at": "2026-06-01T00:00:00+00:00"}
        old_pending = {"status": "pending", "updated_at": "2026-06-01T00:00:00+00:00"}
        sent_no_ts = {"status": "sent"}
        self.assertFalse(twitter_monitor._article_entry_expired(fresh_sent, now=self.NOW))
        self.assertFalse(twitter_monitor._article_entry_expired(old_failed_retryable, now=self.NOW))
        self.assertFalse(twitter_monitor._article_entry_expired(old_pending, now=self.NOW))
        self.assertFalse(twitter_monitor._article_entry_expired(sent_no_ts, now=self.NOW))


class FailureAlertTest(unittest.TestCase):
    """账号连续失败达到阈值只告警一次，成功后清零。"""

    def test_alert_fires_once_at_threshold_and_resets_on_success(self):
        sent = []

        def fake_send_telegram(token, chat_id, text, link=""):
            sent.append(text)
            return {"ok": True}

        failures = {}
        with patch.object(twitter_monitor, "send_telegram", side_effect=fake_send_telegram):
            for _ in range(twitter_monitor.FAIL_ALERT_THRESHOLD - 1):
                twitter_monitor.note_account_failure(failures, "ghost", "Cannot find user", "bot", "chat")
            self.assertEqual(sent, [])

            twitter_monitor.note_account_failure(failures, "ghost", "Cannot find user", "bot", "chat")
            self.assertEqual(len(sent), 1)
            self.assertIn("@ghost", sent[0])
            self.assertTrue(failures["ghost"]["alerted"])

            twitter_monitor.note_account_failure(failures, "ghost", "Cannot find user", "bot", "chat")
            self.assertEqual(len(sent), 1)  # 不重复告警

        twitter_monitor.note_account_success(failures, "ghost")
        self.assertNotIn("ghost", failures)

    def test_dry_run_does_not_send_or_mark_alerted(self):
        sent = []
        failures = {"ghost": {"count": twitter_monitor.FAIL_ALERT_THRESHOLD - 1, "alerted": False}}
        with patch.object(twitter_monitor, "send_telegram", side_effect=lambda *a, **k: sent.append(a) or {"ok": True}):
            twitter_monitor.note_account_failure(failures, "ghost", "err", "bot", "chat", dry_run=True)
        self.assertEqual(sent, [])
        self.assertFalse(failures["ghost"].get("alerted"))


class ArticleSeedGatingTest(unittest.TestCase):
    """article 只对「新且非 seed」推文入队：seed/auto-seed 不灌历史，已 seen 不重复入队。"""

    def _run(self, seed, seen, tweets):
        calls = []
        args = argparse.Namespace(test=False, seed=seed, dry_run=False,
                                  limit=20, max_push_age_minutes=45)
        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=tweets), \
             patch.object(twitter_monitor, "load_seen", return_value=(seen, None)), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "save_article", side_effect=lambda u, a, t: calls.append(a)), \
             patch.object(twitter_monitor, "send_telegram", return_value={"ok": True}), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            twitter_monitor.process_user(pool=None, ai=FakeAI(False), username="dotey",
                                         bot_token="bot", chat_id="chat", args=args)
        return calls

    ARTICLE_TWEET = {
        "id": "art-tweet",
        "text": "新文章发布了，欢迎阅读 https://x.com/i/article/777000111 全文链接",
        "createdAt": "Tue May 12 00:20:00 +0000 2026",
    }

    def test_normal_new_tweet_enqueues_article(self):
        calls = self._run(seed=False, seen={"some-old-id"}, tweets=[self.ARTICLE_TWEET])
        self.assertEqual(calls, ["777000111"])

    def test_seed_and_auto_seed_do_not_enqueue(self):
        self.assertEqual(self._run(seed=True, seen=set(), tweets=[self.ARTICLE_TWEET]), [])
        self.assertEqual(self._run(seed=False, seen=set(), tweets=[self.ARTICLE_TWEET]), [])

    def test_already_seen_tweet_does_not_reenqueue(self):
        calls = self._run(seed=False, seen={"art-tweet", "other"}, tweets=[self.ARTICLE_TWEET])
        self.assertEqual(calls, [])


class RetweetArticleTest(unittest.TestCase):
    """RT 的 article 必须按原推（原作者 status URL）入队抓取，否则抓到空 {}。"""

    # 模拟归一化之后的 RT 推文（article 取自原推、retweeted_status 已展开）
    RT_TWEET = {
        "id": "2062952690750021934",
        "text": "RT @liuren: https://t.co/oa1PZY0g9C",
        "entities": {"urls": [{"expanded_url": "http://x.com/i/article/2062806260563771392"}]},
        "article": {"title": "测试文章", "preview_text": "预览", "rest_id": "2062806260563771392"},
        "retweeted_status": {"id": "2062808278812520765", "screen_name": "liuren"},
    }

    def test_normalizer_unwraps_rt_article(self):
        import json as _json
        import twitter_graphql as tg
        rt_original = {
            "__typename": "Tweet",
            "legacy": {"id_str": "2062808278812520765", "full_text": "原推正文"},
            "core": {"user_results": {"result": {"legacy": {"screen_name": "liuren"}}}},
            "article": {"article_results": {"result": {
                "title": "测试文章", "preview_text": "预览", "rest_id": "2062806260563771392"}}},
        }
        synthetic = {"data": {"user": {"result": {"timeline_v2": {"timeline": {"instructions": [
            {"entries": [
                {"content": {"itemContent": {"tweet_results": {"result": {
                    "__typename": "Tweet",
                    "legacy": {"id_str": "2062952690750021934",
                               "full_text": "RT @liuren: https://t.co/x",
                               "created_at": "Fri Jun 05 17:30:00 +0000 2026",
                               "retweeted_status_result": {"result": rt_original}}}}}}},
            ]}]}}}}}}
        with patch.object(tg, "_auth_headers", lambda: None), \
             patch.object(tg, "_get_guest_token", lambda: "gt"), \
             patch.object(tg, "get_user_id", lambda u: "123"), \
             patch.object(tg, "_curl", lambda *a, **k: _json.dumps(synthetic)):
            tweets = tg.fetch_tweets("dotey", limit=20)
        self.assertEqual(len(tweets), 1)
        self.assertEqual(tweets[0]["article"]["title"], "测试文章")  # 取到原推 article
        self.assertEqual(tweets[0]["retweeted_status"],
                         {"id": "2062808278812520765", "screen_name": "liuren"})

    def test_save_article_enqueues_original_tweet(self):
        import json as _json
        import os as _os
        import tempfile
        with tempfile.TemporaryDirectory() as d, \
             patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d):
            twitter_monitor.save_article("dotey", "2062806260563771392", self.RT_TWEET)
            with open(_os.path.join(d, "dotey_queue.json")) as f:
                entry = _json.load(f)[0]
        self.assertEqual(entry["tweet_id"], "2062808278812520765")  # 原推，不是转推壳
        self.assertEqual(entry["author"], "liuren")
        self.assertEqual(entry["article_title"], "测试文章")
        self.assertEqual(twitter_monitor.article_fetch_url("dotey", entry),
                         "https://x.com/liuren/status/2062808278812520765")

    def test_legacy_entry_without_author_falls_back_to_username(self):
        entry = {"article_id": "111", "tweet_id": "222"}  # 部署前的旧队列条目
        self.assertEqual(twitter_monitor.article_fetch_url("dotey", entry),
                         "https://x.com/dotey/status/222")

    def test_fetch_article_markdown_passes_original_status_url(self):
        import types
        entry = {"article_id": "2062806260563771392",
                 "tweet_id": "2062808278812520765", "author": "liuren"}
        captured = {}

        def fake_run(cmd, **kw):
            captured["cmd"] = cmd
            return types.SimpleNamespace(returncode=0, stdout="# t\n" + "x" * 300, stderr="")

        with patch.object(twitter_monitor, "load_article_markdown_cmd",
                          return_value="/usr/local/bin/x-article-to-markdown"), \
             patch.object(twitter_monitor.subprocess, "run", side_effect=fake_run):
            md, err = twitter_monitor.fetch_article_markdown("dotey", entry)
        self.assertIsNone(err)
        self.assertEqual(captured["cmd"][-1], "https://x.com/liuren/status/2062808278812520765")


class RichPushTest(unittest.TestCase):
    """sendRichMessage 路径：payload 形状 / 400 回退信号 / 队列 rich-first 与回退。"""

    def test_send_telegram_rich_payload_shape(self):
        captured = {}

        def fake_post(token, payload, method="sendMessage"):
            captured["method"] = method
            captured["payload"] = payload
            return {"ok": True, "result": {"message_id": 1}}

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post):
            r = twitter_monitor.send_telegram_rich("tok", "42", "# 标题\n\n正文", "https://x.com/i/article/1")
        self.assertTrue(r["ok"])
        self.assertEqual(captured["method"], "sendRichMessage")
        self.assertEqual(captured["payload"]["rich_message"],
                         {"markdown": "# 标题\n\n正文", "skip_entity_detection": True})
        self.assertEqual(
            captured["payload"]["reply_markup"]["inline_keyboard"][0][0]["url"],
            "https://x.com/i/article/1")
        self.assertNotIn("parse_mode", captured["payload"])
        self.assertNotIn("text", captured["payload"])

    def test_send_telegram_rich_400_returns_fallback_signal(self):
        import io
        import urllib.error

        def fake_post(token, payload, method="sendMessage"):
            raise urllib.error.HTTPError(
                "url", 400, "Bad Request", {},
                io.BytesIO(b'{"ok":false,"description":"Bad Request: rich message is invalid"}'))

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post):
            r = twitter_monitor.send_telegram_rich("tok", "42", "bad md")
        self.assertFalse(r["ok"])
        self.assertTrue(r["rich_fallback"])
        self.assertEqual(r["error_code"], 400)
        self.assertIn("invalid", r["description"])

    def test_format_article_summary_rich_header(self):
        entry = {"article_id": "999", "article_title": "深度长文"}
        md = twitter_monitor.format_article_summary_rich("dotey", entry, "> 一句话结论\n\n### 第一节\n内容")
        self.assertIn("## \U0001f4c4 深度长文", md)
        self.assertIn("**@dotey**", md)
        self.assertIn("[原文](https://x.com/i/article/999)", md)
        self.assertIn("\n---\n", md)
        self.assertTrue(md.endswith("### 第一节\n内容"))

    def test_format_article_summary_rich_escapes_unsafe_title(self):
        entry = {"article_id": "1", "article_title": "AI [新时代]\n*爆发* #1 <hr>"}
        md = twitter_monitor.format_article_summary_rich("u", entry, "正文")
        first_line = md.splitlines()[0]
        self.assertIn(r"\[新时代\]", first_line)   # 方括号转义
        self.assertIn(r"\*爆发\*", first_line)      # 星号转义
        self.assertIn(r"\<hr\>", first_line)        # HTML 标签转义
        self.assertNotIn("\n*", first_line)          # 换行被压成空格

    def test_fallback_heading_wraps_whole_line_dedup_inner_bold(self):
        out = twitter_monitor.markdown_to_telegram_html("### 核心观点：**AI 优先**")
        self.assertEqual(out, "<b>核心观点：AI 优先</b>")
        out2 = twitter_monitor.markdown_to_telegram_html("段一\n> \n段二")
        self.assertEqual(out2, "段一\n\n段二")  # 引用空续行保留段落分隔

    def _run_queue(self, rich_response, summary="> 结论\n\n### 节\n正文"):
        import json as _json
        import os as _os
        import tempfile
        calls = {"rich": 0, "legacy": 0}

        def fake_rich(token, chat_id, markdown, link=""):
            calls["rich"] += 1
            calls["rich_md"] = markdown
            return rich_response

        def fake_legacy(token, chat_id, text, link=""):
            calls["legacy"] += 1
            return {"ok": True}

        with tempfile.TemporaryDirectory() as d:
            cache_dir = _os.path.join(d, "cache")
            qpath = _os.path.join(d, "u_queue.json")
            with open(qpath, "w") as f:
                _json.dump([{"article_id": "777", "tweet_id": "1", "author": "u",
                             "article_title": "T", "status": "pending", "attempts": 0,
                             "content": None}], f)
            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d), \
                 patch.object(twitter_monitor, "ARTICLE_CACHE_DIR", cache_dir), \
                 patch.object(twitter_monitor, "fetch_article_markdown",
                              return_value=("# 全文\n" + "x" * 300, None)), \
                 patch.object(twitter_monitor, "summarize_article",
                              return_value=(summary, "mimo")), \
                 patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
                 patch.object(twitter_monitor, "send_telegram", side_effect=fake_legacy), \
                 patch.object(twitter_monitor.time, "sleep", return_value=None):
                twitter_monitor.process_article_queue(FakeAI(True), "bot", "chat")
            with open(qpath) as f:
                entry = _json.load(f)[0]
        return calls, entry

    def test_queue_rich_success_skips_legacy(self):
        calls, entry = self._run_queue({"ok": True})
        self.assertEqual(calls["rich"], 1)
        self.assertEqual(calls["legacy"], 0)
        self.assertEqual(entry["status"], "sent")
        self.assertIn("## \U0001f4c4 T", calls["rich_md"])  # rich 头部带标题

    def test_queue_rich_rejected_falls_back_to_legacy(self):
        calls, entry = self._run_queue(
            {"ok": False, "rich_fallback": True, "error_code": 400, "description": "nope"})
        self.assertEqual(calls["rich"], 1)
        self.assertGreaterEqual(calls["legacy"], 1)
        self.assertEqual(entry["status"], "sent")  # 回退路径仍送达

    def test_queue_oversized_summary_goes_straight_to_legacy(self):
        calls, entry = self._run_queue({"ok": True}, summary="长" * 30100)
        self.assertEqual(calls["rich"], 0)  # 超长不走 rich
        self.assertGreaterEqual(calls["legacy"], 1)
        self.assertEqual(entry["status"], "sent")

    def test_queue_empty_rendered_fallback_is_failed_not_fake_sent(self):
        # rich 被拒 + 摘要渲染为空（纯 URL 被剥光）→ 必须 failed，不能假 sent
        calls, entry = self._run_queue(
            {"ok": False, "rich_fallback": True, "error_code": 400, "description": "nope"},
            summary="https://example.com/only-a-link")
        self.assertEqual(calls["legacy"], 0)
        self.assertEqual(entry["status"], "failed")
        self.assertIn("empty_rendered_summary", entry["last_error"])


class UserIdCacheTest(unittest.TestCase):
    """user rest_id 持久缓存：miss 写回 / hit 免解析 / 失效自愈 / 损坏容错。"""

    def setUp(self):
        import tempfile
        import twitter_graphql as tg
        self.tg = tg
        self._tmpdir = tempfile.TemporaryDirectory()
        self.cache_path = Path(self._tmpdir.name) / ".user_id_cache.json"
        self._patcher = patch.object(tg, "USER_ID_CACHE", str(self.cache_path))
        self._patcher.start()
        self.addCleanup(self._patcher.stop)
        self.addCleanup(self._tmpdir.cleanup)

    def _read_cache(self):
        import json as _json
        return _json.loads(self.cache_path.read_text()) if self.cache_path.exists() else {}

    def test_miss_resolves_then_writes_back_atomically(self):
        import json as _json
        resp = _json.dumps({"data": {"user": {"result": {"rest_id": "424242"}}}})
        calls = []
        with patch.object(self.tg, "_auth_headers", lambda: {"Authorization": "x"}), \
             patch.object(self.tg, "_curl", lambda *a, **k: calls.append(1) or resp):
            self.assertEqual(self.tg.get_user_id("NewUser"), "424242")
        self.assertEqual(len(calls), 1)
        self.assertEqual(self._read_cache(), {"newuser": "424242"})  # 键小写
        self.assertFalse(Path(str(self.cache_path) + ".tmp").exists())  # 原子写无残留

    def test_hit_skips_resolution_network_call(self):
        self.cache_path.write_text('{"dotey": "123"}')
        with patch.object(self.tg, "_curl", side_effect=AssertionError("cache hit 不应发请求")):
            self.assertEqual(self.tg.get_user_id("dotey"), "123")
            self.assertEqual(self.tg.get_user_id("DoTey"), "123")  # 大小写不敏感

    def test_failed_resolution_never_cached(self):
        import json as _json
        miss = _json.dumps({"data": {"user": {}}})  # Cannot find user
        with patch.object(self.tg, "_auth_headers", lambda: {"Authorization": "x"}), \
             patch.object(self.tg, "_get_guest_token", lambda: "gt"), \
             patch.object(self.tg, "_curl", lambda *a, **k: miss):
            self.assertIsNone(self.tg.get_user_id("deleted_user"))
        self.assertEqual(self._read_cache(), {})  # 空值绝不入缓存

    def test_invalidate_removes_entry(self):
        self.cache_path.write_text('{"dotey": "123", "vista8": "456"}')
        self.tg.invalidate_user_id("DoTey")
        self.assertEqual(self._read_cache(), {"vista8": "456"})
        self.tg.invalidate_user_id("never_cached")  # 不存在的键不报错

    def test_corrupted_cache_falls_back_to_resolution(self):
        import json as _json
        self.cache_path.write_text("{broken json!!!")
        resp = _json.dumps({"data": {"user": {"result": {"rest_id": "777"}}}})
        with patch.object(self.tg, "_auth_headers", lambda: {"Authorization": "x"}), \
             patch.object(self.tg, "_curl", lambda *a, **k: resp):
            self.assertEqual(self.tg.get_user_id("dotey"), "777")  # 不抛异常
        self.assertEqual(self._read_cache(), {"dotey": "777"})  # 损坏文件被修复

    def test_fetch_tweets_suspended_error_invalidates_cache(self):
        import json as _json
        self.cache_path.write_text('{"gone_user": "999"}')
        err = _json.dumps({"errors": [{"message": "Authorization: User has been suspended. (63)"}]})
        with patch.object(self.tg, "_auth_headers", lambda: None), \
             patch.object(self.tg, "_get_guest_token", lambda: "gt"), \
             patch.object(self.tg, "_curl", lambda *a, **k: err):
            with self.assertRaises(RuntimeError):  # 对外仍按原行为抛错
                self.tg.fetch_tweets("gone_user", limit=5)
        self.assertEqual(self._read_cache(), {})  # 下一轮自动重新解析

    def test_fetch_tweets_empty_user_node_invalidates_but_returns_empty(self):
        import json as _json
        self.cache_path.write_text('{"ghost": "888"}')
        empty = _json.dumps({"data": {"user": {}}})
        with patch.object(self.tg, "_auth_headers", lambda: None), \
             patch.object(self.tg, "_get_guest_token", lambda: "gt"), \
             patch.object(self.tg, "_curl", lambda *a, **k: empty):
            self.assertEqual(self.tg.fetch_tweets("ghost", limit=5), [])  # 行为不变
        self.assertEqual(self._read_cache(), {})


if __name__ == "__main__":
    unittest.main()
