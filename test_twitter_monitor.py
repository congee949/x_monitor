import argparse
import json
import sys
import tempfile
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

        def fake_send_telegram(token, chat_id, text, link="", thread_id=None):
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

        def fake_process_user(pool, ai, username, bot_token, chat_id, args,
                              content_chat_id=None, content_thread_id=None):
            processed.append(username)
            if username == "broken":
                raise RuntimeError("api payment required")
            return 1, 1, 0, 0

        try:
            with patch.object(twitter_monitor, "CONFIG_PATH", str(config_path)):
                with patch.object(twitter_monitor, "FAILURES_PATH", str(failures_path)), \
                     patch.object(twitter_monitor, "update_status_dashboard", return_value=None):
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
        msg, _rich, link = twitter_monitor.format_message(
            "vista8",
            {"id": "1", "text": "这是一条普通推文，用来验证 iOS 通知栏里能直接看到内容，而不是只看到账号名。"},
        )
        self.assertEqual(link, "https://x.com/vista8/status/1")
        self.assertIn("📢 @vista8", msg)
        self.assertIn("这是一条普通推文", msg)

    def test_rich_header_on_its_own_line(self):
        # rich html 把裸 \n\n 折叠 → 头部必须 <br><br> 才独占一行；HTML 回退用原生 \n\n。
        msg, rich, _ = twitter_monitor.format_message(
            "vista8", {"id": "1", "note_tweet": {"text": "第一段\n\n第二段正文内容"}}, None)
        self.assertIn("📢 @vista8<br><br>", rich)
        self.assertNotIn("📢 @vista8\n\n", rich)
        self.assertIn("📢 @vista8", msg)  # HTML 回退头部用原生换行

    def test_format_message_regular_tweet_full_text_no_140_cap(self):
        # 普通推文（非长推）也平铺全文，不再砍到 140（用户 2026-07-01）
        long_regular = "普通推文内容片段" * 30  # 240 字，非 note_tweet / 非 article
        msg, rich, _ = twitter_monitor.format_message("vista8", {"id": "1", "text": long_regular})
        self.assertNotIn("…", msg)                       # 不再 140 截断
        self.assertIn("普通推文内容片段" * 25, msg)        # 全文平铺
        self.assertIn("普通推文内容片段" * 25, rich)

    def test_format_message_note_tweet_inlines_full_text(self):
        # 长推文：不生成 TL;DR、不折叠，正文直接平铺（用户指定 2026-07-01）。
        long_text = "Stack Overflow 因为大家都用 AI 导致发帖量下降，但公司靠企业知识库和数据授权收入增长。" * 20
        msg, rich, link = twitter_monitor.format_message(
            "dotey",
            {"id": "2", "text": long_text[:200], "note_tweet": {"text": long_text}},
            FakeAI(True, "不应再生成的 AI 摘要内容"),
        )
        self.assertEqual(link, "https://x.com/dotey/status/2")
        self.assertNotIn("TL;DR", msg)
        self.assertNotIn("不应再生成的 AI 摘要内容", rich)   # 不再调用 AI 总结
        self.assertNotIn("<blockquote", msg)                  # 不再折叠
        self.assertNotIn("<details>", rich)
        self.assertIn("Stack Overflow 因为大家都用 AI", msg)  # 正文直接平铺
        self.assertIn("Stack Overflow", rich)
        self.assertLess(len(msg), 4096)  # HTML 回退仍在单条上限内

    def test_format_message_note_tweet_inlines_without_ai(self):
        # AI 不可用也照样平铺全文，不再走任何 TL;DR/预览回退。
        long_text = "Agent 应用和传统 App + AI 的最大差别，在于执行的主体不同。" * 20
        msg, _rich, _ = twitter_monitor.format_message(
            "dotey",
            {"id": "3", "text": long_text[:200], "note_tweet": {"text": long_text}},
            FakeAI(False),
        )
        self.assertNotIn("TL;DR", msg)
        self.assertNotIn("<blockquote", msg)
        self.assertIn("Agent 应用和传统 App", msg)

    def test_format_message_note_tweet_never_consults_ai(self):
        # 即便传入可用 AI，长推也不会调用它做摘要（其内容不得泄漏到输出）。
        long_text = "Stack Overflow 因为大家都用 AI 导致发帖量下降，但公司靠企业知识库和数据授权收入增长。" * 20
        msg, rich, _ = twitter_monitor.format_message(
            "dotey",
            {"id": "5", "text": long_text[:200], "note_tweet": {"text": long_text}},
            FakeAI(True, "* However, Stack Overflow&#x27;"),
        )
        self.assertNotIn("TL;DR", msg)
        self.assertNotIn("However, Stack Overflow", msg)
        self.assertNotIn("However, Stack Overflow", rich)
        self.assertIn("Stack Overflow 因为大家都用 AI", msg)

    def test_format_message_article_keeps_short_article_hint(self):
        msg, _rich, link = twitter_monitor.format_message(
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


class RichMediaBlockTest(unittest.TestCase):
    """普通推文媒体：照片/视频封面嵌为 rich <img>，HTML 回退不嵌图。"""

    def test_video_embeds_poster_with_play_hint(self):
        t = {"id": "1", "text": "看视频",
             "media": [{"type": "video",
                        "url": "https://pbs.twimg.com/amplify_video_thumb/123/img/abc",
                        "video_url": "https://video.twimg.com/x.mp4",
                        "duration_ms": 214916}]}
        msg, rich, _ = twitter_monitor.format_message("OpenAIDevs", t, None)
        self.assertIn('<img src="https://pbs.twimg.com/amplify_video_thumb/123/img/abc"/>', rich)
        self.assertIn("▶️ 视频 · 3:34", rich)
        self.assertNotIn("<img", msg)          # HTML 回退不嵌图（靠 link preview）
        self.assertNotIn("video.twimg.com", rich)  # 不嵌 mp4 本身

    def test_single_photo_embeds_img(self):
        t = {"id": "2", "text": "图",
             "media": [{"type": "photo", "url": "https://pbs.twimg.com/media/p1.jpg"}]}
        _msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        self.assertIn('<img src="https://pbs.twimg.com/media/p1.jpg"/>', rich)
        self.assertNotIn("<tg-collage>", rich)
        self.assertNotIn("▶️", rich)

    def test_multi_photo_uses_collage_capped_at_four(self):
        t = {"id": "3", "text": "多图",
             "media": [{"type": "photo", "url": f"https://pbs.twimg.com/media/p{i}.jpg"}
                       for i in range(6)]}
        _msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        self.assertIn("<tg-collage>", rich)
        self.assertEqual(rich.count("<img "), 4)   # 上限 4

    def test_no_media_no_img(self):
        t = {"id": "4", "text": "纯文字推文没有任何媒体"}
        _msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        self.assertNotIn("<img", rich)
        self.assertNotIn("<tg-collage>", rich)

    def test_note_tweet_with_video_embeds_poster(self):
        # 长推也能带媒体：正文平铺 + 末尾封面图
        t = {"id": "5", "note_tweet": {"text": "长推正文" * 30},
             "media": [{"type": "video", "url": "https://pbs.twimg.com/x/img/v",
                        "duration_ms": 65000}]}
        _msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        self.assertIn('<img src="https://pbs.twimg.com/x/img/v"/>', rich)
        self.assertIn("▶️ 视频 · 1:05", rich)

    def test_article_path_skips_tweet_media_block(self):
        # 文章自带配图走另一路径，format_message 不在此重复嵌 t['media']
        t = {"id": "6", "text": "https://t.co/x",
             "article": {"title": "标题", "preview_text": "预览"},
             "media": [{"type": "photo", "url": "https://pbs.twimg.com/media/a.jpg"}]}
        _msg, rich, _ = twitter_monitor.format_message("dotey", t, None)
        self.assertNotIn("<img", rich)


class StripMediaTcoTest(unittest.TestCase):
    """带图推文正文尾部的「媒体专属」t.co 短链应被剥掉（图已作为 rich 媒体块内嵌），
    但用户主动分享的真实链接必须保留（精确匹配 entities.media[].url，不用末尾正则）。"""

    def test_media_tco_stripped_when_photo_present(self):
        # 有 photo 媒体 → 正文里对应的媒体 t.co 短链在 body 与 rich 两路径都被剥掉
        t = {"id": "1",
             "text": "看这张图 https://t.co/MEDIALINK",
             "media": [{"type": "photo", "url": "https://pbs.twimg.com/media/p1.jpg"}],
             "extended_entities": {"media": [
                 {"type": "photo", "url": "https://t.co/MEDIALINK",
                  "media_url_https": "https://pbs.twimg.com/media/p1.jpg"}]}}
        msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        self.assertNotIn("https://t.co/MEDIALINK", msg)   # HTML 回退不含媒体裸链
        self.assertNotIn("https://t.co/MEDIALINK", rich)  # rich 也不含
        self.assertIn("看这张图", rich)                    # 正文其余部分保留
        self.assertIn('<img src="https://pbs.twimg.com/media/p1.jpg"/>', rich)  # 图作为媒体块内嵌

    def test_real_shared_link_at_end_is_kept(self):
        # 只删 entities 精确匹配的媒体短链；用户主动分享的真实链接即使在末尾也保留
        t = {"id": "2",
             "text": "配图见下 https://t.co/MEDIALINK 另外强烈推荐这篇 https://t.co/REALLINK",
             "media": [{"type": "photo", "url": "https://pbs.twimg.com/media/p1.jpg"}],
             "extended_entities": {"media": [
                 {"type": "photo", "url": "https://t.co/MEDIALINK",
                  "media_url_https": "https://pbs.twimg.com/media/p1.jpg"}]}}
        msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        self.assertNotIn("https://t.co/MEDIALINK", rich)  # 媒体裸链剥掉
        self.assertIn("https://t.co/REALLINK", rich)      # 真实分享链接（末尾）保留
        self.assertIn("https://t.co/REALLINK", msg)
        self.assertNotIn("https://t.co/MEDIALINK", msg)

    def test_no_strip_without_photo_media(self):
        # 没有 photo 媒体（纯文字 / 仅视频）→ 不触发剥离，正文里的 t.co 原样保留
        t = {"id": "3",
             "text": "分享一个链接 https://t.co/PLAINLINK",
             "extended_entities": {"media": [
                 {"type": "photo", "url": "https://t.co/PLAINLINK",
                  "media_url_https": "https://pbs.twimg.com/media/p.jpg"}]}}
        # 注意：t['media'] 缺失（GraphQL 未提取到 photo）→ 门控不成立，不剥
        _msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        self.assertIn("https://t.co/PLAINLINK", rich)

    def test_rt_reconstructed_text_strips_media_tco(self):
        # 转推重建全文时媒体 t.co 也要剥离：normalizer 把 entities/extended_entities
        # 一并换成原推的（twitter_graphql RT rebuild），format_message 才能精确匹配到短链。
        # 本测试同时守护 twitter_graphql 的 entities 传播修复——没有它 t['extended_entities']
        # 仍是壳的空 dict，下面的 assertEqual 会 KeyError。
        import json as _json
        import twitter_graphql as tg
        rt_original = {
            "__typename": "Tweet",
            "legacy": {"id_str": "888",
                       "full_text": "原推正文含配图 https://t.co/MEDIALINK",
                       "extended_entities": {"media": [
                           {"type": "photo", "url": "https://t.co/MEDIALINK",
                            "media_url_https": "https://pbs.twimg.com/media/X.jpg"}]}},
            "core": {"user_results": {"result": {"legacy": {"screen_name": "orig"}}}},
        }
        synthetic = {"data": {"user": {"result": {"timeline_v2": {"timeline": {"instructions": [
            {"entries": [
                {"content": {"itemContent": {"tweet_results": {"result": {
                    "__typename": "Tweet",
                    "legacy": {"id_str": "999", "full_text": "RT @orig: 壳被砍断的短文本…",
                               "created_at": "Fri Jun 05 17:30:00 +0000 2026",
                               "retweeted_status_result": {"result": rt_original}}}}}}},
            ]}]}}}}}}
        with patch.object(tg, "_auth_headers", lambda: None), \
             patch.object(tg, "_get_guest_token", lambda: "gt"), \
             patch.object(tg, "get_user_id", lambda u: "1"), \
             patch.object(tg, "_curl", lambda *a, **k: _json.dumps(synthetic)):
            t = tg.fetch_tweets("dotey", limit=20)[0]
        # 归一化后：entities/extended_entities 已换成原推的（守护 graphql 修复）
        self.assertEqual(t["extended_entities"]["media"][0]["url"], "https://t.co/MEDIALINK")
        msg, rich, _ = twitter_monitor.format_message("dotey", t, None)
        self.assertNotIn("https://t.co/MEDIALINK", rich)   # 转推重建全文里的媒体裸链也剥掉
        self.assertNotIn("https://t.co/MEDIALINK", msg)
        self.assertIn("原推正文含配图", rich)               # 正文保留
        self.assertIn('<img src="https://pbs.twimg.com/media/X.jpg"/>', rich)  # 原推图内嵌


class LatentFixRegressionTest(unittest.TestCase):
    """Regression guards for the 2026-06-03 latent-bug fixes."""

    def test_esc1_note_text_not_double_escaped(self):
        # ESC-1: '&' in the inline full text must render as a single &amp;, not &amp;amp;.
        msg, _rich, _ = twitter_monitor.format_message(
            "dotey", {"id": "1", "note_tweet": {"text": "包含 A & B 与符号的中文长推正文" + "内容" * 40}}, None)
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

        def fake_send_telegram(token, chat_id, text, link="", thread_id=None):
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


class RetweetReconstructTest(unittest.TestCase):
    """转推非 article：壳被 Twitter 砍到 140，用原推重建全文 + note_tweet(长推) + 媒体。"""

    def _run(self, rt_original):
        import json as _json
        import twitter_graphql as tg
        synthetic = {"data": {"user": {"result": {"timeline_v2": {"timeline": {"instructions": [
            {"entries": [
                {"content": {"itemContent": {"tweet_results": {"result": {
                    "__typename": "Tweet",
                    "legacy": {"id_str": "999", "full_text": "RT @orig: 壳被砍断的短文本…",
                               "created_at": "Fri Jun 05 17:30:00 +0000 2026",
                               "retweeted_status_result": {"result": rt_original}}}}}}},
            ]}]}}}}}}
        with patch.object(tg, "_auth_headers", lambda: None), \
             patch.object(tg, "_get_guest_token", lambda: "gt"), \
             patch.object(tg, "get_user_id", lambda u: "1"), \
             patch.object(tg, "_curl", lambda *a, **k: _json.dumps(synthetic)):
            return tg.fetch_tweets("dotey", limit=20)[0]

    def test_long_rt_reconstructs_note_and_media(self):
        rt_original = {
            "__typename": "Tweet",
            "legacy": {"id_str": "888", "full_text": "短壳",
                       "extended_entities": {"media": [
                           {"type": "photo", "media_url_https": "https://pbs.twimg.com/media/X.jpg"}]}},
            "core": {"user_results": {"result": {"legacy": {"screen_name": "orig"}}}},
            "note_tweet": {"note_tweet_results": {"result": {"text": "完整长正文" * 30}}},
        }
        t = self._run(rt_original)
        self.assertTrue(t["text"].startswith("RT @orig: 完整长正文"))
        self.assertTrue(t["note_tweet"]["text"].startswith("RT @orig: 完整长正文"))
        self.assertGreater(len(t["note_tweet"]["text"]), 140)   # 全文，非 140 壳
        self.assertEqual([m["url"] for m in t["media"]], ["https://pbs.twimg.com/media/X.jpg"])

    def test_short_rt_reconstructs_full_text_no_note(self):
        rt_original = {
            "__typename": "Tweet",
            "legacy": {"id_str": "888", "full_text": "原推完整正文一句话示意"},
            "core": {"user_results": {"result": {"legacy": {"screen_name": "orig"}}}},
        }
        t = self._run(rt_original)
        self.assertEqual(t["text"], "RT @orig: 原推完整正文一句话示意")
        self.assertNotIn("note_tweet", t)
        self.assertEqual(t.get("media", []), [])


class QuoteArticleTest(unittest.TestCase):
    """引用他人 X Article：单条摘要、署名原作者、博主评论作引子；壳推无 URL 也要入队。

    live 样本 @karpathy 引用 @trq212 文章：article rest_id=2052796100608974848，
    被引推文 id=2052809885763747935，原作者 trq212，karpathy 评论无 article URL、
    entities.urls 为空。
    """

    # 归一化之后的引用推文（article 取自被引推、quoted_status 已展开）
    QUOTE_TWEET = {
        "id": "2052810000000000000",
        "text": "this is a great writeup on tokenization https://t.co/abc123",
        "entities": {"urls": []},  # 壳推无 article URL（live 实测）
        "article": {"title": "Tokenization deep dive", "preview_text": "预览",
                    "rest_id": "2052796100608974848"},
        "quoted_status": {"id": "2052809885763747935", "screen_name": "trq212"},
    }

    def test_normalizer_unwraps_quoted_article(self):
        import json as _json
        import twitter_graphql as tg
        quoted_original = {
            "__typename": "Tweet",
            "legacy": {"id_str": "2052809885763747935", "full_text": "原文壳"},
            "core": {"user_results": {"result": {"legacy": {"screen_name": "trq212"}}}},
            "article": {"article_results": {"result": {
                "title": "Tokenization deep dive", "preview_text": "预览",
                "rest_id": "2052796100608974848"}}},
        }
        synthetic = {"data": {"user": {"result": {"timeline_v2": {"timeline": {"instructions": [
            {"entries": [
                {"content": {"itemContent": {"tweet_results": {"result": {
                    "__typename": "Tweet",
                    "legacy": {"id_str": "2052810000000000000",
                               "full_text": "great writeup https://t.co/abc123",
                               "created_at": "Fri Jun 05 17:30:00 +0000 2026",
                               "entities": {"urls": []}},
                    # quoted_status_result 在 tweet_result 顶层，非 legacy 下
                    "quoted_status_result": {"result": quoted_original}}}}}},
            ]}]}}}}}}
        with patch.object(tg, "_auth_headers", lambda: None), \
             patch.object(tg, "_get_guest_token", lambda: "gt"), \
             patch.object(tg, "get_user_id", lambda u: "123"), \
             patch.object(tg, "_curl", lambda *a, **k: _json.dumps(synthetic)):
            tweets = tg.fetch_tweets("karpathy", limit=20)
        self.assertEqual(len(tweets), 1)
        self.assertEqual(tweets[0]["article"]["title"], "Tokenization deep dive")  # 取被引文章
        self.assertEqual(tweets[0]["article"]["rest_id"], "2052796100608974848")
        self.assertEqual(tweets[0]["quoted_status"],
                         {"id": "2052809885763747935", "screen_name": "trq212"})

    def test_normalizer_deleted_quote_degrades_gracefully(self):
        import json as _json
        import twitter_graphql as tg
        synthetic = {"data": {"user": {"result": {"timeline_v2": {"timeline": {"instructions": [
            {"entries": [
                {"content": {"itemContent": {"tweet_results": {"result": {
                    "__typename": "Tweet",
                    "legacy": {"id_str": "2052810000000000000",
                               "full_text": "引用了一条已删除推文",
                               "created_at": "Fri Jun 05 17:30:00 +0000 2026"},
                    "quoted_status_result": {}}}}}},  # 被引推文删除 → 空
            ]}]}}}}}}
        with patch.object(tg, "_auth_headers", lambda: None), \
             patch.object(tg, "_get_guest_token", lambda: "gt"), \
             patch.object(tg, "get_user_id", lambda u: "123"), \
             patch.object(tg, "_curl", lambda *a, **k: _json.dumps(synthetic)):
            tweets = tg.fetch_tweets("karpathy", limit=20)
        self.assertEqual(len(tweets), 1)
        self.assertNotIn("article", tweets[0])
        self.assertNotIn("quoted_status", tweets[0])

    def test_save_article_quote_attributes_original_and_sets_comment(self):
        import json as _json
        import os as _os
        import tempfile
        with tempfile.TemporaryDirectory() as d, \
             patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d):
            twitter_monitor.save_article("karpathy", "2052796100608974848", self.QUOTE_TWEET)
            with open(_os.path.join(d, "karpathy_queue.json")) as f:
                entry = _json.load(f)[0]
        self.assertEqual(entry["tweet_id"], "2052809885763747935")  # 被引推，不是壳
        self.assertEqual(entry["author"], "trq212")
        self.assertEqual(entry["article_id"], "2052796100608974848")
        # 评论取壳 text，尾部 t.co 短链已去掉
        self.assertEqual(entry["quote_comment"], "this is a great writeup on tokenization")
        self.assertEqual(twitter_monitor.article_fetch_url("karpathy", entry),
                         "https://x.com/trq212/status/2052809885763747935")

    def test_save_article_retweet_has_no_quote_comment(self):
        import json as _json
        import os as _os
        import tempfile
        rt_tweet = {
            "id": "2062952690750021934",
            "text": "RT @liuren: https://t.co/oa1PZY0g9C",
            "article": {"title": "测试文章", "preview_text": "预览", "rest_id": "2062806260563771392"},
            "retweeted_status": {"id": "2062808278812520765", "screen_name": "liuren"},
        }
        with tempfile.TemporaryDirectory() as d, \
             patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d):
            twitter_monitor.save_article("dotey", "2062806260563771392", rt_tweet)
            with open(_os.path.join(d, "dotey_queue.json")) as f:
                entry = _json.load(f)[0]
        self.assertEqual(entry["author"], "liuren")  # 原作者
        self.assertEqual(entry["quote_comment"], "")  # 转推无评论引子

    def test_save_article_self_article_has_no_quote_comment(self):
        import json as _json
        import os as _os
        import tempfile
        self_tweet = {
            "id": "300",
            "text": "我自己的文章 https://x.com/i/article/300",
            "article": {"title": "自文", "preview_text": "p", "rest_id": "300"},
        }
        with tempfile.TemporaryDirectory() as d, \
             patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d):
            twitter_monitor.save_article("dotey", "300", self_tweet)
            with open(_os.path.join(d, "dotey_queue.json")) as f:
                entry = _json.load(f)[0]
        self.assertEqual(entry["author"], "dotey")  # 自文归本博主
        self.assertEqual(entry["quote_comment"], "")

    def test_process_user_node_fallback_enqueues_and_suppresses_push(self):
        """引用文章壳推无 URL → 节点兜底入队 article，且不作为普通推送。"""
        saved = []
        sent = []
        # 壳推：text/entities 无 article URL，但归一化挂了 article 节点
        quote_tweet = {
            "id": "2052810000000000000",
            "text": "great writeup https://t.co/abc123",
            "entities": {"urls": []},
            "createdAt": "Tue May 12 00:20:00 +0000 2026",
            "article": {"title": "Tokenization deep dive", "preview_text": "p",
                        "rest_id": "2052796100608974848"},
            "quoted_status": {"id": "2052809885763747935", "screen_name": "trq212"},
        }
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)
        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=[quote_tweet]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "save_article",
                          side_effect=lambda u, a, t: saved.append(a)), \
             patch.object(twitter_monitor, "send_telegram",
                          side_effect=lambda *a, **k: sent.append(a) or {"ok": True}), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            new_count, push_count, filter_count, ai_overridden = twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="karpathy",
                bot_token="bot", chat_id="chat", args=args)
        self.assertEqual(saved, ["2052796100608974848"])  # 节点兜底入队
        self.assertEqual(push_count, 0)  # 未作为普通推送
        self.assertEqual(sent, [])

    def _normalize_single(self, tweet_result):
        import json as _json
        import twitter_graphql as tg
        synthetic = {"data": {"user": {"result": {"timeline_v2": {"timeline": {"instructions": [
            {"entries": [{"content": {"itemContent": {"tweet_results": {
                "result": tweet_result}}}}]}]}}}}}}
        with patch.object(tg, "_auth_headers", lambda: None), \
             patch.object(tg, "_get_guest_token", lambda: "gt"), \
             patch.object(tg, "get_user_id", lambda u: "123"), \
             patch.object(tg, "_curl", lambda *a, **k: _json.dumps(synthetic)):
            return tg.fetch_tweets("u", limit=20)

    def test_normalizer_drops_article_without_rest_id(self):
        """article 节点有标题但 rest_id 空 → 不挂 article 节点（无 id 不可入队/抓取），
        与 process_user 节点兜底 + format_message 的 t['article'] 判定一致，杜绝裸推。"""
        tweets = self._normalize_single({
            "__typename": "Tweet",
            "legacy": {"id_str": "400", "full_text": "无 id 的文章",
                       "created_at": "Fri Jun 05 17:30:00 +0000 2026", "entities": {"urls": []}},
            "article": {"article_results": {"result": {
                "title": "有标题但无 rest_id", "preview_text": "p", "rest_id": ""}}}})
        self.assertEqual(len(tweets), 1)
        self.assertNotIn("article", tweets[0])

    def test_normalizer_drops_unresolvable_quote_author(self):
        """被引推文 legacy 在但作者 core 被剥（screen_name 空）→ 既不取其 article
        也不设 quoted_status，退化普通推（否则错署本博主 + 坏 fetch URL）。"""
        quoted_no_author = {
            "__typename": "Tweet",
            "legacy": {"id_str": "2052809885763747935", "full_text": "原文壳"},
            "core": {},  # 作者节点被剥
            "article": {"article_results": {"result": {
                "title": "Tokenization deep dive", "preview_text": "预览",
                "rest_id": "2052796100608974848"}}}}
        tweets = self._normalize_single({
            "__typename": "Tweet",
            "legacy": {"id_str": "2052810000000000000", "full_text": "引用",
                       "created_at": "Fri Jun 05 17:30:00 +0000 2026", "entities": {"urls": []}},
            "quoted_status_result": {"result": quoted_no_author}})
        self.assertEqual(len(tweets), 1)
        self.assertNotIn("quoted_status", tweets[0])
        self.assertNotIn("article", tweets[0])

    def test_normalizer_quote_without_article_sets_quoted_status(self):
        """引用普通推文（有作者无 article）→ quoted_status 设上、article 不设。
        证明 quote 展开真的跑了（与 baseline 区分：baseline 两者皆无）。"""
        quoted_plain = {
            "__typename": "Tweet",
            "legacy": {"id_str": "999", "full_text": "被引普通推"},
            "core": {"user_results": {"result": {"legacy": {"screen_name": "someone"}}}}}
        tweets = self._normalize_single({
            "__typename": "Tweet",
            "legacy": {"id_str": "1000", "full_text": "我的评论",
                       "created_at": "Fri Jun 05 17:30:00 +0000 2026", "entities": {"urls": []}},
            "quoted_status_result": {"result": quoted_plain}})
        self.assertEqual(tweets[0]["quoted_status"], {"id": "999", "screen_name": "someone"})
        self.assertNotIn("article", tweets[0])

    def test_quote_comment_survives_html_fallback(self):
        """rich 被拒回退 HTML 分块时，引用评论引子不能丢（与 rich 摘要一致）。"""
        entry = {"article_id": "2052796100608974848", "author": "trq212",
                 "article_title": "Tokenization deep dive",
                 "quote_comment": "this is a great writeup on tokenization"}
        msgs = twitter_monitor.format_article_summary_messages("karpathy", entry, "**结论**\n正文")
        self.assertTrue(msgs)
        self.assertIn("<blockquote>@karpathy 引用：\nthis is a great writeup", msgs[0])
        # 无评论条目不加引子（行为不变）
        plain = {"article_id": "1", "author": "dotey", "article_title": "T"}
        msgs2 = twitter_monitor.format_article_summary_messages("dotey", plain, "**结论**\n正文")
        self.assertNotIn("引用：", msgs2[0])
        # 渲染为空时仍返回空：引子不撑空，保留「渲染为空判 failed」防线
        self.assertEqual(
            twitter_monitor.format_article_summary_messages(
                "karpathy", entry, "https://example.com/only-a-link"), [])


class QuoteCommentTextTest(unittest.TestCase):
    """引用评论抽取：保留换行、不砍 200、去尾部 t.co。"""

    def test_preserves_newlines_and_no_200_truncation(self):
        long = "第一行评论\n\n" + "\n".join(f"{i}. 要点内容{i}" for i in range(1, 40))
        out = twitter_monitor._quote_comment_text({"text": long})
        self.assertIn("\n", out)             # 换行保留（不再折成一行）
        self.assertGreater(len(out), 200)    # 不再砍到 200
        self.assertTrue(out.startswith("第一行评论"))

    def test_prefers_note_tweet_and_strips_trailing_tco(self):
        t = {"text": "短壳", "note_tweet": {"text": "完整长评论\n第二行 https://t.co/abc123"}}
        out = twitter_monitor._quote_comment_text(t)
        self.assertEqual(out, "完整长评论\n第二行")


class ArticleCoverImageTest(unittest.TestCase):
    """front matter coverImage 也要进配图（之前只扫正文 ![]() → 封面被漏）。"""

    def test_extracts_front_matter_cover(self):
        md = '---\ncoverImage: "https://pbs.twimg.com/media/ABC.jpg"\n---\n\n# 标题\n\n正文无内嵌图'
        self.assertEqual(twitter_monitor.extract_article_image_urls(md),
                         ["https://pbs.twimg.com/media/ABC.jpg"])

    def test_cover_plus_body_images_deduped(self):
        md = ('---\ncoverImage: "https://pbs.twimg.com/media/COVER.jpg"\n---\n'
              '![](https://pbs.twimg.com/media/B1.jpg)\n'
              '![](https://pbs.twimg.com/media/COVER.jpg)\n'
              '<img src="https://pbs.twimg.com/media/B2.jpg">')
        urls = twitter_monitor.extract_article_image_urls(md)
        self.assertEqual(urls[0], "https://pbs.twimg.com/media/COVER.jpg")
        self.assertEqual(len(urls), 3)  # cover + B1 + B2（COVER 重复去掉）

    def test_cover_and_body_images_separated(self):
        md = ('---\ncoverImage: "https://pbs.twimg.com/media/COVER.jpg"\n---\n'
              '正文\n![](https://pbs.twimg.com/media/B1.jpg)\n<img src="https://pbs.twimg.com/media/B2.jpg">')
        self.assertEqual(twitter_monitor.extract_article_cover(md),
                         "https://pbs.twimg.com/media/COVER.jpg")
        self.assertEqual(twitter_monitor.extract_article_body_images(md),
                         ["https://pbs.twimg.com/media/B1.jpg", "https://pbs.twimg.com/media/B2.jpg"])
        self.assertIsNone(twitter_monitor.extract_article_cover("正文无 front matter"))


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

    def test_format_article_summary_rich_quote_comment_lead_in(self):
        # 引用文章：引子归引用者(username=karpathy)，正文署名原作者(trq212)，单条消息
        entry = {"article_id": "2052796100608974848", "article_title": "Tokenization deep dive",
                 "author": "trq212", "quote_comment": "this is a great writeup"}
        md = twitter_monitor.format_article_summary_rich("karpathy", entry, "> 结论\n\n正文")
        # @user 独占首行，评论逐行加 blockquote 前缀
        self.assertTrue(md.startswith("> @karpathy 引用：\n> this is a great writeup"))
        self.assertIn("## \U0001f4c4 Tokenization deep dive", md)
        self.assertIn("**@trq212**", md)   # 署名原作者，不是引用者

    def test_format_article_summary_rich_quote_comment_preserves_newlines(self):
        # 引子保留原文换行 + 不砍字数（用户 2026-07-01 反馈：之前折成一行 + 200 截断）
        entry = {"article_id": "1", "article_title": "T", "author": "vista8",
                 "quote_comment": "第一行评论\n\n1. 要点一\n2. 要点二"}
        md = twitter_monitor.format_article_summary_rich("vista8", entry, "正文")
        self.assertIn("> @vista8 引用：\n> 第一行评论", md)   # @user 独占首行
        self.assertIn("> 1\\. 要点一", md)                    # 行首数字点转义为字面
        self.assertIn("> 2\\. 要点二", md)
        self.assertIn("\n>\n", md)                            # 空行用 > 维持引用块连续

    def test_format_article_summary_rich_escapes_quote_comment(self):
        entry = {"article_id": "1", "article_title": "标题", "author": "trq212",
                 "quote_comment": "see [this]\n*bold* <hr>"}
        md = twitter_monitor.format_article_summary_rich("karpathy", entry, "正文")
        self.assertIn(r"> see \[this\]", md)      # 同标题转义 + 保留换行
        self.assertIn(r"> \*bold\* \<hr\>", md)

    def test_cover_placed_after_lead_in_before_title(self):
        # 有引用引子：顺序 引子 → 封面 → 标题 → 正文（用户 2026-07-01 选择）
        entry = {"article_id": "1", "article_title": "T", "author": "vista8",
                 "quote_comment": "评论一句话"}
        md = twitter_monitor.format_article_summary_rich(
            "vista8", entry, "摘要正文", image_urls=["https://pbs.twimg.com/media/C.jpg"])
        i_lead = md.index("> @vista8 引用：")
        i_img = md.index("![](https://pbs.twimg.com/media/C.jpg)")
        i_title = md.index("## \U0001f4c4 T")
        i_body = md.index("摘要正文")
        self.assertTrue(i_lead < i_img < i_title < i_body)

    def test_cover_placed_after_title_when_no_lead_in(self):
        # 无引子：顺序 标题 → 封面 → 正文（封面不再挂末尾）
        entry = {"article_id": "1", "article_title": "T", "author": "dotey"}
        md = twitter_monitor.format_article_summary_rich(
            "dotey", entry, "摘要正文", image_urls=["https://pbs.twimg.com/media/C.jpg"])
        i_title = md.index("## \U0001f4c4 T")
        i_img = md.index("![](https://pbs.twimg.com/media/C.jpg)")
        i_body = md.index("摘要正文")
        self.assertTrue(i_title < i_img < i_body)
        self.assertFalse(md.rstrip().endswith(".jpg)"))  # 封面不在末尾

    def test_body_images_injected_into_details(self):
        # 正文内嵌图插进「展开论证与细节」折叠区，封面仍在顶部（用户 2026-07-01）
        summary = "> 结论\n\n### 第一节\n内容一\n\n### 第二节\n内容二\n\n### 第三节\n内容三"
        entry = {"article_id": "1", "article_title": "T", "author": "vista8"}
        md = twitter_monitor.format_article_summary_rich(
            "vista8", entry, summary,
            image_urls=["https://pbs.twimg.com/media/COVER.jpg"],
            detail_image_urls=["https://pbs.twimg.com/media/B1.jpg"])
        self.assertIn("<details><summary>展开论证与细节</summary>", md)
        i_open = md.index("<details>")
        i_body_img = md.index("![](https://pbs.twimg.com/media/B1.jpg)")
        i_close = md.index("</details>")
        self.assertTrue(i_open < i_body_img < i_close)       # 正文图在折叠区内
        self.assertLess(md.index("![](https://pbs.twimg.com/media/COVER.jpg)"), i_open)  # 封面在顶部

    def test_body_images_appended_when_no_details(self):
        # 摘要没分节（无 details）→ 正文图附在末尾
        entry = {"article_id": "1", "article_title": "T", "author": "v"}
        md = twitter_monitor.format_article_summary_rich(
            "v", entry, "只有结论没有分节",
            detail_image_urls=["https://pbs.twimg.com/media/B1.jpg"])
        self.assertNotIn("<details>", md)
        self.assertTrue(md.rstrip().endswith("![](https://pbs.twimg.com/media/B1.jpg)"))

    def test_format_article_summary_rich_no_lead_in_without_comment(self):
        # 转推/自文无 quote_comment → 无引子，行为不变
        entry = {"article_id": "999", "article_title": "深度长文", "author": "liuren"}
        md = twitter_monitor.format_article_summary_rich("dotey", entry, "正文")
        self.assertFalse(md.startswith(">"))
        self.assertTrue(md.startswith("## \U0001f4c4 深度长文"))
        self.assertIn("**@liuren**", md)

    def test_fallback_heading_wraps_whole_line_dedup_inner_bold(self):
        out = twitter_monitor.markdown_to_telegram_html("### 核心观点：**AI 优先**")
        self.assertEqual(out, "<b>核心观点：AI 优先</b>")
        out2 = twitter_monitor.markdown_to_telegram_html("段一\n> \n段二")
        self.assertEqual(out2, "段一\n\n段二")  # 引用空续行保留段落分隔

    def _run_queue(self, rich_response, summary="> 结论\n\n### 节\n正文",
                   markdown=None, fetch_err=None, entry_extra=None):
        import json as _json
        import os as _os
        import tempfile
        calls = {"rich": 0, "legacy": 0, "rich_mds": [], "quiet": []}
        responses = rich_response if isinstance(rich_response, list) else [rich_response]

        def fake_rich(token, chat_id, markdown_, link="", thread_id=None):
            calls["rich"] += 1
            calls["rich_mds"].append(markdown_)
            return responses[min(calls["rich"] - 1, len(responses) - 1)]

        def fake_legacy(token, chat_id, text, link="", thread_id=None):
            calls["legacy"] += 1
            return {"ok": True, "result": {"message_id": 999}}

        def fake_quiet(token, payload, method):
            calls["quiet"].append((method, payload))
            return {"ok": True}

        base = {"article_id": "777", "tweet_id": "1", "author": "u",
                "article_title": "T", "status": "pending", "attempts": 0,
                "content": None}
        if entry_extra:
            base.update(entry_extra)
        md = markdown if markdown is not None else ("# 全文\n" + "x" * 300)
        fetch_ret = (None, fetch_err) if fetch_err else (md, None)
        with tempfile.TemporaryDirectory() as d:
            cache_dir = _os.path.join(d, "cache")
            qpath = _os.path.join(d, "u_queue.json")
            with open(qpath, "w") as f:
                _json.dump([base], f)
            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d), \
                 patch.object(twitter_monitor, "ARTICLE_CACHE_DIR", cache_dir), \
                 patch.object(twitter_monitor, "fetch_article_markdown",
                              return_value=fetch_ret), \
                 patch.object(twitter_monitor, "summarize_article",
                              return_value=(summary, "mimo")), \
                 patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
                 patch.object(twitter_monitor, "send_telegram", side_effect=fake_legacy), \
                 patch.object(twitter_monitor, "_tg_post_quiet", side_effect=fake_quiet), \
                 patch.object(twitter_monitor.time, "sleep", return_value=None):
                twitter_monitor.process_article_queue(FakeAI(True), "bot", "chat")
            with open(qpath) as f:
                entry = _json.load(f)[0]
        calls["rich_md"] = calls["rich_mds"][0] if calls["rich_mds"] else ""
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

    IMG_MD = ("# 全文\n" + "x" * 300 +
              "\n![](https://pbs.twimg.com/media/a.jpg)\n![](https://pbs.twimg.com/media/b.jpg)\n")

    def test_queue_collage_appended_from_article_images(self):
        calls, entry = self._run_queue({"ok": True}, markdown=self.IMG_MD)
        self.assertEqual(entry["status"], "sent")
        self.assertIn("<tg-collage>", calls["rich_mds"][0])
        self.assertIn("![](https://pbs.twimg.com/media/a.jpg)", calls["rich_mds"][0])

    def test_queue_image_rejection_retries_rich_without_images(self):
        # 两级回退：带图 rich 被拒 → 去图 rich 成功 → 不落到 legacy
        calls, entry = self._run_queue(
            [{"ok": False, "rich_fallback": True, "error_code": 400, "description": "img bad"},
             {"ok": True}],
            markdown=self.IMG_MD)
        self.assertEqual(calls["rich"], 2)
        self.assertIn("<tg-collage>", calls["rich_mds"][0])
        self.assertNotIn("<tg-collage>", calls["rich_mds"][1])
        self.assertEqual(calls["legacy"], 0)
        self.assertEqual(entry["status"], "sent")

    def test_queue_failure_notice_closed_on_success(self):
        # 此前失败留下的通知，在重试成功后被原地改写并清除 id
        calls, entry = self._run_queue({"ok": True}, entry_extra={"failure_msg_id": 888})
        methods = [m for m, p in calls["quiet"]]
        self.assertIn("editMessageText", methods)
        edit_payload = [p for m, p in calls["quiet"] if m == "editMessageText"][0]
        self.assertEqual(edit_payload["message_id"], 888)
        self.assertIn("重试成功", edit_payload["text"])
        # 改写必须显式回传按钮（不传 reply_markup = Telegram 移除原键盘）
        self.assertEqual(
            edit_payload["reply_markup"]["inline_keyboard"][0][0]["url"],
            "https://x.com/i/article/777")
        self.assertNotIn("failure_msg_id", entry)

    def test_queue_fetch_failure_captures_notice_msg_id(self):
        calls, entry = self._run_queue({"ok": True}, fetch_err="markdown_fetch_empty_article_body")
        self.assertEqual(entry["status"], "failed")
        self.assertEqual(entry["failure_msg_id"], 999)  # fake_legacy 返回的 message_id

    def test_fold_summary_details(self):
        s = "> 结论\n\n### 一\nA\n\n### 二\nB\n\n### 三\nC"
        out = twitter_monitor._fold_summary_details(s)
        head, folded = out.split("<details><summary>展开论证与细节</summary>")
        self.assertIn("### 一", head)
        self.assertNotIn("### 二", head)
        self.assertIn("### 二", folded)
        self.assertIn("### 三", folded)
        self.assertTrue(out.rstrip().endswith("</details>"))
        # 0-1 个分节不折叠
        single = "> r\n\n### 一\nA"
        self.assertEqual(twitter_monitor._fold_summary_details(single), single)

    def test_single_image_uses_bare_block_not_collage(self):
        entry = {"article_id": "9", "article_title": "T"}
        md = twitter_monitor.format_article_summary_rich(
            "u", entry, "正文", image_urls=["https://pbs.twimg.com/media/x.jpg"])
        self.assertIn("![](https://pbs.twimg.com/media/x.jpg)", md)
        self.assertNotIn("<tg-collage>", md)


class PushCountTest(unittest.TestCase):
    """推送计数 = 实际送达：失败的发送不计入（防重试双计/故障期虚高）。"""

    def test_failed_send_not_counted_as_pushed(self):
        tweets = [{"id": "t1", "text": "这是一条长度足够通过分类过滤器的正常推文内容编号一",
                   "createdAt": "Tue May 12 00:20:00 +0000 2026"},
                  {"id": "t2", "text": "这是一条长度足够通过分类过滤器的正常推文内容编号二",
                   "createdAt": "Tue May 12 00:21:00 +0000 2026"}]
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)

        def flaky_send(token, chat_id, text, link="", thread_id=None):
            return {"ok": "t2" not in text and "t2" not in link}

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=tweets), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "send_telegram", side_effect=flaky_send), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            new, pushed, _f, _a = twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="u",
                bot_token="b", chat_id="c", args=args)
        self.assertEqual(new, 2)
        self.assertEqual(pushed, 1)  # t2 发送失败，不计入送达


class AuthorTldrTest(unittest.TestCase):
    """长推平铺全文：原文自带的 TL;DR 行作为正文一部分照常显示，AI 不再介入。"""

    def test_note_tweet_inlines_text_including_author_tldr_line(self):
        long_text = ("这是很长的正文内容。" * 40 +
                     "\nTL;DR: 作者自己写的一句话总结内容足够长超过十个字符")
        msg, rich, _ = twitter_monitor.format_message(
            "dotey", {"id": "8", "note_tweet": {"text": long_text}},
            FakeAI(True, "AI生成的摘要不应该出现在消息里因为原文自带"))
        self.assertNotIn("AI生成的摘要", msg)            # 不再调用 AI
        self.assertNotIn("AI生成的摘要", rich)
        self.assertNotIn("<blockquote", msg)             # 不折叠
        self.assertNotIn("<details>", rich)
        self.assertIn("作者自己写的一句话总结", rich)     # 原文 TL;DR 行作为正文照常平铺

    def test_extract_author_tldr_variants(self):
        self.assertIsNotNone(twitter_monitor.extract_author_tldr(
            "正文\n太长不看：中文别名写法的总结也要超过十个字符"))
        self.assertIsNone(twitter_monitor.extract_author_tldr("没有摘要行的普通正文"))
        self.assertIsNone(twitter_monitor.extract_author_tldr("TL;DR: 太短"))

    def test_emoji_dense_note_stays_within_utf16_limit(self):
        # astral 表情每个占 2 个 UTF-16 单位：2500 字符 = 5000 单位，必须被收缩
        msg, _rich, _ = twitter_monitor.format_message(
            "dotey", {"id": "10", "note_tweet": {"text": "\U0001f40d" * 2500}}, None)
        self.assertNotIn("<blockquote", msg)
        self.assertLess(len(msg.encode("utf-16-le")) // 2, 4096)


class DashboardTest(unittest.TestCase):
    """置顶状态看板：首轮创建+置顶，后续原地编辑，编辑失败重建，跨日清零。"""

    def _run(self, state=None, edit_ok=True, send_mid=777,
             pushed=2, articles=1, failures=None, dt_cls=None,
             chat_ttl=0, getchat_ok=True, send_date=1_700_000_000):
        import json as _json
        import tempfile
        quiet = []

        def fake_quiet(token, payload, method):
            quiet.append((method, payload))
            if method == "getChat":
                if not getchat_ok:
                    return {"ok": False}
                res = {"id": 1}
                if chat_ttl:
                    res["message_auto_delete_time"] = chat_ttl
                return {"ok": True, "result": res}
            if method == "editMessageText":
                return {"ok": edit_ok}
            if method == "sendMessage":
                return {"ok": True, "result": {"message_id": send_mid, "date": send_date}}
            return {"ok": True}

        with tempfile.TemporaryDirectory() as d:
            path = f"{d}/.dashboard.json"
            if state is not None:
                with open(path, "w") as f:
                    _json.dump(state, f)
            with patch.object(twitter_monitor, "DASHBOARD_PATH", path), \
                 patch.object(twitter_monitor, "datetime", dt_cls or twitter_monitor.datetime), \
                 patch.object(twitter_monitor, "_tg_post_quiet", side_effect=fake_quiet):
                twitter_monitor.update_status_dashboard(
                    "bot", "chat", [{"username": "a"}, {"username": "b"}],
                    failures or {}, pushed=pushed, articles=articles, elapsed=16.0)
            with open(path) as f:
                saved = _json.load(f)
        return quiet, saved

    def test_six_am_day_boundary(self):
        # 日界 = 北京时间 06:00：05:59 仍计入前一天，06:01 翻新天清零
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        cn = _tz(_td(hours=8))

        def at(hh, mm):
            class FakeNow(_dt):
                @classmethod
                def now(cls, tz=None):
                    t = _dt(2026, 6, 13, hh, mm, tzinfo=cn)
                    return t.astimezone(tz) if tz else t.replace(tzinfo=None)
            return FakeNow

        state = {"message_id": 5, "date": "2026-06-12",
                 "tweets_today": 7, "articles_today": 0}
        _, saved = self._run(state=dict(state), dt_cls=at(5, 59))
        self.assertEqual(saved["tweets_today"], 9)   # 7 + 2，仍是 06-12
        self.assertEqual(saved["date"], "2026-06-12")
        _, saved = self._run(state=dict(state), dt_cls=at(6, 1))
        self.assertEqual(saved["tweets_today"], 2)   # 翻天清零后只计本轮
        self.assertEqual(saved["date"], "2026-06-13")

    def test_first_run_creates_pins_and_saves_state(self):
        quiet, saved = self._run()
        methods = [m for m, p in quiet]
        self.assertEqual(methods, ["getChat", "sendMessage", "pinChatMessage"])
        send_payload = quiet[1][1]
        self.assertTrue(send_payload["disable_notification"])  # 创建静默
        self.assertIn("X 监控状态", send_payload["text"])
        self.assertIn("账号 2/2 正常", send_payload["text"])
        self.assertEqual(saved["message_id"], 777)
        self.assertEqual(saved["tweets_today"], 2)

    def test_subsequent_run_edits_in_place_and_accumulates(self):
        from datetime import datetime, timezone, timedelta
        today = (datetime.now(timezone(timedelta(hours=8))) - timedelta(hours=6)).strftime("%Y-%m-%d")
        quiet, saved = self._run(
            state={"message_id": 5, "date": today, "tweets_today": 10, "articles_today": 3})
        self.assertEqual([m for m, p in quiet], ["getChat", "editMessageText"])
        self.assertEqual(quiet[1][1]["message_id"], 5)
        self.assertIn("今日 12 条", quiet[1][1]["text"])  # 10 + 2 累计
        self.assertEqual(saved["tweets_today"], 12)
        self.assertEqual(saved["message_id"], 5)

    def test_edit_failure_recreates_unpins_and_deletes_old(self):
        from datetime import datetime, timezone, timedelta
        today = (datetime.now(timezone(timedelta(hours=8))) - timedelta(hours=6)).strftime("%Y-%m-%d")
        quiet, saved = self._run(
            state={"message_id": 5, "date": today, "tweets_today": 0, "articles_today": 0},
            edit_ok=False, send_mid=9)
        methods = [m for m, p in quiet]
        self.assertEqual(methods, ["getChat", "editMessageText", "sendMessage",
                                   "pinChatMessage", "unpinChatMessage", "deleteMessage"])
        self.assertEqual(saved["message_id"], 9)

    def test_date_rollover_resets_counters(self):
        quiet, saved = self._run(
            state={"message_id": 5, "date": "2020-01-01",
                   "tweets_today": 99, "articles_today": 99})
        self.assertEqual(saved["tweets_today"], 2)   # 只算本轮
        self.assertEqual(saved["articles_today"], 1)

    def _board_text(self, quiet):
        return next(p["text"] for m, p in quiet if m in ("sendMessage", "editMessageText"))

    def test_failures_listed_in_dashboard(self):
        quiet, _ = self._run(failures={"a": {"count": 5, "last_error": "Cannot find user"}})
        text = self._board_text(quiet)
        self.assertIn("账号 1/2 正常", text)
        self.assertIn("@a 连续 5 轮失败", text)

    def test_ghost_failure_records_excluded(self):
        # 已不在配置中的账号（被移除/禁用）的失败记录不得污染看板
        quiet, _ = self._run(failures={"ghost": {"count": 5, "last_error": "x"}})
        text = self._board_text(quiet)
        self.assertIn("账号 2/2 正常", text)
        self.assertNotIn("ghost", text)

    def test_dirty_counter_state_self_heals(self):
        from datetime import datetime, timezone, timedelta
        today = (datetime.now(timezone(timedelta(hours=8))) - timedelta(hours=6)).strftime("%Y-%m-%d")
        quiet, saved = self._run(
            state={"message_id": 5, "date": today,
                   "tweets_today": "garbage", "articles_today": None})
        self.assertEqual(saved["tweets_today"], 2)  # 脏值重置后只计本轮
        self.assertEqual([m for m, p in quiet], ["getChat", "editMessageText"])

    def test_not_modified_edit_does_not_rebuild(self):
        import io
        import urllib.error
        from datetime import datetime, timezone, timedelta
        today = (datetime.now(timezone(timedelta(hours=8))) - timedelta(hours=6)).strftime("%Y-%m-%d")
        calls = []

        def fake_post(token, payload, method="sendMessage"):
            calls.append(method)
            raise urllib.error.HTTPError(
                "url", 400, "Bad Request", {},
                io.BytesIO(b'{"ok":false,"description":"Bad Request: message is not modified"}'))

        import json as _json
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path = f"{d}/.dashboard.json"
            with open(path, "w") as f:
                _json.dump({"message_id": 5, "date": today,
                            "tweets_today": 0, "articles_today": 0}, f)
            with patch.object(twitter_monitor, "DASHBOARD_PATH", path), \
                 patch.object(twitter_monitor, "_tg_post", side_effect=fake_post):
                twitter_monitor.update_status_dashboard(
                    "bot", "chat", [{"username": "a"}], {}, 0, 0, 16.0)
            with open(path) as f:
                saved = _json.load(f)
        self.assertEqual(calls, ["getChat", "editMessageText"])  # 没有触发删旧重建链
        self.assertEqual(saved["message_id"], 5)

    def test_proactive_rebuild_before_ttl_expiry(self):
        import time as _t
        from datetime import datetime, timezone, timedelta
        today = (datetime.now(timezone(timedelta(hours=8))) - timedelta(hours=6)).strftime("%Y-%m-%d")
        old = _t.time() - 80000  # 80000s > 0.85 * 86400 = 73440 → 将近到期
        new_date = old + 80000
        quiet, saved = self._run(
            state={"message_id": 5, "date": today, "tweets_today": 0,
                   "articles_today": 0, "created_at": old},
            chat_ttl=86400, send_mid=20, send_date=new_date)
        methods = [m for m, p in quiet]
        # 主动重建（不编辑旧消息）：getChat → 新发 → 置顶 → 取消旧置顶 → 删旧
        self.assertEqual(methods, ["getChat", "sendMessage", "pinChatMessage",
                                   "unpinChatMessage", "deleteMessage"])
        self.assertEqual(saved["message_id"], 20)
        self.assertEqual(saved["created_at"], new_date)  # 计时基准刷新

    def test_no_rebuild_when_message_fresh(self):
        import time as _t
        from datetime import datetime, timezone, timedelta
        today = (datetime.now(timezone(timedelta(hours=8))) - timedelta(hours=6)).strftime("%Y-%m-%d")
        quiet, _ = self._run(
            state={"message_id": 5, "date": today, "tweets_today": 0,
                   "articles_today": 0, "created_at": _t.time()},
            chat_ttl=86400)
        self.assertEqual([m for m, p in quiet], ["getChat", "editMessageText"])

    def test_no_proactive_rebuild_without_ttl(self):
        from datetime import datetime, timezone, timedelta
        today = (datetime.now(timezone(timedelta(hours=8))) - timedelta(hours=6)).strftime("%Y-%m-%d")
        # 聊天未开 auto-delete：即使消息很老也只编辑，不做无谓重建
        quiet, _ = self._run(
            state={"message_id": 5, "date": today, "tweets_today": 0,
                   "articles_today": 0, "created_at": 0},
            chat_ttl=0)
        self.assertEqual([m for m, p in quiet], ["getChat", "editMessageText"])

    def test_corrupt_created_at_self_heals(self):
        # 脏状态文件里非数值 created_at 不得让 float() 崩整轮（major 修复）
        from datetime import datetime, timezone, timedelta
        today = (datetime.now(timezone(timedelta(hours=8))) - timedelta(hours=6)).strftime("%Y-%m-%d")
        new_date = 1_700_000_000 + 99999
        quiet, saved = self._run(
            state={"message_id": 5, "date": today, "tweets_today": 0,
                   "articles_today": 0, "created_at": "garbage"},
            chat_ttl=86400, send_mid=22, send_date=new_date)
        # 视作 0 → age 巨大 → 主动重建并写回干净数值，不崩
        self.assertIn("sendMessage", [m for m, p in quiet])
        self.assertEqual(saved["message_id"], 22)
        self.assertEqual(saved["created_at"], new_date)
        self.assertIsInstance(saved["created_at"], (int, float))

    def test_ttl_cache_used_when_getchat_fails(self):
        import time as _t
        from datetime import datetime, timezone, timedelta
        today = (datetime.now(timezone(timedelta(hours=8))) - timedelta(hours=6)).strftime("%Y-%m-%d")
        old = _t.time() - 80000
        quiet, saved = self._run(
            state={"message_id": 5, "date": today, "tweets_today": 0,
                   "articles_today": 0, "created_at": old, "ttl": 86400},
            getchat_ok=False, send_mid=21)
        # getChat 失败但沿用缓存 ttl=86400 仍判将近到期 → 重建
        self.assertIn("sendMessage", [m for m, p in quiet])
        self.assertEqual(saved["message_id"], 21)


class AlertClosureTest(unittest.TestCase):
    """告警置顶 + 恢复闭环：告警时 pin，恢复时原地改写并 unpin。"""

    def test_alert_pins_then_recovery_edits_and_unpins(self):
        quiet = []

        def fake_send(token, chat_id, text, link="", thread_id=None):
            return {"ok": True, "result": {"message_id": 555}}

        def fake_quiet(token, payload, method):
            quiet.append((method, payload))
            return {"ok": True}

        failures = {}
        with patch.object(twitter_monitor, "send_telegram", side_effect=fake_send), \
             patch.object(twitter_monitor, "_tg_post_quiet", side_effect=fake_quiet):
            for _ in range(twitter_monitor.FAIL_ALERT_THRESHOLD):
                twitter_monitor.note_account_failure(failures, "ghost", "err", "bot", "chat")
            self.assertEqual(failures["ghost"]["alert_msg_id"], 555)
            self.assertEqual([m for m, p in quiet], ["pinChatMessage"])

            twitter_monitor.note_account_success(failures, "ghost", "bot", "chat")
        methods = [m for m, p in quiet]
        self.assertEqual(methods, ["pinChatMessage", "editMessageText", "unpinChatMessage"])
        edit = [p for m, p in quiet if m == "editMessageText"][0]
        self.assertEqual(edit["message_id"], 555)
        self.assertIn("已恢复", edit["text"])
        self.assertNotIn("ghost", failures)

    def test_recovery_without_alert_is_silent(self):
        quiet = []
        failures = {"u": {"count": 2, "alerted": False}}
        with patch.object(twitter_monitor, "_tg_post_quiet",
                          side_effect=lambda *a: quiet.append(a)):
            twitter_monitor.note_account_success(failures, "u", "bot", "chat")
        self.assertEqual(quiet, [])
        self.assertNotIn("u", failures)


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


class SendTweetTest(unittest.TestCase):
    """普通推文统一推送入口 send_tweet：rich-first（html 字段）→ HTML 分块回退。"""

    TWEET = {"id": "9", "text": "一条足够长的普通推文内容，用来覆盖 send_tweet 的推送路径测试"}

    def test_rich_success_skips_html_fallback(self):
        calls = {"rich": 0, "legacy": 0, "html": ""}

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None):
            calls["rich"] += 1
            calls["html"] = html
            return {"ok": True, "result": {"message_id": 1}}

        def fake_legacy(token, chat_id, text, link="", thread_id=None):
            calls["legacy"] += 1
            return {"ok": True}

        with patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
             patch.object(twitter_monitor, "send_telegram", side_effect=fake_legacy):
            r = twitter_monitor.send_tweet("tok", "42", "vista8", self.TWEET)
        self.assertTrue(r["ok"])
        self.assertEqual(calls["rich"], 1)
        self.assertEqual(calls["legacy"], 0)          # rich 成功不回退
        self.assertIn("\U0001f4e2 @vista8", calls["html"])  # 走 html 字段（非 markdown）

    def test_rich_rejected_falls_back_to_html(self):
        calls = {"legacy": 0}

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None):
            return {"ok": False, "rich_fallback": True}

        def fake_legacy(token, chat_id, text, link="", thread_id=None):
            calls["legacy"] += 1
            return {"ok": True, "result": {"message_id": 2}}

        with patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
             patch.object(twitter_monitor, "send_telegram", side_effect=fake_legacy):
            r = twitter_monitor.send_tweet("tok", "42", "vista8", self.TWEET)
        self.assertTrue(r["ok"])
        self.assertEqual(calls["legacy"], 1)          # rich_fallback → 回退 HTML

    def test_rich_hard_failure_does_not_fall_back(self):
        # ok=False 但非 rich_fallback（如 429 重试穷尽）→ 直接返回，不二次发送
        calls = {"legacy": 0}

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None):
            return {"ok": False}

        def fake_legacy(token, chat_id, text, link="", thread_id=None):
            calls["legacy"] += 1
            return {"ok": True}

        with patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
             patch.object(twitter_monitor, "send_telegram", side_effect=fake_legacy):
            r = twitter_monitor.send_tweet("tok", "42", "vista8", self.TWEET)
        self.assertFalse(r["ok"])
        self.assertEqual(calls["legacy"], 0)          # 硬失败不回退（避免重复发送）

    def test_oversized_note_rich_html_capped_within_limit(self):
        # format_message 把超大 note 的 rich 折叠截在 RICH_MESSAGE_MAX_CHARS 内，
        # 所以 send_tweet 仍走 rich，不会产出超限内容
        calls = {"rich": 0, "legacy": 0, "html_len": 0}

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None):
            calls["rich"] += 1
            calls["html_len"] = len(html)
            return {"ok": True}

        def fake_legacy(token, chat_id, text, link="", thread_id=None):
            calls["legacy"] += 1
            return {"ok": True}

        big = {"id": "9", "note_tweet": {"text": "长" * 40000}}
        with patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
             patch.object(twitter_monitor, "send_telegram", side_effect=fake_legacy):
            r = twitter_monitor.send_tweet("tok", "42", "vista8", big, None)
        self.assertEqual(calls["rich"], 1)
        self.assertEqual(calls["legacy"], 0)
        self.assertLessEqual(calls["html_len"], twitter_monitor.RICH_MESSAGE_MAX_CHARS)


class _FakeBackend:
    def __init__(self, name, result=None, exc=None):
        self.name = name
        self._result = result
        self._exc = exc

    def complete(self, prompt, max_tokens=1200, temperature=0.2):
        if self._exc:
            raise self._exc
        return self._result


class AIClassifierCompleteTest(unittest.TestCase):
    """Issue 1: 推理模型 token 耗尽返回空串时，必须继续下一后端而不是当成功。"""

    def test_empty_backend_falls_through_to_next(self):
        ai = twitter_monitor.AIClassifier([
            _FakeBackend("mimo", result=""),          # 推理吃光 token → 空 content
            _FakeBackend("gemini", result="好摘要"),
        ])
        self.assertEqual(ai.complete("p"), ("好摘要", "gemini"))

    def test_whitespace_only_treated_as_empty(self):
        ai = twitter_monitor.AIClassifier([
            _FakeBackend("mimo", result="  \n  "),
            _FakeBackend("gemini", result="ok"),
        ])
        self.assertEqual(ai.complete("p"), ("ok", "gemini"))

    def test_all_empty_returns_none(self):
        ai = twitter_monitor.AIClassifier([
            _FakeBackend("mimo", result=""),
            _FakeBackend("gemini", result=None),
        ])
        self.assertEqual(ai.complete("p"), (None, "all_ai_failed"))

    def test_raising_backend_skipped(self):
        ai = twitter_monitor.AIClassifier([
            _FakeBackend("mimo", exc=RuntimeError("boom")),
            _FakeBackend("gemini", result="ok"),
        ])
        self.assertEqual(ai.complete("p"), ("ok", "gemini"))


class NoteTweetSummaryBudgetTest(unittest.TestCase):
    """Issue 1: summarize_note_tweet 必须给推理模型足够 token（防回退到 220）。"""

    def test_uses_large_token_budget(self):
        captured = {}

        class RecAI:
            def is_available(self):
                return True

            def complete(self, prompt, max_tokens=1200, temperature=0.2):
                captured["max_tokens"] = max_tokens
                return "这是一条足够长且全是中文的摘要内容用来通过质量门控检查。", "rec"

        out = twitter_monitor.summarize_note_tweet(RecAI(), "dotey", "原始长推正文" * 50)
        self.assertGreaterEqual(captured["max_tokens"], 1000)
        self.assertTrue(out)


class RtBreakTest(unittest.TestCase):
    """Issue 3: 转推 'RT @用户名: 正文' 在归属后换行。"""

    def test_break_rt_prefix_inserts_blank_line(self):
        self.assertEqual(twitter_monitor._break_rt_prefix("RT @someone: 正文内容"),
                         "RT @someone:\n\n正文内容")

    def test_break_rt_prefix_noop_without_prefix(self):
        self.assertEqual(twitter_monitor._break_rt_prefix("普通推文 @someone 你好"),
                         "普通推文 @someone 你好")
        self.assertEqual(twitter_monitor._break_rt_prefix("RT 没有冒号"),
                         "RT 没有冒号")

    def test_format_message_regular_rt_breaks_in_both_paths(self):
        msg, rich, _ = twitter_monitor.format_message(
            "dotey", {"id": "1", "text": "RT @paulwalker: 这是被转发的正文内容需要单独成段显示出来。"})
        self.assertIn("RT @paulwalker:\n\n", msg)         # HTML 回退：原生换行
        self.assertIn("RT @paulwalker:<br><br>", rich)    # rich：<br>

    def test_format_message_long_rt_breaks_in_note_path(self):
        # 长转推平铺全文时，RT 归属在两条路径都折行
        note = "RT @bigaccount: " + "这是一条很长的转推正文内容。" * 30
        msg, rich, _ = twitter_monitor.format_message(
            "dotey", {"id": "9", "note_tweet": {"text": note}}, None)
        self.assertIn("RT @bigaccount:\n\n", msg)
        self.assertIn("RT @bigaccount:<br><br>", rich)


class RichPreserveTest(unittest.TestCase):
    """Issue 2: rich 全文保留换行(<br>)与连续空格(&nbsp;)，HTML 回退用原生换行。"""

    def test_format_message_rich_preserves_note_structure(self):
        note = "第一行\n第二行\n  缩进两格的行\n普通"
        msg, rich, _ = twitter_monitor.format_message(
            "dotey", {"id": "2", "note_tweet": {"text": note}}, None)
        self.assertIn("第一行<br>第二行<br>", rich)
        self.assertIn("&nbsp;&nbsp;缩进两格的行", rich)
        self.assertNotIn("<details>", rich)                # 不再折叠，平铺全文
        self.assertNotIn("<blockquote", msg)               # 回退也平铺，不折叠
        self.assertIn("第一行\n第二行", msg)                # HTML 回退用原生换行
        self.assertNotIn("<br>", msg)                      # 回退用原生换行

    def test_rich_full_within_budget_with_newline_expansion(self):
        # 每行很短 + 大量换行：<br> 膨胀最狠的情形，仍须收缩到 RICH_MESSAGE_MAX_CHARS 内
        note = "\n".join(["行"] * 20000)
        _msg, rich, _ = twitter_monitor.format_message(
            "dotey", {"id": "3", "note_tweet": {"text": note}}, None)
        self.assertLessEqual(len(rich), twitter_monitor.RICH_MESSAGE_MAX_CHARS)
        self.assertIn("<br>", rich)


class ArticleAttributionTest(unittest.TestCase):
    """Issue 4b: 转推他人 article 的摘要必须署原作者，而不是转推的本博主。"""

    def test_summary_rich_uses_original_author(self):
        entry = {"article_id": "999", "article_title": "标题", "author": "liuren"}
        md = twitter_monitor.format_article_summary_rich("dotey", entry, "正文")
        self.assertIn("**@liuren**", md)
        self.assertNotIn("@dotey", md)

    def test_summary_rich_falls_back_to_username(self):
        entry = {"article_id": "999", "article_title": "标题"}  # 旧条目无 author
        md = twitter_monitor.format_article_summary_rich("dotey", entry, "正文")
        self.assertIn("**@dotey**", md)

    def test_failure_message_uses_original_author(self):
        entry = {"article_id": "9", "article_title": "标题",
                 "author": "liuren", "failed_stage": "fetch_markdown"}
        msg, _ = twitter_monitor.format_article_failure_message("dotey", entry, "原因")
        self.assertIn("@liuren", msg)
        self.assertNotIn("@dotey", msg)

    def test_summarize_article_prompt_names_original_author(self):
        # summarize_article 平时被 mock，单独验证 entry["author"] 真的进了 AI prompt
        captured = {}

        class CaptureAI:
            def is_available(self):
                return True

            def complete(self, prompt, max_tokens=1200, temperature=0.2):
                captured["prompt"] = prompt
                return "摘要正文", "cap"

            def complete_with_images(self, prompt, images, max_tokens=1200, temperature=0.2):
                captured["prompt"] = prompt
                return "摘要正文", "cap"

        entry = {"article_id": "2062806260563771392", "article_title": "测试文章",
                 "author": "liuren"}
        with patch.object(twitter_monitor, "fetch_article_images", return_value=[]):
            summary, _backend = twitter_monitor.summarize_article(
                CaptureAI(), "dotey", entry, "# 测试文章\n\n正文内容，无图片。")
        self.assertEqual(summary, "摘要正文")
        self.assertIn("作者 @liuren", captured["prompt"])
        self.assertNotIn("@dotey", captured["prompt"])


class ArticleDedupPushTest(unittest.TestCase):
    """Issue 4a: 带 article 的推文只入队，不再作为普通推文重复推送。"""

    ARTICLE_TWEET = {
        "id": "art-1",
        "text": "新文章发布 https://x.com/i/article/777 欢迎阅读全文内容",
        "createdAt": "Tue May 12 00:20:00 +0000 2026",
    }

    def test_article_tweet_queued_not_pushed(self):
        sent, saved = [], []
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)
        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=[self.ARTICLE_TWEET]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "save_article", side_effect=lambda u, a, t: saved.append(a)), \
             patch.object(twitter_monitor, "send_tweet",
                          side_effect=lambda *a, **k: (sent.append(a), {"ok": True})[1]), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            new, pushed, filt, ov = twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="dotey",
                bot_token="b", chat_id="c", args=args)
        self.assertEqual(saved, ["777"])   # article 入队
        self.assertEqual(sent, [])         # 但没有作为普通推文推送
        self.assertEqual(pushed, 0)


class PushRetryTest(unittest.TestCase):
    """推送失败跨轮重试：push_retry 绕过 push-age 窗口，成功后清除。"""

    STALE_TWEET = {
        "id": "retry-me",
        "text": "这是一条长度足够通过分类过滤器的正常推文内容等待重试",
        "createdAt": "Mon May 11 15:55:43 +0000 2026",
    }

    def test_push_retry_bypasses_stale_window_and_clears_on_success(self):
        sent = []
        saved_retry = {}
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)

        def fake_send(token, chat_id, username, t, ai=None, thread_id=None):
            sent.append(t["id"])
            return {"ok": True}

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=[self.STALE_TWEET]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "load_push_retry", return_value={"retry-me"}), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "save_push_retry",
                          side_effect=lambda u, r: saved_retry.update({"retry": set(r)})), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            new, pushed, _f, _a = twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="u",
                bot_token="b", chat_id="c", args=args)

        self.assertEqual(new, 1)
        self.assertEqual(pushed, 1)
        self.assertEqual(sent, ["retry-me"])
        self.assertEqual(saved_retry.get("retry"), set())

    def test_push_failure_persists_retry_and_stays_unseen(self):
        saved = {}
        saved_retry = {}
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)
        tweet = {
            "id": "fail-tweet",
            "text": "这是一条长度足够通过分类过滤器的正常推文内容编号失败",
            "createdAt": "Tue May 12 00:20:00 +0000 2026",
        }

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=[tweet]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "load_push_retry", return_value=set()), \
             patch.object(twitter_monitor, "save_seen",
                          side_effect=lambda u, s, last_post_ts=None: saved.update({"seen": set(s)})), \
             patch.object(twitter_monitor, "save_push_retry",
                          side_effect=lambda u, r: saved_retry.update({"retry": set(r)})), \
             patch.object(twitter_monitor, "send_tweet", return_value={"ok": False}), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="u",
                bot_token="b", chat_id="c", args=args)

        self.assertNotIn("fail-tweet", saved.get("seen", set()))
        self.assertEqual(saved_retry.get("retry"), {"fail-tweet"})


class SaveSeenRecoveryTest(unittest.TestCase):
    """P0-3: save_seen 写盘失败时创建 recovery 备份；load_seen 合并/重建主文件。"""

    def test_save_seen_failure_creates_backup_and_load_merges(self):
        import json as _json
        import os as _os
        import tempfile
        username = "p03_user"
        with tempfile.TemporaryDirectory() as d:
            seen_dir = _os.path.join(d, "twitter_seen")
            recovery_dir = _os.path.join(seen_dir, ".seen_recovery")
            main_path = _os.path.join(seen_dir, f"{username}.json")
            backup_path = _os.path.join(recovery_dir, f"{username}.json")
            _os.makedirs(seen_dir, exist_ok=True)
            with open(main_path, "w") as f:
                _json.dump({"ids": ["c"], "last_post_ts": "2026-05-11T12:00:00+00:00"}, f)

            def fake_atomic_write(path, data):
                if path == main_path:
                    raise OSError("disk full")
                # Allow recovery backup (and any other path) to succeed
                with open(path, "w", encoding="utf-8") as f:
                    f.write(data)
                    f.flush()
                    _os.fsync(f.fileno())

            with patch.object(twitter_monitor, "SEEN_DIR", seen_dir), \
                 patch.object(twitter_monitor, "SEEN_RECOVERY_DIR", recovery_dir), \
                 patch.object(twitter_monitor, "_atomic_write", side_effect=fake_atomic_write):
                with self.assertRaises(OSError):
                    twitter_monitor.save_seen(username, {"a", "b"})

            self.assertTrue(_os.path.exists(backup_path))
            with open(backup_path) as f:
                backup_data = _json.load(f)
            self.assertEqual(set(backup_data["ids"]), {"a", "b"})

            # load_seen 应合并主文件与 recovery 备份，并重建主文件
            with patch.object(twitter_monitor, "SEEN_DIR", seen_dir), \
                 patch.object(twitter_monitor, "SEEN_RECOVERY_DIR", recovery_dir):
                ids, ts = twitter_monitor.load_seen(username)
            self.assertEqual(ids, {"a", "b", "c"})
            self.assertTrue(_os.path.exists(main_path))
            self.assertFalse(_os.path.exists(backup_path))
            self.assertEqual(ts, "2026-05-11T12:00:00+00:00")


class AIFailClosedTest(unittest.TestCase):
    """P0-4: confirm_promo 全部 AI 失败时 fail-closed；process_user 降级为 filter。"""

    class FailingBackend:
        name = "fail-backend"

        def classify(self, username: str, text: str):
            raise RuntimeError("api down")

    def test_confirm_promo_all_failed_returns_fail_closed(self):
        ai = twitter_monitor.AIClassifier([self.FailingBackend()])
        self.assertEqual(ai.confirm_promo("u", "text"), (False, "all_ai_failed"))

        empty_ai = twitter_monitor.AIClassifier([])
        self.assertEqual(empty_ai.confirm_promo("u", "text"), (False, "all_ai_failed"))

    def test_process_user_suspicious_all_ai_failed_goes_to_filtered(self):
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)
        suspicious_tweet = {
            "id": "s1",
            "text": "byteplus seedance 2.0 api 文档访问体验开通模型冲 200 立即体验方舟平台",
            "createdAt": "Tue May 12 00:20:00 +0000 2026",
        }
        ai = twitter_monitor.AIClassifier([self.FailingBackend()])
        pushed_ids = []

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None):
            pushed_ids.append(t["id"])
            return {"ok": True}

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=[suspicious_tweet]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send_tweet), \
             patch.object(twitter_monitor, "_alert_ai_all_failed", return_value=None), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            new, pushed, filt, ov = twitter_monitor.process_user(
                pool=None, ai=ai, username="u",
                bot_token="b", chat_id="c", args=args)
        self.assertEqual(new, 1)
        self.assertEqual(pushed, 0)
        self.assertEqual(filt, 1)
        self.assertEqual(ov, 0)
        self.assertEqual(pushed_ids, [])


class LoadSeenCorruptedTest(unittest.TestCase):
    """P0-5: load_seen 文件损坏时不自动 seed；process_user 放宽 push age。"""

    def test_load_seen_corrupted_returns_empty_and_marker(self):
        import json as _json
        import os as _os
        import tempfile
        username = "p05_user"
        with tempfile.TemporaryDirectory() as d:
            main_path = _os.path.join(d, f"{username}.json")
            with open(main_path, "w") as f:
                f.write("not json")
            recovery_dir = _os.path.join(d, ".seen_recovery")
            with patch.object(twitter_monitor, "SEEN_DIR", d), \
                 patch.object(twitter_monitor, "SEEN_RECOVERY_DIR", recovery_dir):
                ids, ts = twitter_monitor.load_seen(username)
            self.assertEqual(ids, set())
            self.assertEqual(ts, "corrupted")

    def test_process_user_corrupted_seen_relaxes_push_age(self):
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)
        # 1.5 小时前，正常 45min 窗口会跳过；seen_corrupted 放宽到 1440min 后应推送
        stale_tweet = {
            "id": "old1",
            "text": "这是一条长度足够通过分类过滤器的正常推文内容，在 seen 损坏时应放宽时间窗推送",
            "createdAt": "Mon May 11 23:00:00 +0000 2026",
        }
        pushed_ids = []

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None):
            pushed_ids.append(t["id"])
            return {"ok": True}

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=[stale_tweet]), \
             patch.object(twitter_monitor, "load_seen", return_value=(set(), "corrupted")), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send_tweet), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            new, pushed, filt, ov = twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="u",
                bot_token="b", chat_id="c", args=args)
        self.assertEqual(new, 1)
        self.assertEqual(pushed, 1)
        self.assertEqual(filt, 0)
        self.assertEqual(pushed_ids, ["old1"])


class ArticleStalledProcessingTest(unittest.TestCase):
    """P0-2: Article processing 僵尸回退为 pending；process_article_queue 会处理它。"""

    def test_revert_stalled_processing_resets_old_processing(self):
        from datetime import timedelta as _td
        old_ts = (datetime.now(timezone.utc) - _td(minutes=60)).isoformat()
        queue = [{"article_id": "1", "status": "processing", "updated_at": old_ts}]
        changed = twitter_monitor._revert_stalled_processing(queue, "u")
        self.assertTrue(changed)
        self.assertEqual(queue[0]["status"], "pending")
        self.assertEqual(queue[0]["last_error"], "stalled_processing_reverted")
        self.assertIsNotNone(queue[0].get("updated_at"))

    def test_process_article_queue_handles_reverted_processing_entry(self):
        import json as _json
        import os as _os
        import tempfile
        from datetime import timedelta as _td
        username = "u"
        with tempfile.TemporaryDirectory() as d:
            cache_dir = _os.path.join(d, "cache")
            qpath = _os.path.join(d, f"{username}_queue.json")
            old_ts = (datetime.now(timezone.utc) - _td(minutes=60)).isoformat()
            entry = {
                "article_id": "777", "tweet_id": "1", "author": username,
                "article_title": "T", "status": "processing", "attempts": 0,
                "content": None, "updated_at": old_ts, "detected_at": old_ts,
            }
            with open(qpath, "w") as f:
                _json.dump([entry], f)
            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d), \
                 patch.object(twitter_monitor, "ARTICLE_CACHE_DIR", cache_dir), \
                 patch.object(twitter_monitor, "fetch_article_markdown",
                              return_value=("# 全文\n" + "x" * 300, None)), \
                 patch.object(twitter_monitor, "summarize_article",
                              return_value=("> 结论\n\n### 节\n正文", "mimo")), \
                 patch.object(twitter_monitor, "send_telegram_rich", return_value={"ok": True}), \
                 patch.object(twitter_monitor, "_tg_post_quiet", return_value={"ok": True}), \
                 patch.object(twitter_monitor.time, "sleep", return_value=None):
                processed = twitter_monitor.process_article_queue(FakeAI(True), "bot", "chat")
            self.assertEqual(processed, 1)
            with open(qpath) as f:
                saved = _json.load(f)[0]
            self.assertEqual(saved["status"], "sent")


class GraphqlEmptyListTest(unittest.TestCase):
    """P1-7: GraphQL 返回空列表不应触发 6551.io fallback。"""

    def test_graphql_empty_list_returns_empty_without_6551_fallback(self):
        with patch.object(twitter_monitor.twitter_graphql, "fetch_tweets", return_value=[]):
            result = twitter_monitor.fetch_tweets(pool=None, username="u", limit=20)
        self.assertEqual(result, [])


class GraphqlCurlTest(unittest.TestCase):
    def test_curl_file_not_found_returns_empty(self):
        import twitter_graphql as tg
        with patch.object(tg.subprocess, "run", side_effect=FileNotFoundError):
            self.assertEqual(tg._curl("https://example.com", {"A": "b"}), "")

    def test_get_user_id_without_cookie_uses_guest(self):
        import twitter_graphql as tg
        with patch.object(tg, "_auth_headers", return_value=None), \
             patch.object(tg, "_load_user_id_cache", return_value={}), \
             patch.object(tg, "_get_guest_token", return_value="gt"), \
             patch.object(tg, "_curl", return_value=json.dumps({
                 "data": {"user": {"result": {"rest_id": "123"}}}
             })), \
             patch.object(tg, "_save_user_id_cache", return_value=None):
            self.assertEqual(tg.get_user_id("someone"), "123")

    def test_curl_raises_on_non_2xx_http_status(self):
        import types
        import twitter_graphql as tg
        stdout = "some body\n403\n0"
        with patch.object(tg.subprocess, "run", return_value=types.SimpleNamespace(
                returncode=0, stdout=stdout, stderr="")):
            with self.assertRaises(tg.CurlError):
                tg._curl("https://example.com")

    def test_curl_raises_on_nonzero_exit_code(self):
        import types
        import twitter_graphql as tg
        stdout = "some body\n200\n7"
        with patch.object(tg.subprocess, "run", return_value=types.SimpleNamespace(
                returncode=0, stdout=stdout, stderr="")):
            with self.assertRaises(tg.CurlError):
                tg._curl("https://example.com")


class ContentRoutingTest(unittest.TestCase):
    """X 内容（推文+article 摘要）路由到 content_chat_id/content_thread_id（通知群「X」话题）；
    账号级失败告警仍走 chat_id（DM），未传 content 目标时回落 chat_id（行为不变）。"""

    def test_send_telegram_includes_thread_id_when_set(self):
        captured = {}

        def fake_post(token, payload, method="sendMessage"):
            captured["payload"] = payload
            return {"ok": True}

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post):
            twitter_monitor.send_telegram("tok", "chat", "hello", thread_id=19)
        self.assertEqual(captured["payload"].get("message_thread_id"), 19)

    def test_send_telegram_omits_thread_id_when_not_set(self):
        captured = {}

        def fake_post(token, payload, method="sendMessage"):
            captured["payload"] = payload
            return {"ok": True}

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post):
            twitter_monitor.send_telegram("tok", "chat", "hello")
        self.assertNotIn("message_thread_id", captured["payload"])

    def test_send_telegram_rich_includes_thread_id_when_set(self):
        captured = {}

        def fake_post(token, payload, method="sendMessage"):
            captured["payload"] = payload
            return {"ok": True}

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post):
            twitter_monitor.send_telegram_rich("tok", "chat", html="hi", thread_id=19)
        self.assertEqual(captured["payload"].get("message_thread_id"), 19)

    def test_send_tweet_propagates_thread_id_to_rich_and_fallback(self):
        rich_calls = []
        legacy_calls = []

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None):
            rich_calls.append(thread_id)
            return {"ok": False, "rich_fallback": True}

        def fake_legacy(token, chat_id, text, link="", thread_id=None):
            legacy_calls.append(thread_id)
            return {"ok": True}

        tweet = {"id": "1", "text": "短推", "createdAt": "Tue May 12 00:20:00 +0000 2026"}
        with patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
             patch.object(twitter_monitor, "send_telegram", side_effect=fake_legacy):
            twitter_monitor.send_tweet("tok", "chat", "u", tweet, FakeAI(False), thread_id=19)
        self.assertEqual(rich_calls, [19])
        self.assertEqual(legacy_calls, [19])

    def test_process_user_routes_push_to_content_target_alerts_stay_on_chat_id(self):
        pushed_to = []
        alert_to = []

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None):
            pushed_to.append((chat_id, thread_id))
            return {"ok": True}

        def fake_alert(bot_token, chat_id, username, error):
            alert_to.append(chat_id)

        tweets = [{"id": "t1", "text": "这是一条长度足够通过分类过滤器的正常推文内容编号一",
                   "createdAt": "Tue May 12 00:20:00 +0000 2026"}]
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=tweets), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "save_seen", side_effect=OSError("disk full")), \
             patch.object(twitter_monitor, "_alert_seen_save_failure", side_effect=fake_alert), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send_tweet), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            with self.assertRaises(OSError):
                twitter_monitor.process_user(
                    pool=None, ai=FakeAI(False), username="u",
                    bot_token="b", chat_id="dm-chat", args=args,
                    content_chat_id="group-chat", content_thread_id=19)

        self.assertEqual(pushed_to, [("group-chat", 19)])  # 推文走 content 目标
        self.assertEqual(alert_to, ["dm-chat"])             # seen 写盘失败告警仍走 DM

    def test_process_user_falls_back_to_chat_id_when_content_target_unset(self):
        pushed_to = []

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None):
            pushed_to.append((chat_id, thread_id))
            return {"ok": True}

        tweets = [{"id": "t1", "text": "这是一条长度足够通过分类过滤器的正常推文内容编号一",
                   "createdAt": "Tue May 12 00:20:00 +0000 2026"}]
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=tweets), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send_tweet), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="u",
                bot_token="b", chat_id="dm-chat", args=args)

        self.assertEqual(pushed_to, [("dm-chat", None)])  # 未传 content_* 回落 chat_id，无 thread_id

    def test_process_article_queue_forwards_thread_id(self):
        import json as _json
        import os as _os
        import tempfile
        captured = {"rich": []}

        def fake_rich(token, chat_id, markdown_, link="", thread_id=None):
            captured["rich"].append(thread_id)
            return {"ok": True}

        def fake_legacy(token, chat_id, text, link="", thread_id=None):
            return {"ok": True}

        entry = {"article_id": "777", "tweet_id": "1", "author": "u",
                 "article_title": "T", "status": "pending", "attempts": 0,
                 "content": None}
        with tempfile.TemporaryDirectory() as d:
            cache_dir = _os.path.join(d, "cache")
            qpath = _os.path.join(d, "u_queue.json")
            with open(qpath, "w") as f:
                _json.dump([entry], f)
            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d), \
                 patch.object(twitter_monitor, "ARTICLE_CACHE_DIR", cache_dir), \
                 patch.object(twitter_monitor, "fetch_article_markdown",
                              return_value=("# 全文\n" + "x" * 300, None)), \
                 patch.object(twitter_monitor, "summarize_article",
                              return_value=("> 结论\n\n### 节\n正文", "mimo")), \
                 patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
                 patch.object(twitter_monitor, "send_telegram", side_effect=fake_legacy), \
                 patch.object(twitter_monitor.time, "sleep", return_value=None):
                twitter_monitor.process_article_queue(FakeAI(True), "bot", "chat", thread_id=19)
        self.assertEqual(captured["rich"], [19])


class MainContentRoutingTest(unittest.TestCase):
    """main()：telegram_group_chat_id/telegram_twitter_thread_id 解析为 content 路由目标；
    --chat-id 手动覆盖时整体让位（content_thread_id 同时清空）。"""

    def _run_main(self, config_extra="", argv_extra=None):
        captured = {}
        config_path = Path("test_config_routing.json")
        config_path.write_text(
            '{"telegram_bot_token": "bot", "telegram_chat_id": "dm-chat"' + config_extra + '}')
        failures_path = Path("test_failures_routing.json")

        def fake_process_user(pool, ai, username, bot_token, chat_id, args,
                              content_chat_id=None, content_thread_id=None):
            captured["chat_id"] = chat_id
            captured["content_chat_id"] = content_chat_id
            captured["content_thread_id"] = content_thread_id
            return 1, 1, 0, 0

        def fake_process_article_queue(ai, bot_token, chat_id, dry_run=False, *, thread_id=None):
            captured["article_chat_id"] = chat_id
            captured["article_thread_id"] = thread_id
            return 0

        argv = ["twitter_monitor.py"] + (argv_extra or [])
        try:
            with patch.object(twitter_monitor, "CONFIG_PATH", str(config_path)), \
                 patch.object(twitter_monitor, "FAILURES_PATH", str(failures_path)), \
                 patch.object(twitter_monitor, "update_status_dashboard", return_value=None), \
                 patch.object(twitter_monitor.TokenPool, "load", return_value=None), \
                 patch.object(twitter_monitor.AIClassifier, "load", return_value=FakeAI(False)), \
                 patch.object(twitter_monitor, "load_accounts", return_value=[{"username": "u"}]), \
                 patch.object(twitter_monitor, "process_user", side_effect=fake_process_user), \
                 patch.object(twitter_monitor, "process_article_queue",
                              side_effect=fake_process_article_queue), \
                 patch.object(sys, "argv", argv):
                result = twitter_monitor.main()
        finally:
            config_path.unlink(missing_ok=True)
            failures_path.unlink(missing_ok=True)
        self.assertEqual(result, 0)
        return captured

    def test_group_and_thread_routed_when_configured(self):
        captured = self._run_main(
            config_extra=', "telegram_group_chat_id": "-100123", "telegram_twitter_thread_id": 19')
        self.assertEqual(captured["chat_id"], "dm-chat")
        self.assertEqual(captured["content_chat_id"], "-100123")
        self.assertEqual(captured["content_thread_id"], 19)
        self.assertEqual(captured["article_chat_id"], "-100123")
        self.assertEqual(captured["article_thread_id"], 19)

    def test_falls_back_to_dm_when_group_not_configured(self):
        captured = self._run_main()
        self.assertEqual(captured["content_chat_id"], "dm-chat")
        self.assertIsNone(captured["content_thread_id"])
        self.assertEqual(captured["article_chat_id"], "dm-chat")
        self.assertIsNone(captured["article_thread_id"])

    def test_chat_id_cli_override_suppresses_group_routing(self):
        captured = self._run_main(
            config_extra=', "telegram_group_chat_id": "-100123", "telegram_twitter_thread_id": 19',
            argv_extra=["--chat-id", "debug-chat"])
        self.assertEqual(captured["chat_id"], "debug-chat")
        self.assertEqual(captured["content_chat_id"], "debug-chat")
        self.assertIsNone(captured["content_thread_id"])


class TgPostDeliveryClassificationTest(unittest.TestCase):
    """_tg_post 的发送分相语义：发出前失败可重试（URLError/InvalidURL 原样抛），
    发出后失败（读响应超时/连接中断/响应体损坏）→ TgAmbiguousDelivery。"""

    def setUp(self):
        twitter_monitor._AMBIGUOUS_STREAK = 0

    def _response(self, body: bytes):
        class _Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self):
                return body

        return _Resp()

    def test_success_returns_parsed_json_with_60s_timeout(self):
        with patch("urllib.request.urlopen",
                   return_value=self._response(b'{"ok": true, "result": {"message_id": 7}}')) as m:
            r = twitter_monitor._tg_post("tok", {"chat_id": "1"})
        self.assertTrue(r["ok"])
        self.assertEqual(r["result"]["message_id"], 7)
        # 15s 读超时是本次重复推送事故的直接诱因，60s 是修复主体之一，锚死
        m.assert_called_once()
        self.assertEqual(m.call_args.kwargs["timeout"], 60)

    def test_pre_send_urlerror_propagates_unchanged(self):
        import urllib.error
        err = urllib.error.URLError(ConnectionRefusedError("refused"))
        with patch("urllib.request.urlopen", side_effect=err):
            with self.assertRaises(urllib.error.URLError) as ctx:
                twitter_monitor._tg_post("tok", {"chat_id": "1"})
        self.assertNotIsInstance(ctx.exception, twitter_monitor.TgAmbiguousDelivery)

    def test_http_error_propagates_unchanged(self):
        # HTTPError（4xx/5xx）= 有响应，必须原样抛给状态码分支；误归歧义会静默丢弃
        import io
        import urllib.error
        err = urllib.error.HTTPError("url", 400, "Bad Request", {}, io.BytesIO(b"{}"))
        with patch("urllib.request.urlopen", side_effect=err):
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                twitter_monitor._tg_post("tok", {"chat_id": "1"})
        self.assertNotIsInstance(ctx.exception, twitter_monitor.TgAmbiguousDelivery)

    def test_local_invalid_url_propagates_unchanged(self):
        # token 脏字符（空格/换行）→ InvalidURL 在联网前抛出：必须响亮失败，
        # 误归歧义会把配置错误变成「全部标 seen 的永久静默丢推」
        import http.client
        with patch("urllib.request.urlopen", side_effect=http.client.InvalidURL("bad token")):
            with self.assertRaises(http.client.InvalidURL):
                twitter_monitor._tg_post("tok\n", {"chat_id": "1"})

    def test_local_unicode_error_propagates_unchanged(self):
        # 非 ASCII token → UnicodeEncodeError（ValueError 子类，联网前抛出）：响亮失败
        err = UnicodeEncodeError("ascii", "x", 0, 1, "ordinal not in range")
        with patch("urllib.request.urlopen", side_effect=err):
            with self.assertRaises(UnicodeEncodeError):
                twitter_monitor._tg_post("tok​", {"chat_id": "1"})

    def test_post_send_read_timeout_becomes_ambiguous(self):
        import socket
        with patch("urllib.request.urlopen", side_effect=socket.timeout("timed out")):
            with self.assertRaises(twitter_monitor.TgAmbiguousDelivery):
                twitter_monitor._tg_post("tok", {"chat_id": "1"})

    def test_body_read_failure_becomes_ambiguous(self):
        import socket

        class _BrokenResp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self):
                raise socket.timeout("read timed out")

        with patch("urllib.request.urlopen", return_value=_BrokenResp()):
            with self.assertRaises(twitter_monitor.TgAmbiguousDelivery):
                twitter_monitor._tg_post("tok", {"chat_id": "1"})

    def test_definite_response_resets_ambiguous_streak(self):
        twitter_monitor._AMBIGUOUS_STREAK = 1
        with patch("urllib.request.urlopen", return_value=self._response(b'{"ok": true}')):
            twitter_monitor._tg_post("tok", {"chat_id": "1"})
        self.assertEqual(twitter_monitor._AMBIGUOUS_STREAK, 0)

    def test_5xx_does_not_reset_ambiguous_streak(self):
        """502/504 多为边缘 nginx 在后端挂死时生成：清零会让「一半 502 一半超时」
        的大面积故障绕过熔断、批量静默丢推。"""
        import io
        import urllib.error
        twitter_monitor._AMBIGUOUS_STREAK = 1
        err = urllib.error.HTTPError("url", 502, "Bad Gateway", {}, io.BytesIO(b""))
        with patch("urllib.request.urlopen", side_effect=err):
            with self.assertRaises(urllib.error.HTTPError):
                twitter_monitor._tg_post("tok", {"chat_id": "1"})
        self.assertEqual(twitter_monitor._AMBIGUOUS_STREAK, 1)

    def test_4xx_resets_ambiguous_streak(self):
        """4xx（含 429）确由 Bot API 后端产生，证明链路在处理请求。"""
        import io
        import urllib.error
        twitter_monitor._AMBIGUOUS_STREAK = 1
        err = urllib.error.HTTPError("url", 429, "Too Many Requests", {}, io.BytesIO(b"{}"))
        with patch("urllib.request.urlopen", side_effect=err):
            with self.assertRaises(urllib.error.HTTPError):
                twitter_monitor._tg_post("tok", {"chat_id": "1"})
        self.assertEqual(twitter_monitor._AMBIGUOUS_STREAK, 0)

    def test_garbage_2xx_body_does_not_reset_streak(self):
        """2xx + 非 JSON 响应体（网关异常页）不清零：连续垃圾 2xx 第 2 条起要能熔断。"""
        twitter_monitor._AMBIGUOUS_STREAK = 1
        with patch("urllib.request.urlopen", return_value=self._response(b"<html>gateway</html>")):
            with self.assertRaises(twitter_monitor.TgAmbiguousDelivery):
                twitter_monitor._tg_post("tok", {"chat_id": "1"})
        self.assertEqual(twitter_monitor._AMBIGUOUS_STREAK, 1)


class SendAmbiguousDeliveryTest(unittest.TestCase):
    """发送函数对 TgAmbiguousDelivery 按已送达处理：不重发、不回退、返回 ok；
    连续歧义熔断按失败；痕迹落盘隔离到临时文件。"""

    def setUp(self):
        twitter_monitor._AMBIGUOUS_STREAK = 0
        self._tmp = tempfile.TemporaryDirectory()
        self._path_patch = patch.object(
            twitter_monitor, "ASSUMED_DELIVERY_PATH",
            str(Path(self._tmp.name) / ".assumed_delivered.json"))
        self._path_patch.start()
        self.addCleanup(self._path_patch.stop)
        self.addCleanup(self._tmp.cleanup)

    def test_rich_ambiguous_returns_ok_without_resend(self):
        calls = []

        def fake_post(token, payload, method="sendMessage"):
            calls.append(method)
            raise twitter_monitor.TgAmbiguousDelivery("socket.timeout: timed out")

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post), \
                patch("time.sleep"):
            r = twitter_monitor.send_telegram_rich("tok", "1", html="<b>hi</b>")
        self.assertTrue(r["ok"])
        self.assertTrue(r["assumed_delivered"])
        self.assertEqual(len(calls), 1)  # 不盲目重发

    def test_html_ambiguous_returns_ok_without_resend(self):
        calls = []

        def fake_post(token, payload, method="sendMessage"):
            calls.append(method)
            raise twitter_monitor.TgAmbiguousDelivery("socket.timeout: timed out")

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post), \
                patch("time.sleep"):
            r = twitter_monitor.send_telegram("tok", "1", "hi")
        self.assertTrue(r["ok"])
        self.assertTrue(r["assumed_delivered"])
        self.assertEqual(len(calls), 1)

    def test_send_tweet_ambiguous_rich_does_not_fall_back_to_html(self):
        """rich 疑似已送达时绝不能再走 HTML 回退——那会造成第二条消息。"""
        calls = []

        def fake_post(token, payload, method="sendMessage"):
            calls.append(method)
            raise twitter_monitor.TgAmbiguousDelivery("socket.timeout: timed out")

        tweet = {"id": "1", "text": "hello world", "created_at": "Wed Jul 01 16:29:48 +0000 2026"}
        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post), \
                patch("time.sleep"):
            r = twitter_monitor.send_tweet("tok", "1", "someone", tweet, None)
        self.assertTrue(r["ok"])
        self.assertEqual(calls, ["sendRichMessage"])  # 无 sendMessage 回退

    def test_consecutive_ambiguous_breaks_to_failure(self):
        """熔断：进程内第 2 条连续歧义按失败抛出（进 push_retry），防大面积故障批量丢推。"""

        def fake_post(token, payload, method="sendMessage"):
            raise twitter_monitor.TgAmbiguousDelivery("socket.timeout: timed out")

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post), \
                patch("time.sleep"):
            first = twitter_monitor.send_telegram("tok", "1", "msg-1")
            self.assertTrue(first["assumed_delivered"])
            with self.assertRaises(twitter_monitor.TgAmbiguousDelivery):
                twitter_monitor.send_telegram("tok", "1", "msg-2")

    def test_ambiguous_leaves_persistent_trace(self):
        """按已送达处理必须留痕：下一轮汇总 DM 靠这个文件发现真丢推。"""

        def fake_post(token, payload, method="sendMessage"):
            raise twitter_monitor.TgAmbiguousDelivery("socket.timeout: timed out")

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post), \
                patch("time.sleep"):
            twitter_monitor.send_telegram("tok", "1", "hi", "https://x.com/u/status/1")
        entries = json.loads(Path(twitter_monitor.ASSUMED_DELIVERY_PATH).read_text())
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["link"], "https://x.com/u/status/1")

    def test_alert_path_does_not_latch_on_assumed_delivery(self):
        """告警通道宁重勿漏：assumed_delivered 不落定 alerted，下轮重发。"""
        failures = {"u1": {"count": 3, "last_error": "boom", "alerted": False}}
        with patch.object(twitter_monitor, "send_telegram",
                          return_value={"ok": True, "assumed_delivered": True}):
            twitter_monitor.note_account_failure(
                failures, "u1", "boom", "tok", "1", False)
        self.assertFalse(failures["u1"].get("alerted"))

    def test_pre_send_failure_still_retries(self):
        """发出前失败（URLError）保持原重试语义：3 次后抛出。"""
        import urllib.error
        calls = []

        def fake_post(token, payload, method="sendMessage"):
            calls.append(method)
            raise urllib.error.URLError(ConnectionRefusedError("refused"))

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post), \
                patch("time.sleep"):
            with self.assertRaises(urllib.error.URLError):
                twitter_monitor.send_telegram_rich("tok", "1", html="<b>hi</b>")
        self.assertEqual(len(calls), 3)


class FlushAssumedDeliveryNoticeTest(unittest.TestCase):
    """轮首汇总核对 DM：直发 _tg_post（不留痕、不占熔断额度），送达确认才删账本。"""

    def setUp(self):
        twitter_monitor._AMBIGUOUS_STREAK = 0
        self._tmp = tempfile.TemporaryDirectory()
        self.ledger = Path(self._tmp.name) / ".assumed_delivered.json"
        self._path_patch = patch.object(
            twitter_monitor, "ASSUMED_DELIVERY_PATH", str(self.ledger))
        self._path_patch.start()
        self.addCleanup(self._path_patch.stop)
        self.addCleanup(self._tmp.cleanup)

    def _write_ledger(self, entries):
        self.ledger.write_text(json.dumps(entries), encoding="utf-8")

    def test_confirmed_send_clears_ledger(self):
        self._write_ledger([{"ts": "2026-07-02T00:30:00", "method": "sendRichMessage",
                             "link": "https://x.com/u/status/1"}])
        sent = []

        def fake_post(token, payload, method="sendMessage"):
            sent.append(payload)
            return {"ok": True, "result": {"message_id": 9}}

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post):
            twitter_monitor._flush_assumed_delivery_notice("tok", "1")
        self.assertEqual(len(sent), 1)
        self.assertIn("https://x.com/u/status/1", sent[0]["text"])
        self.assertFalse(self.ledger.exists())

    def test_ambiguous_notice_keeps_ledger_without_self_recording(self):
        """通知自身歧义：账本原样保留（不追加自指条目）、不占熔断额度。"""
        original = [{"ts": "2026-07-02T00:30:00", "method": "sendRichMessage",
                     "link": "https://x.com/u/status/1"}]
        self._write_ledger(original)

        def fake_post(token, payload, method="sendMessage"):
            raise twitter_monitor.TgAmbiguousDelivery("socket.timeout: timed out")

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post):
            twitter_monitor._flush_assumed_delivery_notice("tok", "1")
        self.assertEqual(json.loads(self.ledger.read_text()), original)
        self.assertEqual(twitter_monitor._AMBIGUOUS_STREAK, 0)  # 额度未被通知消耗

    def test_send_failure_keeps_ledger(self):
        import urllib.error
        self._write_ledger([{"ts": "t", "method": "sendMessage", "link": ""}])
        with patch.object(twitter_monitor, "_tg_post",
                          side_effect=urllib.error.URLError("down")):
            twitter_monitor._flush_assumed_delivery_notice("tok", "1")
        self.assertTrue(self.ledger.exists())

    def test_corrupt_ledger_removed_without_send(self):
        self.ledger.write_text('{"a": 1}', encoding="utf-8")
        with patch.object(twitter_monitor, "_tg_post") as m:
            twitter_monitor._flush_assumed_delivery_notice("tok", "1")
        m.assert_not_called()
        self.assertFalse(self.ledger.exists())

    def test_missing_ledger_is_noop(self):
        with patch.object(twitter_monitor, "_tg_post") as m:
            twitter_monitor._flush_assumed_delivery_notice("tok", "1")
        m.assert_not_called()


class ArticleSentPersistBeforeQuietEditTest(unittest.TestCase):
    """已送达的 article 必须在 quiet 编辑前落盘，且落盘失败不得把 sent 翻成 failed。"""

    def test_mid_save_oserror_does_not_flip_sent_to_failed(self):
        saves = []
        real_save = twitter_monitor._save_article_queue

        def flaky_save(queue_path, queue, dry_run):
            saves.append([dict(e) for e in queue])
            # 第一次带 status=sent 的落盘 = send 成功后的即刻落盘，模拟磁盘满
            sent_saves = [s for s in saves if s and s[0].get("status") == "sent"]
            if queue and queue[0].get("status") == "sent" and len(sent_saves) == 1:
                raise OSError("disk full")
            return real_save(queue_path, queue, dry_run)

        with tempfile.TemporaryDirectory() as tmp:
            queue_path = str(Path(tmp) / "u_queue.json")
            entry = {"article_id": "a1", "tweet_id": "t1", "author": "u",
                     "status": "pending", "attempts": 0, "content": None,
                     "detected_at": datetime.now(timezone.utc).isoformat(),
                     "tweet_text": "", "note_tweet_text": "",
                     "article_title": "T", "article_preview": "P", "quote_comment": ""}
            Path(queue_path).write_text(json.dumps([entry]), encoding="utf-8")
            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", tmp), \
                    patch.object(twitter_monitor, "cleanup_old_article_cache"), \
                    patch.object(twitter_monitor, "_save_article_queue", side_effect=flaky_save), \
                    patch.object(twitter_monitor, "fetch_article_markdown",
                                 return_value=("# T\n\ncontent body", "")), \
                    patch.object(twitter_monitor, "cache_article_markdown", return_value=""), \
                    patch.object(twitter_monitor, "summarize_article",
                                 return_value=("summary text", "fake")), \
                    patch.object(twitter_monitor, "send_telegram_rich",
                                 return_value={"ok": True}), \
                    patch.object(twitter_monitor, "delete_article_cache"), \
                    patch("time.sleep"):
                twitter_monitor.process_article_queue(FakeAI(), "tok", "1", False)
            final = json.loads(Path(queue_path).read_text())
        # 即刻落盘（sent）确实发生过且失败被吞掉，最终盘面必须是 sent 而非 failed
        self.assertGreaterEqual(
            len([s for s in saves if s and s[0].get("status") == "sent"]), 2)
        self.assertEqual(final[0]["status"], "sent")


if __name__ == "__main__":
    unittest.main()
