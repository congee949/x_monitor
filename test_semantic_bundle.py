import argparse
import copy
import json
import os
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import twitter_graphql as tg
import twitter_monitor as tm


def raw(tweet_id, author, text, *, media=None, quote=None, retweet=None, article=None):
    legacy = {
        "id_str": str(tweet_id), "full_text": text,
        "created_at": "Thu Aug 13 01:30:00 +0000 2026",
        "entities": {},
    }
    if media:
        legacy["extended_entities"] = {"media": media}
        legacy["entities"] = {"media": media}
    if quote is not None:
        legacy["is_quote_status"] = True
        legacy["quoted_status_id_str"] = str((quote.get("legacy") or {}).get("id_str"))
    if retweet is not None:
        legacy["retweeted_status_result"] = {"result": retweet}
    node = {
        "__typename": "Tweet", "legacy": legacy,
        "core": {"user_results": {"result": {"legacy": {"screen_name": author}}}},
    }
    if quote is not None:
        node["quoted_status_result"] = {"result": quote}
    if article is not None:
        node["article"] = {"article_results": {"result": article}}
    return node


class SemanticBundleTest(unittest.TestCase):
    def setUp(self):
        self._enabled = tm._SEMANTIC_BUNDLE_ENABLED
        self._allowlist = set(tm._SEMANTIC_CURATOR_ALLOWLIST)
        tm._SEMANTIC_BUNDLE_ENABLED = True
        tm._SEMANTIC_CURATOR_ALLOWLIST = set()
        tg._semantic_detail_cache.clear()
        tg._semantic_detail_inflight.clear()
        tg.reset_semantic_resolver_run()
        self._journal_tmp = tempfile.TemporaryDirectory()
        self._journal_path = tm.SEMANTIC_DECISION_JOURNAL
        tm.SEMANTIC_DECISION_JOURNAL = os.path.join(self._journal_tmp.name, "decisions.jsonl")
        self._cache_path = tg.SEMANTIC_DETAIL_CACHE_PATH
        tg.SEMANTIC_DETAIL_CACHE_PATH = os.path.join(self._journal_tmp.name, "detail-cache.json")
        tg._semantic_cache_loaded = True

    def tearDown(self):
        tm._SEMANTIC_BUNDLE_ENABLED = self._enabled
        tm._SEMANTIC_CURATOR_ALLOWLIST = self._allowlist
        tm._PUSHED_INDEX_CACHE = None
        tm.SEMANTIC_DECISION_JOURNAL = self._journal_path
        tg.SEMANTIC_DETAIL_CACHE_PATH = self._cache_path
        tg._semantic_cache_loaded = False
        self._journal_tmp.cleanup()

    def fixture(self):
        image = {"type": "photo", "url": "https://t.co/IMG",
                 "media_url_https": "https://pbs.twimg.com/media/fable.jpg",
                 "original_info": {"width": 1200, "height": 800}}
        c = raw("2087615069790388457", "Arcadia_Bao", "大家都是 fable 级 https://t.co/IMG",
                media=[image])
        b = raw("2087700436036079825", "Gorden_Sun", "草，太传神了", quote=c)
        a = raw("2087708053009273250", "dotey", "RT @Gorden_Sun: 草，太传神了",
                retweet=b)
        return a, b, c

    @staticmethod
    def args(**updates):
        values = dict(test=False, seed=False, dry_run=False, limit=20,
                      max_push_age_minutes=999999, test_count=1)
        values.update(updates)
        return argparse.Namespace(**values)

    class NoAI:
        def is_available(self):
            return False

    def test_repost_quote_photo_is_embedded_complete_without_fetch(self):
        a, _b, _c = self.fixture()
        bundle = tg.build_semantic_bundle(a, "dotey", fetch_mode="graphql_auth")
        self.assertEqual(bundle["anchor"]["tweet_id"], "2087700436036079825")
        self.assertEqual(bundle["anchor"]["author"], "Gorden_Sun")
        self.assertEqual(bundle["repost_path"], [
            {"tweet_id": "2087708053009273250", "author": "dotey"}])
        self.assertEqual(bundle["context_nodes"][0]["author"], "Arcadia_Bao")
        self.assertEqual(bundle["assets"][0]["owner_tweet_id"], "2087615069790388457")
        self.assertEqual(bundle["resolution"]["status"], "complete")
        self.assertEqual(bundle["resolution"]["request_count"], 0)
        self.assertEqual(bundle["identity"]["bundle_key"], "t:2087700436036079825")
        self.assertNotIn("t:2087615069790388457", bundle["identity"]["alias_keys"])

    def test_anchor_renderer_has_context_media_and_no_rt_shell(self):
        a, _b, _c = self.fixture()
        t = {"id": "2087708053009273250", "text": "RT @Gorden_Sun: 草，太传神了",
             "semantic_bundle": tg.build_semantic_bundle(a, "dotey")}
        plain, rich, link = tm.format_message("dotey", t)
        self.assertTrue(plain.startswith("📢 @Gorden_Sun"))
        self.assertIn("经 @dotey 转发发现", plain)
        self.assertIn("↳ 引用 @Arcadia_Bao", plain)
        self.assertIn("大家都是 fable 级", plain)
        self.assertNotIn("RT @", plain)
        self.assertIn('<img src="https://pbs.twimg.com/media/fable.jpg"/>', rich)
        self.assertEqual(link, "https://x.com/Gorden_Sun/status/2087700436036079825")

    def test_charliermarsh_quote_keeps_quoted_text_and_photo(self):
        photo = {
            "type": "photo",
            "url": "https://t.co/ZD8xcfKq3u",
            "media_url_https": "https://pbs.twimg.com/media/HPt-AdIaQAAREiR.png",
            "original_info": {"width": 896, "height": 322},
        }
        quoted = raw(
            "2088401536057827344",
            "ajambrosino",
            "always nice to see internal slack messages like this– thanks @btraut "
            "https://t.co/ZD8xcfKq3u",
            media=[photo],
        )
        outer = raw(
            "2088405149119127653",
            "charliermarsh",
            "This is looking good",
            quote=quoted,
        )
        tweet = {
            "id": "2088405149119127653",
            "text": "This is looking good",
            "semantic_bundle": tg.build_semantic_bundle(
                outer, "charliermarsh", fetch_mode="graphql_auth"
            ),
        }

        plain, rich, link = tm.format_message("charliermarsh", tweet)

        self.assertIn("This is looking good", plain)
        self.assertIn("↳ 引用 @ajambrosino", plain)
        self.assertIn("always nice to see internal slack messages", plain)
        self.assertIn(
            '<img src="https://pbs.twimg.com/media/HPt-AdIaQAAREiR.png"/>', rich
        )
        self.assertEqual(
            link, "https://x.com/charliermarsh/status/2088405149119127653"
        )

    def test_semantic_renderer_respects_utf16_limits(self):
        c = raw("300", "c", "😀" * 20000)
        b = raw("200", "b", "😀" * 20000, quote=c)
        t = {"id": "200", "semantic_bundle": tg.build_semantic_bundle(b, "b")}
        plain, rich, _ = tm.format_message("b", t)
        self.assertLessEqual(tm._utf16_len(plain), 3900)
        self.assertLessEqual(tm._utf16_len(rich), 29000)

    def test_short_anchor_classifies_after_context(self):
        a, _b, _c = self.fixture()
        t = {"id": "2087708053009273250", "semantic_bundle": tg.build_semantic_bundle(a, "dotey")}
        self.assertEqual(tm.classify_semantic_bundle(t), ("pass", "semantic_context"))

    def test_short_anchor_with_negative_context_does_not_auto_pass(self):
        c = raw("300", "c", "注册送返佣，使用邀请码领取奖励")
        b = raw("200", "b", "this", quote=c)
        bundle = tg.build_semantic_bundle(b, "b")
        self.assertEqual(tm.classify_semantic_bundle(
            {"id": "200", "semantic_bundle": bundle})[0], "filter")

    def test_normal_anchor_with_malicious_context_uses_formal_classifier(self):
        c = raw("300", "c", "注册送返佣，使用邀请码领取奖励")
        b = raw("200", "b", "这是一段长度正常、看起来完全无害的评论文字", quote=c)
        status, reason = tm.classify_semantic_bundle(
            {"id": "200", "semantic_bundle": tg.build_semantic_bundle(b, "b")})
        self.assertEqual(status, "filter")
        self.assertIn("semantic_context:", reason)

    def test_consecutive_reposts_choose_first_non_repost(self):
        c = raw("300", "c", "足够长的最终正文内容用于分类和展示")
        b = raw("200", "b", "RT @c: 壳", retweet=c)
        a = raw("100", "a", "RT @b: 壳", retweet=b)
        bundle = tg.build_semantic_bundle(a, "a")
        self.assertEqual(bundle["anchor"]["tweet_id"], "300")
        self.assertEqual([n["tweet_id"] for n in bundle["repost_path"]], ["100", "200"])
        self.assertEqual(bundle["identity"]["alias_keys"], ["t:100", "t:200"])

    def test_nested_quotes_keep_anchor(self):
        d = raw("400", "d", "D 的上下文正文")
        c = raw("300", "c", "C 的引用评论", quote=d)
        b = raw("200", "b", "B 是锚点评论", quote=c)
        a = raw("100", "a", "RT @b: 壳", retweet=b)
        bundle = tg.build_semantic_bundle(a, "a")
        self.assertEqual(bundle["anchor"]["tweet_id"], "200")
        self.assertEqual([n["tweet_id"] for n in bundle["context_nodes"]], ["300", "400"])

    def test_context_repost_is_unwrapped_without_changing_anchor(self):
        d = raw("400", "d", "D 是被 context repost 的实际正文")
        c = raw("300", "c", "RT @d: 壳", retweet=d)
        b = raw("200", "b", "B 是锚点评论", quote=c)
        bundle = tg.build_semantic_bundle(b, "b")
        self.assertEqual(bundle["anchor"]["tweet_id"], "200")
        self.assertEqual([n["tweet_id"] for n in bundle["context_nodes"]], ["400"])
        self.assertEqual(bundle["context_nodes"][0]["author"], "d")

    def test_missing_required_quote_is_transient(self):
        b = raw("200", "b", "this")
        b["legacy"]["is_quote_status"] = True
        b["legacy"]["quoted_status_id_str"] = "300"
        a = raw("100", "a", "RT @b: this", retweet=b)
        t = {"id": "100", "semantic_bundle": tg.build_semantic_bundle(a, "a")}
        self.assertEqual(t["semantic_bundle"]["resolution"]["status"],
                         "context_unresolved_transient")
        self.assertEqual(tm.classify_semantic_bundle(t)[0], "defer")

    def test_missing_repost_is_required_even_when_shell_is_long(self):
        a = raw("100", "a", "RT @b: " + "这是一段很长的转发壳文字" * 20)
        a["legacy"]["retweeted_status_id_str"] = "200"
        bundle = tg.build_semantic_bundle(a, "a")
        self.assertEqual(bundle["resolution"]["status"], "context_unresolved_transient")

    def test_detail_resolver_recovers_missing_repost_with_bounded_fetch(self):
        b = raw("200", "b", "真正的 anchor 正文内容足够完整")
        a = raw("100", "a", "RT @b: shell")
        a["legacy"]["retweeted_status_id_str"] = "200"
        calls = []
        bundle = tg.resolve_semantic_bundle(a, "a", fetcher=lambda tid: calls.append(tid) or b)
        self.assertEqual(calls, ["200"])
        self.assertEqual(bundle["anchor"]["tweet_id"], "200")
        self.assertEqual(bundle["resolution"]["request_count"], 1)

    def test_detail_terminal_is_typed(self):
        b = raw("200", "b", "this")
        b["legacy"].update({"is_quote_status": True, "quoted_status_id_str": "300"})
        bundle = tg.resolve_semantic_bundle(
            b, "b", fetcher=lambda _tid: {"__typename": "TweetTombstone"})
        self.assertEqual(bundle["resolution"]["status"], "context_unavailable_terminal")

    def test_optional_terminal_context_degrades_instead_of_suppressing(self):
        b = raw("200", "b", "这段正文已经完整解释了发布内容、适用范围和明确结论，不依赖引用也能独立理解。")
        b["legacy"].update({"is_quote_status": True, "quoted_status_id_str": "300"})
        bundle = tg.resolve_semantic_bundle(
            b, "b", fetcher=lambda _tid: {"__typename": "TweetTombstone"})
        self.assertEqual(bundle["resolution"]["status"], "degraded_optional")

    def test_detail_per_run_budget_is_hard(self):
        tg._semantic_run_requests = tg.SEMANTIC_DETAIL_PER_RUN
        out = tg._semantic_detail("999", fetcher=lambda _tid: self.fail("must not fetch"))
        self.assertEqual((out["status"], out["reason"]), ("transient", "run_budget"))

    def test_detail_cache_and_singleflight_do_not_double_fetch(self):
        node = raw("999", "u", "cached body")
        calls = []
        first = tg._semantic_detail("999", fetcher=lambda tid: calls.append(tid) or node)
        second = tg._semantic_detail("999", fetcher=lambda tid: self.fail("cache miss"))
        self.assertFalse(first["cache_hit"])
        self.assertTrue(second["cache_hit"])
        self.assertEqual(calls, ["999"])
        tg._semantic_detail_cache.clear()
        tg._semantic_detail_inflight.add("999")
        busy = tg._semantic_detail("999", fetcher=lambda tid: self.fail("inflight fetched"))
        self.assertEqual(busy["reason"], "single_flight_busy")

    def test_persistent_detail_cache_survives_process_boundary(self):
        node = raw("999", "u", "persisted body")
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(tg, "SEMANTIC_DETAIL_CACHE_PATH",
                          os.path.join(directory, "cache.json")), \
             patch.object(tg, "fetch_article_tweet", return_value=node) as fetch:
            tg._semantic_cache_loaded = True
            first = tg._semantic_detail("999")
            self.assertEqual(first["status"], "complete")
            tg._semantic_detail_cache.clear()
            tg._semantic_cache_loaded = False
            fetch.side_effect = AssertionError("persistent cache miss")
            second = tg._semantic_detail("999")
            self.assertTrue(second["cache_hit"])
            self.assertEqual(second["node"]["legacy"]["id_str"], "999")

    def test_429_retries_once_caps_delay_and_latches_run(self):
        calls = []
        class Clock:
            value = 100.0
            def monotonic(self): return self.value
            def sleep(self, seconds): self.value += seconds
        clock = Clock()
        def rate_limited(tweet_id):
            calls.append(tweet_id)
            raise tg.CurlError("limited", status_code=429, retry_after=60)
        tg._semantic_run_started = clock.monotonic()
        with patch.object(tg.time, "monotonic", side_effect=clock.monotonic), \
             patch.object(tg.time, "sleep", side_effect=clock.sleep) as sleep:
            out = tg._semantic_detail("900", fetcher=rate_limited)
            blocked = tg._semantic_detail("901", fetcher=lambda _tid: self.fail("latched"))
        self.assertEqual(calls, ["900", "900"])
        sleep.assert_called_once_with(30)
        self.assertEqual(out["reason"], "rate_limited")
        self.assertEqual(blocked["reason"], "rate_limit_cooldown")
        self.assertEqual(out["physical_attempts"], 2)
        self.assertEqual(tg._semantic_run_requests, 2)
        self.assertAlmostEqual(tg._semantic_cooldown_until, 160.0)

    def test_429_retry_rechecks_deadline_before_physical_attempt(self):
        class Clock:
            value = 100.0
            def monotonic(self): return self.value
            def sleep(self, seconds): self.value += seconds
        clock = Clock()
        tg._semantic_run_started = 39.0  # 61s elapsed, then 30s crosses deadline
        calls = []
        def limited(_tid):
            calls.append(1)
            raise tg.CurlError("limited", status_code=429, retry_after=30)
        with patch.object(tg.time, "monotonic", side_effect=clock.monotonic), \
             patch.object(tg.time, "sleep", side_effect=clock.sleep):
            out = tg._semantic_detail("902", fetcher=limited)
        self.assertEqual(len(calls), 1)
        self.assertEqual(out["reason"], "run_budget")
        self.assertEqual(out["physical_attempts"], 1)

    def test_run_budget_counts_physical_http_attempts(self):
        tg._semantic_run_requests = 11
        calls = []
        def limited(_tid):
            calls.append(1)
            raise tg.CurlError("limited", status_code=429, retry_after=1)
        with patch.object(tg.time, "sleep"):
            out = tg._semantic_detail("903", fetcher=limited)
        self.assertEqual(len(calls), 1)
        self.assertEqual(tg._semantic_run_requests, 12)
        self.assertEqual(out["physical_attempts"], 1)
        self.assertEqual(out["reason"], "run_budget")

    def test_guest_detail_marks_auth_bundle_degraded(self):
        b = raw("200", "b", "this")
        b["legacy"].update({"is_quote_status": True, "quoted_status_id_str": "300"})
        c = raw("300", "c", "完整的 guest detail context 正文")
        with patch.object(tg, "fetch_article_tweet", return_value=c):
            bundle = tg.resolve_semantic_bundle(b, "b", fetch_mode="graphql_auth")
        self.assertEqual(bundle["resolution"]["status"], "auth_degraded")
        self.assertIn("graphql_guest_detail", bundle["resolution"]["fetch_modes"])

    def test_terminal_detail_status_wins_over_guest_transport(self):
        b = raw("200", "b", "this")
        b["legacy"].update({"is_quote_status": True, "quoted_status_id_str": "300"})
        with patch.object(tg, "fetch_article_tweet",
                          return_value={"__typename": "TweetTombstone"}):
            bundle = tg.resolve_semantic_bundle(b, "b", fetch_mode="graphql_auth")
        self.assertEqual(bundle["resolution"]["status"], "context_unavailable_terminal")
        self.assertIn("graphql_guest_detail", bundle["resolution"]["fetch_modes"])

    def test_outer_auth_plus_guest_detail_is_gray_fail_closed_to_legacy(self):
        b = raw("200", "b", "这是一段足够长的 auth outer legacy 正文")
        b["legacy"].update({"is_quote_status": True, "quoted_status_id_str": "300"})
        c = raw("300", "c", "完整的 guest detail context 正文")
        embedded = tg.build_semantic_bundle(b, "b", fetch_mode="graphql_auth")
        tweet = {"id": "200", "text": b["legacy"]["full_text"],
                 "created_at": b["legacy"]["created_at"], "_semantic_raw": b,
                 "semantic_bundle": embedded}
        tm._SEMANTIC_CURATOR_ALLOWLIST = {"b"}
        observed = []
        with patch.object(tm, "fetch_tweets", return_value=[tweet]), \
             patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
             patch.object(tm, "load_push_retry", return_value=set()), \
             patch.object(tg, "fetch_article_tweet", return_value=c), \
             patch.object(tm, "save_seen"), patch.object(tm, "save_push_retry"), \
             patch.object(tm, "send_tweet",
                          side_effect=lambda *a, **k: observed.append(
                              (a[3].get("_semantic_active"), a[3]["semantic_bundle"]["resolution"]))
                          or {"ok": True}), \
             patch.object(tm, "_CROSS_DEDUP_ENABLED", False), \
             patch.object(tm, "_article_queue_time_remaining", return_value=9999), \
             patch.object(tm.time, "sleep"):
            tm.process_user(None, self.NoAI(), "b", "bot", "chat", self.args())
        self.assertIsNone(observed[0][0])
        self.assertEqual(observed[0][1]["status"], "auth_degraded")

    def test_context_note_tweet_and_expanded_affiliate_url_use_formal_rules(self):
        c = raw("300", "c", "legacy text is intentionally harmless")
        c["note_tweet"] = {"note_tweet_results": {"result": {
            "text": "这是一段足够长的 NoteTweet 内容 https://t.co/abc",
            "entity_set": {"urls": [{"url": "https://t.co/abc",
                                      "expanded_url": "https://example.com/?ref=affiliate"}]}}}}
        b = raw("200", "b", "这是一段正常长度的 anchor 评论", quote=c)
        status, reason = tm.classify_semantic_bundle(
            {"id": "200", "semantic_bundle": tg.build_semantic_bundle(b, "b")})
        self.assertEqual(status, "filter")
        self.assertIn("affiliate_link", reason)

    def test_curl_parses_and_caps_retry_after_header(self):
        def fake_run(cmd, **_kwargs):
            header_path = cmd[cmd.index("-D") + 1]
            with open(header_path, "w") as stream:
                stream.write("HTTP/2 429\r\nRetry-After: 60\r\n\r\n")
            return types.SimpleNamespace(stdout="limited\n429\n0", stderr="")
        with patch.object(tg.subprocess, "run", side_effect=fake_run):
            with self.assertRaises(tg.CurlError) as raised:
                tg._curl("https://example.com")
        self.assertEqual(raised.exception.status_code, 429)
        self.assertEqual(raised.exception.retry_after, 30)

    def test_terminal_graphql_error_fixtures_are_typed(self):
        fixtures = [
            ({"errors": [{"code": 144, "message": "No status found"}]}, "TweetNotFound"),
            ({"errors": [{"code": 179, "message": "Not authorized"}]}, "TweetUnavailable"),
        ]
        for payload, typename in fixtures:
            with self.subTest(typename=typename), \
                 patch.object(tg, "_get_guest_token", return_value="guest"), \
                 patch.object(tg, "_curl", return_value=json.dumps(payload)):
                self.assertEqual(tg.fetch_article_tweet("999", raise_errors=True),
                                 {"__typename": typename})

    def test_detail_deadline_is_hard(self):
        tg._semantic_run_started -= tg.SEMANTIC_RESOLVER_DEADLINE_SECONDS + 1
        out = tg._semantic_detail("999", fetcher=lambda _tid: self.fail("deadline fetched"))
        self.assertEqual(out["reason"], "run_budget")

    def test_per_bundle_detail_budget_never_exceeds_two(self):
        c = raw("300", "c", "this")
        c["legacy"].update({"is_quote_status": True, "quoted_status_id_str": "400"})
        b = raw("200", "b", "RT @c: shell")
        b["legacy"]["retweeted_status_id_str"] = "300"
        a = raw("100", "a", "RT @b: shell")
        a["legacy"]["retweeted_status_id_str"] = "200"
        nodes = {"200": b, "300": c, "400": raw("400", "d", "deep")}
        calls = []
        bundle = tg.resolve_semantic_bundle(
            a, "a", fetcher=lambda tid: calls.append(tid) or nodes[tid])
        self.assertEqual(len(calls), 2)
        self.assertLessEqual(bundle["resolution"]["request_count"], 2)

    def test_guest_bundle_is_explicitly_auth_degraded(self):
        b = raw("200", "b", "完整正文内容足够通过常规判断")
        bundle = tg.build_semantic_bundle(b, "b", fetch_mode="graphql_guest")
        self.assertEqual(bundle["resolution"]["status"], "auth_degraded")

    def test_terminal_resolution_journal_before_seen(self):
        b = raw("200", "b", "this")
        bundle = tg.build_semantic_bundle(b, "b")
        bundle["resolution"]["status"] = "context_unavailable_terminal"
        tweet = {"id": "200", "text": "this", "created_at": b["legacy"]["created_at"],
                 "semantic_bundle": bundle}
        order = []
        with patch.object(tm, "fetch_tweets", return_value=[tweet]), \
             patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
             patch.object(tm, "load_push_retry", return_value=set()), \
             patch.object(tm, "_journal_semantic_decision",
                          side_effect=lambda *a, **k: order.append("journal")), \
             patch.object(tm, "save_seen", side_effect=lambda *a, **k: order.append("seen")), \
             patch.object(tm, "save_push_retry"):
            tm.process_user(None, self.NoAI(), "b", "bot", "chat", self.args())
        self.assertLess(order.index("journal"), order.index("seen"))

    def test_terminal_journal_failure_keeps_unseen(self):
        b = raw("200", "b", "this")
        bundle = tg.build_semantic_bundle(b, "b")
        bundle["resolution"]["status"] = "context_unavailable_terminal"
        tweet = {"id": "200", "text": "this", "created_at": b["legacy"]["created_at"],
                 "semantic_bundle": bundle}
        saved = []
        with patch.object(tm, "fetch_tweets", return_value=[tweet]), \
             patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
             patch.object(tm, "load_push_retry", return_value=set()), \
             patch.object(tm, "_journal_semantic_decision", side_effect=OSError("disk")), \
             patch.object(tm, "save_seen", side_effect=lambda u, s, ts=None: saved.append(set(s))), \
             patch.object(tm, "save_push_retry"):
            tm.process_user(None, self.NoAI(), "b", "bot", "chat", self.args())
        self.assertNotIn("200", saved[-1])

    def test_expired_transient_journals_before_seen(self):
        b = raw("200", "b", "this")
        b["legacy"]["created_at"] = "Thu Aug 10 01:30:00 +0000 2023"
        b["legacy"].update({"is_quote_status": True, "quoted_status_id_str": "300"})
        tweet = {"id": "200", "text": "this", "created_at": b["legacy"]["created_at"],
                 "semantic_bundle": tg.build_semantic_bundle(b, "b")}
        order = []
        with patch.object(tm, "fetch_tweets", return_value=[tweet]), \
             patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
             patch.object(tm, "load_push_retry", return_value={"200"}), \
             patch.object(tm, "_journal_semantic_decision",
                          side_effect=lambda *a, **k: order.append("journal")), \
             patch.object(tm, "save_seen", side_effect=lambda *a, **k: order.append("seen")), \
             patch.object(tm, "save_push_retry"):
            tm.process_user(None, self.NoAI(), "b", "bot", "chat", self.args())
        self.assertLess(order.index("journal"), order.index("seen"))

    def test_retry_v2_persists_attempts_and_observation_times(self):
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(tm, "SEEN_DIR", directory):
            tm._PUSH_RETRY_STATE_BY_USER.clear()
            record = tm.note_push_retry("b", {
                "id": "200", "created_at": "Thu Aug 13 01:30:00 +0000 2026"})
            tm.save_push_retry("b", {"200"})
            tm._PUSH_RETRY_STATE_BY_USER.clear()
            self.assertEqual(tm.load_push_retry("b"), {"200"})
            loaded = tm._PUSH_RETRY_STATE_BY_USER["b"]["200"]
            self.assertEqual(loaded["attempts"], record["attempts"])
            self.assertEqual(loaded["outer_created_at"],
                             "Thu Aug 13 01:30:00 +0000 2026")
            self.assertTrue(loaded["first_deferred_at"])

    def test_retry_expires_by_first_deferred_time_without_outer_time(self):
        record = {"attempts": 1, "first_deferred_at":
                  (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()}
        self.assertTrue(tm._semantic_retry_expired(record))

    def test_transient_resolution_keeps_outer_unseen_and_retries(self):
        b = raw("200", "b", "this")
        b["legacy"]["is_quote_status"] = True
        b["legacy"]["quoted_status_id_str"] = "300"
        a = raw("100", "a", "RT @b: this", retweet=b)
        tweet = {"id": "100", "text": "RT @b: this",
                 "created_at": datetime.now(timezone.utc).strftime(
                     "%a %b %d %H:%M:%S +0000 %Y"),
                 "semantic_bundle": tg.build_semantic_bundle(a, "a")}
        final_seen = []
        final_retry = []

        class NoAI:
            def is_available(self):
                return False

        args = argparse.Namespace(test=False, seed=False, dry_run=False, limit=20,
                                  max_push_age_minutes=45, test_count=1)
        with patch.object(tm, "fetch_tweets", return_value=[tweet]), \
             patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
             patch.object(tm, "load_push_retry", return_value=set()), \
             patch.object(tm, "save_seen", side_effect=lambda u, s, ts=None: final_seen.append(set(s))), \
             patch.object(tm, "save_push_retry", side_effect=lambda u, s: final_retry.append(set(s))), \
             patch.object(tm, "_CROSS_DEDUP_ENABLED", False):
            tm.process_user(None, NoAI(), "a", "bot", "chat", args)
        self.assertNotIn("100", final_seen[-1])
        self.assertIn("100", final_retry[-1])

    def test_dry_run_does_not_persist_seen_or_retry(self):
        a, _b, _c = self.fixture()
        tweet = {"id": "2087708053009273250", "text": "RT shell",
                 "created_at": "Thu Aug 13 01:30:00 +0000 2026",
                 "semantic_bundle": tg.build_semantic_bundle(a, "dotey")}

        class NoAI:
            def is_available(self):
                return False

        args = argparse.Namespace(test=False, seed=False, dry_run=True, limit=20,
                                  max_push_age_minutes=45, test_count=1)
        with patch.object(tm, "fetch_tweets", return_value=[tweet]), \
             patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
             patch.object(tm, "load_push_retry", return_value=set()), \
             patch.object(tm, "save_seen") as save_seen, \
             patch.object(tm, "save_push_retry") as save_retry, \
             patch.object(tm, "_journal_semantic_decision") as journal, \
             patch.object(tm, "_CROSS_DEDUP_ENABLED", False):
            tm.process_user(None, NoAI(), "dotey", "bot", "chat", args)
        save_seen.assert_not_called()
        save_retry.assert_not_called()
        journal.assert_not_called()

    def test_independent_anchor_can_degrade_when_quote_missing(self):
        b = raw("200", "b", "这段正文已经完整解释了发布内容、适用范围和明确结论，不依赖引用也能独立理解。")
        b["legacy"]["is_quote_status"] = True
        b["legacy"]["quoted_status_id_str"] = "300"
        bundle = tg.build_semantic_bundle(b, "b")
        self.assertEqual(bundle["resolution"]["status"], "degraded_optional")
        result = tm.classify_semantic_bundle({"id": "200", "semantic_bundle": bundle})
        self.assertNotEqual(result[0], "defer")

    def test_anchor_dedup_is_symmetric_and_v1_compatible(self):
        a, b, _c = self.fixture()
        repost = {"id": "2087708053009273250", "semantic_bundle": tg.build_semantic_bundle(a, "dotey")}
        direct = {"id": "2087700436036079825", "semantic_bundle": tg.build_semantic_bundle(b, "Gorden_Sun")}
        tm._PUSHED_INDEX_CACHE = {}
        self.assertIsNone(tm._cross_dup_hit(repost))
        with patch.object(tm, "save_pushed_index"):
            tm._record_pushed(repost, "dotey")
        self.assertEqual(tm._canonical_key(direct), "t:2087700436036079825")
        self.assertEqual(tm._cross_dup_hit(direct)["by"], "dotey")

    def test_context_article_keeps_context_owner_and_anchor_comment(self):
        article = {"rest_id": "777", "title": "C article", "preview_text": "preview"}
        c = raw("300", "c", "article shell", article=article)
        b = raw("200", "b", "这篇文章值得看", quote=c)
        a = raw("100", "a", "RT @b: 壳", retweet=b)
        t = {"id": "100", "semantic_bundle": tg.build_semantic_bundle(a, "a")}
        ref = t["semantic_bundle"]["article_refs"][0]
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(tm, "ARTICLE_QUEUE_DIR", directory), \
             patch.object(tm, "_CROSS_DEDUP_ENABLED", False):
            tm.save_semantic_article("a", t, ref)
            import json
            with open(os.path.join(directory, "a_queue.json")) as stream:
                entry = json.load(stream)[0]
        self.assertEqual(entry["tweet_id"], "300")
        self.assertEqual(entry["author"], "c")
        self.assertEqual(entry["quote_comment"], "这篇文章值得看")
        self.assertEqual(entry["comment_author"], "b")
        rendered = tm.format_article_summary_rich("a", entry, "摘要正文")
        self.assertIn("> @b 引用：", rendered)

    def test_anchor_article_keeps_anchor_author_and_url(self):
        article = {"rest_id": "777", "title": "B article", "preview_text": "preview"}
        b = raw("200", "b", "anchor article", article=article)
        t = {"id": "200", "semantic_bundle": tg.build_semantic_bundle(b, "observer")}
        ref = t["semantic_bundle"]["article_refs"][0]
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(tm, "ARTICLE_QUEUE_DIR", directory), \
             patch.object(tm, "_CROSS_DEDUP_ENABLED", False):
            tm.save_semantic_article("observer", t, ref)
            import json
            with open(os.path.join(directory, "observer_queue.json")) as stream:
                entry = json.load(stream)[0]
        self.assertEqual((entry["tweet_id"], entry["author"]), ("200", "b"))
        self.assertEqual(tm.article_fetch_url("observer", entry), "https://x.com/b/status/200")

    def test_official_retweet_article_is_filtered_before_queue(self):
        article = {"rest_id": "777", "title": "B article", "preview_text": "preview"}
        b = raw("200", "b", "We've reset weekly limits for paid users.", article=article)
        a = raw("100", "official", "RT @b: We've reset weekly limits for paid users.",
                retweet=b)
        tweet = {"id": "100", "text": "RT @b: We've reset weekly limits for paid users.",
                 "retweeted_status": {"id": "200", "screen_name": "b"},
                 "created_at": "Thu Aug 13 01:30:00 +0000 2026",
                 "semantic_bundle": tg.build_semantic_bundle(a, "official")}
        prior = dict(tm._ACCOUNT_CONFIG_BY_USERNAME)
        tm._SEMANTIC_CURATOR_ALLOWLIST = {"official"}
        tm._ACCOUNT_CONFIG_BY_USERNAME = {
            "official": {"username": "official", "push_policy": "claude_dev_original"}}
        try:
            with patch.object(tm, "fetch_tweets", return_value=[tweet]), \
                 patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
                 patch.object(tm, "load_push_retry", return_value=set()), \
                 patch.object(tm, "save_seen"), patch.object(tm, "save_push_retry"), \
                 patch.object(tm, "save_semantic_article") as save_article, \
                 patch.object(tm, "_journal_semantic_decision"):
                tm.process_user(None, self.NoAI(), "official", "bot", "chat", self.args())
            save_article.assert_not_called()
        finally:
            tm._ACCOUNT_CONFIG_BY_USERNAME = prior

    def test_live_pending_claim_does_not_advance_seen(self):
        b = raw("200", "b", "足够长的正常正文用于通过筛选并进入发送阶段")
        tweet = {"id": "200", "text": b["legacy"]["full_text"],
                 "created_at": "Thu Aug 13 01:30:00 +0000 2026",
                 "semantic_bundle": tg.build_semantic_bundle(b, "b")}
        saved = []
        with patch.object(tm, "fetch_tweets", return_value=[tweet]), \
             patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
             patch.object(tm, "load_push_retry", return_value=set()), \
             patch.object(tm, "save_seen", side_effect=lambda u, s, ts=None: saved.append(set(s))), \
             patch.object(tm, "save_push_retry"), \
             patch.object(tm, "_EVENT_DEDUP_EFFECTIVE_MODE", "observe"), \
             patch.object(tm, "claim_event_delivery", return_value={"claimed": False, "state": "pending"}), \
             patch.object(tm, "_article_queue_time_remaining", return_value=9999):
            tm.process_user(None, self.NoAI(), "b", "bot", "chat", self.args())
        self.assertNotIn("200", saved[-1])

    def test_same_article_different_quote_anchors_are_distinct_queue_entries(self):
        article = {"rest_id": "777", "title": "C article", "preview_text": "preview"}
        c = raw("300", "c", "article", article=article)
        b1 = raw("201", "b1", "第一种解读足够长", quote=c)
        b2 = raw("202", "b2", "第二种解读足够长", quote=c)
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(tm, "ARTICLE_QUEUE_DIR", directory), \
             patch.object(tm, "_CROSS_DEDUP_ENABLED", True), \
             patch.object(tm, "load_pushed_index", return_value={"a:777": {"by": "old"}}):
            for node in (b1, b2):
                tweet = {"id": node["legacy"]["id_str"],
                         "semantic_bundle": tg.build_semantic_bundle(node, "observer")}
                tm.save_semantic_article("observer", tweet,
                                         tweet["semantic_bundle"]["article_refs"][0])
            import json
            with open(os.path.join(directory, "observer_queue.json")) as stream:
                entries = json.load(stream)
        self.assertEqual(len(entries), 2)
        self.assertNotEqual(entries[0]["bundle_key"], entries[1]["bundle_key"])

    def test_v1_article_index_suppresses_no_comment_but_preserves_real_quote(self):
        article = {"rest_id": "777", "title": "C article", "preview_text": "preview"}
        direct = raw("300", "c", "article", article=article)
        quoted = raw("200", "b", "这是有实质观点的引用评论", quote=direct)
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(tm, "ARTICLE_QUEUE_DIR", directory), \
             patch.object(tm, "_CROSS_DEDUP_ENABLED", True), \
             patch.object(tm, "load_pushed_index", return_value={"a:777": {"by": "legacy"}}):
            direct_t = {"id": "300", "semantic_bundle": tg.build_semantic_bundle(direct, "c")}
            tm.save_semantic_article("c", direct_t,
                                     direct_t["semantic_bundle"]["article_refs"][0])
            quoted_t = {"id": "200", "semantic_bundle": tg.build_semantic_bundle(quoted, "b")}
            tm.save_semantic_article("b", quoted_t,
                                     quoted_t["semantic_bundle"]["article_refs"][0])
            self.assertFalse(os.path.exists(os.path.join(directory, "c_queue.json")))
            with open(os.path.join(directory, "b_queue.json")) as stream:
                entry = json.load(stream)[0]
        self.assertEqual(entry["bundle_key"], "t:200")
        self.assertEqual(entry["quote_comment"], "这是有实质观点的引用评论")

    def test_article_queue_sends_same_asset_for_two_quote_bundle_identities(self):
        now = datetime.now(timezone.utc).isoformat()
        entries = [{"article_id": "777", "bundle_key": key, "tweet_id": key[2:],
                    "author": "c", "comment_author": author,
                    "article_title": "T", "status": "pending", "attempts": 0,
                    "detected_at": now, "content": None, "quote_comment": comment}
                   for key, author, comment in (
                       ("t:201", "b1", "第一种实质解读"),
                       ("t:202", "b2", "第二种实质解读"))]
        sent = []
        with tempfile.TemporaryDirectory() as directory:
            queue_path = os.path.join(directory, "observer_queue.json")
            with open(queue_path, "w") as stream:
                json.dump(entries, stream)
            tm._PUSHED_INDEX_CACHE = {}
            with patch.object(tm, "ARTICLE_QUEUE_DIR", directory), \
                 patch.object(tm, "ARTICLE_CACHE_DIR", os.path.join(directory, "cache")), \
                 patch.object(tm, "_CROSS_DEDUP_ENABLED", True), \
                 patch.object(tm, "save_pushed_index"), \
                 patch.object(tm, "fetch_article_markdown", return_value=("# T\n\n正文" * 30, None)), \
                 patch.object(tm, "summarize_article", return_value=("摘要正文", "fake")), \
                 patch.object(tm, "send_telegram_rich",
                              side_effect=lambda *a, **k: sent.append(a[2]) or {"ok": True}), \
                 patch.object(tm, "cache_article_markdown", return_value=""), \
                 patch.object(tm, "delete_article_cache"), \
                 patch.object(tm, "cleanup_old_article_cache"), \
                 patch.object(tm, "_tg_post_quiet", return_value={"ok": True}), \
                 patch.object(tm.time, "sleep"):
                processed = tm.process_article_queue(self.NoAI(), "bot", "chat")
        self.assertEqual(processed, 2)
        self.assertEqual(len(sent), 2)
        self.assertIn("@b1", sent[0])
        self.assertIn("@b2", sent[1])

    def test_article_delivery_gate_uses_bundle_identity_not_shared_asset(self):
        tm._PUSHED_INDEX_CACHE = {}
        with patch.object(tm, "save_pushed_index"):
            tm._record_pushed_article("777", "observer", "t:201")
            tm._record_pushed_article("777", "observer", "t:202")
        self.assertIn("ab:t:201", tm._PUSHED_INDEX_CACHE)
        self.assertIn("ab:t:202", tm._PUSHED_INDEX_CACHE)
        self.assertNotIn("a:777", tm._PUSHED_INDEX_CACHE)

    def test_media_host_allowlist(self):
        self.assertEqual(tm._safe_x_media_url("https://evil.example/a.jpg"), "")
        self.assertEqual(tm._safe_x_media_url("https://pbs.twimg.com/a.jpg"),
                         "https://pbs.twimg.com/a.jpg")

    def test_curator_allowlist_is_enforced(self):
        tm._SEMANTIC_CURATOR_ALLOWLIST = {"dotey"}
        self.assertTrue(tm._semantic_gray_for("dotey"))
        self.assertFalse(tm._semantic_gray_for("official"))

    def test_fallback_and_auth_degraded_never_enter_gray_delivery(self):
        b = raw("200", "b", "足够长的传统路径正文，用于确认降级来源不会启用语义发送")
        for marker in ("fallback", "guest"):
            with self.subTest(marker=marker):
                tweet = {"id": "200", "text": b["legacy"]["full_text"],
                         "created_at": b["legacy"]["created_at"],
                         "semantic_bundle": tg.build_semantic_bundle(
                             b, "b", fetch_mode="graphql_guest" if marker == "guest"
                             else "graphql_auth")}
                if marker == "fallback":
                    tweet["_fetch_source_mode"] = "6551_degraded_no_reposts"
                tm._SEMANTIC_CURATOR_ALLOWLIST = {"b"}
                observed = []
                with patch.object(tm, "fetch_tweets", return_value=[tweet]), \
                     patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
                     patch.object(tm, "load_push_retry", return_value=set()), \
                     patch.object(tm, "save_seen"), patch.object(tm, "save_push_retry"), \
                     patch.object(tm, "send_tweet",
                                  side_effect=lambda *a, **k: observed.append(a[3].get("_semantic_active"))
                                  or {"ok": True}), \
                     patch.object(tm, "_article_queue_time_remaining", return_value=9999), \
                     patch.object(tm, "_CROSS_DEDUP_ENABLED", False), \
                     patch.object(tm.time, "sleep"):
                    tm.process_user(None, self.NoAI(), "b", "bot", "chat", self.args())
                self.assertEqual(observed, [None])

    def test_shadow_mode_changes_only_append_only_ledger(self):
        b = raw("200", "b", "这是一段足够长且正常的正文，用于验证 shadow 不改变任何投递状态")
        base = {"id": "200", "text": b["legacy"]["full_text"],
                "created_at": b["legacy"]["created_at"],
                "semantic_bundle": tg.build_semantic_bundle(b, "b"), "_semantic_raw": b}
        prior_enabled, prior_shadow = tm._SEMANTIC_BUNDLE_ENABLED, tm._SEMANTIC_BUNDLE_SHADOW
        tm._SEMANTIC_BUNDLE_ENABLED = False
        outcomes = []
        try:
            with tempfile.TemporaryDirectory() as directory, \
                 patch.object(tm, "SEMANTIC_SHADOW_LEDGER", os.path.join(directory, "shadow.jsonl")):
                for shadow in (False, True):
                    tm._SEMANTIC_BUNDLE_SHADOW = shadow
                    state = {"sent": [], "seen": [], "retry": [], "journal": []}
                    with patch.object(tm, "fetch_tweets", return_value=[copy.deepcopy(base)]), \
                         patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
                         patch.object(tm, "load_push_retry", return_value=set()), \
                         patch.object(tm, "save_seen",
                                      side_effect=lambda _u, ids, _ts=None: state["seen"].append(sorted(ids))), \
                         patch.object(tm, "save_push_retry",
                                      side_effect=lambda _u, ids: state["retry"].append(sorted(ids))), \
                         patch.object(tm, "send_tweet",
                                      side_effect=lambda *a, **k: state["sent"].append(a[3]["id"])
                                      or {"ok": True}), \
                         patch.object(tm, "_journal_semantic_decision",
                                      side_effect=lambda *a, **k: state["journal"].append(a)), \
                         patch.object(tm, "_article_queue_time_remaining", return_value=9999), \
                         patch.object(tm, "_CROSS_DEDUP_ENABLED", False), \
                         patch.object(tm.time, "sleep"):
                        result = tm.process_user(None, self.NoAI(), "b", "bot", "chat", self.args())
                    outcomes.append((result, state))
                self.assertEqual(outcomes[0], outcomes[1])
                with open(tm.SEMANTIC_SHADOW_LEDGER) as stream:
                    ledger = [json.loads(line) for line in stream if line.strip()]
                self.assertEqual(len(ledger), 1)
                self.assertIn("legacy_classification", ledger[0])
                self.assertIn("semantic_classification", ledger[0])
                self.assertIn("resolution", ledger[0])
        finally:
            tm._SEMANTIC_BUNDLE_ENABLED = prior_enabled
            tm._SEMANTIC_BUNDLE_SHADOW = prior_shadow

    def test_shadow_resolver_and_ledger_failures_are_process_level_fail_open(self):
        b = raw("200", "b", "这是一段足够长且正常的正文，用于验证增强异常不影响旧发送路径")
        base = {"id": "200", "text": b["legacy"]["full_text"],
                "created_at": b["legacy"]["created_at"], "_semantic_raw": b,
                "semantic_bundle": tg.build_semantic_bundle(b, "b")}
        old_enabled, old_shadow = tm._SEMANTIC_BUNDLE_ENABLED, tm._SEMANTIC_BUNDLE_SHADOW
        tm._SEMANTIC_BUNDLE_ENABLED, tm._SEMANTIC_BUNDLE_SHADOW = False, True
        outcomes = []
        try:
            for failure in ("none", "resolver", "ledger"):
                state = {"sent": [], "seen": [], "retry": [], "index": []}
                resolver_effect = (RuntimeError("resolver boom") if failure == "resolver"
                                   else tg.build_semantic_bundle(b, "b"))
                ledger_effect = OSError("ledger boom") if failure == "ledger" else None
                with patch.object(tm, "fetch_tweets", return_value=[copy.deepcopy(base)]), \
                     patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
                     patch.object(tm, "load_push_retry", return_value=set()), \
                     patch.object(tg, "resolve_semantic_bundle", side_effect=resolver_effect), \
                     patch.object(tm, "_append_shadow_observation", side_effect=ledger_effect), \
                     patch.object(tm, "save_seen",
                                  side_effect=lambda _u, ids, _ts=None: state["seen"].append(sorted(ids))), \
                     patch.object(tm, "save_push_retry",
                                  side_effect=lambda _u, ids: state["retry"].append(sorted(ids))), \
                     patch.object(tm, "send_tweet",
                                  side_effect=lambda *a, **k: state["sent"].append(a[3]["id"])
                                  or {"ok": True}), \
                     patch.object(tm, "_record_pushed",
                                  side_effect=lambda t, u: state["index"].append((t["id"], u))), \
                     patch.object(tm, "_CROSS_DEDUP_ENABLED", True), \
                     patch.object(tm, "_cross_dup_hit", return_value=None), \
                     patch.object(tm, "_article_queue_time_remaining", return_value=9999), \
                     patch.object(tm.time, "sleep"):
                    result = tm.process_user(None, self.NoAI(), "b", "bot", "chat", self.args())
                outcomes.append((result, state))
        finally:
            tm._SEMANTIC_BUNDLE_ENABLED, tm._SEMANTIC_BUNDLE_SHADOW = old_enabled, old_shadow
        self.assertEqual(outcomes[0], outcomes[1])
        self.assertEqual(outcomes[0], outcomes[2])

    def test_shadow_ledger_has_one_observation_for_every_provider_input(self):
        body = "这是足够长的普通正文，用于验证 shadow 分母包含所有来源和失败样本"
        tweets = [
            {"id": "201", "text": body, "created_at": "Thu Aug 13 01:30:00 +0000 2026"},
            {"id": "202", "text": body, "created_at": "Thu Aug 13 01:30:00 +0000 2026",
             "_fetch_source_mode": "6551_degraded_no_reposts"},
            {"id": "203", "text": body, "created_at": "Thu Aug 13 01:30:00 +0000 2026",
             "semantic_bundle_error": "RuntimeError"},
        ]
        old_enabled, old_shadow = tm._SEMANTIC_BUNDLE_ENABLED, tm._SEMANTIC_BUNDLE_SHADOW
        tm._SEMANTIC_BUNDLE_ENABLED, tm._SEMANTIC_BUNDLE_SHADOW = False, True
        try:
            with tempfile.TemporaryDirectory() as directory, \
                 patch.object(tm, "SEMANTIC_SHADOW_LEDGER", os.path.join(directory, "shadow.jsonl")), \
                 patch.object(tm, "fetch_tweets", return_value=copy.deepcopy(tweets)), \
                 patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
                 patch.object(tm, "load_push_retry", return_value=set()), \
                 patch.object(tm, "save_seen"), patch.object(tm, "save_push_retry"), \
                 patch.object(tm, "send_tweet", return_value={"ok": True}), \
                 patch.object(tm, "_CROSS_DEDUP_ENABLED", False), \
                 patch.object(tm, "_article_queue_time_remaining", return_value=9999), \
                 patch.object(tm.time, "sleep"):
                tm.process_user(None, self.NoAI(), "b", "bot", "chat", self.args())
                with open(tm.SEMANTIC_SHADOW_LEDGER) as stream:
                    rows = [json.loads(line) for line in stream]
        finally:
            tm._SEMANTIC_BUNDLE_ENABLED, tm._SEMANTIC_BUNDLE_SHADOW = old_enabled, old_shadow
        self.assertEqual([row["tweet_id"] for row in rows], ["201", "202", "203"])
        for row in rows:
            self.assertEqual(row["account"], "b")
            self.assertIn("observation", row)
            self.assertIn("legacy_classification", row)
            self.assertIn("semantic_classification", row)
            self.assertIn("physical_attempts", row["resolver_io"])
            self.assertIn("run_physical_attempts", row["resolver_io"])
            self.assertIn("deadline_remaining_seconds", row["resolver_io"])
            self.assertIn("cooldown_remaining_seconds", row["resolver_io"])
        self.assertEqual(rows[1]["source_mode"], "6551_degraded_no_reposts")
        self.assertEqual(rows[2]["exception"], "RuntimeError")

    def test_shadow_records_pre_ai_and_final_ai_decision_once(self):
        class PromoAI:
            def is_available(self): return True
            def confirm_promo(self, _username, _text): return False, "not_promo"
        tweet = {"id": "204", "text":
                 "byteplus seedance 2.0 api 文档访问体验开通模型冲 200 立即体验方舟平台",
                 "created_at": "Thu Aug 13 01:30:00 +0000 2026"}
        old_enabled, old_shadow = tm._SEMANTIC_BUNDLE_ENABLED, tm._SEMANTIC_BUNDLE_SHADOW
        tm._SEMANTIC_BUNDLE_ENABLED, tm._SEMANTIC_BUNDLE_SHADOW = False, True
        try:
            with tempfile.TemporaryDirectory() as directory, \
                 patch.object(tm, "SEMANTIC_SHADOW_LEDGER", os.path.join(directory, "shadow.jsonl")), \
                 patch.object(tm, "fetch_tweets", return_value=[tweet]), \
                 patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
                 patch.object(tm, "load_push_retry", return_value=set()), \
                 patch.object(tm, "save_seen"), patch.object(tm, "save_push_retry"), \
                 patch.object(tm, "send_tweet", return_value={"ok": True}), \
                 patch.object(tm, "_CROSS_DEDUP_ENABLED", False), \
                 patch.object(tm, "_article_queue_time_remaining", return_value=9999), \
                 patch.object(tm.time, "sleep"):
                tm.process_user(None, PromoAI(), "b", "bot", "chat", self.args())
                with open(tm.SEMANTIC_SHADOW_LEDGER) as stream:
                    rows = [json.loads(line) for line in stream]
        finally:
            tm._SEMANTIC_BUNDLE_ENABLED, tm._SEMANTIC_BUNDLE_SHADOW = old_enabled, old_shadow
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["pre_ai_classification"]["status"], "suspicious")
        self.assertEqual(rows[0]["final_classification"]["status"], "pass")
        self.assertIn("not_promo", rows[0]["final_classification"]["reason"])

    def test_seen_shadow_is_embedded_only_one_record_and_spends_no_detail_budget(self):
        node = raw("205", "b", "this")
        node["legacy"].update({"is_quote_status": True, "quoted_status_id_str": "305"})
        tweet = {"id": "205", "text": "this", "_semantic_raw": node,
                 "semantic_bundle": tg.build_semantic_bundle(node, "b")}
        old_enabled, old_shadow = tm._SEMANTIC_BUNDLE_ENABLED, tm._SEMANTIC_BUNDLE_SHADOW
        tm._SEMANTIC_BUNDLE_ENABLED, tm._SEMANTIC_BUNDLE_SHADOW = False, True
        tg._semantic_run_requests = 0
        try:
            with tempfile.TemporaryDirectory() as directory, \
                 patch.object(tm, "SEMANTIC_SHADOW_LEDGER", os.path.join(directory, "shadow.jsonl")), \
                 patch.object(tm, "fetch_tweets", return_value=[tweet]), \
                 patch.object(tm, "load_seen", return_value=({"205"}, None)), \
                 patch.object(tm, "load_push_retry", return_value=set()), \
                 patch.object(tg, "resolve_semantic_bundle",
                              side_effect=AssertionError("seen must not resolve")), \
                 patch.object(tm, "save_seen"), patch.object(tm, "save_push_retry"), \
                 patch.object(tm, "send_tweet") as send:
                tm.process_user(None, self.NoAI(), "b", "bot", "chat", self.args())
                with open(tm.SEMANTIC_SHADOW_LEDGER) as stream:
                    rows = [json.loads(line) for line in stream]
        finally:
            tm._SEMANTIC_BUNDLE_ENABLED, tm._SEMANTIC_BUNDLE_SHADOW = old_enabled, old_shadow
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["disposition"], "duplicate_seen_embedded_only")
        self.assertEqual(rows[0]["final_classification"],
                         {"status": "duplicate", "reason": "already_seen"})
        self.assertEqual(rows[0]["resolver_io"]["physical_attempts"], 0)
        self.assertEqual(tg._semantic_run_requests, 0)
        send.assert_not_called()

    def test_seen_missing_context_does_not_steal_new_candidate_detail_budget(self):
        old = raw("205", "b", "this")
        old["legacy"].update({"is_quote_status": True, "quoted_status_id_str": "305"})
        new = raw("206", "b", "this")
        new["legacy"].update({"is_quote_status": True, "quoted_status_id_str": "306"})
        tweets = [{"id": "205", "text": "this", "_semantic_raw": old,
                   "semantic_bundle": tg.build_semantic_bundle(old, "b")},
                  {"id": "206", "text": "this", "_semantic_raw": new,
                   "created_at": "Thu Aug 13 01:30:00 +0000 2026",
                   "semantic_bundle": tg.build_semantic_bundle(new, "b")}]
        tm._SEMANTIC_CURATOR_ALLOWLIST = {"b"}
        calls = []
        with patch.object(tm, "fetch_tweets", return_value=tweets), \
             patch.object(tm, "load_seen", return_value=({"205"}, None)), \
             patch.object(tm, "load_push_retry", return_value=set()), \
             patch.object(tg, "fetch_article_tweet",
                          side_effect=lambda tid, **_k: calls.append(tid) or raw(tid, "c", "context")), \
             patch.object(tm, "save_seen"), patch.object(tm, "save_push_retry"), \
             patch.object(tm, "send_tweet", return_value={"ok": True}), \
             patch.object(tm, "_CROSS_DEDUP_ENABLED", False), \
             patch.object(tm, "_article_queue_time_remaining", return_value=9999), \
             patch.object(tm.time, "sleep"):
            tm.process_user(None, self.NoAI(), "b", "bot", "chat", self.args())
        self.assertEqual(calls, ["306"])

    def test_retry_outer_missing_from_timeline_is_recovered(self):
        b = raw("200", "b", "恢复后的正文足够长且能够正常进入候选处理")
        outer = raw("100", "dotey", "RT @b: shell", retweet=b)
        recovered = {"id": "100", "text": "RT shell",
                     "created_at": outer["legacy"]["created_at"],
                     "semantic_bundle": tg.build_semantic_bundle(outer, "dotey")}
        tm._SEMANTIC_CURATOR_ALLOWLIST = {"dotey"}
        pushed = []
        with patch.object(tm, "fetch_tweets", return_value=[{
                "id": "999", "text": "old ordinary timeline item long enough",
                "created_at": outer["legacy"]["created_at"]}]), \
             patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
             patch.object(tm, "load_push_retry", return_value={"100"}), \
             patch.object(tg, "fetch_semantic_tweet", return_value=recovered) as recover, \
             patch.object(tm, "save_seen"), patch.object(tm, "save_push_retry"), \
             patch.object(tm, "send_tweet", side_effect=lambda *a, **k: pushed.append(a[3]["id"]) or {"ok": True}), \
             patch.object(tm, "_article_queue_time_remaining", return_value=9999), \
             patch.object(tm.time, "sleep"):
            tm.process_user(None, self.NoAI(), "dotey", "bot", "chat", self.args())
        recover.assert_called_once_with("100", "dotey")
        self.assertIn("100", pushed)

    def test_dropout_transient_sixth_attempt_expires_journal_before_seen(self):
        recovered = {"id": "100", "text": "this", "created_at": "",
                     "semantic_bundle": {"anchor": {"tweet_id": "100", "author": "b",
                                                       "text": "this"},
                        "observation": {"outer_id": "100", "observed_via": "b",
                                        "fetch_mode": "detail_retry"},
                        "context_nodes": [], "assets": [], "article_refs": [],
                        "identity": {"bundle_key": "t:100"},
                        "resolution": {"status": "context_unresolved_transient",
                                       "required_context_complete": False,
                                       "reasons": ["missing"]}}}
        tm._SEMANTIC_CURATOR_ALLOWLIST = {"b"}
        tm._PUSH_RETRY_STATE_BY_USER["b"] = {"100": {
            "attempts": 5, "first_deferred_at": datetime.now(timezone.utc).isoformat(),
            "outer_created_at": "Thu Aug 13 01:30:00 +0000 2026"}}
        order = []
        with patch.object(tm, "fetch_tweets", return_value=[{
                "id": "999", "text": "ordinary timeline content long enough",
                "created_at": "Thu Aug 13 01:30:00 +0000 2026"}]), \
             patch.object(tm, "load_seen", return_value=({"warm"}, None)), \
             patch.object(tm, "load_push_retry", return_value={"100"}), \
             patch.object(tg, "fetch_semantic_tweet", return_value=recovered), \
             patch.object(tm, "_journal_semantic_decision",
                          side_effect=lambda *a, **k: order.append("journal")), \
             patch.object(tm, "save_seen", side_effect=lambda *a, **k: order.append("seen")), \
             patch.object(tm, "save_push_retry"), patch.object(tm, "send_tweet", return_value={"ok": True}), \
             patch.object(tm, "_CROSS_DEDUP_ENABLED", False), \
             patch.object(tm, "_article_queue_time_remaining", return_value=9999), \
             patch.object(tm.time, "sleep"):
            tm.process_user(None, self.NoAI(), "b", "bot", "chat", self.args())
        self.assertEqual(tm._PUSH_RETRY_STATE_BY_USER["b"]["100"]["attempts"], 6)
        self.assertLess(order.index("journal"), order.index("seen"))

    def test_provider_builder_error_is_additive_fail_open(self):
        # The actual provider loop catches bundle resolver errors and preserves flat
        # tweet fields; guard the contract at the callable boundary used there.
        node = raw("200", "b", "flat tweet survives")
        normalized = tg._tweet_result_snapshot(node, fallback_author="b")
        try:
            with patch.object(tg, "resolve_semantic_bundle", side_effect=RuntimeError("boom")):
                tg.resolve_semantic_bundle(node, "b")
        except RuntimeError as exc:
            normalized["semantic_bundle_error"] = type(exc).__name__
        self.assertEqual(normalized["text"], "flat tweet survives")
        self.assertEqual(normalized["semantic_bundle_error"], "RuntimeError")


if __name__ == "__main__":
    unittest.main()
