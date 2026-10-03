import argparse
import concurrent.futures
import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import twitter_monitor


_TEST_STATE_TMP = None
_TEST_STATE_PATCHERS = []


def setUpModule():
    """Keep every default runtime-state path inside a disposable test sandbox."""
    global _TEST_STATE_TMP, _TEST_STATE_PATCHERS
    import macrumors_daily

    _TEST_STATE_TMP = tempfile.TemporaryDirectory()
    root = Path(_TEST_STATE_TMP.name)
    paths = {
        "SEEN_DIR": root / "twitter_seen",
        "SEEN_RECOVERY_DIR": root / "twitter_seen" / ".seen_recovery",
        "PUSHED_INDEX_PATH": root / "twitter_seen" / ".pushed_index.json",
        "ASSUMED_DELIVERY_PATH": root / "twitter_seen" / ".assumed_delivered.json",
        "EVENT_LEDGER_PATH": root / "twitter_seen" / ".event_ledger.sqlite3",
        "SENT_CONTENT_LEDGER_PATH": root / "state" / "x_monitor_sent_content_ledger.jsonl",
        "ARTICLE_QUEUE_DIR": root / "twitter_articles",
        "ARTICLE_CACHE_DIR": root / "twitter_articles" / "cache",
        "FAILURES_PATH": root / ".account_failures.json",
        "DASHBOARD_PATH": root / ".dashboard.json",
        "COOKIE_HEALTH_PATH": root / ".cookie_health.json",
    }
    _TEST_STATE_PATCHERS = [
        patch.object(twitter_monitor, name, str(path)) for name, path in paths.items()
    ]
    _TEST_STATE_PATCHERS.append(
        patch.object(macrumors_daily, "SEEN_PATH", str(root / ".macrumors_seen.json"))
    )
    _TEST_STATE_PATCHERS.append(
        patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "off")
    )
    for patcher in _TEST_STATE_PATCHERS:
        patcher.start()


def tearDownModule():
    global _TEST_STATE_TMP, _TEST_STATE_PATCHERS
    for patcher in reversed(_TEST_STATE_PATCHERS):
        patcher.stop()
    _TEST_STATE_PATCHERS = []
    if _TEST_STATE_TMP is not None:
        _TEST_STATE_TMP.cleanup()
        _TEST_STATE_TMP = None


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        current = cls(2026, 5, 12, 0, 30, 0, tzinfo=timezone.utc)
        return current if tz else current.replace(tzinfo=None)


class SentContentLedgerTest(unittest.TestCase):
    def test_exact_schema_one_row_per_confirmed_message_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.jsonl"
            written = twitter_monitor._record_confirmed_sent_content(
                [
                    {"ok": True, "result": {"message_id": 901}},
                    {"ok": True, "result": {"message_id": "902"}},
                ],
                chat_id="-1004424841223", thread_id="19",
                source_kind="x_tweet",
                source_ref="https://x.com/openai/status/12345",
                source_message_ids=["12345"],
                url="https://x.com/openai/status/12345",
                content="给 Hermes 理解偏好的公开推文内容。",
                content_id="x-tweet:12345", path=str(path))

            self.assertEqual(written, 2)
            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual([row["message_id"] for row in rows], [901, 902])
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            for row in rows:
                self.assertEqual(set(row), {
                    "schema", "chat_id", "thread_id", "message_id", "producer",
                    "source_kind", "source_ref", "source_message_ids", "url",
                    "content", "content_hash", "delivery_state", "sent_at", "content_id",
                    "links",
                })
                self.assertEqual(row["links"], [])
                self.assertEqual(row["schema"], "sent-content.v1")
                self.assertEqual(row["producer"], "x_monitor")
                self.assertEqual(row["delivery_state"], "confirmed")
                self.assertEqual(row["chat_id"], -1004424841223)
                self.assertEqual(row["thread_id"], 19)
                self.assertEqual(row["source_message_ids"], [12345])
                self.assertEqual(
                    row["content_hash"],
                    twitter_monitor.hashlib.sha256(row["content"].encode("utf-8")).hexdigest())
                self.assertNotIn("cookie", row)
                self.assertNotIn("api_response", row)

    def test_assumed_missing_id_disabled_and_non_x_ref_never_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.jsonl"
            common = dict(
                chat_id=-1001, thread_id=None, source_kind="x_tweet",
                source_ref="https://x.com/u/status/10", source_message_ids=[10],
                url="https://x.com/u/status/10", content="public content",
                content_id="x-tweet:10", path=str(path))
            self.assertEqual(twitter_monitor._record_confirmed_sent_content(
                {"ok": True, "assumed_delivered": True,
                 "result": {"message_id": 7}}, **common), 0)
            self.assertEqual(twitter_monitor._record_confirmed_sent_content(
                {"ok": True}, **common), 0)
            self.assertEqual(twitter_monitor._record_confirmed_sent_content(
                {"ok": True, "result": {"message_id": -7}}, **common), 0)
            with patch.object(twitter_monitor, "_SENT_CONTENT_LEDGER_ENABLED", False):
                self.assertEqual(twitter_monitor._record_confirmed_sent_content(
                    {"ok": True, "result": {"message_id": 7}}, **common), 0)
            unsafe = dict(common)
            unsafe["source_ref"] = "https://api.example.invalid/token/secret"
            self.assertEqual(twitter_monitor._record_confirmed_sent_content(
                {"ok": True, "result": {"message_id": 7}}, **unsafe), 0)
            self.assertFalse(path.exists())

    def test_content_is_bounded_and_write_failure_is_fail_open(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.jsonl"
            common = dict(
                chat_id=-1001, thread_id=None, source_kind="x_article",
                source_ref="https://x.com/i/article/11", source_message_ids=[11],
                url="https://x.com/i/article/11", content="长" * 20000,
                content_id="x-article:11")
            self.assertEqual(twitter_monitor._record_confirmed_sent_content(
                {"ok": True, "result": {"message_id": 8}}, path=str(path), **common), 1)
            row = json.loads(path.read_text(encoding="utf-8"))
            self.assertLessEqual(len(row["content"]), twitter_monitor.SENT_CONTENT_MAX_CHARS)
            self.assertTrue(row["content"].endswith("…[truncated]"))

            blocker = Path(tmp) / "not-a-directory"
            blocker.write_text("block", encoding="utf-8")
            # No exception may escape after Telegram has already confirmed send.
            self.assertEqual(twitter_monitor._record_confirmed_sent_content(
                {"ok": True, "result": {"message_id": 9}},
                path=str(blocker / "ledger.jsonl"), **common), 0)

    def test_links_sorted_unique_capped_and_omitted_is_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.jsonl"
            common = dict(
                chat_id=-1001, thread_id=None, source_kind="x_tweet",
                source_ref="https://x.com/u/status/10", source_message_ids=[10],
                url="https://x.com/u/status/10", content="public content",
                content_id="x-tweet:10", path=str(path))
            extras = ["https://n.example/%02d" % i for i in range(18)]
            written = twitter_monitor._record_confirmed_sent_content(
                {"ok": True, "result": {"message_id": 1}},
                links=["https://b.example/z", "https://a.example/y",
                       "https://b.example/z"] + extras,
                **common)
            self.assertEqual(written, 1)
            row = json.loads(path.read_text(encoding="utf-8"))
            expected = sorted(set(
                ["https://a.example/y", "https://b.example/z"] + extras))[:20]
            self.assertEqual(row["links"], expected)
            self.assertEqual(len(row["links"]), 20)
            self.assertEqual(row["links"], sorted(set(row["links"])))

            empty_path = Path(tmp) / "empty.jsonl"
            common["path"] = str(empty_path)
            written = twitter_monitor._record_confirmed_sent_content(
                {"ok": True, "result": {"message_id": 2}}, **common)
            self.assertEqual(written, 1)
            row = json.loads(empty_path.read_text(encoding="utf-8"))
            self.assertEqual(row["links"], [])


class CanonicalLinkTest(unittest.TestCase):
    def test_outbound_links_include_quoted_status_key_and_entities(self):
        tweet = {
            "id": "1", "text": "看这个 https://t.co/abc",
            "entities": {"urls": [{
                "url": "https://t.co/abc",
                "expanded_url": "https://z.ai/blog/glm-built-its-inference-infrastructure/?utm_source=x",
            }]},
            "quoted_status": {"id": "2100494599475155288", "screen_name": "dotey"},
        }
        self.assertEqual(
            twitter_monitor._tweet_outbound_links(tweet),
            ["https://z.ai/blog/glm-built-its-inference-infrastructure",
             "x.com/status/2100494599475155288"])
        self.assertEqual(twitter_monitor._tweet_outbound_links({"id": "1", "text": "no links"}), [])
        self.assertEqual(twitter_monitor._tweet_outbound_links({"quoted_status": {"id": "abc"}}), [])

    def test_tracker_host_slash_and_fragment(self):
        self.assertEqual(
            twitter_monitor._canonical_link(
                "https://www.Z.ai/blog/glm-built-its-inference-infrastructure/"
                "?utm_source=x#top"),
            "https://z.ai/blog/glm-built-its-inference-infrastructure")

    def test_x_status_shapes(self):
        expected = "x.com/status/123"
        for url in (
            "https://x.com/dotey/status/123",
            "https://twitter.com/dotey/status/123",
            "https://x.com/i/web/status/123",
            "https://twitter.com/dotey/status/123?s=20&t=abc",
            "https://mobile.twitter.com/dotey/statuses/123",
        ):
            self.assertEqual(twitter_monitor._canonical_link(url), expected, url)

    def test_x_article(self):
        self.assertEqual(
            twitter_monitor._canonical_link("https://x.com/i/article/99"),
            "x.com/i/article/99")

    def test_rejected_hosts_and_schemes(self):
        self.assertEqual(twitter_monitor._canonical_link("https://t.co/abc"), "")
        self.assertEqual(
            twitter_monitor._canonical_link("https://pbs.twimg.com/media/x.jpg"), "")
        self.assertEqual(twitter_monitor._canonical_link("javascript:alert(1)"), "")
        self.assertEqual(twitter_monitor._canonical_link("ftp://example.com/a"), "")
        self.assertEqual(twitter_monitor._canonical_link(""), "")


class EventDeliveryLedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "ledger.sqlite3")

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def event(tid, text, event_type="model_launch"):
        return {"id": tid, "text": text, "_push_event_type": event_type}

    def test_different_tweet_ids_same_event_observed_but_not_suppressed(self):
        first = self.event("101", "Introducing GPT-5.6, now in ChatGPT, Codex and the API.")
        paraphrase = self.event("102", "GPT 5.6 is now available in ChatGPT, Codex, and API.")
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "observe"):
            c1 = twitter_monitor.claim_event_delivery(first, "OpenAI", path=self.db)
            self.assertTrue(c1["claimed"])
            twitter_monitor.finish_event_delivery(c1, "confirmed", {"result": {"message_id": 77}}, path=self.db)
            c2 = twitter_monitor.claim_event_delivery(paraphrase, "OpenAIDevs", path=self.db)
        self.assertTrue(c2["claimed"])
        with twitter_monitor._event_ledger_connect(self.db) as db:
            obs = db.execute("SELECT decision FROM event_observations").fetchall()
        self.assertEqual([r["decision"] for r in obs], ["would_suppress"])

    def test_claim_is_scoped_to_actual_chat_and_thread(self):
        tweet = self.event("103", "GPT-5.6 is now available in the API.")
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "observe"):
            group_x = twitter_monitor.claim_event_delivery(
                tweet, "OpenAI", target_chat_id="group", target_thread_id=10,
                path=self.db)
            self.assertTrue(group_x["claimed"])
            twitter_monitor.finish_event_delivery(group_x, "confirmed", path=self.db)
            group_other_topic = twitter_monitor.claim_event_delivery(
                tweet, "OpenAI", target_chat_id="group", target_thread_id=11,
                path=self.db)
            direct = twitter_monitor.claim_event_delivery(
                tweet, "OpenAI", target_chat_id="direct", path=self.db)
            duplicate_x = twitter_monitor.claim_event_delivery(
                tweet, "OpenAI", target_chat_id="group", target_thread_id=10,
                path=self.db)
        self.assertTrue(group_other_topic["claimed"])
        self.assertTrue(direct["claimed"])
        self.assertFalse(duplicate_x["claimed"])
        with twitter_monitor._event_ledger_connect(self.db) as db:
            targets = db.execute("""SELECT target_chat_id,target_thread_id
                FROM deliveries ORDER BY target_chat_id,target_thread_id""").fetchall()
        self.assertEqual([tuple(row) for row in targets],
                         [("direct", ""), ("group", "10"), ("group", "11")])

    def test_added_fact_is_a_distinct_update(self):
        first = self.event("201", "GPT-5.6 is now in ChatGPT and API.")
        update = self.event("202", "GPT-5.6 is now in ChatGPT and API, with 50% more weekly credits for Team.")
        self.assertEqual(twitter_monitor.event_identity(first)["event_key"],
                         twitter_monitor.event_identity(update)["event_key"])
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "enforce"):
            c1 = twitter_monitor.claim_event_delivery(first, "OpenAI", path=self.db)
            twitter_monitor.finish_event_delivery(c1, "confirmed", path=self.db)
            c2 = twitter_monitor.claim_event_delivery(update, "OpenAI", path=self.db)
        self.assertTrue(c2["claimed"])
        with twitter_monitor._event_ledger_connect(self.db) as db:
            self.assertEqual(db.execute("SELECT decision FROM event_observations").fetchone()[0],
                             "material_update")

    def test_real_anthropic_model_release_companion_posts_share_primary_anchor(self):
        launch = self.event(
            "205", "Introducing Claude Opus 5. It approaches Fable 5 at half the price.",
            "model_launch")
        access = self.event(
            "206", "Opus 5 is available today on Pro, Max and the API at the Opus 4.8 price.",
            "model_access")
        launch_id = twitter_monitor.event_identity(launch)
        access_id = twitter_monitor.event_identity(access)
        self.assertEqual(launch_id["anchors"], ["opus-5"])
        self.assertEqual(access_id["anchors"], ["opus-5"])
        self.assertEqual(launch_id["event_family"], "model_release")
        self.assertEqual(launch_id["event_key"], access_id["event_key"])
        self.assertIn("fable-5", launch_id["facts"])
        self.assertIn("opus-4.8", access_id["facts"])

    def test_companion_release_with_new_availability_facts_is_preserved(self):
        launch = self.event("207", "Introducing Claude Opus 5 at half the price.",
                            "model_launch")
        access = self.event("208", "Opus 5 is now available on Pro, Max and API.",
                            "model_access")
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "enforce"):
            c1 = twitter_monitor.claim_event_delivery(launch, "claudeai", path=self.db)
            twitter_monitor.finish_event_delivery(c1, "confirmed", path=self.db)
            c2 = twitter_monitor.claim_event_delivery(access, "claudeai", path=self.db)
        self.assertTrue(c2["claimed"])
        with twitter_monitor._event_ledger_connect(self.db) as db:
            self.assertEqual(db.execute(
                "SELECT decision FROM event_observations").fetchone()[0], "material_update")

    def test_paraphrase_with_fewer_known_facts_is_duplicate_in_enforce(self):
        first = self.event("211", "GPT-5.6 is now in ChatGPT, Codex and API for Team.")
        shorter = self.event("212", "GPT-5.6 is now available in the API.")
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "enforce"):
            c1 = twitter_monitor.claim_event_delivery(first, "OpenAI", path=self.db)
            twitter_monitor.finish_event_delivery(c1, "confirmed", path=self.db)
            c2 = twitter_monitor.claim_event_delivery(shorter, "OpenAIDevs", path=self.db)
        self.assertFalse(c2["claimed"])
        self.assertTrue(c2["duplicate"])

    def test_short_ascii_fact_terms_use_token_boundaries(self):
        facts = twitter_monitor._event_facts(self.event(
            "220", "We maximize product quality for the API without changing plans."))
        self.assertNotIn("max", facts)
        self.assertNotIn("pro", facts)
        self.assertIn("api", facts)

    def test_atomic_concurrent_claim_has_one_winner(self):
        tweet = self.event("301", "Claude-5.1 is now in Claude Code and API.")
        def claim(_):
            return twitter_monitor.claim_event_delivery(tweet, "claudeai", path=self.db)
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "enforce"):
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(claim, range(8)))
        self.assertEqual(sum(bool(r["claimed"]) for r in results), 1)
        self.assertEqual(sum(bool(r["duplicate"]) for r in results), 7)

    def test_crash_recovery_promotes_stale_pending_to_ambiguous(self):
        tweet = self.event("401", "Gemini-4.2 is now available in API.")
        old = datetime(2026, 5, 11, 23, 0, tzinfo=timezone.utc)
        later = old + timedelta(seconds=twitter_monitor.EVENT_LEDGER_PENDING_TTL_SECONDS + 1)
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "observe"):
            c1 = twitter_monitor.claim_event_delivery(tweet, "u", path=self.db, now=old)
            self.assertTrue(c1["claimed"])
            c2 = twitter_monitor.claim_event_delivery(tweet, "u", path=self.db, now=later)
        self.assertFalse(c2["claimed"])
        self.assertEqual(c2["state"], "ambiguous")

    def test_startup_recovers_stale_pending_even_if_tweet_is_never_seen_again(self):
        tweet = self.event("405", "Gemini-4.2 is now available in API.")
        old = datetime(2026, 5, 11, 23, 0, tzinfo=timezone.utc)
        later = old + timedelta(seconds=twitter_monitor.EVENT_LEDGER_PENDING_TTL_SECONDS + 1)
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "observe"):
            twitter_monitor.claim_event_delivery(tweet, "u", path=self.db, now=old)
        self.assertEqual(twitter_monitor.recover_stale_event_claims(self.db, now=later), 1)
        with twitter_monitor._event_ledger_connect(self.db) as db:
            row = db.execute("SELECT state,detail FROM deliveries WHERE tweet_id='405'").fetchone()
        self.assertEqual(row["state"], "ambiguous")
        self.assertEqual(row["detail"], "startup_stale_pending_recovery")

    def test_all_required_state_transitions_and_message_id(self):
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "observe"):
            confirmed = twitter_monitor.claim_event_delivery(self.event("501", "GPT-5.7 API"), "u", path=self.db)
            self.assertTrue(twitter_monitor.finish_event_delivery(
                confirmed, "confirmed", {"result": {"message_id": 9001}}, path=self.db))
            ambiguous = twitter_monitor.claim_event_delivery(self.event("502", "Claude-5.2 API"), "u", path=self.db)
            self.assertTrue(twitter_monitor.finish_event_delivery(ambiguous, "ambiguous", detail="HTTP 504", path=self.db))
            failed = twitter_monitor.claim_event_delivery(self.event("503", "Gemini-4.3 API"), "u", path=self.db)
            self.assertTrue(twitter_monitor.finish_event_delivery(failed, "failed_pre_send", detail="DNS", path=self.db))
            retry = twitter_monitor.claim_event_delivery(self.event("503", "Gemini-4.3 API"), "u", path=self.db)
        self.assertTrue(retry["claimed"])
        with twitter_monitor._event_ledger_connect(self.db) as db:
            rows = {r["tweet_id"]: (r["state"], r["message_id"]) for r in db.execute(
                "SELECT tweet_id,state,message_id FROM deliveries")}
        self.assertEqual(rows["501"], ("confirmed", "9001"))
        self.assertEqual(rows["502"][0], "ambiguous")
        self.assertEqual(rows["503"][0], "pending")

    def test_enforce_gate_requires_reviewed_low_false_positive_samples(self):
        twitter_monitor._event_ledger_connect(self.db).close()
        now = datetime.now(timezone.utc).isoformat()
        with twitter_monitor._event_ledger_connect(self.db) as db:
            db.executemany("""INSERT INTO event_observations
                (observed_at,event_key,candidate_tweet_id,candidate_username,decision,reviewed,false_positive)
                VALUES (?,?,?,?,?,?,?)""", [
                (now, f"e{i}", f"t{i}", "u", "would_suppress", 1, 0)
                for i in range(twitter_monitor.EVENT_ENFORCE_MIN_REVIEWED)
            ])
        self.assertTrue(twitter_monitor.event_dedup_gate_report(self.db)["ready"])

    def test_enforce_gate_rejects_reviewed_but_unlabelled_samples(self):
        twitter_monitor._event_ledger_connect(self.db).close()
        now = datetime.now(timezone.utc).isoformat()
        with twitter_monitor._event_ledger_connect(self.db) as db:
            db.executemany("""INSERT INTO event_observations
                (observed_at,event_key,candidate_tweet_id,candidate_username,decision,reviewed)
                VALUES (?,?,?,?,?,?)""", [
                (now, f"e{i}", f"t{i}", "u", "would_suppress", 1)
                for i in range(twitter_monitor.EVENT_ENFORCE_MIN_REVIEWED)
            ])
        report = twitter_monitor.event_dedup_gate_report(self.db)
        self.assertEqual(report["reviewed"], 0)
        self.assertFalse(report["ready"])

    def test_repeated_candidate_does_not_inflate_observation_count(self):
        first = self.event("241", "GPT-5.6 is now in ChatGPT, Codex and API.")
        repeat = self.event("242", "GPT-5.6 is now available in the API.")
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "observe"):
            c1 = twitter_monitor.claim_event_delivery(first, "OpenAI", path=self.db)
            twitter_monitor.finish_event_delivery(c1, "confirmed", path=self.db)
            c2 = twitter_monitor.claim_event_delivery(repeat, "OpenAIDevs", path=self.db)
            twitter_monitor.finish_event_delivery(c2, "confirmed", path=self.db)
            c3 = twitter_monitor.claim_event_delivery(repeat, "OpenAIDevs", path=self.db)
        self.assertFalse(c3["claimed"])
        with twitter_monitor._event_ledger_connect(self.db) as db:
            self.assertEqual(db.execute(
                "SELECT count(*) FROM event_observations").fetchone()[0], 1)

    def test_changed_prior_does_not_duplicate_same_candidate_sample(self):
        first = self.event("243", "GPT-5.6 is now in ChatGPT, Codex and API.")
        repeat = self.event("244", "GPT-5.6 is now available in the API.")
        update = self.event(
            "245", "GPT-5.6 is now in ChatGPT, Codex and API with weekly Team credits.")
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "observe"):
            c1 = twitter_monitor.claim_event_delivery(first, "OpenAI", path=self.db)
            twitter_monitor.finish_event_delivery(c1, "confirmed", path=self.db)
            c2 = twitter_monitor.claim_event_delivery(repeat, "OpenAIDevs", path=self.db)
            twitter_monitor.finish_event_delivery(c2, "confirmed", path=self.db)
            c3 = twitter_monitor.claim_event_delivery(update, "OpenAI", path=self.db)
            twitter_monitor.finish_event_delivery(c3, "confirmed", path=self.db)
            self.assertFalse(twitter_monitor.claim_event_delivery(
                repeat, "OpenAIDevs", path=self.db)["claimed"])
        with twitter_monitor._event_ledger_connect(self.db) as db:
            rows = db.execute("""SELECT candidate_tweet_id,decision
                FROM event_observations WHERE candidate_tweet_id='244'""").fetchall()
        self.assertEqual([tuple(row) for row in rows], [("244", "would_suppress")])

    def test_legacy_duplicate_observations_migrate_preserving_label(self):
        # Simulate a database created before the candidate-unique index existed.
        with twitter_monitor._event_ledger_connect(self.db) as db:
            db.execute("DROP INDEX event_observations_candidate_idx")
            db.executemany("""INSERT INTO event_observations
                (observed_at,event_key,prior_delivery_key,candidate_tweet_id,
                 candidate_username,decision,reviewed,false_positive,note)
                VALUES (?,?,?,?,?,?,?,?,?)""", [
                ("2026-08-09T00:00:00+00:00", "e", "p1", "c", "u",
                 "would_suppress", 0, None, None),
                ("2026-08-09T00:01:00+00:00", "e", "p2", "c", "u",
                 "would_suppress", 1, 1, "reviewed false positive"),
            ])
        with twitter_monitor._event_ledger_connect(self.db) as db:
            rows = db.execute("""SELECT reviewed,false_positive,note
                FROM event_observations""").fetchall()
            columns = [row["name"] for row in db.execute(
                "PRAGMA index_info(event_observations_candidate_idx)")]
        self.assertEqual([tuple(row) for row in rows],
                         [(1, 1, "reviewed false positive")])
        self.assertEqual(columns, ["target_chat_id", "target_thread_id", "event_key",
                                   "candidate_tweet_id", "decision"])

    def test_review_api_lists_urls_labels_candidate_and_updates_gate(self):
        first = self.event("251", "GPT-5.6 is now in ChatGPT, Codex and API.")
        repeat = self.event("252", "GPT-5.6 is now available in the API.")
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "observe"):
            c1 = twitter_monitor.claim_event_delivery(first, "OpenAI", path=self.db)
            twitter_monitor.finish_event_delivery(c1, "confirmed", path=self.db)
            c2 = twitter_monitor.claim_event_delivery(repeat, "OpenAIDevs", path=self.db)
        rows = twitter_monitor.event_review_rows(self.db)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["prior_url"], "https://x.com/OpenAI/status/251")
        self.assertEqual(rows[0]["candidate_url"],
                         "https://x.com/OpenAIDevs/status/252")
        self.assertTrue(twitter_monitor.review_event_observation(
            rows[0]["id"], False, "same announcement", path=self.db))
        reviewed = twitter_monitor.event_review_rows(self.db)[0]
        self.assertEqual((reviewed["reviewed"], reviewed["false_positive"], reviewed["note"]),
                         (1, 0, "same announcement"))
        report = twitter_monitor.event_dedup_gate_report(self.db)
        self.assertEqual((report["candidates"], report["reviewed"], report["false_positives"]),
                         (1, 1, 0))

    def test_enforce_compares_candidate_with_all_known_event_facts(self):
        first = self.event("231", "GPT-5.6 is now in ChatGPT and API.")
        incomparable = self.event("232", "GPT-5.6 is now in Codex for Team.")
        older_repeat = self.event("233", "GPT-5.6 is available in the API.")
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "enforce"):
            c1 = twitter_monitor.claim_event_delivery(first, "OpenAI", path=self.db)
            twitter_monitor.finish_event_delivery(c1, "confirmed", path=self.db)
            c2 = twitter_monitor.claim_event_delivery(incomparable, "OpenAIDevs", path=self.db)
            self.assertTrue(c2["claimed"])
            twitter_monitor.finish_event_delivery(c2, "confirmed", path=self.db)
            c3 = twitter_monitor.claim_event_delivery(older_repeat, "OpenAI", path=self.db)
        self.assertFalse(c3["claimed"])
        self.assertTrue(c3["duplicate"])

    def test_send_ok_then_ledger_finalize_crash_never_enters_retry(self):
        tweet = {"id": "601", "text": "A sufficiently long substantive API release update.",
                 "createdAt": "Tue May 12 00:20:00 +0000 2026"}
        args = argparse.Namespace(test=False, seed=False, dry_run=False, limit=20,
                                  max_push_age_minutes=45)
        saved_retries = []
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "observe"), \
             patch.object(twitter_monitor, "EVENT_LEDGER_PATH", self.db), \
             patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=[tweet]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "save_push_retry",
                          side_effect=lambda _u, ids: saved_retries.append(set(ids))), \
             patch.object(twitter_monitor, "send_tweet",
                          return_value={"ok": True, "result": {"message_id": 42}}), \
             patch.object(twitter_monitor, "finish_event_delivery",
                          side_effect=sqlite3.OperationalError("disk I/O")), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            result = twitter_monitor.process_user(None, FakeAI(False), "u", "b", "c", args)
        self.assertEqual(result[1], 1)
        self.assertEqual(saved_retries[-1], set())
        with twitter_monitor._event_ledger_connect(self.db) as db:
            self.assertEqual(db.execute("SELECT state FROM deliveries WHERE tweet_id='601'").fetchone()[0],
                             "pending")


class MacRumorsMergeJsonTests(unittest.TestCase):
    """AI 聚类 JSON 是非可信输入；混合类型必须降级而不是终止日报。"""

    def test_mixed_root_array_skips_scalars_and_preserves_full_coverage(self):
        import macrumors_daily as md
        quality = {}
        text = json.dumps([
            {"indices": [0, 2], "zh_title": "同一事件", "zh_summary": "摘要"},
            7, None, "noise", [1],
            {"indices": [1], "zh_title": "另一事件", "zh_summary": "摘要"},
        ])

        groups = md.parse_merge_json(text, 4, quality)

        self.assertEqual([g["indices"] for g in groups], [[0, 2], [1], [3]])
        self.assertEqual(quality["status"], "degraded")
        self.assertEqual(quality["invalid_elements"], 4)
        self.assertEqual(quality["missing_indices"], 1)

    def test_bad_indices_are_counted_without_bool_or_float_truncation(self):
        import macrumors_daily as md
        quality = {}
        text = json.dumps([
            {"indices": [0, True, 1.5, "2", -1, 5, 0],
             "zh_title": "标题", "zh_summary": "摘要"},
            {"indices": [1]},
        ])

        groups = md.parse_merge_json(text, 3, quality)

        self.assertEqual([g["indices"] for g in groups], [[0], [1], [2]])
        self.assertEqual(quality["invalid_indices"], 5)
        self.assertEqual(quality["duplicate_indices"], 1)
        self.assertEqual(quality["missing_indices"], 1)
        self.assertEqual(quality["status"], "degraded")

    def test_cross_group_duplicate_is_removed_from_later_group(self):
        import macrumors_daily as md
        quality = {}
        text = json.dumps([
            {"indices": [0, 1], "zh_title": "首组"},
            {"indices": [1, 2], "zh_title": "后组"},
        ])

        groups = md.parse_merge_json(text, 3, quality)

        self.assertEqual([g["indices"] for g in groups], [[0, 1], [2]])
        self.assertEqual(quality["duplicate_indices"], 1)
        self.assertEqual(quality["missing_indices"], 0)
        self.assertEqual(quality["status"], "degraded")

    def test_unbounded_json_integer_is_rejected_without_overflow(self):
        import macrumors_daily as md
        quality = {}
        huge = int("9" * 400)

        groups = md.parse_merge_json(json.dumps([
            {"indices": [huge]}, {"indices": [0]},
        ]), 1, quality)

        self.assertEqual([g["indices"] for g in groups], [[0]])
        self.assertEqual(quality["invalid_indices"], 1)
        self.assertEqual(quality["status"], "degraded")

    def test_all_invalid_elements_returns_none_with_auditable_quality(self):
        import macrumors_daily as md
        quality = {}

        self.assertIsNone(md.parse_merge_json("[1, null, \"x\"]", 2, quality))
        self.assertEqual(quality["status"], "no_valid_groups")
        self.assertEqual(quality["invalid_elements"], 3)
        self.assertEqual(quality["missing_indices"], 2)

    def test_valid_payload_reports_ok(self):
        import macrumors_daily as md
        quality = {}
        groups = md.parse_merge_json(
            '[{"indices":[0]},{"indices":[1]}]', 2, quality)
        self.assertEqual(len(groups), 2)
        self.assertEqual(quality["status"], "ok")

    def test_merge_similar_logs_degradation_and_keeps_every_item(self):
        import contextlib
        import io
        import macrumors_daily as md

        class MixedResponseAI:
            @staticmethod
            def complete(_prompt, max_tokens=4000):
                return json.dumps([
                    {"indices": [0]}, 7, {"indices": [1]},
                    {"indices": [2]}, {"indices": [3]},
                ]), "fake"

        items = [{"zh_title": f"title-{i}"} for i in range(4)]
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            result = md.merge_similar(MixedResponseAI(), items)

        self.assertEqual(result, items)
        line = stderr.getvalue().strip()
        self.assertTrue(line.startswith("MACRUMORS_QUALITY merge_parse "))
        metric = json.loads(line.split(" ", 2)[2])
        self.assertEqual(metric["status"], "degraded")
        self.assertEqual(metric["invalid_elements"], 1)
        self.assertEqual(metric["missing_indices"], 0)
        self.assertEqual(metric["attempt"], 1)


class FakeAI:
    def __init__(self, available=False, summary=None, *,
                 promo=False, musing=False, promo_reason="ok", musing_reason="ok"):
        self.available = available
        self.summary = summary
        self.promo = promo
        self.musing = musing
        self.promo_reason = promo_reason
        self.musing_reason = musing_reason

    def is_available(self):
        return self.available

    def complete(self, prompt, max_tokens=1200, temperature=0.2):
        return self.summary, "fake"

    def confirm_promo(self, username, text):
        return self.promo, f"fake:{self.promo_reason}"

    def confirm_musing(self, username, text):
        return self.musing, f"fake:{self.musing_reason}"


class SentContentFlowTest(unittest.TestCase):
    TWEET = {
        "id": "2091427402140557652",
        "text": "A substantive public release update with enough detail for the monitored feed.",
        "createdAt": "Tue May 12 00:20:00 +0000 2026",
    }

    @staticmethod
    def _args(**overrides):
        values = dict(test=False, seed=False, dry_run=False, test_count=3,
                      limit=20, max_push_age_minutes=45)
        values.update(overrides)
        return argparse.Namespace(**values)

    def _process_tweet(self, ledger_path, result, **mode):
        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "SENT_CONTENT_LEDGER_PATH", str(ledger_path)), \
             patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "off"), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=[dict(self.TWEET)]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "load_push_retry", return_value=set()), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "save_push_retry", return_value=None), \
             patch.object(twitter_monitor, "send_tweet", return_value=result), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            return twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="u",
                bot_token="b", chat_id="-1001", args=self._args(**mode),
                content_chat_id="-1002", content_thread_id=19)

    def test_process_user_records_confirmed_result_but_not_assumed_or_test(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            confirmed = root / "confirmed.jsonl"
            result = self._process_tweet(
                confirmed, {"ok": True, "result": {"message_id": 42},
                            "send_method": "sendMessage"})
            self.assertEqual(result[1], 1)
            row = json.loads(confirmed.read_text(encoding="utf-8"))
            self.assertEqual((row["chat_id"], row["thread_id"], row["message_id"]),
                             (-1002, 19, 42))
            self.assertEqual(row["source_kind"], "x_tweet")

            assumed = root / "assumed.jsonl"
            result = self._process_tweet(
                assumed, {"ok": True, "assumed_delivered": True,
                          "send_method": "sendRichMessage"})
            self.assertEqual(result[1], 1)
            self.assertFalse(assumed.exists())

            test_mode = root / "test.jsonl"
            result = self._process_tweet(
                test_mode, {"ok": True, "result": {"message_id": 43}}, test=True)
            self.assertEqual(result[1], 1)
            self.assertFalse(test_mode.exists())

    def test_article_rich_confirmation_records_x_article(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            queue_path = root / "u_queue.json"
            ledger_path = root / "sent.jsonl"
            entry = {
                "article_id": "777", "tweet_id": "778", "author": "u",
                "article_title": "A public article", "status": "pending", "attempts": 0,
                "content": None, "detected_at": datetime.now(timezone.utc).isoformat(),
            }
            queue_path.write_text(json.dumps([entry]), encoding="utf-8")
            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", str(root)), \
                 patch.object(twitter_monitor, "ARTICLE_CACHE_DIR", str(root / "cache")), \
                 patch.object(twitter_monitor, "SENT_CONTENT_LEDGER_PATH", str(ledger_path)), \
                 patch.object(twitter_monitor, "_CROSS_DEDUP_ENABLED", False), \
                 patch.object(twitter_monitor, "_ARTICLE_SUPERSEDE_ENABLED", False), \
                 patch.object(twitter_monitor, "learning_feed", None), \
                 patch.object(twitter_monitor, "fetch_article_markdown",
                              return_value=("# Article\n\nPublic body", None)), \
                 patch.object(twitter_monitor, "summarize_article",
                              return_value=("Public summary", "fake")), \
                 patch.object(twitter_monitor, "send_telegram_rich",
                              return_value={"ok": True, "result": {"message_id": 88}}), \
                 patch.object(twitter_monitor, "delete_article_cache"), \
                 patch.object(twitter_monitor.time, "sleep", return_value=None):
                processed = twitter_monitor.process_article_queue(
                    FakeAI(True), "bot", "-1002", thread_id=19)
            self.assertEqual(processed, 1)
            row = json.loads(ledger_path.read_text(encoding="utf-8"))
            self.assertEqual((row["source_kind"], row["message_id"], row["source_message_ids"]),
                             ("x_article", 88, [777]))
            self.assertEqual(row["content_id"], "x-article:777")

    def test_outbound_links_land_in_ledger_and_pushed_index(self):
        tweet = dict(self.TWEET)
        tweet["entities"] = {
            "urls": [
                {
                    "url": "https://t.co/glm",
                    "expanded_url": (
                        "https://z.ai/blog/glm-built-its-inference-infrastructure"
                        "?utm_source=x"),
                },
                {"url": "https://t.co/abc", "expanded_url": "https://t.co/abc"},
            ],
        }
        expected_links = [
            "https://z.ai/blog/glm-built-its-inference-infrastructure",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger = root / "confirmed.jsonl"
            pushed = root / "pushed.json"
            with patch.object(twitter_monitor, "datetime", FixedDatetime), \
                 patch.object(twitter_monitor, "SENT_CONTENT_LEDGER_PATH", str(ledger)), \
                 patch.object(twitter_monitor, "PUSHED_INDEX_PATH", str(pushed)), \
                 patch.object(twitter_monitor, "_PUSHED_INDEX_CACHE", None), \
                 patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "off"), \
                 patch.object(twitter_monitor, "_CROSS_DEDUP_ENABLED", True), \
                 patch.object(twitter_monitor, "fetch_tweets", return_value=[tweet]), \
                 patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
                 patch.object(twitter_monitor, "load_push_retry", return_value=set()), \
                 patch.object(twitter_monitor, "save_seen", return_value=None), \
                 patch.object(twitter_monitor, "save_push_retry", return_value=None), \
                 patch.object(twitter_monitor, "send_tweet",
                              return_value={"ok": True, "result": {"message_id": 42},
                                            "send_method": "sendMessage"}), \
                 patch.object(twitter_monitor.time, "sleep", return_value=None):
                result = twitter_monitor.process_user(
                    pool=None, ai=FakeAI(False), username="u",
                    bot_token="b", chat_id="-1001", args=self._args(),
                    content_chat_id="-1002", content_thread_id=19)
            self.assertEqual(result[1], 1)
            row = json.loads(ledger.read_text(encoding="utf-8"))
            self.assertEqual(row["links"], expected_links)
            idx = json.loads(pushed.read_text(encoding="utf-8"))
            entry = idx["entries"]["t:" + tweet["id"]]
            self.assertEqual(entry["links"], expected_links)


class OfficialPushPolicyTest(unittest.TestCase):
    def classify(self, policy, text, **extra):
        tweet = {"id": "1", "text": text}
        tweet.update(extra)
        return twitter_monitor.classify_official_push(policy, tweet)

    def test_all_official_policies_reject_retweets(self):
        for policy in twitter_monitor.OFFICIAL_PUSH_POLICIES:
            with self.subTest(policy=policy):
                status, reason, event = self.classify(
                    policy,
                    "We've reset weekly usage limits for all paid users.",
                    retweeted_status={"id": "99", "screen_name": "source"},
                )
                self.assertEqual((status, event), ("filter", None))
                self.assertIn("originality", reason)

    def test_claude_dev_reset_and_developer_release_pass(self):
        self.assertEqual(
            self.classify(
                "claude_dev_original",
                "We've reset 5-hour and weekly rate limits for all users.",
            )[2],
            "quota_reset",
        )
        self.assertEqual(
            self.classify(
                "claude_dev_original",
                "Claude Code can now run code review with new effort levels.",
            )[2],
            "dev_release",
        )

    def test_claude_dev_quota_policy_announcement_passes(self):
        # 2026-07-18 实测漏推样本：额度政策公告发在 ClaudeDevs 而非 claudeai
        status, reason, event = self.classify(
            "claude_dev_original",
            "We're also keeping Claude Code weekly limits 50% higher, now through "
            "August 19, for all Pro, Max, Team, and seat-based Enterprise users.",
        )
        self.assertEqual((status, event), ("pass", "quota_policy"))

    def test_claude_dev_promotional_credits_still_filtered(self):
        self.assertEqual(
            self.classify(
                "claude_dev_original",
                "Compete for weekly API credit prizes in our Claude Code hackathon.",
            )[0],
            "filter",
        )

    def test_claude_dev_non_quota_entitlement_still_filtered(self):
        # model_access / plan_entitlement 类权益仍归 claudeai，不从 dev 号放行
        self.assertEqual(
            self.classify(
                "claude_dev_original",
                "Claude Fable is now included in the Max plan.",
            )[0],
            "filter",
        )

    def test_openai_devs_filters_events_but_keeps_codex_features(self):
        self.assertEqual(
            self.classify(
                "openai_dev_original",
                "Join us for Codex Office Hours and a community showcase.",
            )[0],
            "filter",
        )
        self.assertEqual(
            self.classify(
                "openai_dev_original",
                "Review pull requests in Codex. Inline code editing lets you update the patch.",
            )[2],
            "dev_release",
        )

    def test_claude_entitlement_does_not_confuse_hackathon_credits(self):
        self.assertEqual(
            self.classify(
                "claude_entitlement_original",
                "Win $100k API credits in our developer hackathon.",
            )[0],
            "filter",
        )
        self.assertEqual(
            self.classify(
                "claude_entitlement_original",
                "Starting July 20, Max and Team plans include weekly usage credits.",
            )[2],
            "quota_policy",
        )

    def test_claude_main_model_launch_is_not_misclassified_as_plan_entitlement(self):
        status, reason, event = self.classify(
            "claude_entitlement_original",
            "Introducing Claude Opus 5. It approaches Fable 5 at half the price.",
        )
        self.assertEqual((status, reason, event),
                         ("pass", "policy:model_launch", "model_launch"))

    def test_openai_major_filters_research_and_keeps_model_launch(self):
        self.assertEqual(
            self.classify(
                "openai_major_original",
                "Our research paper explores new approaches to interpretability.",
            )[0],
            "filter",
        )
        self.assertEqual(
            self.classify(
                "openai_major_original",
                "Introducing GPT-5.6, now available in ChatGPT, Codex, and the API.",
            )[2],
            "model_launch",
        )

    def test_tibo_completed_resets_pass_and_jokes_fail(self):
        good = (
            "We've reset usage limits for all paid users, including Codex and ChatGPT Work."
        )
        self.assertEqual(self.classify("codex_quota_original", good)[2], "quota_reset")
        self.assertEqual(
            self.classify("codex_quota_original", "Should we reset Codex limits for all paid users?")[0],
            "filter",
        )
        self.assertEqual(
            self.classify(
                "codex_quota_original",
                "Thinking I am about to announce a Codex reset for everyone. But no.",
            )[0],
            "filter",
        )
        self.assertEqual(
            self.classify(
                "codex_quota_original",
                "If this gets blocked, I owe all Codex users a reset.",
            )[0],
            "filter",
        )

    def test_unknown_policy_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unknown push_policy"):
            self.classify("typo_policy", "anything")


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

        def fake_send_tweet(token, chat_id, username, tweet, ai=None, thread_id=None,
                            reply_to_message_id=None):
            sent.append(f"https://x.com/{username}/status/{tweet['id']}")
            return {"ok": True}

        with patch.object(twitter_monitor, "datetime", FixedDatetime):
            with patch.object(twitter_monitor, "fetch_tweets", return_value=tweets):
                with patch.object(twitter_monitor, "load_seen", return_value=({"already-seen"}, "2026-05-11T15:00:00+00:00")):
                    with patch.object(twitter_monitor, "save_seen", side_effect=fake_save_seen):
                        with patch.object(twitter_monitor, "send_tweet", side_effect=fake_send_tweet):
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


class EventPushWindowTest(unittest.TestCase):
    """官方号高价值事件按 _push_event_type 覆盖统一 45 分钟新鲜度窗口。"""

    RELEASE_CLASS = (
        "quota_reset", "quota_compensation", "credit_grant",
        "dev_release", "model_api", "model_launch", "major_product_launch",
    )
    ENTITLEMENT_CLASS = (
        "quota_policy", "plan_entitlement", "model_access", "permanent_plan_change",
    )

    def test_no_annotation_keeps_base_window(self):
        self.assertEqual(twitter_monitor.effective_push_window_minutes({}, 45), 45)

    def test_unknown_event_type_falls_back_to_base(self):
        t = {"_push_event_type": "mystery_event"}
        self.assertEqual(twitter_monitor.effective_push_window_minutes(t, 45), 45)

    def test_release_class_events_extend_to_360(self):
        for event in self.RELEASE_CLASS:
            with self.subTest(event=event):
                t = {"_push_event_type": event}
                self.assertEqual(twitter_monitor.effective_push_window_minutes(t, 45), 360)

    def test_entitlement_class_events_extend_to_1440(self):
        for event in self.ENTITLEMENT_CLASS:
            with self.subTest(event=event):
                t = {"_push_event_type": event}
                self.assertEqual(twitter_monitor.effective_push_window_minutes(t, 45), 1440)

    def test_event_window_never_shrinks_relaxed_base(self):
        # seen 损坏安全模式放宽到 1440，事件窗口 360 不得反向缩小
        t = {"_push_event_type": "quota_reset"}
        self.assertEqual(twitter_monitor.effective_push_window_minutes(t, 1440), 1440)

    def test_disabled_window_stays_disabled(self):
        # base <= 0 表示不限龄，事件窗口不得重新收紧
        t = {"_push_event_type": "quota_policy"}
        self.assertEqual(twitter_monitor.effective_push_window_minutes(t, 0), 0)

    def test_official_events_survive_beyond_default_window(self):
        """20h 前的 quota_policy 与 5h 前的 quota_reset 补推；8h 前的 reset 超窗只记 seen。"""
        # FixedDatetime now = 2026-05-12 00:30 UTC
        tweets = [
            {   # quota_policy → 1440min 窗口，20h 前应补推
                "id": "policy-20h",
                "text": "Weekly usage limits will be 50% higher for all paid users through August 19.",
                "createdAt": "Mon May 11 04:30:00 +0000 2026",
            },
            {   # quota_reset → 360min 窗口，5h 前应补推
                "id": "reset-5h",
                "text": "We've reset 5-hour and weekly rate limits for all users.",
                "createdAt": "Mon May 11 19:30:00 +0000 2026",
            },
            {   # quota_reset 8h 前，超 360min 窗口 → skip stale，只记 seen
                "id": "reset-8h",
                "text": "We've reset the weekly rate limits for Pro users after the incident.",
                "createdAt": "Mon May 11 16:30:00 +0000 2026",
            },
        ]
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)
        pushed_ids = []
        saved = {}

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None,
                            reply_to_message_id=None):
            pushed_ids.append(t["id"])
            return {"ok": True}

        def fake_save_seen(username, seen, last_post_ts=None):
            saved["seen"] = set(seen)

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "_ACCOUNT_CONFIG_BY_USERNAME",
                          {"officialu": {"push_policy": "claude_dev_original"}}), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=tweets), \
             patch.object(twitter_monitor, "load_seen",
                          return_value=({"already-seen"}, "2026-05-11T00:00:00+00:00")), \
             patch.object(twitter_monitor, "save_seen", side_effect=fake_save_seen), \
             patch.object(twitter_monitor, "load_push_retry", return_value=set()), \
             patch.object(twitter_monitor, "save_push_retry", return_value=None), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send_tweet), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            new, pushed, filt, ov = twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="officialu",
                bot_token="b", chat_id="c", args=args)
        self.assertEqual(new, 3)
        self.assertEqual(pushed, 2)
        self.assertEqual(pushed_ids, ["policy-20h", "reset-5h"])
        # 超窗的 reset-8h 仍进 seen，不进 push_retry
        self.assertIn("reset-8h", saved["seen"])

    def test_normal_account_not_extended_by_event_windows(self):
        """无 push_policy 的普通账号不带事件注解，46 分钟前的推文仍判 stale。"""
        tweets = [{
            "id": "plain-46m",
            "text": "This ordinary tweet is long enough to pass the classifier filters easily.",
            "createdAt": "Mon May 11 23:44:00 +0000 2026",
        }]
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)
        pushed_ids = []

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None,
                            reply_to_message_id=None):
            pushed_ids.append(t["id"])
            return {"ok": True}

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "_ACCOUNT_CONFIG_BY_USERNAME", {}), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=tweets), \
             patch.object(twitter_monitor, "load_seen",
                          return_value=({"already-seen"}, "2026-05-11T00:00:00+00:00")), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "load_push_retry", return_value=set()), \
             patch.object(twitter_monitor, "save_push_retry", return_value=None), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send_tweet), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            new, pushed, filt, ov = twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="plainu",
                bot_token="b", chat_id="c", args=args)
        self.assertEqual(new, 1)
        self.assertEqual(pushed, 0)
        self.assertEqual(pushed_ids, [])


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
                     patch.object(twitter_monitor, "update_status_dashboard", return_value=None), \
                     patch.object(twitter_monitor, "check_cookie_health", return_value=None):
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


class RichVideoEmbedTest(unittest.TestCase):
    """F3：rich_video_embed 开启时短视频嵌可播放 <video>（HEAD 精确选流优先、
    估算回退、全超限回封面），send_tweet 剥视频降级梯，歧义不双发。"""

    def setUp(self):
        p = patch.object(twitter_monitor, "_RICH_VIDEO_ENABLED", True)
        p.start()
        self.addCleanup(p.stop)

    def _video_tweet(self, variants, duration_ms=42000):
        return {"id": "1", "text": "看视频",
                "media": [{"type": "video",
                           "url": "https://pbs.twimg.com/thumb/img/abc",
                           "video_url": variants[0]["url"] if variants else None,
                           "bitrate": variants[0]["bitrate"] if variants else None,
                           "variants": variants,
                           "duration_ms": duration_ms}]}

    def test_video_embeds_playable_video_when_enabled(self):
        t = self._video_tweet([{"url": "https://video.twimg.com/hi.mp4", "bitrate": 2176000}])
        with patch.object(twitter_monitor, "_head_content_length", return_value=3_715_988):
            _msg, rich, _ = twitter_monitor.format_message("OpenAIDevs", t, None)
        self.assertIn('<video src="https://video.twimg.com/hi.mp4"/>', rich)
        self.assertNotIn("▶️", rich)          # 可播视频不再要提示
        self.assertNotIn("pbs.twimg.com/thumb", rich)  # 不再嵌封面
        self.assertNotIn("<video", _msg)      # HTML 回退不嵌媒体

    def test_picks_largest_variant_under_cap_via_head(self):
        t = self._video_tweet([
            {"url": "https://video.twimg.com/2160p.mp4", "bitrate": 25128000},
            {"url": "https://video.twimg.com/720p.mp4", "bitrate": 2176000},
        ])
        sizes = {"https://video.twimg.com/2160p.mp4": 49_000_000,
                 "https://video.twimg.com/720p.mp4": 9_400_000}
        with patch.object(twitter_monitor, "_head_content_length",
                          side_effect=lambda u: sizes[u]):
            _msg, rich, _ = twitter_monitor.format_message("u", t, None)
        self.assertIn('720p.mp4"/>', rich)
        self.assertNotIn("2160p", rich)

    def test_head_failure_falls_back_to_estimate(self):
        # 104s：10.368Mbps 估 ~128MB 超限；950kbps 估 ~12MB 可嵌
        t = self._video_tweet([
            {"url": "https://video.twimg.com/1080p.mp4", "bitrate": 10368000},
            {"url": "https://video.twimg.com/360p.mp4", "bitrate": 950000},
        ], duration_ms=104000)
        with patch.object(twitter_monitor, "_head_content_length", return_value=None):
            _msg, rich, _ = twitter_monitor.format_message("u", t, None)
        self.assertIn('360p.mp4"/>', rich)

    def test_all_variants_oversize_keeps_poster(self):
        t = self._video_tweet([{"url": "https://video.twimg.com/big.mp4",
                                "bitrate": 25128000}], duration_ms=734000)
        with patch.object(twitter_monitor, "_head_content_length",
                          return_value=2_199_000_000):
            _msg, rich, _ = twitter_monitor.format_message("u", t, None)
        self.assertNotIn("<video", rich)
        self.assertIn('<img src="https://pbs.twimg.com/thumb/img/abc"/>', rich)
        self.assertIn("▶️ 视频 · 12:14", rich)

    def test_gif_embeds_video_without_bitrate(self):
        t = {"id": "2", "text": "动图",
             "media": [{"type": "animated_gif",
                        "url": "https://pbs.twimg.com/tweet_video_thumb/x.jpg",
                        "video_url": "https://video.twimg.com/tweet_video/x.mp4",
                        "bitrate": 0, "variants": [], "duration_ms": None}]}
        with patch.object(twitter_monitor, "_head_content_length", return_value=None):
            _msg, rich, _ = twitter_monitor.format_message("u", t, None)
        self.assertIn('<video src="https://video.twimg.com/tweet_video/x.mp4"/>', rich)

    def test_mixed_photo_video_collage(self):
        t = {"id": "3", "text": "图+视频",
             "media": [
                 {"type": "photo", "url": "https://pbs.twimg.com/media/p1.jpg"},
                 {"type": "video", "url": "https://pbs.twimg.com/thumb2.jpg",
                  "video_url": "https://video.twimg.com/v.mp4",
                  "variants": [{"url": "https://video.twimg.com/v.mp4", "bitrate": 1000}],
                  "duration_ms": 5000, "bitrate": 1000}]}
        with patch.object(twitter_monitor, "_head_content_length", return_value=1_000_000):
            _msg, rich, _ = twitter_monitor.format_message("u", t, None)
        self.assertIn("<tg-collage>", rich)
        self.assertIn('<img src="https://pbs.twimg.com/media/p1.jpg"/>', rich)
        self.assertIn('<video src="https://video.twimg.com/v.mp4"/>', rich)

    def test_send_tweet_strips_video_and_retries_rich_on_400(self):
        t = self._video_tweet([{"url": "https://video.twimg.com/hi.mp4", "bitrate": 1000}])
        rich_calls, legacy_calls = [], []

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None,
                      reply_to_message_id=None):
            rich_calls.append(html)
            if "<video" in html:
                return {"ok": False, "rich_fallback": True, "description": "WEBPAGE_MEDIA_EMPTY"}
            return {"ok": True, "result": {"message_id": 9}}

        with patch.object(twitter_monitor, "_head_content_length", return_value=1000), \
             patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
             patch.object(twitter_monitor, "send_telegram",
                          side_effect=lambda *a, **k: legacy_calls.append(a) or {"ok": True}):
            r = twitter_monitor.send_tweet("b", "c", "u", t, None)
        self.assertTrue(r.get("ok"))
        self.assertEqual(len(rich_calls), 2)
        self.assertIn("<video", rich_calls[0])
        self.assertNotIn("<video", rich_calls[1])   # 第二发剥了视频（封面）
        self.assertIn("<img", rich_calls[1])
        self.assertEqual(legacy_calls, [])          # 不落 HTML

    def test_send_tweet_video_then_second_reject_keeps_poster_photo(self):
        t = self._video_tweet([{"url": "https://video.twimg.com/hi.mp4", "bitrate": 1000}])
        photo_calls, legacy_calls = [], []

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None,
                      reply_to_message_id=None):
            return {"ok": False, "rich_fallback": True, "description": "nope"}

        with patch.object(twitter_monitor, "_head_content_length", return_value=1000), \
             patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
             patch.object(twitter_monitor, "send_telegram_photo",
                          side_effect=lambda *a, **k: photo_calls.append((a, k)) or {"ok": True}), \
             patch.object(twitter_monitor, "send_telegram",
                          side_effect=lambda *a, **k: legacy_calls.append(a) or {"ok": True}):
            r = twitter_monitor.send_tweet("b", "c", "u", t, None)
        self.assertTrue(r.get("ok"))
        self.assertEqual(len(photo_calls), 1)       # 两级 rich 均拒 → 保留视频封面
        self.assertEqual(photo_calls[0][0][2], "https://pbs.twimg.com/thumb/img/abc")
        self.assertEqual(legacy_calls, [])

    def test_send_tweet_ambiguous_video_never_retries(self):
        t = self._video_tweet([{"url": "https://video.twimg.com/hi.mp4", "bitrate": 1000}])
        rich_calls = []

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None,
                      reply_to_message_id=None):
            rich_calls.append(html)
            return {"ok": True, "assumed_delivered": True}

        with patch.object(twitter_monitor, "_head_content_length", return_value=1000), \
             patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich):
            r = twitter_monitor.send_tweet("b", "c", "u", t, None)
        self.assertTrue(r.get("ok"))
        self.assertEqual(len(rich_calls), 1)        # 歧义按已送达，绝不第二发

    def test_disabled_flag_keeps_poster_behavior(self):
        t = self._video_tweet([{"url": "https://video.twimg.com/hi.mp4", "bitrate": 1000}])
        with patch.object(twitter_monitor, "_RICH_VIDEO_ENABLED", False), \
             patch.object(twitter_monitor, "_head_content_length", return_value=1000):
            _msg, rich, _ = twitter_monitor.format_message("u", t, None)
        self.assertNotIn("<video", rich)
        self.assertIn("▶️ 视频", rich)


class StripAnsiTest(unittest.TestCase):
    """ANSI escape codes from colored CLI stderr must not reach TG alerts."""

    def test_strip_ansi_removes_sgr_bold(self):
        raw = "\x1b[1mgrok-4.5[1M]\x1b[22m failed"
        self.assertEqual(twitter_monitor.strip_ansi(raw), "grok-4.5[1M] failed")

    def test_strip_ansi_removes_colors_and_keeps_text(self):
        raw = "\x1b[31mERROR\x1b[0m: boom \x1b[1mbold\x1b[22m"
        self.assertEqual(twitter_monitor.strip_ansi(raw), "ERROR: boom bold")

    def test_strip_ansi_none_and_empty(self):
        self.assertEqual(twitter_monitor.strip_ansi(""), "")
        self.assertEqual(twitter_monitor.strip_ansi(None), "")

    def test_fetch_article_markdown_failure_strips_ansi_from_cli_stderr(self):
        colored = "\x1b[31mfetch failed\x1b[0m: cookie expired"
        entry = {
            "article_id": "123",
            "author": "dotey",
            "tweet_id": "999",
        }
        fake = argparse.Namespace(
            returncode=1, stdout="", stderr=colored,
        )
        with patch.object(twitter_monitor, "load_article_markdown_cmd",
                          return_value="/usr/local/bin/x-article-to-markdown"), \
             patch.object(twitter_monitor.subprocess, "run", return_value=fake):
            md, err = twitter_monitor.fetch_article_markdown("dotey", entry)
        self.assertIsNone(md)
        self.assertTrue(err.startswith("markdown_fetch_failed:"))
        self.assertNotIn("\x1b", err)
        self.assertNotIn("[31m", err)
        self.assertNotIn("[0m", err)
        self.assertIn("fetch failed: cookie expired", err)

    def test_format_article_failure_message_reason_has_no_ansi(self):
        entry = {
            "article_id": "123",
            "author": "dotey",
            "article_title": "t",
            "attempts": 1,
            "failed_stage": "fetch",
        }
        reason = "markdown_fetch_failed:\x1b[1mbad\x1b[22m"
        msg, _ = twitter_monitor.format_article_failure_message("dotey", entry, reason)
        self.assertNotIn("\x1b", msg)
        self.assertNotIn("[1m", msg)
        self.assertNotIn("[22m", msg)
        self.assertIn("markdown_fetch_failed:bad", msg)

    def test_note_account_failure_strips_ansi_in_stored_and_alert(self):
        failures = {}
        err = "HTTP 401: \x1b[31munauthorized\x1b[0m"
        with patch.object(twitter_monitor, "send_telegram",
                          return_value={"ok": True, "result": {"message_id": 1}}) as send, \
             patch.object(twitter_monitor, "_tg_post_quiet", return_value={"ok": True}), \
             patch.object(twitter_monitor, "FAIL_ALERT_THRESHOLD", 1):
            twitter_monitor.note_account_failure(
                failures, "dotey", err, "tok", "chat", dry_run=False)
        self.assertEqual(failures["dotey"]["last_error"], "HTTP 401: unauthorized")
        sent_text = send.call_args[0][2]
        self.assertNotIn("\x1b", sent_text)
        self.assertIn("HTTP 401: unauthorized", sent_text)


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

    def test_no_strip_without_media(self):
        # 没有任何媒体（t['media'] 缺失，GraphQL 未提取到）→ 门控不成立，不剥，
        # 正文里的 t.co 原样保留
        t = {"id": "3",
             "text": "分享一个链接 https://t.co/PLAINLINK",
             "extended_entities": {"media": [
                 {"type": "photo", "url": "https://t.co/PLAINLINK",
                  "media_url_https": "https://pbs.twimg.com/media/p.jpg"}]}}
        _msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        self.assertIn("https://t.co/PLAINLINK", rich)

    def test_media_tco_stripped_for_video_tweet(self):
        # 视频推文（2026-07-13 修复）：视频已嵌 rich 媒体块（可播放 video 或封面+时长），
        # 正文尾部的媒体 t.co 同样冗余，必须剥掉——此前门控只认 photo，视频推文漏剥。
        t = {"id": "4",
             "text": "新版的ChatGPT对话学英语 https://t.co/VIDEOLINK",
             "media": [{"type": "video", "url": "https://pbs.twimg.com/cover.jpg",
                        "duration_ms": 67000}],
             "extended_entities": {"media": [
                 {"type": "video", "url": "https://t.co/VIDEOLINK",
                  "media_url_https": "https://pbs.twimg.com/cover.jpg"}]}}
        with patch.object(twitter_monitor, "_RICH_VIDEO_ENABLED", False):
            msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        self.assertNotIn("https://t.co/VIDEOLINK", msg)   # HTML 回退不含媒体裸链
        self.assertNotIn("https://t.co/VIDEOLINK", rich)  # rich 也不含
        self.assertIn("新版的ChatGPT对话学英语", rich)      # 正文其余部分保留

    def test_user_shared_tco_expanded_to_real_url(self):
        # 用户主动分享的链接（entities.urls）：t.co 还原为 expanded_url，而不是保留
        # 不透明短链，也不能误删（2026-07-13 第二形态：vista8 QMReader 推文）。
        t = {"id": "5",
             "text": "介绍视频不错。\n\nhttps://t.co/SHARELINK https://t.co/VIDEOLINK",
             "media": [{"type": "video", "url": "https://pbs.twimg.com/cover.jpg",
                        "duration_ms": 62000}],
             "entities": {"urls": [
                 {"url": "https://t.co/SHARELINK",
                  "expanded_url": "https://rss.example.ai/",
                  "display_url": "rss.example.ai"}]},
             "extended_entities": {"media": [
                 {"type": "video", "url": "https://t.co/VIDEOLINK",
                  "media_url_https": "https://pbs.twimg.com/cover.jpg"}]}}
        with patch.object(twitter_monitor, "_RICH_VIDEO_ENABLED", False):
            msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        for out in (msg, rich):
            self.assertNotIn("https://t.co/SHARELINK", out)  # 短链不裸露
            self.assertIn("https://rss.example.ai/", out)     # 换成真实 URL
            self.assertNotIn("https://t.co/VIDEOLINK", out)  # 媒体短链仍剥掉

    def test_shared_url_is_explicit_clickable_anchor_with_media(self):
        t = {"id": "5a", "text": "开源地址 https://t.co/SHARELINK",
             "entities": {"urls": [{
                 "url": "https://t.co/SHARELINK",
                 "expanded_url": "https://github.com/joeseesun/qiaomu-youtube-download",
                 "display_url": "github.com/joeseesun/qiaomu-youtube-download"}]},
             "media": [{"type": "photo", "url": "https://pbs.twimg.com/media/p1.jpg"}]}
        msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        anchor = ('<a href="https://github.com/joeseesun/qiaomu-youtube-download">'
                  'github.com/joeseesun/qiaomu-youtube-download</a>')
        self.assertIn(anchor, msg)
        self.assertIn(anchor, rich)
        self.assertIn('<img src="https://pbs.twimg.com/media/p1.jpg"/>', rich)

    def test_unsafe_entity_destination_is_not_rendered_as_anchor(self):
        t = {"id": "5b", "text": "不要打开 https://t.co/BADLINK",
             "entities": {"urls": [{"url": "https://t.co/BADLINK",
                                      "expanded_url": "javascript:alert(1)"}]}}
        msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        self.assertNotIn('<a href="javascript:', msg)
        self.assertNotIn('<a href="javascript:', rich)
        self.assertEqual(twitter_monitor._primary_external_url(t), "")

    def test_link_label_is_escaped_and_capped(self):
        t = {"id": "5c", "text": "链接 https://t.co/LABEL",
             "entities": {"urls": [{
                 "url": "https://t.co/LABEL", "expanded_url": "https://example.com/post",
                 "display_url": "<b>not markup</b>" + "x" * 600}]}}
        _msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        self.assertNotIn("<b>not markup</b>", rich)
        self.assertIn("&lt;b&gt;not markup&lt;/b&gt;", rich)
        self.assertLessEqual(len(twitter_monitor._tweet_link_replacements(t)["https://t.co/LABEL"][1]),
                             512)

    def test_note_tweet_urls_expanded(self):
        # 长推（note_tweet）里的分享短链也要用 note_tweet.entities.urls 还原
        t = {"id": "6",
             "text": "壳截断…",
             "note_tweet": {"text": "长文正文，推荐 https://t.co/NOTELINK 这篇。",
                             "entities": {"urls": [
                                 {"url": "https://t.co/NOTELINK",
                                  "expanded_url": "https://blog.example.com/post"}]}}}
        msg, rich, _ = twitter_monitor.format_message("vista8", t, None)
        for out in (msg, rich):
            self.assertNotIn("https://t.co/NOTELINK", out)
            self.assertIn("https://blog.example.com/post", out)

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

    def test_timeline_modules_are_flattened(self):
        """Thread-heavy accounts can return only TimelineTimelineModule entries."""
        import json as _json
        import twitter_graphql as tg
        def item(tweet_id, text):
            return {"item": {"itemContent": {"tweet_results": {"result": {
                "__typename": "Tweet",
                "legacy": {"id_str": tweet_id, "full_text": text,
                           "created_at": "Sat Jul 18 02:14:43 +0000 2026"},
            }}}}}
        synthetic = {"data": {"user": {"result": {"timeline_v2": {"timeline": {
            "instructions": [{"entries": [{"content": {
                "entryType": "TimelineTimelineModule",
                "items": [item("301", "thread root"), item("302", "self reply")],
            }}]}],
        }}}}}}
        with patch.object(tg, "_auth_headers", lambda: None), \
             patch.object(tg, "_get_guest_token", lambda: "gt"), \
             patch.object(tg, "get_user_id", lambda u: "123"), \
             patch.object(tg, "_curl", lambda *a, **k: _json.dumps(synthetic)):
            tweets = tg.fetch_tweets("claudeai", limit=20)
        self.assertEqual([t["id"] for t in tweets], ["301", "302"])


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


class NoteTweetEntityNormalizationTest(unittest.TestCase):
    """Long-form URL entities must survive GraphQL normalization into rendering."""

    def test_original_note_entity_becomes_clickable_link(self):
        import json as _json
        import twitter_graphql as tg
        short = "https://t.co/NOTELINK"
        raw = {
            "__typename": "Tweet",
            "legacy": {"id_str": "777", "full_text": "壳文本",
                       "created_at": "Fri Jun 05 17:30:00 +0000 2026", "entities": {"urls": []}},
            "note_tweet": {"note_tweet_results": {"result": {
                "text": "完整长文推荐 " + short,
                "entity_set": {"urls": [{
                    "url": short, "expanded_url": "https://blog.example.com/post",
                    "display_url": "blog.example.com/post"}]},
            }}},
        }
        synthetic = {"data": {"user": {"result": {"timeline_v2": {"timeline": {"instructions": [
            {"entries": [{"content": {"itemContent": {"tweet_results": {"result": raw}}}}]},
        ]}}}}}}
        with patch.object(tg, "_auth_headers", lambda: None), \
             patch.object(tg, "_get_guest_token", lambda: "gt"), \
             patch.object(tg, "get_user_id", lambda u: "1"), \
             patch.object(tg, "_curl", lambda *a, **k: _json.dumps(synthetic)):
            tweet = tg.fetch_tweets("vista8", limit=20)[0]
        self.assertEqual(tweet["note_tweet"]["entities"]["urls"][0]["url"], short)
        _msg, rich, _ = twitter_monitor.format_message("vista8", tweet)
        self.assertIn('<a href="https://blog.example.com/post">blog.example.com/post</a>', rich)


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

        def fake_legacy(token, chat_id, text, link="", thread_id=None,
                        reply_to_message_id=None):
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

        def flaky_send(token, chat_id, username, tweet, ai=None, thread_id=None,
                       reply_to_message_id=None):
            return {"ok": tweet["id"] != "t2"}

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=tweets), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "load_push_retry", return_value=set()), \
             patch.object(twitter_monitor, "save_push_retry", return_value=None), \
             patch.object(twitter_monitor, "send_tweet", side_effect=flaky_send), \
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

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None,
                      reply_to_message_id=None):
            calls["rich"] += 1
            calls["html"] = html
            return {"ok": True, "result": {"message_id": 1}}

        def fake_legacy(token, chat_id, text, link="", thread_id=None,
                        reply_to_message_id=None):
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

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None,
                      reply_to_message_id=None):
            return {"ok": False, "rich_fallback": True}

        def fake_legacy(token, chat_id, text, link="", thread_id=None,
                        reply_to_message_id=None):
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

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None,
                      reply_to_message_id=None):
            return {"ok": False}

        def fake_legacy(token, chat_id, text, link="", thread_id=None,
                        reply_to_message_id=None):
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

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None,
                      reply_to_message_id=None):
            calls["rich"] += 1
            calls["html_len"] = len(html)
            return {"ok": True}

        def fake_legacy(token, chat_id, text, link="", thread_id=None,
                        reply_to_message_id=None):
            calls["legacy"] += 1
            return {"ok": True}

        big = {"id": "9", "note_tweet": {"text": "长" * 40000}}
        with patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
             patch.object(twitter_monitor, "send_telegram", side_effect=fake_legacy):
            r = twitter_monitor.send_tweet("tok", "42", "vista8", big, None)
        self.assertEqual(calls["rich"], 1)
        self.assertEqual(calls["legacy"], 0)
        self.assertLessEqual(calls["html_len"], twitter_monitor.RICH_MESSAGE_MAX_CHARS)

    def test_text_only_external_link_uses_standard_preview_path(self):
        calls = {"rich": 0, "legacy": 0, "preview_url": "", "link": ""}
        tweet = {
            "id": "10", "text": "推荐 https://t.co/EXTERNAL",
            "entities": {"urls": [{
                "url": "https://t.co/EXTERNAL",
                "expanded_url": "https://example.com/post",
                "display_url": "example.com/post"}]},
        }

        def fake_rich(*args, **kwargs):
            calls["rich"] += 1
            return {"ok": True}

        def fake_legacy(token, chat_id, text, link="", *, preview_url="", thread_id=None,
                        reply_to_message_id=None):
            calls["legacy"] += 1
            calls["preview_url"] = preview_url
            calls["link"] = link
            return {"ok": True}

        with patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
             patch.object(twitter_monitor, "send_telegram", side_effect=fake_legacy):
            r = twitter_monitor.send_tweet("tok", "42", "vista8", tweet)
        self.assertTrue(r["ok"])
        self.assertEqual(calls["rich"], 0)
        self.assertEqual(calls["legacy"], 1)
        self.assertEqual(calls["preview_url"], "https://example.com/post")
        self.assertEqual(calls["link"], "https://x.com/vista8/status/10")

    def test_unrenderable_media_does_not_suppress_external_preview(self):
        tweet = {
            "id": "11", "text": "推荐 https://t.co/EXTERNAL",
            "entities": {"urls": [{"url": "https://t.co/EXTERNAL",
                                      "expanded_url": "https://example.com/post"}]},
            "media": [{"type": "photo", "url": "http://invalid.example/photo.jpg"}],
        }
        calls = {"rich": 0, "legacy": 0}

        def fake_legacy(*args, **kwargs):
            calls["legacy"] += 1
            self.assertEqual(kwargs["preview_url"], "https://example.com/post")
            return {"ok": True}

        with patch.object(twitter_monitor, "send_telegram_rich",
                          side_effect=lambda *a, **k: calls.__setitem__("rich", calls["rich"] + 1)), \
             patch.object(twitter_monitor, "send_telegram", side_effect=fake_legacy):
            r = twitter_monitor.send_tweet("tok", "42", "vista8", tweet)
        self.assertTrue(r["ok"])
        self.assertEqual(calls, {"rich": 0, "legacy": 1})

    def test_rich_media_rejection_keeps_photo_external_anchor_and_x_button(self):
        tweet = {
            "id": "12", "text": "开源地址 https://t.co/EXTERNAL",
            "entities": {"urls": [{
                "url": "https://t.co/EXTERNAL",
                "expanded_url": "https://github.com/joeseesun/qiaomu-youtube-download",
                "display_url": "github.com/joeseesun/qiaomu-youtube-download",
            }]},
            "media": [{"type": "photo", "url": "https://pbs.twimg.com/media/original.jpg"}],
        }
        calls = {"rich": 0, "photo": [], "legacy": 0}

        def fake_photo(token, chat_id, photo, caption="", link="", *, thread_id=None,
                       reply_to_message_id=None):
            calls["photo"].append({"photo": photo, "caption": caption,
                                   "link": link, "thread_id": thread_id})
            return {"ok": True, "result": {"message_id": 12}}

        with patch.object(twitter_monitor, "send_telegram_rich",
                          side_effect=lambda *a, **k: calls.__setitem__("rich", calls["rich"] + 1)
                          or {"ok": False, "rich_fallback": True}), \
             patch.object(twitter_monitor, "send_telegram_photo", side_effect=fake_photo), \
             patch.object(twitter_monitor, "send_telegram",
                          side_effect=lambda *a, **k: calls.__setitem__("legacy", calls["legacy"] + 1)
                          or {"ok": True}):
            r = twitter_monitor.send_tweet("tok", "42", "vista8", tweet, thread_id=19)

        self.assertTrue(r["ok"])
        self.assertEqual(calls["rich"], 1)
        self.assertEqual(calls["legacy"], 0)
        self.assertEqual(len(calls["photo"]), 1)
        sent = calls["photo"][0]
        self.assertEqual(sent["photo"], "https://pbs.twimg.com/media/original.jpg")
        self.assertIn('href="https://github.com/joeseesun/qiaomu-youtube-download"',
                      sent["caption"])
        self.assertEqual(sent["link"], "https://x.com/vista8/status/12")
        self.assertEqual(sent["thread_id"], 19)

    def test_rejected_photo_falls_back_to_full_html_text(self):
        tweet = {"id": "13", "text": "带图正文", "media": [
            {"type": "photo", "url": "https://pbs.twimg.com/media/original.jpg"}]}
        calls = {"photo": 0, "legacy": 0}

        def fake_legacy(token, chat_id, text, link="", *, preview_url="", thread_id=None,
                        reply_to_message_id=None):
            calls["legacy"] += 1
            self.assertIn("带图正文", text)
            self.assertEqual(link, "https://x.com/vista8/status/13")
            return {"ok": True}

        with patch.object(twitter_monitor, "send_telegram_rich",
                          return_value={"ok": False, "rich_fallback": True}), \
             patch.object(twitter_monitor, "send_telegram_photo",
                          side_effect=lambda *a, **k: calls.__setitem__("photo", calls["photo"] + 1)
                          or {"ok": False, "photo_fallback": True, "description": "bad image"}), \
             patch.object(twitter_monitor, "send_telegram", side_effect=fake_legacy):
            r = twitter_monitor.send_tweet("tok", "42", "vista8", tweet)

        self.assertTrue(r["ok"])
        self.assertEqual(calls, {"photo": 1, "legacy": 1})

    def test_photo_caption_stays_inside_caption_budget_with_external_link(self):
        tweet = {
            "text": "🐍" * 1200 + " https://t.co/EXTERNAL",
            "entities": {"urls": [{
                "url": "https://t.co/EXTERNAL", "expanded_url": "https://example.com/post",
                "display_url": "example.com/post"}]},
        }
        caption = twitter_monitor._tweet_photo_caption(
            "📢 @vista8\n\n" + twitter_monitor._render_tweet_urls(tweet["text"], tweet, rich=False), tweet)
        self.assertLessEqual(len(twitter_monitor._html_to_plain(caption).encode("utf-16-le")) // 2,
                             twitter_monitor.PHOTO_CAPTION_MAX_UTF16)
        self.assertIn('href="https://example.com/post"', caption)

    def test_send_telegram_keeps_x_button_but_previews_external_url(self):
        captured = []
        with patch.object(twitter_monitor, "_tg_post",
                          side_effect=lambda token, payload, method="sendMessage": captured.append(payload) or {"ok": True}):
            r = twitter_monitor.send_telegram(
                "tok", "42", '<a href="https://example.com/post">example.com/post</a>',
                "https://x.com/vista8/status/10", preview_url="https://example.com/post")
        self.assertTrue(r["ok"])
        payload = captured[0]
        self.assertEqual(payload["link_preview_options"]["url"], "https://example.com/post")
        self.assertEqual(payload["reply_markup"]["inline_keyboard"][0][0]["url"],
                         "https://x.com/vista8/status/10")


class _FakeBackend:
    def __init__(self, name, result=None, exc=None):
        self.name = name
        self._result = result
        self._exc = exc

    def complete(self, prompt, max_tokens=1200, temperature=0.2):
        if self._exc:
            raise self._exc
        return self._result

    def complete_with_images(self, prompt, images, max_tokens=1200, temperature=0.2):
        if self._exc:
            raise self._exc
        return self._result

    def classify(self, username, text):
        if self._exc:
            raise self._exc
        return False, self._result or ""

    def classify_musing(self, username, text):
        if self._exc:
            raise self._exc
        return False, self._result or ""


def _http_error(code, body, reason="Forbidden"):
    return urllib.error.HTTPError(
        "http://example.test/v1", code, reason, {}, io.BytesIO(body.encode("utf-8")))


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

    def test_http_403_body_surfaced_instead_of_all_ai_failed(self):
        body = json.dumps({
            "error": {
                "code": 403,
                "message": "Your project has been denied access. Please contact support.",
                "status": "PERMISSION_DENIED",
            }
        })
        ai = twitter_monitor.AIClassifier([
            _FakeBackend("gemini", exc=_http_error(403, body)),
        ])
        text, reason = ai.complete("p")
        self.assertIsNone(text)
        self.assertNotEqual(reason, "all_ai_failed")
        self.assertIn("gemini:http_403", reason)
        self.assertIn("PERMISSION_DENIED", reason)
        self.assertIn("denied access", reason)

    def test_http_403_body_surfaced_for_vision_complete(self):
        body = json.dumps({
            "error": {"code": "permission_denied", "message": "project denied"}
        })
        ai = twitter_monitor.AIClassifier([
            _FakeBackend("gemini", exc=_http_error(403, body)),
        ])
        text, reason = ai.complete_with_images("p", [])
        self.assertIsNone(text)
        self.assertIn("gemini:http_403", reason)
        self.assertIn("project denied", reason)
        self.assertNotEqual(reason, "all_image_ai_failed")

    def test_format_ai_backend_error_reads_http_body(self):
        err = twitter_monitor.format_ai_backend_error(
            "gemini", _http_error(403, '{"error":{"status":"PERMISSION_DENIED","message":"nope"}}'))
        self.assertEqual(err, "gemini:http_403:PERMISSION_DENIED: nope")

    def test_provider_auth_failure_helper(self):
        self.assertTrue(twitter_monitor._is_ai_provider_auth_failure(
            "gemini:http_403:PERMISSION_DENIED: nope"))
        self.assertFalse(twitter_monitor._is_ai_provider_auth_failure("all_ai_failed"))
        self.assertFalse(twitter_monitor._is_ai_provider_auth_failure(
            "gemini:http_429:RESOURCE_EXHAUSTED"))

    def test_ai_call_failed_covers_http_and_exhausted(self):
        self.assertTrue(twitter_monitor._is_ai_call_failed("all_ai_failed"))
        self.assertTrue(twitter_monitor._is_ai_call_failed(
            "gemini:http_403:PERMISSION_DENIED: nope"))
        self.assertTrue(twitter_monitor._is_ai_call_failed("gemini:http_429:RESOURCE_EXHAUSTED"))
        self.assertFalse(twitter_monitor._is_ai_call_failed("gemini:not_promo"))


class GeminiApiBaseGuardTest(unittest.TestCase):
    """Gemini backends must never fall back to Google's official endpoint."""

    def test_empty_api_base_rejected(self):
        with self.assertRaises(ValueError) as ctx:
            twitter_monitor.resolve_gemini_api_base("")
        self.assertNotIn("generativelanguage.googleapis.com/v1beta/models", str(ctx.exception))

    def test_google_direct_url_rejected(self):
        with self.assertRaises(ValueError):
            twitter_monitor.resolve_gemini_api_base(
                "https://generativelanguage.googleapis.com/v1beta")
        self.assertTrue(twitter_monitor.is_direct_google_gemini_base(
            "https://generativelanguage.googleapis.com/v1beta"))
        self.assertFalse(twitter_monitor.is_direct_google_gemini_base(
            "http://100.71.254.40:8317/v1beta"))

    def test_backend_init_rejects_google_default(self):
        with self.assertRaises(ValueError):
            twitter_monitor.AIBackend("gemini", "", "k", "gemini-3.7-flash-high", "gemini")
        with self.assertRaises(ValueError):
            twitter_monitor.AIBackend(
                "gemini",
                "https://generativelanguage.googleapis.com/v1beta",
                "k",
                "gemini-3.7-flash-high",
                "gemini",
            )

    def test_generate_url_uses_configured_proxy_only(self):
        backend = twitter_monitor.AIBackend(
            "gemini",
            "http://127.0.0.1:8317/v1beta",
            "proxy-key",
            "gemini-3.7-flash-high",
            "gemini",
        )
        url = backend._gemini_generate_url()
        self.assertTrue(url.startswith("http://127.0.0.1:8317/v1beta/models/"))
        self.assertIn("generateContent", url)
        self.assertNotIn("generativelanguage.googleapis.com", url)

    def test_load_skips_google_direct_and_missing_api_base(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "twitter_ai.json")
            with open(path, "w") as f:
                json.dump({
                    "backends": [
                        {
                            "name": "google-dead",
                            "type": "gemini",
                            "api_base": "https://generativelanguage.googleapis.com/v1beta",
                            "api_key": "k",
                            "model": "gemini-3.5-flash",
                        },
                        {
                            "name": "no-base",
                            "type": "gemini",
                            "api_key": "k",
                            "model": "gemini-3.7-flash-high",
                        },
                        {
                            "name": "cliproxy",
                            "type": "gemini",
                            "api_base": "http://127.0.0.1:8317/v1beta",
                            "api_key": "k",
                            "model": "gemini-3.7-flash-high",
                        },
                    ]
                }, f)
            with patch.object(twitter_monitor, "AI_CONFIG_PATH", path):
                ai = twitter_monitor.AIClassifier.load()
        self.assertEqual([b.name for b in ai._backends], ["cliproxy"])
        self.assertEqual(ai._backends[0].api_base, "http://127.0.0.1:8317/v1beta")

    def test_article_model_overrides_only_article_backends(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "twitter_ai.json")
            with open(path, "w") as f:
                json.dump({
                    "backends": [{
                        "name": "cliproxy",
                        "type": "gemini",
                        "api_base": "http://127.0.0.1:8317/v1beta",
                        "api_key": "k",
                        "model": "gemini-3.7-flash-high",
                        "timeout": 45,
                    }],
                    "article_model": "gemini-3.8-flash-high",
                }, f)
            with patch.object(twitter_monitor, "AI_CONFIG_PATH", path):
                ai = twitter_monitor.AIClassifier.load()
        self.assertEqual(ai._backends[0].model, "gemini-3.7-flash-high")
        article_ai = ai.for_articles()
        self.assertIsNot(article_ai, ai)
        self.assertEqual(article_ai._backends[0].model, "gemini-3.8-flash-high")
        self.assertEqual(article_ai._backends[0].api_base, "http://127.0.0.1:8317/v1beta")
        self.assertEqual(article_ai._backends[0].timeout, 45)
        url = article_ai._backends[0]._gemini_generate_url()
        self.assertIn("/models/gemini-3.8-flash-high:generateContent", url)
        self.assertNotIn("gemini-3.7-flash-high", url)

    def test_without_article_model_for_articles_reuses_classifier(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "twitter_ai.json")
            with open(path, "w") as f:
                json.dump({
                    "backends": [{
                        "name": "cliproxy",
                        "type": "gemini",
                        "api_base": "http://127.0.0.1:8317/v1beta",
                        "api_key": "k",
                        "model": "gemini-3.7-flash-high",
                    }]
                }, f)
            with patch.object(twitter_monitor, "AI_CONFIG_PATH", path):
                ai = twitter_monitor.AIClassifier.load()
        self.assertIs(ai.for_articles(), ai)


class AIKeyResolveTest(unittest.TestCase):
    def test_api_key_file_used_when_inline_empty(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "device.key")
            with open(path, "w") as f:
                f.write("sk-cpa-test-key\n")
            self.assertEqual(
                twitter_monitor.resolve_ai_api_key({"api_key_file": path}),
                "sk-cpa-test-key")

    def test_inline_key_wins_over_file(self):
        self.assertEqual(
            twitter_monitor.resolve_ai_api_key({
                "api_key": "inline",
                "api_key_file": "/no/such/file",
            }),
            "inline")


class ArticleAIHttpFailureTest(unittest.TestCase):
    def test_queue_surfaces_http_403_and_does_not_burn_attempts(self):
        reason = "gemini:http_403:PERMISSION_DENIED: Your project has been denied access."
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            qpath = root / "u_queue.json"
            entry = {
                "article_id": "2095047892482609152",
                "tweet_id": "1",
                "author": "Khazix0918",
                "article_title": "T",
                "status": "pending",
                "attempts": 0,
                "content": None,
                "detected_at": datetime.now(timezone.utc).isoformat(),
            }
            qpath.write_text(json.dumps([entry]), encoding="utf-8")
            sent = []

            def capture_send(*args, **kwargs):
                sent.append(args[2] if len(args) > 2 else kwargs.get("text"))
                return {"ok": True, "result": {"message_id": 11}}

            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", str(root)), \
                 patch.object(twitter_monitor, "ARTICLE_CACHE_DIR", str(root / "cache")), \
                 patch.object(twitter_monitor, "fetch_article_markdown",
                              return_value=("# Article\n\n" + "x" * 300, None)), \
                 patch.object(twitter_monitor, "summarize_article",
                              return_value=(None, reason)), \
                 patch.object(twitter_monitor, "send_telegram", side_effect=capture_send), \
                 patch.object(twitter_monitor.time, "sleep", return_value=None):
                twitter_monitor.process_article_queue(
                    twitter_monitor.AIClassifier([]), "bot", "chat")
            saved = json.loads(qpath.read_text(encoding="utf-8"))[0]
            self.assertEqual(saved["status"], "failed")
            self.assertEqual(saved["failed_stage"], "ai_summary")
            self.assertEqual(saved["last_error"], reason)
            self.assertEqual(saved["attempts"], 0)
            self.assertEqual(len(sent), 1)
            self.assertIn("gemini:http_403", sent[0])
            self.assertIn("PERMISSION_DENIED", sent[0])

    def test_repeat_auth_failure_skips_telegram(self):
        reason = "gemini:http_403:PERMISSION_DENIED: denied"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            qpath = root / "u_queue.json"
            entry = {
                "article_id": "1", "tweet_id": "1", "author": "u",
                "article_title": "T", "status": "failed", "attempts": 0,
                "content": None, "failure_msg_id": 7404,
                "detected_at": datetime.now(timezone.utc).isoformat(),
            }
            qpath.write_text(json.dumps([entry]), encoding="utf-8")
            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", str(root)), \
                 patch.object(twitter_monitor, "ARTICLE_CACHE_DIR", str(root / "cache")), \
                 patch.object(twitter_monitor, "fetch_article_markdown",
                              return_value=("# Article\n\n" + "x" * 300, None)), \
                 patch.object(twitter_monitor, "summarize_article",
                              return_value=(None, reason)), \
                 patch.object(twitter_monitor, "send_telegram",
                              side_effect=AssertionError("should not notify")) as send, \
                 patch.object(twitter_monitor.time, "sleep", return_value=None):
                twitter_monitor.process_article_queue(
                    twitter_monitor.AIClassifier([]), "bot", "chat")
            self.assertFalse(send.called)
            saved = json.loads(qpath.read_text(encoding="utf-8"))[0]
            self.assertEqual(saved["attempts"], 0)
            self.assertEqual(saved["last_error"], reason)


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

    def test_retweet_source_button_targets_rendered_original(self):
        _msg, _rich, link = twitter_monitor.format_message(
            "dotey", {"id": "999", "text": "RT @orig: 原推正文",
                      "retweeted_status": {"id": "888", "screen_name": "orig"}}, None)
        self.assertEqual(link, "https://x.com/orig/status/888")


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

    def test_summarize_article_uses_for_articles_backend(self):
        class ArticleAI:
            def is_available(self):
                return True

            def complete(self, prompt, max_tokens=1200, temperature=0.2):
                return "from-article-model", "gemini-3.8"

            def complete_with_images(self, prompt, images, max_tokens=1200, temperature=0.2):
                raise AssertionError("no images in this test")

        class WrapperAI:
            def is_available(self):
                return True

            def for_articles(self):
                return ArticleAI()

            def complete(self, prompt, max_tokens=1200, temperature=0.2):
                raise AssertionError("must not use default complete")

        entry = {"article_id": "1", "article_title": "t", "author": "liuren"}
        with patch.object(twitter_monitor, "fetch_article_images", return_value=[]):
            summary, backend = twitter_monitor.summarize_article(
                WrapperAI(), "dotey", entry, "# t\n\nbody")
        self.assertEqual(summary, "from-article-model")
        self.assertEqual(backend, "gemini-3.8")


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

        def fake_send(token, chat_id, username, t, ai=None, thread_id=None,
                      reply_to_message_id=None):
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

        def classify_musing(self, username: str, text: str):
            raise RuntimeError("api down")

    def test_confirm_promo_all_failed_returns_fail_closed(self):
        ai = twitter_monitor.AIClassifier([self.FailingBackend()])
        self.assertEqual(ai.confirm_promo("u", "text"), (False, "all_ai_failed"))

        empty_ai = twitter_monitor.AIClassifier([])
        self.assertEqual(empty_ai.confirm_promo("u", "text"), (False, "all_ai_failed"))

    def test_confirm_musing_all_failed_returns_fail_closed(self):
        ai = twitter_monitor.AIClassifier([self.FailingBackend()])
        self.assertEqual(ai.confirm_musing("u", "text"), (False, "all_ai_failed"))

        empty_ai = twitter_monitor.AIClassifier([])
        self.assertEqual(empty_ai.confirm_musing("u", "text"), (False, "all_ai_failed"))

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

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None,
                            reply_to_message_id=None):
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

    def test_process_user_http_403_is_fail_closed_not_ai_veto(self):
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)
        suspicious_tweet = {
            "id": "s403",
            "text": "byteplus seedance 2.0 api 文档访问体验开通模型冲 200 立即体验方舟平台",
            "createdAt": "Tue May 12 00:20:00 +0000 2026",
        }

        class AuthFailBackend:
            name = "gemini"

            def classify(self, username, text):
                raise _http_error(403, '{"error":{"status":"PERMISSION_DENIED","message":"denied"}}')

        ai = twitter_monitor.AIClassifier([AuthFailBackend()])
        pushed_ids = []

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None,
                            reply_to_message_id=None):
            pushed_ids.append(t["id"])
            return {"ok": True}

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=[suspicious_tweet]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send_tweet), \
             patch.object(twitter_monitor, "_alert_ai_all_failed", return_value=None) as alert, \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            new, pushed, filt, ov = twitter_monitor.process_user(
                pool=None, ai=ai, username="u",
                bot_token="b", chat_id="c", args=args)
        self.assertEqual((new, pushed, filt, ov), (1, 0, 1, 0))
        self.assertEqual(pushed_ids, [])
        self.assertTrue(alert.called)
        self.assertEqual(
            ai.confirm_promo("u", suspicious_tweet["text"])[1].split(":")[1],
            "http_403")


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

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None,
                            reply_to_message_id=None):
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
    def test_x_html_entities_are_normalized_once_before_telegram_rendering(self):
        import html as std_html
        import twitter_graphql as tg
        normalized = tg.normalize_x_text("写作 -&gt; 为了写作去学习实践 &amp; 分享")
        self.assertEqual(normalized, "写作 -> 为了写作去学习实践 & 分享")
        fallback, rich, _ = twitter_monitor.format_message("dotey", {"id": "entity-1", "text": normalized}, None)
        self.assertNotIn("&amp;gt;", fallback)
        self.assertNotIn("&amp;gt;", rich)
        self.assertIn("写作 ->", std_html.unescape(fallback))
        self.assertIn("写作 ->", std_html.unescape(rich))

    def test_x_html_entity_normalization_does_not_weaken_html_escaping(self):
        import twitter_graphql as tg
        normalized = tg.normalize_x_text("&lt;script&gt;alert(1)&lt;/script&gt;")
        fallback, rich, _ = twitter_monitor.format_message("dotey", {"id": "entity-2", "text": normalized}, None)
        for rendered in (fallback, rich):
            self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", rendered)
            self.assertNotIn("<script>alert(1)</script>", rendered)

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

    def test_send_telegram_photo_keeps_caption_button_and_thread(self):
        captured = {}

        def fake_post(token, payload, method="sendMessage"):
            captured["payload"] = payload
            captured["method"] = method
            return {"ok": True}

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post):
            r = twitter_monitor.send_telegram_photo(
                "tok", "chat", "https://pbs.twimg.com/media/a.jpg",
                '<a href="https://example.com">打开外链</a>',
                "https://x.com/u/status/1", thread_id=19)
        self.assertTrue(r["ok"])
        self.assertEqual(captured["method"], "sendPhoto")
        self.assertEqual(captured["payload"].get("message_thread_id"), 19)
        self.assertEqual(captured["payload"]["photo"], "https://pbs.twimg.com/media/a.jpg")
        self.assertEqual(captured["payload"]["reply_markup"]["inline_keyboard"][0][0]["url"],
                         "https://x.com/u/status/1")

    def test_send_tweet_propagates_thread_id_to_rich_and_fallback(self):
        rich_calls = []
        legacy_calls = []

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None,
                      reply_to_message_id=None):
            rich_calls.append(thread_id)
            return {"ok": False, "rich_fallback": True}

        def fake_legacy(token, chat_id, text, link="", thread_id=None,
                        reply_to_message_id=None):
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

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None,
                            reply_to_message_id=None):
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

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None,
                            reply_to_message_id=None):
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

        def fake_legacy(token, chat_id, text, link="", thread_id=None,
                        reply_to_message_id=None):
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


class CrossAccountDedupTest(unittest.TestCase):
    """跨账号去重（F1）：纯转发按原推 id 全局去重、引用/原创不抑制；
    索引 send-then-mark、--test/dry-run 不写、TTL/容量 GC、article 双闸。"""

    FRESH = "Tue May 12 00:20:00 +0000 2026"

    def setUp(self):
        self._patches = [
            patch.object(twitter_monitor, "_CROSS_DEDUP_ENABLED", True),
            patch.object(twitter_monitor, "_PUSHED_INDEX_CACHE", {}),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)

    def _args(self, **kw):
        base = dict(test=False, seed=False, dry_run=False, limit=20,
                    max_push_age_minutes=45, test_count=1)
        base.update(kw)
        return argparse.Namespace(**base)

    def _rt(self, tid="rt1", orig="orig9"):
        return {"id": tid, "createdAt": self.FRESH,
                "text": "RT @o: 这是一条足够长的正常转发推文内容用于通过分类过滤器检查",
                "retweeted_status": {"id": orig, "screen_name": "o"}}

    def _run(self, tweet, username="u", args=None, seen=None):
        pushed, saved_seen = [], {}

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None,
                            reply_to_message_id=None):
            pushed.append(t["id"])
            return {"ok": True}

        def fake_save_seen(u, s, ts=None):
            saved_seen["final"] = set(s)

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=[tweet]), \
             patch.object(twitter_monitor, "load_seen",
                          return_value=(seen if seen is not None else {"warm"}, None)), \
             patch.object(twitter_monitor, "save_seen", side_effect=fake_save_seen), \
             patch.object(twitter_monitor, "save_pushed_index", lambda: None), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send_tweet), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username=username,
                bot_token="b", chat_id="c", args=args or self._args())
        return pushed, saved_seen.get("final", set())

    def test_rt_suppressed_when_origin_in_index(self):
        twitter_monitor._PUSHED_INDEX_CACHE["t:orig9"] = {"ts": "2026-05-12T00:00:00+00:00",
                                                          "by": "other"}
        pushed, final_seen = self._run(self._rt())
        self.assertEqual(pushed, [])
        self.assertIn("rt1", final_seen)  # 被抑制的壳 id 仍进 seen，下轮不复查

    def test_first_rt_pushes_and_records_origin(self):
        pushed, _ = self._run(self._rt())
        self.assertEqual(pushed, ["rt1"])
        self.assertEqual(
            twitter_monitor._PUSHED_INDEX_CACHE.get("t:orig9", {}).get("by"), "u")

    def test_same_run_two_accounts_rt_same_origin(self):
        p1, _ = self._run(self._rt(tid="rt1"), username="u1")
        p2, _ = self._run(self._rt(tid="rt2"), username="u2")
        self.assertEqual(p1, ["rt1"])
        self.assertEqual(p2, [])  # 内存缓存即同轮共享

    def test_quote_not_suppressed_records_own_id(self):
        twitter_monitor._PUSHED_INDEX_CACHE["t:orig9"] = {"ts": "2026-05-12T00:00:00+00:00",
                                                          "by": "other"}
        quote = {"id": "qt1", "createdAt": self.FRESH,
                 "text": "带评论的引用是新内容不应该被跨账号去重抑制掉的完整推文",
                 "quoted_status": {"id": "orig9", "screen_name": "o"}}
        pushed, _ = self._run(quote)
        self.assertEqual(pushed, ["qt1"])
        self.assertEqual(
            twitter_monitor._PUSHED_INDEX_CACHE.get("t:qt1", {}).get("by"), "u")
        self.assertEqual(
            twitter_monitor._PUSHED_INDEX_CACHE["t:orig9"]["by"], "other")  # 不穿透覆盖

    def test_original_not_suppressed_even_if_in_index(self):
        twitter_monitor._PUSHED_INDEX_CACHE["t:og1"] = {"ts": "2026-05-12T00:00:00+00:00",
                                                        "by": "other"}
        original = {"id": "og1", "createdAt": self.FRESH,
                    "text": "原创推文即使原推 id 已在索引中也不该被抑制（只抑制纯转发）"}
        pushed, _ = self._run(original)
        self.assertEqual(pushed, ["og1"])

    def test_dry_run_does_not_record(self):
        self._run(self._rt(), args=self._args(dry_run=True))
        self.assertNotIn("t:orig9", twitter_monitor._PUSHED_INDEX_CACHE)

    def test_test_mode_pushes_but_does_not_record(self):
        pushed, _ = self._run(self._rt(), args=self._args(test=True))
        self.assertEqual(pushed, ["rt1"])  # --test 发调试目标
        self.assertNotIn("t:orig9", twitter_monitor._PUSHED_INDEX_CACHE)  # 不写生产索引

    def test_disabled_by_default_no_suppress(self):
        with patch.object(twitter_monitor, "_CROSS_DEDUP_ENABLED", False):
            twitter_monitor._PUSHED_INDEX_CACHE["t:orig9"] = {"ts": "x", "by": "other"}
            pushed, _ = self._run(self._rt())
        self.assertEqual(pushed, ["rt1"])

    def test_index_ttl_gc_and_cap(self):
        import os as _os
        import tempfile
        old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        fresh = datetime.now(timezone.utc).isoformat()
        cache = {"t:old": {"ts": old, "by": "a"}}
        for i in range(twitter_monitor.PUSHED_INDEX_MAX_ENTRIES + 5):
            cache[f"t:{i}"] = {"ts": fresh, "by": "a"}
        with tempfile.TemporaryDirectory() as d:
            path = _os.path.join(d, "idx.json")
            with patch.object(twitter_monitor, "PUSHED_INDEX_PATH", path), \
                 patch.object(twitter_monitor, "_PUSHED_INDEX_CACHE", cache):
                twitter_monitor.save_pushed_index()
                with open(path) as f:
                    saved = json.load(f)["entries"]
        self.assertNotIn("t:old", saved)
        self.assertLessEqual(len(saved), twitter_monitor.PUSHED_INDEX_MAX_ENTRIES)

    def test_save_article_cross_dup_skips_enqueue(self):
        import os as _os
        import tempfile
        twitter_monitor._PUSHED_INDEX_CACHE["a:art1"] = {"ts": "x", "by": "other"}
        with tempfile.TemporaryDirectory() as d:
            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d):
                twitter_monitor.save_article("u", "art1", {"id": "1", "text": "t"})
                qpath = _os.path.join(d, "u_queue.json")
                self.assertFalse(_os.path.exists(qpath))

    def test_article_queue_second_gate_marks_skipped(self):
        import json as _json
        import os as _os
        import tempfile
        twitter_monitor._PUSHED_INDEX_CACHE["a:art1"] = {"ts": "x", "by": "other"}
        entry = {"article_id": "art1", "tweet_id": "1", "author": "o",
                 "status": "pending", "attempts": 0, "content": None}
        with tempfile.TemporaryDirectory() as d:
            qpath = _os.path.join(d, "u_queue.json")
            with open(qpath, "w") as f:
                _json.dump([entry], f)
            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d), \
                 patch.object(twitter_monitor, "ARTICLE_CACHE_DIR",
                              _os.path.join(d, "cache")):
                twitter_monitor.process_article_queue(FakeAI(True), "bot", "chat")
            with open(qpath) as f:
                saved = _json.load(f)[0]
        self.assertEqual(saved["status"], "skipped")
        self.assertEqual(saved["skip_reason"], "cross_dup")
        # skipped 是终态，纳入 7 天保留期清理
        expired = twitter_monitor._article_entry_expired(
            {"status": "skipped",
             "updated_at": (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()})
        self.assertTrue(expired)


class ArticleSupersedeTest(unittest.TestCase):
    """X Article 删减：同篇文章的引用版取代裸摘要（= 投递时没有引用评论那次）。

    两个方向都要覆盖。线上两周 22 篇文章有 5 篇被推两次，其中 3 次裸摘要先到
    （间隔 1~3h）→ 删已发的那条；另 2 次引用版先到（间隔 15~17s、同一轮）→
    裸摘要根本不该发。判定用记录里的 quoted 字段而非 ab:/a: 键前缀：flat 路径
    的引用文章也落 a:<id> 键。
    """

    MD = "# 标题\n\n" + "正文内容。" * 40
    SUMMARY = "> 结论\n\n### 小节\n正文"

    def setUp(self):
        self._patches = [
            patch.object(twitter_monitor, "_ARTICLE_SUPERSEDE_ENABLED", True),
            patch.object(twitter_monitor, "_CROSS_DEDUP_ENABLED", True),
            patch.object(twitter_monitor, "_PUSHED_INDEX_CACHE", {}),
            patch.object(twitter_monitor, "save_pushed_index", lambda: None),
            patch.object(twitter_monitor, "learning_feed", None),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)

    @staticmethod
    def _record(quoted, *, mids=(5681,), by="vista8", ts="2026-08-16T06:00:00+00:00",
                chat="-1004424841223", **extra):
        rec = {"ts": ts, "by": by, "article_key": "a:777", "message_ids": list(mids),
               "chat_id": chat, "thread_id": 19, "quoted": quoted}
        rec.update(extra)
        return rec

    def _entry(self, **kw):
        entry = {"article_id": "777", "tweet_id": "1", "author": "vista8",
                 "article_title": "T", "article_preview": "P", "quote_comment": "",
                 "comment_author": "vista8", "status": "pending", "attempts": 0,
                 "content": None,
                 "detected_at": datetime.now(timezone.utc).isoformat()}
        entry.update(kw)
        # save_semantic_article 里 quote_comment 与 _bundle_key 由同一个
        # has_substantive_comment 同时置位；带评论却无 bundle_key 的条目在跨账号
        # 去重开启时会先被入队闸拦掉，根本到不了删减逻辑 → 夹具跟着一起置位。
        if entry["quote_comment"] and not entry.get("bundle_key"):
            entry["bundle_key"] = "t:888"
        return entry

    def _run_queue(self, entry, *, delete_ok=True, rich_ok=True, parts=1,
                   chat_id="-1004424841223"):
        """跑一遍 article 队列，返回 (最终盘面条目, quiet 调用序列, 抓取次数)。"""
        import json as _json
        import os as _os
        import tempfile
        quiet_calls, fetches, next_mid = [], [], [9001]

        def fake_quiet(token, payload, method):
            quiet_calls.append((method, payload))
            return {"ok": delete_ok if method == "deleteMessage" else True}

        def fake_fetch(username, e):
            fetches.append(e.get("article_id"))
            return (self.MD, None)

        def fake_rich(token, cid, markdown_, link="", thread_id=None):
            if not rich_ok:
                return {"ok": False, "rich_fallback": True, "description": "rejected"}
            next_mid[0] += 1
            return {"ok": True, "result": {"message_id": next_mid[0]}}

        def fake_plain(token, cid, text, link="", thread_id=None):
            next_mid[0] += 1
            return {"ok": True, "result": {"message_id": next_mid[0]}}

        with tempfile.TemporaryDirectory() as d:
            qpath = _os.path.join(d, "vista8_queue.json")
            with open(qpath, "w") as f:
                _json.dump([entry], f)
            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d), \
                 patch.object(twitter_monitor, "ARTICLE_CACHE_DIR",
                              _os.path.join(d, "cache")), \
                 patch.object(twitter_monitor, "fetch_article_markdown",
                              side_effect=fake_fetch), \
                 patch.object(twitter_monitor, "summarize_article",
                              return_value=(self.SUMMARY, "mimo")), \
                 patch.object(twitter_monitor, "format_article_summary_messages",
                              return_value=[f"part{i}" for i in range(1, parts + 1)]), \
                 patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
                 patch.object(twitter_monitor, "send_telegram", side_effect=fake_plain), \
                 patch.object(twitter_monitor, "_tg_post_quiet", side_effect=fake_quiet), \
                 patch.object(twitter_monitor.time, "sleep", return_value=None):
                twitter_monitor.process_article_queue(FakeAI(True), "bot", chat_id,
                                                      thread_id=19)
            with open(qpath) as f:
                final = _json.load(f)[0]
        return final, quiet_calls, fetches

    # ── 方向一：裸摘要先到，引用版后到 → 删掉裸摘要 ──

    def test_quoted_delivery_deletes_earlier_bare_message(self):
        idx = twitter_monitor._PUSHED_INDEX_CACHE
        idx["a:777"] = self._record(False, mids=(5681,))
        final, quiet, _ = self._run_queue(
            self._entry(quote_comment="这篇值得看", comment_author="dotey"))
        self.assertEqual(final["status"], "sent")
        deletes = [p for m, p in quiet if m == "deleteMessage"]
        self.assertEqual([p["message_id"] for p in deletes], [5681])
        self.assertEqual(deletes[0]["chat_id"], "-1004424841223")
        # 撤回落痕 = 幂等闸；引用版本身登记为 quoted
        self.assertTrue(idx["a:777"].get("retracted_at"))
        self.assertEqual(idx["a:777"]["retracted_by"], "dotey")

    def test_quoted_delivery_with_bundle_key_records_quoted_and_keeps_itself(self):
        idx = twitter_monitor._PUSHED_INDEX_CACHE
        idx["a:777"] = self._record(False)
        self._run_queue(self._entry(quote_comment="值得看", comment_author="dotey",
                                    bundle_key="t:888"))
        self.assertTrue(idx["ab:t:888"]["quoted"])
        self.assertTrue(idx["ab:t:888"]["message_ids"])   # 自己不被当成撤回对象
        self.assertNotIn("retracted_at", idx["ab:t:888"])

    def test_chunked_fallback_deletes_every_part(self):
        idx = twitter_monitor._PUSHED_INDEX_CACHE
        idx["a:777"] = self._record(False, mids=(11, 12, 13))
        _, quiet, _ = self._run_queue(
            self._entry(quote_comment="值得看", comment_author="dotey"))
        self.assertEqual([p["message_id"] for m, p in quiet if m == "deleteMessage"],
                         [11, 12, 13])

    def test_chunked_send_records_all_message_ids(self):
        idx = twitter_monitor._PUSHED_INDEX_CACHE
        self._run_queue(self._entry(), rich_ok=False, parts=3)
        # 分块回退路径每条都要记，否则后续删减只删得掉最后一条
        self.assertEqual(len(idx["a:777"]["message_ids"]), 3)
        self.assertFalse(idx["a:777"]["quoted"])

    def test_delete_rejected_falls_back_to_edit_pointer(self):
        twitter_monitor._PUSHED_INDEX_CACHE["a:777"] = self._record(False, mids=(5681,))
        _, quiet, _ = self._run_queue(
            self._entry(quote_comment="值得看", comment_author="dotey"), delete_ok=False)
        edits = [p for m, p in quiet if m == "editMessageText"]
        self.assertEqual(len(edits), 1)
        self.assertEqual(edits[0]["message_id"], 5681)
        self.assertIn("@dotey", edits[0]["text"])
        # 深链指向刚发出的引用版，旧消息不留整篇重复摘要
        self.assertIn("https://t.me/c/4424841223/19/", edits[0]["text"])

    def test_retraction_is_idempotent_across_runs(self):
        twitter_monitor._PUSHED_INDEX_CACHE["a:777"] = self._record(
            False, mids=(5681,), retracted_at="2026-08-16T07:00:00+00:00")
        _, quiet, _ = self._run_queue(
            self._entry(quote_comment="值得看", comment_author="dotey"))
        self.assertEqual([m for m, _ in quiet if m == "deleteMessage"], [])

    def test_assumed_delivery_without_message_id_is_not_retractable(self):
        # 响应缺失的投递没有 message_id，删不了也不能崩
        twitter_monitor._PUSHED_INDEX_CACHE["a:777"] = self._record(False, mids=())
        final, quiet, _ = self._run_queue(
            self._entry(quote_comment="值得看", comment_author="dotey"))
        self.assertEqual(final["status"], "sent")
        self.assertEqual([m for m, _ in quiet if m == "deleteMessage"], [])

    def test_other_article_bare_delivery_untouched(self):
        idx = twitter_monitor._PUSHED_INDEX_CACHE
        idx["a:999"] = self._record(False, mids=(4242,), article_key="a:999")
        _, quiet, _ = self._run_queue(
            self._entry(quote_comment="值得看", comment_author="dotey"))
        self.assertEqual([m for m, _ in quiet if m == "deleteMessage"], [])
        self.assertNotIn("retracted_at", idx["a:999"])

    # ── 方向二：引用版先到 → 裸摘要根本不发 ──

    def test_bare_entry_skipped_before_fetch_when_quoted_already_delivered(self):
        twitter_monitor._PUSHED_INDEX_CACHE["ab:t:888"] = self._record(
            True, by="dotey", mids=(6001,))
        final, quiet, fetches = self._run_queue(self._entry())
        self.assertEqual(final["status"], "skipped")
        self.assertEqual(final["skip_reason"], "superseded_by_quote")
        self.assertEqual(fetches, [])          # 省掉抓取 + AI 摘要
        self.assertEqual([m for m, _ in quiet if m == "deleteMessage"], [])
        # skipped 是终态，走 7 天保留期清理
        self.assertTrue(twitter_monitor._article_entry_expired(
            {"status": "skipped",
             "updated_at": (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()}))

    def test_quoted_entry_not_suppressed_by_another_quoted_delivery(self):
        # 多人引用同一篇：引用版之间互不抑制（各自是独立的编辑单元）
        twitter_monitor._PUSHED_INDEX_CACHE["ab:t:888"] = self._record(
            True, by="dotey", mids=(6001,))
        final, quiet, fetches = self._run_queue(
            self._entry(quote_comment="我也说两句", comment_author="vista8",
                        bundle_key="t:999"))
        self.assertEqual(final["status"], "sent")
        self.assertEqual(fetches, ["777"])
        self.assertEqual([m for m, _ in quiet if m == "deleteMessage"], [])

    # ── 开关与兼容 ──

    def test_disabled_by_default_keeps_both_deliveries(self):
        with patch.object(twitter_monitor, "_ARTICLE_SUPERSEDE_ENABLED", False):
            twitter_monitor._PUSHED_INDEX_CACHE["a:777"] = self._record(False, mids=(5681,))
            final, quiet, fetches = self._run_queue(
                self._entry(quote_comment="值得看", comment_author="dotey",
                            bundle_key="t:888"))
        self.assertEqual(final["status"], "sent")
        self.assertEqual(fetches, ["777"])
        self.assertEqual([m for m, _ in quiet if m == "deleteMessage"], [])

    def test_legacy_records_without_quoted_field_fall_back_to_key_prefix(self):
        # 8-14 之前的老条目没有 quoted/message_ids：能识别归属但无从撤回，不能崩
        twitter_monitor._PUSHED_INDEX_CACHE["a:777"] = {"ts": "2026-08-05T08:31:02+00:00",
                                                        "by": "vista8"}
        self.assertTrue(twitter_monitor._is_bare_article_delivery(
            "a:777", twitter_monitor._PUSHED_INDEX_CACHE["a:777"]))
        self.assertIsNone(twitter_monitor._find_retractable_bare_delivery("a:777"))
        final, quiet, _ = self._run_queue(
            self._entry(quote_comment="值得看", comment_author="dotey"))
        self.assertEqual(final["status"], "sent")
        self.assertEqual([m for m, _ in quiet if m == "deleteMessage"], [])

    def test_legacy_ab_record_without_article_key_is_ignored(self):
        # ab: 老条目无从还原文章 id → 不参与删减，不能误伤同名裸摘要
        twitter_monitor._PUSHED_INDEX_CACHE["ab:t:888"] = {"ts": "x", "by": "dotey"}
        self.assertEqual(twitter_monitor._article_delivery_rows("a:777"), [])
        self.assertIsNone(twitter_monitor._quoted_article_delivered("a:777"))

    def test_dry_run_neither_records_nor_deletes(self):
        import json as _json
        import os as _os
        import tempfile
        idx = twitter_monitor._PUSHED_INDEX_CACHE
        idx["a:777"] = self._record(False, mids=(5681,))
        quiet = []
        with tempfile.TemporaryDirectory() as d:
            qpath = _os.path.join(d, "vista8_queue.json")
            with open(qpath, "w") as f:
                _json.dump([self._entry(quote_comment="值得看",
                                        comment_author="dotey")], f)
            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d), \
                 patch.object(twitter_monitor, "ARTICLE_CACHE_DIR",
                              _os.path.join(d, "cache")), \
                 patch.object(twitter_monitor, "fetch_article_markdown",
                              return_value=(self.MD, None)), \
                 patch.object(twitter_monitor, "summarize_article",
                              return_value=(self.SUMMARY, "mimo")), \
                 patch.object(twitter_monitor, "_tg_post_quiet",
                              side_effect=lambda t, p, m: quiet.append(m)), \
                 patch.object(twitter_monitor.time, "sleep", return_value=None):
                twitter_monitor.process_article_queue(FakeAI(True), "bot", "chat",
                                                      dry_run=True, thread_id=19)
        self.assertEqual(quiet, [])
        self.assertNotIn("retracted_at", idx["a:777"])

    def test_main_wires_config_flag_both_ways(self):
        seen = {}

        def fake_queue(ai, bot_token, chat_id, dry_run=False, *, thread_id=None,
                       thread_map=None):
            seen["flag"] = twitter_monitor._ARTICLE_SUPERSEDE_ENABLED
            return 0

        for value, expected in (("true", True), ("false", False), (None, False)):
            key = "" if value is None else f', "article_supersede_enabled": {value}'
            cfg = Path("test_config_supersede.json")
            cfg.write_text('{"telegram_bot_token": "b", "telegram_chat_id": "c"'
                           + key + "}")
            try:
                with patch.object(twitter_monitor, "CONFIG_PATH", str(cfg)), \
                     patch.object(twitter_monitor, "ROUTE_TABLE_PATH",
                                  "test_route_table_absent.json"), \
                     patch.object(twitter_monitor, "FAILURES_PATH",
                                  "test_failures_supersede.json"), \
                     patch.object(twitter_monitor, "update_status_dashboard"), \
                     patch.object(twitter_monitor, "check_cookie_health"), \
                     patch.object(twitter_monitor.TokenPool, "load", return_value=None), \
                     patch.object(twitter_monitor.AIClassifier, "load",
                                  return_value=FakeAI(False)), \
                     patch.object(twitter_monitor, "load_accounts",
                                  return_value=[{"username": "u"}]), \
                     patch.object(twitter_monitor, "process_user",
                                  return_value=(0, 0, 0, 0)), \
                     patch.object(twitter_monitor, "process_article_queue",
                                  side_effect=fake_queue), \
                     patch.object(sys, "argv", ["twitter_monitor.py"]):
                    self.assertEqual(twitter_monitor.main(), 0)
            finally:
                cfg.unlink(missing_ok=True)
                Path("test_failures_supersede.json").unlink(missing_ok=True)
            self.assertIs(seen["flag"], expected, f"config value {value!r}")

    def test_message_link_forms(self):
        link = twitter_monitor._tg_message_link
        self.assertEqual(link("-1004424841223", 19, 5681),
                         "https://t.me/c/4424841223/19/5681")
        self.assertEqual(link("-1004424841223", None, 5681),
                         "https://t.me/c/4424841223/5681")
        self.assertEqual(link("123456", 19, 5681), "")    # DM 无该链接形式
        self.assertEqual(link("-1004424841223", 19, 0), "")


class ArticleQuoteCardTest(unittest.TestCase):
    """多人引用同一篇文章：首条发完整摘要，后续引用者只发增量评论卡片 reply 其下。

    锚点必须是完整摘要而非卡片，否则第三个引用者会挂到第二个人的卡片下越挂越深；
    锚点已被撤回（正文不在了）时必须退回发完整摘要。卡片路径完全跳过 Markdown
    抓取和 AI 摘要 —— N 个引用者的成本从 N 次降到 1 次，这是本方案的全部意义。
    """

    MD = "# 标题\n\n" + "正文内容。" * 40
    SUMMARY = "> 结论\n\n### 小节\n正文"

    def setUp(self):
        self._patches = [
            patch.object(twitter_monitor, "_ARTICLE_QUOTE_CARD_ENABLED", True),
            patch.object(twitter_monitor, "_ARTICLE_SUPERSEDE_ENABLED", True),
            patch.object(twitter_monitor, "_CROSS_DEDUP_ENABLED", True),
            patch.object(twitter_monitor, "_PUSHED_INDEX_CACHE", {}),
            patch.object(twitter_monitor, "save_pushed_index", lambda: None),
            patch.object(twitter_monitor, "learning_feed", None),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)

    @staticmethod
    def _record(**extra):
        rec = {"ts": "2026-08-16T06:00:00+00:00", "by": "vista8", "article_key": "a:777",
               "message_ids": [5681], "chat_id": "-1004424841223", "thread_id": 19,
               "quoted": False, "form": "summary"}
        rec.update(extra)
        return rec

    def _entry(self, **kw):
        entry = {"article_id": "777", "tweet_id": "1", "author": "vista8",
                 "article_title": "Agent 的上下文工程", "article_preview": "P",
                 "quote_comment": "这篇把上下文窗口的取舍讲透了", "comment_author": "dotey",
                 "bundle_key": "t:888", "status": "pending", "attempts": 0,
                 "content": None,
                 "detected_at": datetime.now(timezone.utc).isoformat()}
        entry.update(kw)
        return entry

    def _run_queue(self, entry, *, send_result=None, chat_id="-1004424841223"):
        """返回 (最终盘面条目, sendMessage 调用列表, 抓取次数, AI 摘要次数)。"""
        import json as _json
        import os as _os
        import tempfile
        plain, fetches, summaries = [], [], []

        def fake_plain(token, cid, text, link="", *, preview_url="", thread_id=None,
                       reply_to_message_id=None, button_text=""):
            plain.append({"chat_id": cid, "text": text, "link": link,
                          "thread_id": thread_id, "reply_to": reply_to_message_id,
                          "button_text": button_text})
            return send_result if send_result is not None else {
                "ok": True, "result": {"message_id": 6001}}

        def fake_fetch(u, e):
            fetches.append(e.get("article_id"))
            return (self.MD, None)

        def fake_summary(ai, u, e, md):
            summaries.append(e.get("article_id"))
            return (self.SUMMARY, "mimo")

        with tempfile.TemporaryDirectory() as d:
            qpath = _os.path.join(d, "vista8_queue.json")
            with open(qpath, "w") as f:
                _json.dump([entry], f)
            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d), \
                 patch.object(twitter_monitor, "ARTICLE_CACHE_DIR",
                              _os.path.join(d, "cache")), \
                 patch.object(twitter_monitor, "fetch_article_markdown",
                              side_effect=fake_fetch), \
                 patch.object(twitter_monitor, "summarize_article",
                              side_effect=fake_summary), \
                 patch.object(twitter_monitor, "send_telegram", side_effect=fake_plain), \
                 patch.object(twitter_monitor, "send_telegram_rich",
                              return_value={"ok": True, "result": {"message_id": 7001}}), \
                 patch.object(twitter_monitor, "_tg_post_quiet",
                              return_value={"ok": True}), \
                 patch.object(twitter_monitor.time, "sleep", return_value=None):
                twitter_monitor.process_article_queue(FakeAI(True), "bot", chat_id,
                                                      thread_id=19)
            with open(qpath) as f:
                final = _json.load(f)[0]
        return final, plain, fetches, summaries

    def test_second_quoter_gets_card_replying_to_summary_without_resummarizing(self):
        idx = twitter_monitor._PUSHED_INDEX_CACHE
        idx["a:777"] = self._record()
        final, plain, fetches, summaries = self._run_queue(self._entry())
        self.assertEqual(final["status"], "sent")
        self.assertEqual(final["delivery_form"], "quote_card")
        # 全方案的意义所在：一次抓取都没有，一次 AI 摘要都没有
        self.assertEqual(fetches, [])
        self.assertEqual(summaries, [])
        self.assertEqual(len(plain), 1)
        self.assertEqual(plain[0]["reply_to"], 5681)
        self.assertEqual(plain[0]["thread_id"], 19)
        self.assertIn("这篇把上下文窗口的取舍讲透了", plain[0]["text"])
        self.assertIn("Agent 的上下文工程", plain[0]["text"])
        # 不带署名：卡片正文只有标题行 + 评论
        self.assertNotIn("也引用了这篇文章", plain[0]["text"])
        self.assertNotIn("@dotey", plain[0]["text"])
        # 按钮指向写这条评论的引用推，不是文章原文页（那条锚点摘要已经给过）
        self.assertEqual(plain[0]["link"], "https://x.com/dotey/status/888")
        self.assertEqual(plain[0]["button_text"], "\U0001f517 查看引用推文")
        # 卡片自身登记为 form=card，不能成为下一个引用者的锚点
        self.assertEqual(idx["ab:t:888"]["form"], "card")
        self.assertEqual(idx["ab:t:888"]["message_ids"], [6001])

    def test_third_quoter_anchors_on_summary_not_on_previous_card(self):
        idx = twitter_monitor._PUSHED_INDEX_CACHE
        idx["a:777"] = self._record()
        idx["ab:t:888"] = self._record(ts="2026-08-16T07:00:00+00:00", by="dotey",
                                       message_ids=[6001], quoted=True, form="card")
        _, plain, _, _ = self._run_queue(
            self._entry(comment_author="_philschmid", bundle_key="t:999"))
        self.assertEqual(plain[0]["reply_to"], 5681)   # 不是 6001

    def test_earliest_summary_wins_as_anchor(self):
        idx = twitter_monitor._PUSHED_INDEX_CACHE
        idx["ab:t:111"] = self._record(ts="2026-08-16T09:00:00+00:00", by="late",
                                       message_ids=[7777], quoted=True)
        idx["a:777"] = self._record(ts="2026-08-16T06:00:00+00:00", message_ids=[5681])
        _, plain, _, _ = self._run_queue(self._entry())
        self.assertEqual(plain[0]["reply_to"], 5681)

    def test_retracted_summary_is_not_an_anchor_falls_back_to_full_summary(self):
        # 锚点被删减撤回后正文已不在群里 → 必须退回发完整摘要，否则只剩一条评论
        twitter_monitor._PUSHED_INDEX_CACHE["a:777"] = self._record(
            retracted_at="2026-08-16T07:00:00+00:00")
        final, plain, fetches, summaries = self._run_queue(self._entry())
        self.assertEqual(final["status"], "sent")
        self.assertNotIn("delivery_form", final)
        self.assertEqual(fetches, ["777"])
        self.assertEqual(summaries, ["777"])

    def test_no_prior_summary_delivers_full_summary(self):
        final, plain, fetches, _ = self._run_queue(self._entry())
        self.assertEqual(final["status"], "sent")
        self.assertNotIn("delivery_form", final)
        self.assertEqual(fetches, ["777"])

    def test_anchor_without_message_id_falls_back_to_full_summary(self):
        # 响应缺失的投递没有 message_id，挂不上去 → 只能重发完整摘要
        twitter_monitor._PUSHED_INDEX_CACHE["a:777"] = self._record(message_ids=[])
        final, _, fetches, _ = self._run_queue(self._entry())
        self.assertEqual(final["status"], "sent")
        self.assertEqual(fetches, ["777"])

    def test_bare_entry_never_gets_a_card(self):
        # 无评论的裸条目没有可补充的内容，走已有的 superseded_by_quote 抑制。
        # 引用版记在 ab: 键下（semantic 路径的真实形状），a:777 尚空 = 跨账号
        # 二闸不会先以 cross_dup 拦下，走到的正是删减那道闸。
        twitter_monitor._PUSHED_INDEX_CACHE["ab:t:888"] = self._record(
            by="dotey", message_ids=[6001], quoted=True)
        final, plain, fetches, _ = self._run_queue(
            self._entry(quote_comment="", bundle_key=""))
        self.assertEqual(final["status"], "skipped")
        self.assertEqual(final["skip_reason"], "superseded_by_quote")
        self.assertEqual(plain, [])
        self.assertEqual(fetches, [])

    def test_card_send_failure_is_retryable_not_silently_dropped(self):
        twitter_monitor._PUSHED_INDEX_CACHE["a:777"] = self._record()
        final, _, _, _ = self._run_queue(
            self._entry(), send_result={"ok": False, "description": "boom"})
        self.assertEqual(final["status"], "failed")
        self.assertEqual(final["failed_stage"], "quote_card_send")
        self.assertEqual(final["attempts"], 1)     # 计入重试次数，下轮再来
        self.assertNotIn("ab:t:888", twitter_monitor._PUSHED_INDEX_CACHE)

    def test_disabled_by_default_resummarizes(self):
        with patch.object(twitter_monitor, "_ARTICLE_QUOTE_CARD_ENABLED", False):
            twitter_monitor._PUSHED_INDEX_CACHE["a:777"] = self._record()
            final, _, fetches, summaries = self._run_queue(self._entry())
        self.assertEqual(final["status"], "sent")
        self.assertEqual(fetches, ["777"])         # 回到今天的行为：重新抓取 + 摘要
        self.assertEqual(summaries, ["777"])

    def test_legacy_record_without_form_counts_as_summary_anchor(self):
        rec = self._record()
        rec.pop("form")
        twitter_monitor._PUSHED_INDEX_CACHE["a:777"] = rec
        _, plain, fetches, _ = self._run_queue(self._entry())
        self.assertEqual(plain[0]["reply_to"], 5681)
        self.assertEqual(fetches, [])

    def test_send_telegram_builds_reply_parameters_with_fallback(self):
        captured = {}

        def fake_post(token, payload, method="sendMessage"):
            captured["payload"] = payload
            return {"ok": True}

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post):
            twitter_monitor.send_telegram("tok", "chat", "hi", "https://x.com/i/article/1",
                                          thread_id=19, reply_to_message_id=5681)
        rp = captured["payload"]["reply_parameters"]
        self.assertEqual(rp["message_id"], 5681)
        # 锚点可能已被删；缺目标必须降级而不是 400 丢掉这条评论
        self.assertTrue(rp["allow_sending_without_reply"])
        self.assertEqual(captured["payload"]["message_thread_id"], 19)

    def test_send_telegram_omits_reply_parameters_when_not_replying(self):
        captured = {}
        with patch.object(twitter_monitor, "_tg_post",
                          side_effect=lambda t, p, method="sendMessage": (
                              captured.update(payload=p), {"ok": True})[1]):
            twitter_monitor.send_telegram("tok", "chat", "hi")
        self.assertNotIn("reply_parameters", captured["payload"])

    def test_card_escapes_html_in_title_and_comment(self):
        card, _ = twitter_monitor.format_article_quote_card(
            "vista8", {"article_id": "777", "article_title": "<b>标题</b> & 更多",
                       "quote_comment": "看 <script>alert(1)</script>",
                       "comment_author": "@dotey", "bundle_key": "t:888"})
        self.assertNotIn("<script>", card)
        self.assertNotIn("<b>标题</b>", card)
        self.assertIn("&lt;script&gt;", card)
        self.assertIn("&amp;", card)

    def test_card_is_title_plus_comment_only(self):
        card, _ = twitter_monitor.format_article_quote_card(
            "vista8", {"article_id": "777", "article_title": "标题",
                       "quote_comment": "评论", "comment_author": "dotey",
                       "bundle_key": "t:888"})
        self.assertEqual(card, "\U0001f4c4 标题\n<blockquote>评论</blockquote>")

    def test_card_without_title_is_comment_only(self):
        card, _ = twitter_monitor.format_article_quote_card(
            "vista8", {"article_id": "777", "quote_comment": "评论",
                       "comment_author": "dotey", "bundle_key": "t:888"})
        self.assertEqual(card, "<blockquote>评论</blockquote>")

    def test_button_targets_quote_tweet_not_article(self):
        url = twitter_monitor._quote_tweet_url
        # bundle_key 是引用推 id，comment_author 是它的作者
        self.assertEqual(url({"bundle_key": "t:888", "comment_author": "dotey"}),
                         "https://x.com/dotey/status/888")
        self.assertEqual(url({"bundle_key": "t:888", "comment_author": "@dotey"}),
                         "https://x.com/dotey/status/888")

    def test_flat_path_without_bundle_key_falls_back_to_article_url(self):
        # flat 路径没有 bundle_key；entry["tweet_id"] 是文章所有者的推，不是评论推
        url = twitter_monitor._quote_tweet_url
        self.assertEqual(url({"bundle_key": "", "comment_author": "dotey",
                              "tweet_id": "999"}), "")
        self.assertEqual(url({"bundle_key": "t:abc", "comment_author": "dotey"}), "")
        self.assertEqual(url({"bundle_key": "t:888", "comment_author": ""}), "")
        _, link = twitter_monitor.format_article_quote_card(
            "vista8", {"article_id": "777", "quote_comment": "评论",
                       "comment_author": "dotey", "bundle_key": ""})
        self.assertEqual(link, "https://x.com/i/article/777")

    def test_fallback_to_article_url_keeps_original_button_label(self):
        # 无 bundle_key 时按钮回落文章页，文案也要跟着回落（否则「查看引用推文」
        # 点开的是文章，名不副实）。直调 deliver：flat 引用条目在跨账号去重开启时
        # 走不到这里（会先被 a:<id> 二闸以 cross_dup 拦掉）。
        sent = {}

        def fake_plain(token, cid, text, link="", *, preview_url="", thread_id=None,
                       reply_to_message_id=None, button_text=""):
            sent.update(link=link, button_text=button_text)
            return {"ok": True, "result": {"message_id": 6002}}

        entry = self._entry(bundle_key="")
        with patch.object(twitter_monitor, "send_telegram", side_effect=fake_plain), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            twitter_monitor._deliver_article_quote_card(
                "bot", "-1004424841223", "vista8", entry,
                {"message_ids": [5681], "chat_id": "-1004424841223",
                 "form": "summary", "cover_url": ""}, thread_id=19)
        self.assertEqual(entry["status"], "sent")
        self.assertEqual(sent["link"], "https://x.com/i/article/777")
        self.assertEqual(sent["button_text"], "\U0001f517 打开原文")

    # ── 卡片配图（复用首条摘要记下的封面，不为一张图重抓全文）──

    def _run_card_with_cover(self, *, cover="https://pbs.twimg.com/media/cover.jpg",
                             comment=None, photo_result=None):
        import json as _json
        import os as _os
        import tempfile
        photos, plains = [], []

        def fake_photo(token, cid, ph, caption="", link="", *, thread_id=None,
                       reply_to_message_id=None, button_text=""):
            photos.append({"photo": ph, "caption": caption, "link": link,
                           "reply_to": reply_to_message_id, "thread_id": thread_id,
                           "button_text": button_text})
            return photo_result if photo_result is not None else {
                "ok": True, "result": {"message_id": 6001}, "send_method": "sendPhoto"}

        def fake_plain(token, cid, text, link="", *, preview_url="", thread_id=None,
                       reply_to_message_id=None, button_text=""):
            plains.append({"text": text, "reply_to": reply_to_message_id,
                           "link": link, "button_text": button_text})
            return {"ok": True, "result": {"message_id": 6002}}

        entry = self._entry() if comment is None else self._entry(quote_comment=comment)
        twitter_monitor._PUSHED_INDEX_CACHE["a:777"] = self._record(cover_url=cover)
        with tempfile.TemporaryDirectory() as d:
            with open(_os.path.join(d, "vista8_queue.json"), "w") as f:
                _json.dump([entry], f)
            with patch.object(twitter_monitor, "ARTICLE_QUEUE_DIR", d), \
                 patch.object(twitter_monitor, "ARTICLE_CACHE_DIR",
                              _os.path.join(d, "cache")), \
                 patch.object(twitter_monitor, "fetch_article_markdown",
                              side_effect=AssertionError("卡片路径不该抓取")), \
                 patch.object(twitter_monitor, "send_telegram_photo",
                              side_effect=fake_photo), \
                 patch.object(twitter_monitor, "send_telegram", side_effect=fake_plain), \
                 patch.object(twitter_monitor, "_tg_post_quiet",
                              return_value={"ok": True}), \
                 patch.object(twitter_monitor.time, "sleep", return_value=None):
                twitter_monitor.process_article_queue(FakeAI(True), "bot",
                                                      "-1004424841223", thread_id=19)
            with open(_os.path.join(d, "vista8_queue.json")) as f:
                final = _json.load(f)[0]
        return final, photos, plains

    def test_card_carries_cover_as_photo_reply(self):
        final, photos, plains = self._run_card_with_cover()
        self.assertEqual(final["status"], "sent")
        self.assertEqual(final["delivery_form"], "quote_card")
        self.assertEqual(len(photos), 1)
        self.assertEqual(plains, [])           # 带图时不再发纯文本
        self.assertEqual(photos[0]["photo"], "https://pbs.twimg.com/media/cover.jpg")
        self.assertEqual(photos[0]["reply_to"], 5681)
        self.assertEqual(photos[0]["thread_id"], 19)
        self.assertEqual(photos[0]["link"], "https://x.com/dotey/status/888")
        self.assertEqual(photos[0]["button_text"], "\U0001f517 查看引用推文")
        self.assertNotIn("也引用了这篇文章", photos[0]["caption"])
        self.assertIn("这篇把上下文窗口的取舍讲透了", photos[0]["caption"])

    def test_summary_delivery_records_cover_for_later_cards(self):
        # 卡片不抓 markdown，封面只能来自首条摘要投递时的登记
        idx = twitter_monitor._PUSHED_INDEX_CACHE
        with patch.object(twitter_monitor, "extract_article_cover",
                          return_value="https://pbs.twimg.com/media/c.jpg"):
            self._run_queue(self._entry(quote_comment="", bundle_key=""))
        self.assertEqual(idx["a:777"]["cover_url"], "https://pbs.twimg.com/media/c.jpg")

    def test_no_cover_recorded_falls_back_to_plain_text_card(self):
        final, photos, plains = self._run_card_with_cover(cover="")
        self.assertEqual(final["status"], "sent")
        self.assertEqual(photos, [])
        self.assertEqual(len(plains), 1)
        self.assertEqual(plains[0]["reply_to"], 5681)

    def test_rejected_photo_falls_back_to_plain_text_not_dropped(self):
        # 图挂了不能把评论一起丢掉
        final, photos, plains = self._run_card_with_cover(
            photo_result={"ok": False, "photo_fallback": True,
                          "description": "wrong file identifier"})
        self.assertEqual(final["status"], "sent")
        self.assertEqual(len(photos), 1)
        self.assertEqual(len(plains), 1)
        self.assertIn("这篇把上下文窗口的取舍讲透了", plains[0]["text"])

    def test_long_comment_shrinks_to_fit_caption_limit(self):
        _, photos, plains = self._run_card_with_cover(comment="很长的评论。" * 200)
        self.assertEqual(len(photos), 1)
        self.assertLessEqual(
            twitter_monitor._utf16_len(photos[0]["caption"]),
            twitter_monitor.ARTICLE_CARD_CAPTION_MAX)
        self.assertEqual(plains, [])

    def test_caption_still_too_long_uses_plain_text_instead_of_400(self):
        # 标题本身就撑满 caption 时收紧评论也救不回来 → 走 4096 上限的纯文本
        long_title = "标" * 900
        with patch.object(twitter_monitor, "ARTICLE_CARD_CAPTION_MAX", 40):
            final, photos, plains = self._run_card_with_cover(comment=long_title)
        self.assertEqual(final["status"], "sent")
        self.assertEqual(photos, [])
        self.assertEqual(len(plains), 1)

    def test_photo_send_builds_reply_parameters(self):
        captured = {}
        with patch.object(twitter_monitor, "_tg_post",
                          side_effect=lambda t, p, method="sendMessage": (
                              captured.update(payload=p, method=method), {"ok": True})[1]):
            twitter_monitor.send_telegram_photo(
                "tok", "chat", "https://pbs.twimg.com/media/a.jpg", "cap",
                "https://x.com/i/article/1", thread_id=19, reply_to_message_id=5681)
        self.assertEqual(captured["method"], "sendPhoto")
        self.assertEqual(captured["payload"]["reply_parameters"]["message_id"], 5681)
        self.assertTrue(
            captured["payload"]["reply_parameters"]["allow_sending_without_reply"])
        # 配图卡片仍要保留「打开原文」按钮
        self.assertEqual(
            captured["payload"]["reply_markup"]["inline_keyboard"][0][0]["url"],
            "https://x.com/i/article/1")

    def test_photo_send_omits_reply_parameters_when_not_replying(self):
        captured = {}
        with patch.object(twitter_monitor, "_tg_post",
                          side_effect=lambda t, p, method="sendMessage": (
                              captured.update(payload=p), {"ok": True})[1]):
            twitter_monitor.send_telegram_photo("tok", "chat", "u", "cap")
        self.assertNotIn("reply_parameters", captured["payload"])

    def test_main_wires_quote_card_flag(self):
        seen = {}

        def fake_queue(ai, bot_token, chat_id, dry_run=False, *, thread_id=None,
                       thread_map=None):
            seen["flag"] = twitter_monitor._ARTICLE_QUOTE_CARD_ENABLED
            return 0

        for value, expected in (("true", True), (None, False)):
            key = "" if value is None else f', "article_quote_card_enabled": {value}'
            cfg = Path("test_config_quotecard.json")
            cfg.write_text('{"telegram_bot_token": "b", "telegram_chat_id": "c"'
                           + key + "}")
            try:
                with patch.object(twitter_monitor, "CONFIG_PATH", str(cfg)), \
                     patch.object(twitter_monitor, "ROUTE_TABLE_PATH",
                                  "test_route_table_absent.json"), \
                     patch.object(twitter_monitor, "FAILURES_PATH",
                                  "test_failures_quotecard.json"), \
                     patch.object(twitter_monitor, "update_status_dashboard"), \
                     patch.object(twitter_monitor, "check_cookie_health"), \
                     patch.object(twitter_monitor.TokenPool, "load", return_value=None), \
                     patch.object(twitter_monitor.AIClassifier, "load",
                                  return_value=FakeAI(False)), \
                     patch.object(twitter_monitor, "load_accounts",
                                  return_value=[{"username": "u"}]), \
                     patch.object(twitter_monitor, "process_user",
                                  return_value=(0, 0, 0, 0)), \
                     patch.object(twitter_monitor, "process_article_queue",
                                  side_effect=fake_queue), \
                     patch.object(sys, "argv", ["twitter_monitor.py"]):
                    self.assertEqual(twitter_monitor.main(), 0)
            finally:
                cfg.unlink(missing_ok=True)
                Path("test_failures_quotecard.json").unlink(missing_ok=True)
            self.assertIs(seen["flag"], expected, f"config value {value!r}")


class MainContentRoutingTest(unittest.TestCase):
    """main()：telegram_group_chat_id/telegram_twitter_thread_id 解析为 content 路由目标；
    --chat-id 手动覆盖时整体让位（content_thread_id 同时清空）。"""

    def _run_main(self, config_extra="", argv_extra=None, accounts=None):
        captured = {"threads": {}}
        config_path = Path("test_config_routing.json")
        config_path.write_text(
            '{"telegram_bot_token": "bot", "telegram_chat_id": "dm-chat"' + config_extra + '}')
        failures_path = Path("test_failures_routing.json")

        def fake_process_user(pool, ai, username, bot_token, chat_id, args,
                              content_chat_id=None, content_thread_id=None):
            captured["chat_id"] = chat_id
            captured["content_chat_id"] = content_chat_id
            captured["content_thread_id"] = content_thread_id
            captured["threads"][username] = content_thread_id
            return 1, 1, 0, 0

        def fake_process_article_queue(ai, bot_token, chat_id, dry_run=False, *,
                                       thread_id=None, thread_map=None):
            captured["article_chat_id"] = chat_id
            captured["article_thread_id"] = thread_id
            captured["article_thread_map"] = thread_map
            return 0

        argv = ["twitter_monitor.py"] + (argv_extra or [])
        try:
            with patch.object(twitter_monitor, "CONFIG_PATH", str(config_path)), \
                 patch.object(twitter_monitor, "ROUTE_TABLE_PATH",
                              "test_route_table_absent.json"), \
                 patch.object(twitter_monitor, "FAILURES_PATH", str(failures_path)), \
                 patch.object(twitter_monitor, "update_status_dashboard", return_value=None), \
                 patch.object(twitter_monitor, "check_cookie_health", return_value=None), \
                 patch.object(twitter_monitor.TokenPool, "load", return_value=None), \
                 patch.object(twitter_monitor.AIClassifier, "load", return_value=FakeAI(False)), \
                 patch.object(twitter_monitor, "load_accounts",
                              return_value=accounts or [{"username": "u"}]), \
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

    def test_topic_field_routes_accounts_to_mapped_threads(self):
        twitter_monitor._UNKNOWN_TOPIC_WARNED.clear()
        captured = self._run_main(
            config_extra=(', "telegram_group_chat_id": "-100123", "telegram_twitter_thread_id": 19'
                          ', "telegram_topic_threads": {"biz": 497}'),
            accounts=[{"username": "u"},
                      {"username": "v", "topic": "biz"},
                      {"username": "w", "topic": "nope"}])
        # 无 topic → 默认 19；命中 map → 497；未知 topic → 回退 19（不丢）
        self.assertEqual(captured["threads"], {"u": 19, "v": 497, "w": 19})
        # article 队列拿到同一份全量映射（不受 --user 影响的解析源）
        self.assertEqual(captured["article_thread_map"], {"u": 19, "v": 497, "w": 19})
        self.assertEqual(captured["article_thread_id"], 19)

    def test_chat_id_override_neutralizes_topic_map(self):
        twitter_monitor._UNKNOWN_TOPIC_WARNED.clear()
        captured = self._run_main(
            config_extra=(', "telegram_group_chat_id": "-100123", "telegram_twitter_thread_id": 19'
                          ', "telegram_topic_threads": {"biz": 497}'),
            argv_extra=["--chat-id", "debug-chat"],
            accounts=[{"username": "v", "topic": "biz"}])
        # --chat-id 覆盖：topic map 随群组路由整体让位，thread 全空
        self.assertEqual(captured["threads"], {"v": None})
        self.assertEqual(captured["article_thread_map"], {"v": None})


class ResolveTopicThreadTest(unittest.TestCase):
    """_resolve_topic_thread：topic→map→默认 的解析优先级与未知 topic 告警一次。"""

    def setUp(self):
        twitter_monitor._UNKNOWN_TOPIC_WARNED.clear()

    def test_priority_and_fallback(self):
        m = {"biz": 497}
        self.assertEqual(
            twitter_monitor._resolve_topic_thread({"username": "a", "topic": "biz"}, m, 19), 497)
        self.assertEqual(
            twitter_monitor._resolve_topic_thread({"username": "b"}, m, 19), 19)
        self.assertEqual(
            twitter_monitor._resolve_topic_thread({"username": "c", "topic": "  "}, m, 19), 19)
        self.assertEqual(
            twitter_monitor._resolve_topic_thread({"username": "d", "topic": "nope"}, m, 19), 19)

    def test_unknown_topic_warns_once(self):
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            twitter_monitor._resolve_topic_thread({"username": "a", "topic": "nope"}, {}, 19)
            twitter_monitor._resolve_topic_thread({"username": "b", "topic": "nope"}, {}, 19)
        self.assertEqual(buf.getvalue().count("未知 topic"), 1)


class ApplyRouteOverlayTest(unittest.TestCase):
    """apply_route_overlay：舰队路由表存在时覆盖群 chat 与话题 thread（config.json
    退居回落）；表缺失/损坏时 cfg 原样返回，不抛异常。"""

    def test_overlay_overrides_group_and_known_threads(self):
        table = Path("test_route_table_overlay.json")
        table.write_text(json.dumps({
            "chat_id": -100999,
            "topics": {"twitter": 42, "macrumors": 43, "growth": 44, "unrelated": 99},
        }))
        cfg = {"telegram_group_chat_id": "-100123", "telegram_twitter_thread_id": 19,
               "telegram_chat_id": "dm-chat",
               "telegram_topic_threads": {"twitter": 1, "cfg_only": 7}}
        try:
            with patch.object(twitter_monitor, "ROUTE_TABLE_PATH", str(table)):
                out = twitter_monitor.apply_route_overlay(cfg)
        finally:
            table.unlink(missing_ok=True)
        self.assertIs(out, cfg)
        self.assertEqual(cfg["telegram_group_chat_id"], "-100999")
        self.assertEqual(cfg["telegram_twitter_thread_id"], 42)
        self.assertEqual(cfg["telegram_macrumors_thread_id"], 43)
        self.assertEqual(cfg["telegram_growth_thread_id"], 44)
        self.assertEqual(cfg["telegram_chat_id"], "dm-chat")  # DM 不受路由表影响
        # 话题整表合并：路由表键覆盖 config 同名键，config 独有键保留
        self.assertEqual(cfg["telegram_topic_threads"],
                         {"twitter": 42, "macrumors": 43, "growth": 44,
                          "unrelated": 99, "cfg_only": 7})

    def test_overlay_missing_table_keeps_cfg(self):
        cfg = {"telegram_group_chat_id": "-100123", "telegram_twitter_thread_id": 19}
        with patch.object(twitter_monitor, "ROUTE_TABLE_PATH",
                          "test_route_table_absent.json"):
            out = twitter_monitor.apply_route_overlay(dict(cfg))
        self.assertEqual(out, cfg)


class ThreadNotFoundFallbackTest(unittest.TestCase):
    """话题失效自愈：400 message thread not found → 换回退话题重试一次；
    轮末 _alert_thread_fallback 汇总一条 DM。话题被删不能变成静默断流。"""

    def setUp(self):
        twitter_monitor._THREAD_FALLBACK_EVENTS.clear()
        twitter_monitor._AMBIGUOUS_STREAK = 0
        self._fallback_patch = patch.object(twitter_monitor, "_THREAD_FALLBACK_ID", 19)
        self._fallback_patch.start()
        self.addCleanup(self._fallback_patch.stop)

    def _thread_not_found_post(self, calls):
        import io
        import urllib.error

        def fake_post(token, payload, method="sendMessage"):
            calls.append(dict(payload))
            if payload.get("message_thread_id") == 555:
                raise urllib.error.HTTPError(
                    "url", 400, "Bad Request", {},
                    io.BytesIO(b'{"ok":false,"description":"Bad Request: message thread not found"}'))
            return {"ok": True, "result": {"message_id": 1}}

        return fake_post

    def test_send_telegram_falls_back_to_default_thread(self):
        calls = []
        with patch.object(twitter_monitor, "_tg_post",
                          side_effect=self._thread_not_found_post(calls)):
            r = twitter_monitor.send_telegram("bot", "chat", "hi", thread_id=555)
        self.assertTrue(r.get("ok"))
        self.assertEqual([c.get("message_thread_id") for c in calls], [555, 19])
        self.assertEqual(twitter_monitor._THREAD_FALLBACK_EVENTS, [555])

    def test_send_telegram_rich_falls_back_to_default_thread(self):
        calls = []
        with patch.object(twitter_monitor, "_tg_post",
                          side_effect=self._thread_not_found_post(calls)):
            r = twitter_monitor.send_telegram_rich("bot", "chat", html="hi", thread_id=555)
        self.assertTrue(r.get("ok"))
        self.assertNotIn("rich_fallback", r)
        self.assertEqual([c.get("message_thread_id") for c in calls], [555, 19])

    def test_send_telegram_photo_falls_back_to_default_thread(self):
        calls = []
        with patch.object(twitter_monitor, "_tg_post",
                          side_effect=self._thread_not_found_post(calls)):
            r = twitter_monitor.send_telegram_photo(
                "bot", "chat", "https://pbs.twimg.com/media/a.jpg", "hi", thread_id=555)
        self.assertTrue(r.get("ok"))
        self.assertEqual([c.get("message_thread_id") for c in calls], [555, 19])

    def test_no_fallback_id_drops_to_general(self):
        calls = []
        with patch.object(twitter_monitor, "_THREAD_FALLBACK_ID", None), \
             patch.object(twitter_monitor, "_tg_post",
                          side_effect=self._thread_not_found_post(calls)):
            r = twitter_monitor.send_telegram("bot", "chat", "hi", thread_id=555)
        self.assertTrue(r.get("ok"))
        self.assertNotIn("message_thread_id", calls[1])

    def test_other_400_still_strips_parse_mode(self):
        import io
        import urllib.error
        calls = []

        def fake_post(token, payload, method="sendMessage"):
            calls.append(dict(payload))
            if payload.get("parse_mode"):
                raise urllib.error.HTTPError(
                    "url", 400, "Bad Request", {},
                    io.BytesIO(b'{"ok":false,"description":"can\'t parse entities"}'))
            return {"ok": True}

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post):
            r = twitter_monitor.send_telegram("bot", "chat", "<b>hi</b>", thread_id=555)
        self.assertTrue(r.get("ok"))
        # 剥 parse_mode 重试，thread 保留（不是话题问题）
        self.assertNotIn("parse_mode", calls[1])
        self.assertEqual(calls[1].get("message_thread_id"), 555)
        self.assertEqual(twitter_monitor._THREAD_FALLBACK_EVENTS, [])

    def test_alert_thread_fallback_flushes_once(self):
        quiet = []
        twitter_monitor._THREAD_FALLBACK_EVENTS.extend([555, 555, 666])
        with patch.object(twitter_monitor, "_tg_post_quiet",
                          side_effect=lambda t, p, m: quiet.append(p) or {"ok": True}):
            twitter_monitor._alert_thread_fallback("bot", "dm")
            twitter_monitor._alert_thread_fallback("bot", "dm")  # 第二次应无事件可发
        self.assertEqual(len(quiet), 1)
        self.assertIn("555, 666", quiet[0]["text"])
        self.assertEqual(twitter_monitor._THREAD_FALLBACK_EVENTS, [])


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
        self.addCleanup(err.close)
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

    def test_504_becomes_ambiguous_not_retry(self):
        """504=网关超时，上游可能已处理：必须归歧义，走 5xx 盲重试会重复。"""
        import io
        import urllib.error
        err = urllib.error.HTTPError("url", 504, "Gateway Timeout", {}, io.BytesIO(b""))
        with patch("urllib.request.urlopen", side_effect=err):
            with self.assertRaises(twitter_monitor.TgAmbiguousDelivery):
                twitter_monitor._tg_post("tok", {"chat_id": "1"})

    def test_5xx_becomes_ambiguous_and_does_not_reset_streak(self):
        """5xx 不能证明请求未被接收；必须禁止重试且不能清零熔断。"""
        import io
        import urllib.error
        twitter_monitor._AMBIGUOUS_STREAK = 1
        err = urllib.error.HTTPError("url", 502, "Bad Gateway", {}, io.BytesIO(b""))
        with patch("urllib.request.urlopen", side_effect=err):
            with self.assertRaises(twitter_monitor.TgAmbiguousDelivery):
                twitter_monitor._tg_post("tok", {"chat_id": "1"})
        self.assertEqual(twitter_monitor._AMBIGUOUS_STREAK, 1)

    def test_4xx_resets_ambiguous_streak(self):
        """4xx（含 429）确由 Bot API 后端产生，证明链路在处理请求。"""
        import io
        import urllib.error
        twitter_monitor._AMBIGUOUS_STREAK = 1
        err = urllib.error.HTTPError("url", 429, "Too Many Requests", {}, io.BytesIO(b"{}"))
        self.addCleanup(err.close)
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
    连续歧义也不得改判为可重试失败；痕迹落盘隔离到临时文件。"""

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

    def test_consecutive_ambiguous_remain_unknown_without_retry(self):
        """连续未知结果仍不能证明未送达；每条只尝试一次并持久留痕。"""

        def fake_post(token, payload, method="sendMessage"):
            raise twitter_monitor.TgAmbiguousDelivery("socket.timeout: timed out")

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post), \
                patch("time.sleep"):
            first = twitter_monitor.send_telegram("tok", "1", "msg-1")
            self.assertTrue(first["assumed_delivered"])
            second = twitter_monitor.send_telegram("tok", "1", "msg-2")
            self.assertTrue(second["assumed_delivered"])
        entries = json.loads(Path(twitter_monitor.ASSUMED_DELIVERY_PATH).read_text())
        self.assertEqual(len(entries), 2)  # 每个未知结果都必须可审计

    def test_success_results_include_send_method_for_ledger_audit(self):
        # Each HTTP call returns a fresh decoded object in production. A shared
        # return_value would retain the first wrapper's setdefault annotation.
        with patch.object(twitter_monitor, "_tg_post",
                          side_effect=lambda *a, **k: {"ok": True}), \
             patch("time.sleep"):
            self.assertEqual(twitter_monitor.send_telegram("t", "c", "x")["send_method"],
                             "sendMessage")
            self.assertEqual(twitter_monitor.send_telegram_photo(
                "t", "c", "https://example.com/a.jpg")["send_method"], "sendPhoto")
            self.assertEqual(twitter_monitor.send_telegram_rich(
                "t", "c", html="x")["send_method"], "sendRichMessage")

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


class PushBudgetAndCheckpointTest(unittest.TestCase):
    """推送循环的时间预算与送达即刻 checkpoint（25m kill 窗口防重复）。"""

    def _args(self):
        return argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)

    def _tweet(self, tid, minute):
        return {"id": tid,
                "text": f"这是一条长度足够通过分类过滤器的正常推文内容编号{tid}",
                "createdAt": f"Tue May 12 00:{minute:02d}:00 +0000 2026"}

    def test_budget_exhausted_defers_to_push_retry_without_sending(self):
        sent, saved, saved_retry = [], {}, {}
        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets",
                          return_value=[self._tweet("t1", 20), self._tweet("t2", 21)]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "load_push_retry", return_value=set()), \
             patch.object(twitter_monitor, "save_seen",
                          side_effect=lambda u, s, last_post_ts=None: saved.update({"seen": set(s)})), \
             patch.object(twitter_monitor, "save_push_retry",
                          side_effect=lambda u, r: saved_retry.update({"retry": set(r)})), \
             patch.object(twitter_monitor, "_article_queue_time_remaining", return_value=10.0), \
             patch.object(twitter_monitor, "send_tweet",
                          side_effect=lambda *a, **k: sent.append(1) or {"ok": True}), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            _n, pushed, _f, _a = twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="u",
                bot_token="b", chat_id="c", args=self._args())
        self.assertEqual(sent, [])  # 预算不足绝不发起发送
        self.assertEqual(pushed, 0)
        self.assertEqual(saved_retry.get("retry"), {"t1", "t2"})
        self.assertNotIn("t1", saved.get("seen", set()))
        self.assertNotIn("t2", saved.get("seen", set()))

    def test_delivered_tweet_checkpointed_before_mid_loop_kill(self):
        """第 1 条送达后进程被 SIGALRM 杀（SystemExit）：seen 必须已落盘。"""
        seen_snapshots = []

        def fake_send(token, chat_id, username, t, ai=None, thread_id=None,
                      reply_to_message_id=None):
            if t["id"] == "t2":
                raise SystemExit(1)  # 模拟 SIGALRM handler 的 sys.exit
            return {"ok": True}

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets",
                          return_value=[self._tweet("t1", 20), self._tweet("t2", 21)]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "load_push_retry", return_value=set()), \
             patch.object(twitter_monitor, "save_seen",
                          side_effect=lambda u, s, last_post_ts=None: seen_snapshots.append(set(s))), \
             patch.object(twitter_monitor, "save_push_retry", return_value=None), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            with self.assertRaises(SystemExit):
                twitter_monitor.process_user(
                    pool=None, ai=FakeAI(False), username="u",
                    bot_token="b", chat_id="c", args=self._args())
        # kill 前的 checkpoint 已把 t1 落盘（末尾统一落盘没机会执行）
        self.assertTrue(seen_snapshots, "送达后必须有 checkpoint 落盘")
        self.assertIn("t1", seen_snapshots[-1])
        self.assertNotIn("t2", seen_snapshots[-1])

    def test_retry_tweet_checkpoint_clears_retry_file(self):
        """push_retry 里的推文送达后，checkpoint 立即从 retry 文件移除。"""
        retry_snapshots = []
        tweet = {"id": "retry-me",
                 "text": "这是一条长度足够通过分类过滤器的正常推文内容等待重试",
                 "createdAt": "Mon May 11 15:55:43 +0000 2026"}

        def fake_send(token, chat_id, username, t, ai=None, thread_id=None,
                      reply_to_message_id=None):
            return {"ok": True}

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=[tweet]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "load_push_retry", return_value={"retry-me"}), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "save_push_retry",
                          side_effect=lambda u, r: retry_snapshots.append(set(r))), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="u",
                bot_token="b", chat_id="c", args=self._args())
        # 第一次 save_push_retry 就是送达 checkpoint，retry-me 已移除
        self.assertTrue(retry_snapshots)
        self.assertNotIn("retry-me", retry_snapshots[0])

    def test_test_mode_does_not_checkpoint_into_production_seen(self):
        """--test 推送发往调试目标：绝不能写生产 seen / 摘 push_retry（否则生产群永久漏推）。"""
        saved, saved_retry = {}, {}
        args = argparse.Namespace(test=True, test_count=5, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)
        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets",
                          return_value=[self._tweet("t1", 20)]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "load_push_retry", return_value=set()), \
             patch.object(twitter_monitor, "save_seen",
                          side_effect=lambda u, s, last_post_ts=None: saved.update({"seen": set(s)})), \
             patch.object(twitter_monitor, "save_push_retry",
                          side_effect=lambda u, r: saved_retry.update({"retry": set(r)})), \
             patch.object(twitter_monitor, "send_tweet", return_value={"ok": True}), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="u",
                bot_token="b", chat_id="c", args=args)
        self.assertNotIn("t1", saved.get("seen", set()))

    def test_send_attempt_budget_gate_refuses_to_start_request(self):
        """逐次预算门槛：剩余不足 65s 时绝不发起请求（未发出=无重复风险）。"""
        with patch.object(twitter_monitor, "_article_queue_time_remaining",
                          return_value=10.0), \
             patch.object(twitter_monitor, "_tg_post") as m:
            with self.assertRaises(RuntimeError):
                twitter_monitor.send_telegram("tok", "1", "hi")
            with self.assertRaises(RuntimeError):
                twitter_monitor.send_telegram_rich("tok", "1", html="<b>hi</b>")
        m.assert_not_called()

    def test_ambiguous_circuit_trip_checkpoints_current_and_defers_rest(self):
        """The tripping request is unknown, while later requests were never sent."""
        tweets = [self._tweet(f"t{i}", 19 + i) for i in range(1, 4)]
        calls, seen_snapshots, retry_snapshots = [], [], []
        ledger_tmp = tempfile.TemporaryDirectory()
        self.addCleanup(ledger_tmp.cleanup)
        ledger = str(Path(ledger_tmp.name) / "events.sqlite3")

        def fake_send(_token, _chat_id, _username, tweet, _ai=None, thread_id=None,
                      reply_to_message_id=None):
            calls.append(tweet["id"])
            if tweet["id"] == "t2":
                raise twitter_monitor.TgAmbiguousDelivery("HTTP 504: Gateway Timeout")
            return {"ok": True, "result": {"message_id": 1},
                    "send_method": "sendRichMessage"}

        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "observe"), \
             patch.object(twitter_monitor, "EVENT_LEDGER_PATH", ledger), \
             patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=tweets), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "load_push_retry", return_value=set()), \
             patch.object(twitter_monitor, "save_seen",
                          side_effect=lambda _u, ids, last_post_ts=None:
                          seen_snapshots.append(set(ids))), \
             patch.object(twitter_monitor, "save_push_retry",
                          side_effect=lambda _u, ids: retry_snapshots.append(set(ids))), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            _new, pushed, _filtered, _overridden = twitter_monitor.process_user(
                None, FakeAI(False), "u", "b", "c", self._args())

        self.assertEqual(calls, ["t1", "t2"])
        self.assertEqual(pushed, 2)
        self.assertTrue(any("t2" in snapshot for snapshot in seen_snapshots))
        self.assertIn("t3", retry_snapshots[-1])
        with twitter_monitor._event_ledger_connect(ledger) as db:
            states = {row["tweet_id"]: row["state"] for row in db.execute(
                "SELECT tweet_id,state FROM deliveries")}
        self.assertEqual(states, {"t1": "confirmed", "t2": "ambiguous"})

    def test_orphan_retry_entry_already_seen_is_pruned_not_repushed(self):
        """checkpoint 顺序被杀留下的孤儿（seen ∧ push_retry）：清理且不重推。"""
        sent, saved_retry = [], {}
        tweet = {"id": "orphan",
                 "text": "这是一条长度足够通过分类过滤器的正常推文内容孤儿条目",
                 "createdAt": "Tue May 12 00:20:00 +0000 2026"}
        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=[tweet]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"orphan"}, None)), \
             patch.object(twitter_monitor, "load_push_retry", return_value={"orphan"}), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "save_push_retry",
                          side_effect=lambda u, r: saved_retry.update({"retry": set(r)})), \
             patch.object(twitter_monitor, "send_tweet",
                          side_effect=lambda *a, **k: sent.append(1) or {"ok": True}), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            twitter_monitor.process_user(
                pool=None, ai=FakeAI(False), username="u",
                bot_token="b", chat_id="c", args=self._args())
        self.assertEqual(sent, [])  # 已 seen，绝不重推
        self.assertEqual(saved_retry.get("retry"), set())  # 孤儿已清理


class MacrumorsAmbiguousDeliveryTest(unittest.TestCase):
    """macrumors_daily 对 TgAmbiguousDelivery 的同款语义：按已送达、不重试、不回落。"""

    def setUp(self):
        import macrumors_daily
        self.md = macrumors_daily
        twitter_monitor._AMBIGUOUS_STREAK = 0
        self.recorded = []
        self._rec_patch = patch.object(
            twitter_monitor, "_record_assumed_delivery",
            side_effect=lambda method, link: self.recorded.append((method, link)))
        self._rec_patch.start()
        self.addCleanup(self._rec_patch.stop)

    def test_send_card_ambiguous_returns_normally_no_text_fallback(self):
        """卡片歧义不得抛出——抛出会走 main 的回落文字 = 已送达卡片再发一遍。"""
        def fake_post(token, payload, method="sendMessage"):
            raise twitter_monitor.TgAmbiguousDelivery("socket.timeout: timed out")

        item = {"zh_title": "标题", "zh_summary": "摘要", "image": "https://i/1.jpg",
                "link": "https://www.macrumors.com/x", "sub_items": None}
        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post):
            self.md.send_card("tok", "1", item)  # 不应抛出
        self.assertEqual(self.recorded,
                         [("sendPhoto(macrumors)", "https://www.macrumors.com/x")])

    def test_send_html_ambiguous_returns_without_retry(self):
        calls = []

        def fake_post(token, payload, method="sendMessage"):
            calls.append(method)
            raise twitter_monitor.TgAmbiguousDelivery("socket.timeout: timed out")

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post), \
                patch("time.sleep"):
            self.md.send_html("tok", "1", "hello", trace_id="digest 1/2")  # 不应抛出
        self.assertEqual(len(calls), 1)  # 不盲目重发
        self.assertEqual(self.recorded, [("sendMessage(macrumors)", "digest 1/2")])

    def test_send_html_consecutive_ambiguous_never_retries(self):
        """连续歧义仍按未知送达收口，不让 digest 次日盲重发。"""

        def fake_post(token, payload, method="sendMessage"):
            raise twitter_monitor.TgAmbiguousDelivery("socket.timeout: timed out")

        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post), \
                patch("time.sleep"):
            self.md.send_html("tok", "1", "part1", trace_id="digest 1/2")  # 首条按已送达
            self.md.send_html("tok", "1", "part2", trace_id="digest 2/2")
        self.assertEqual(self.recorded, [
            ("sendMessage(macrumors)", "digest 1/2"),
            ("sendMessage(macrumors)", "digest 2/2"),
        ])

    def test_card_then_html_ambiguous_are_both_audited_without_retry(self):
        """卡片后续文字都无回执时，两者均只留痕而不重发。"""

        def fake_post(token, payload, method="sendMessage"):
            raise twitter_monitor.TgAmbiguousDelivery("socket.timeout: timed out")

        item = {"zh_title": "标题", "zh_summary": "", "image": "https://i/1.jpg",
                "link": "https://www.macrumors.com/x", "sub_items": None}
        with patch.object(twitter_monitor, "_tg_post", side_effect=fake_post), \
                patch("time.sleep"):
            self.md.send_card("tok", "1", item)  # 卡片按已送达，但计数=1
            self.md.send_html("tok", "1", "part1", trace_id="digest 1/1")
        self.assertEqual(self.recorded, [
            ("sendPhoto(macrumors)", "https://www.macrumors.com/x"),
            ("sendMessage(macrumors)", "digest 1/1"),
        ])


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


class CookieHealthAlertTest(unittest.TestCase):
    """cookie 认证看门狗：连续"整轮降级 guest"达阈值只告警一次，authed 恢复后清零。"""

    def _health(self, degraded, cookies_loaded=False, degrade_events=0, authed_success=0):
        import twitter_graphql as tg
        return patch.object(tg, "auth_health_summary", return_value={
            "cookies_loaded": cookies_loaded,
            "authed_success": authed_success,
            "degrade_events": degrade_events,
            "degraded": degraded,
        })

    def test_alert_fires_once_at_threshold_then_resets_on_recovery(self):
        sent = []

        def fake_send(token, chat_id, text, link="", thread_id=None):
            sent.append(text)
            return {"ok": True, "result": {"message_id": 111}}

        with tempfile.TemporaryDirectory() as d:
            path = str(Path(d) / ".cookie_health.json")
            with patch.object(twitter_monitor, "COOKIE_HEALTH_PATH", path), \
                 patch.object(twitter_monitor, "send_telegram", side_effect=fake_send), \
                 patch.object(twitter_monitor, "_tg_post_quiet", return_value={"ok": True}):
                # 降级但未到阈值：不告警
                with self._health(degraded=True, cookies_loaded=True, degrade_events=1):
                    for _ in range(twitter_monitor.COOKIE_DEGRADE_ALERT_THRESHOLD - 1):
                        twitter_monitor.check_cookie_health("bot", "chat")
                    self.assertEqual(sent, [])
                    # 第 THRESHOLD 轮：告警一次
                    twitter_monitor.check_cookie_health("bot", "chat")
                    self.assertEqual(len(sent), 1)
                    self.assertIn("cookie", sent[0].lower())
                    st = twitter_monitor.load_cookie_health()
                    self.assertTrue(st["alerted"])
                    self.assertEqual(st["consecutive_degraded"],
                                     twitter_monitor.COOKIE_DEGRADE_ALERT_THRESHOLD)
                    # 继续降级：不重复告警
                    twitter_monitor.check_cookie_health("bot", "chat")
                    self.assertEqual(len(sent), 1)
                # authed 恢复：清零、alerted 复位
                with self._health(degraded=False, cookies_loaded=True, authed_success=3):
                    twitter_monitor.check_cookie_health("bot", "chat")
                st = twitter_monitor.load_cookie_health()
                self.assertEqual(st["consecutive_degraded"], 0)
                self.assertFalse(st["alerted"])

    def test_dry_run_counts_but_never_sends_or_latches(self):
        sent = []
        with tempfile.TemporaryDirectory() as d:
            path = str(Path(d) / ".cookie_health.json")
            with patch.object(twitter_monitor, "COOKIE_HEALTH_PATH", path), \
                 patch.object(twitter_monitor, "send_telegram",
                              side_effect=lambda *a, **k: sent.append(a) or {"ok": True}), \
                 patch.object(twitter_monitor, "_tg_post_quiet", return_value={"ok": True}), \
                 self._health(degraded=True, cookies_loaded=False):
                for _ in range(twitter_monitor.COOKIE_DEGRADE_ALERT_THRESHOLD + 1):
                    twitter_monitor.check_cookie_health("bot", "chat", dry_run=True)
                st = json.loads(Path(path).read_text())
            self.assertEqual(sent, [])
            self.assertFalse(st.get("alerted", False))
            self.assertGreaterEqual(st["consecutive_degraded"],
                                    twitter_monitor.COOKIE_DEGRADE_ALERT_THRESHOLD)

    def test_healthy_run_stays_silent_and_zeroed(self):
        sent = []
        with tempfile.TemporaryDirectory() as d:
            path = str(Path(d) / ".cookie_health.json")
            with patch.object(twitter_monitor, "COOKIE_HEALTH_PATH", path), \
                 patch.object(twitter_monitor, "send_telegram",
                              side_effect=lambda *a, **k: sent.append(a) or {"ok": True}), \
                 patch.object(twitter_monitor, "_tg_post_quiet", return_value={"ok": True}), \
                 self._health(degraded=False, cookies_loaded=True, authed_success=5):
                for _ in range(3):
                    twitter_monitor.check_cookie_health("bot", "chat")
                st = json.loads(Path(path).read_text())
            self.assertEqual(sent, [])
            self.assertEqual(st["consecutive_degraded"], 0)


class MusingClassifyTest(unittest.TestCase):
    """碎碎念规则层：classify 启发式 + 不误伤实质内容。"""

    def test_pocket3_fishing_sample_is_musing_suspicious(self):
        t = {
            "text": "把pocket3充满电，准备去钓鱼。",
            "media": [{"type": "photo", "url": "https://pbs.twimg.com/media/x.jpg"}],
        }
        status, reason = twitter_monitor.classify(t)
        self.assertEqual(status, "suspicious")
        self.assertTrue(
            reason.startswith(twitter_monitor.REASON_MUSING_PREFIX),
            msg=reason,
        )

    def test_short_photo_status_is_musing(self):
        t = {
            # ≥ MIN_LEN=18，否则会先被 too_short 硬过滤
            "text": "今天天气真的不错呀，出门去晒太阳了。",
            "media": [{"type": "photo", "url": "https://pbs.twimg.com/media/x.jpg"}],
        }
        status, reason = twitter_monitor.classify(t)
        self.assertEqual(status, "suspicious")
        self.assertTrue(reason.startswith(twitter_monitor.REASON_MUSING_PREFIX), msg=reason)

    def test_life_kw_without_photo_is_musing(self):
        t = {"text": "周末宅家追剧，什么都不想干就这样过。"}
        status, reason = twitter_monitor.classify(t)
        self.assertEqual(status, "suspicious")
        self.assertIn("musing_life_kw", reason)

    def test_technical_post_still_passes(self):
        t = {"text": "Claude 新 API 支持 prompt caching，延迟降一半。"}
        status, reason = twitter_monitor.classify(t)
        self.assertEqual((status, reason), ("pass", "ok"))

    def test_substantive_keyword_blocks_musing(self):
        t = {
            "text": "把手机充满电后继续跑本地模型评测，结果很意外。",
            "media": [{"type": "photo", "url": "https://pbs.twimg.com/media/x.jpg"}],
        }
        status, reason = twitter_monitor.classify(t)
        self.assertEqual((status, reason), ("pass", "ok"))

    def test_non_media_url_blocks_musing(self):
        t = {
            "text": "出门钓鱼前看这篇 https://example.com/guide 讲得不错。",
            "media": [{"type": "photo", "url": "https://pbs.twimg.com/media/x.jpg"}],
            "entities": {
                "urls": [{"url": "https://t.co/abc", "expanded_url": "https://example.com/guide"}],
            },
        }
        status, reason = twitter_monitor.classify(t)
        self.assertEqual((status, reason), ("pass", "ok"))

    def test_too_short_still_hard_filter(self):
        status, reason = twitter_monitor.classify({"text": "短"})
        self.assertEqual(status, "filter")
        self.assertTrue(reason.startswith("too_short"), msg=reason)

    def test_long_note_tweet_not_musing(self):
        t = {
            "text": "把pocket3充满电，准备去钓鱼。",
            "note_tweet": {"text": "关于 AI agent 工作流的一些观察：" + ("细节" * 80)},
            "media": [{"type": "photo", "url": "https://pbs.twimg.com/media/x.jpg"}],
        }
        status, reason = twitter_monitor.classify(t)
        self.assertEqual((status, reason), ("pass", "ok"))

    def test_commercial_still_suspicious_not_musing(self):
        t = {
            "text": "byteplus seedance 2.0 api 文档访问体验开通模型冲 200 立即体验方舟平台",
        }
        status, reason = twitter_monitor.classify(t)
        self.assertEqual(status, "suspicious")
        self.assertTrue(reason.startswith("commercial"), msg=reason)


class MusingProcessUserTest(unittest.TestCase):
    """碎碎念 process_user：AI 确认/否决/失败/无 AI 默认 filter。"""

    SAMPLE = {
        "id": "m1",
        "text": "把pocket3充满电，准备去钓鱼。",
        "media": [{"type": "photo", "url": "https://pbs.twimg.com/media/x.jpg"}],
        "createdAt": "Tue May 12 00:20:00 +0000 2026",
    }

    def _run(self, ai, tweet=None):
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)
        pushed_ids = []

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None,
                            reply_to_message_id=None):
            pushed_ids.append(t["id"])
            return {"ok": True}

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets",
                          return_value=[tweet or self.SAMPLE]), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send_tweet), \
             patch.object(twitter_monitor, "_alert_ai_all_failed", return_value=None), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            new, pushed, filt, ov = twitter_monitor.process_user(
                pool=None, ai=ai, username="vista8",
                bot_token="b", chat_id="c", args=args)
        return new, pushed, filt, ov, pushed_ids

    def test_ai_confirms_musing_filters(self):
        new, pushed, filt, ov, ids = self._run(FakeAI(True, musing=True, musing_reason="life"))
        self.assertEqual((new, pushed, filt, ov), (1, 0, 1, 0))
        self.assertEqual(ids, [])

    def test_ai_rejects_musing_pushes(self):
        new, pushed, filt, ov, ids = self._run(
            FakeAI(True, musing=False, musing_reason="has_insight"))
        self.assertEqual((new, pushed, filt), (1, 1, 0))
        self.assertEqual(ov, 1)
        self.assertEqual(ids, ["m1"])

    def test_no_ai_filters_musing_by_default(self):
        new, pushed, filt, ov, ids = self._run(FakeAI(False))
        self.assertEqual((new, pushed, filt, ov), (1, 0, 1, 0))
        self.assertEqual(ids, [])

    def test_ai_all_failed_filters_musing(self):
        class FailAI:
            def is_available(self):
                return True

            def confirm_musing(self, username, text):
                return False, "all_ai_failed"

            def confirm_promo(self, username, text):
                return False, "all_ai_failed"

        new, pushed, filt, ov, ids = self._run(FailAI())
        self.assertEqual((new, pushed, filt, ov), (1, 0, 1, 0))
        self.assertEqual(ids, [])

    def test_promo_no_ai_still_passes(self):
        promo = {
            "id": "p1",
            "text": "byteplus seedance 2.0 api 文档访问体验开通模型冲 200 立即体验方舟平台",
            "createdAt": "Tue May 12 00:20:00 +0000 2026",
        }
        new, pushed, filt, ov, ids = self._run(FakeAI(False), tweet=promo)
        self.assertEqual((new, pushed, filt), (1, 1, 0))
        self.assertEqual(ids, ["p1"])


class SelfReplyParentTest(unittest.TestCase):
    """self_reply_parent_id：只认「同作者接自己上一条」，其余一律不接线程。"""

    @staticmethod
    def reply(parent_id="100", screen_name="vista8", user_id="", **extra):
        t = {"id": "101", "text": "补充一句",
             "user": {"screen_name": "vista8", "id_str": "42"},
             "in_reply_to_status": {"id": parent_id, "screen_name": screen_name,
                                    "user_id": user_id}}
        t.update(extra)
        return t

    def test_self_reply_matches_on_screen_name(self):
        self.assertEqual(
            twitter_monitor.self_reply_parent_id(self.reply(), "vista8"), "100")

    def test_screen_name_match_is_case_insensitive(self):
        self.assertEqual(
            twitter_monitor.self_reply_parent_id(self.reply(screen_name="ViSta8"), "vista8"),
            "100")

    def test_reply_to_another_author_never_threads(self):
        # 接到自己某条消息下面会张冠李戴：别人的推文不是本账号的串
        self.assertEqual(
            twitter_monitor.self_reply_parent_id(self.reply(screen_name="dotey"), "vista8"), "")

    def test_missing_screen_name_falls_back_to_numeric_user_id(self):
        self.assertEqual(
            twitter_monitor.self_reply_parent_id(
                self.reply(screen_name="", user_id="42"), "vista8"), "100")
        self.assertEqual(
            twitter_monitor.self_reply_parent_id(
                self.reply(screen_name="", user_id="43"), "vista8"), "")

    def test_unresolvable_author_never_threads(self):
        self.assertEqual(
            twitter_monitor.self_reply_parent_id(
                self.reply(screen_name="", user_id=""), "vista8"), "")

    def test_retweet_shell_never_threads(self):
        # 转推壳正文来自他人，即便 legacy 带 in_reply_to 也不能接自己的串
        tweet = self.reply(retweeted_status={"id": "900", "screen_name": "other"})
        self.assertEqual(twitter_monitor.self_reply_parent_id(tweet, "vista8"), "")

    def test_non_reply_and_malformed_parent_return_empty(self):
        self.assertEqual(twitter_monitor.self_reply_parent_id({"id": "1"}, "vista8"), "")
        self.assertEqual(
            twitter_monitor.self_reply_parent_id(self.reply(parent_id="not-an-id"), "vista8"), "")
        self.assertEqual(
            twitter_monitor.self_reply_parent_id({"in_reply_to_status": "x"}, "vista8"), "")


class TweetAnchorStoreTest(unittest.TestCase):
    """推文 → Telegram 消息锚点：按投递目标隔离 + TTL 回收 + 一次性回填。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "ledger.sqlite3")

    def tearDown(self):
        self.tmp.cleanup()

    def test_roundtrip_and_overwrite(self):
        twitter_monitor.record_tweet_anchor("100", 5754, username="Khazix0918",
                                            target_chat_id="g", target_thread_id=1145,
                                            path=self.db)
        self.assertEqual(twitter_monitor.lookup_tweet_anchor(
            "100", target_chat_id="g", target_thread_id=1145, path=self.db), 5754)
        twitter_monitor.record_tweet_anchor("100", 5999, target_chat_id="g",
                                            target_thread_id=1145, path=self.db)
        self.assertEqual(twitter_monitor.lookup_tweet_anchor(
            "100", target_chat_id="g", target_thread_id=1145, path=self.db), 5999)

    def test_lookup_is_scoped_to_chat_and_thread(self):
        # 话题改路由后旧锚点属于别的话题，跨话题回复会被 Telegram 400 拒；
        # 查不到才是正确的降级。
        twitter_monitor.record_tweet_anchor("100", 5754, target_chat_id="g",
                                            target_thread_id=1145, path=self.db)
        for chat, thread in (("g", 1146), ("other", 1145), ("g", None)):
            self.assertIsNone(twitter_monitor.lookup_tweet_anchor(
                "100", target_chat_id=chat, target_thread_id=thread, path=self.db))

    def test_missing_message_id_is_not_recorded(self):
        # 歧义送达没有 message_id：宁可退化成独立消息，也不能记个空锚点
        self.assertFalse(twitter_monitor.record_tweet_anchor("100", None, path=self.db))
        self.assertIsNone(twitter_monitor.lookup_tweet_anchor("100", path=self.db))

    def test_write_evicts_entries_past_ttl(self):
        stale = datetime.now(timezone.utc) - timedelta(
            days=twitter_monitor.TWEET_ANCHOR_TTL_DAYS + 1)
        twitter_monitor.record_tweet_anchor("old", 1, path=self.db, now=stale)
        self.assertEqual(twitter_monitor.lookup_tweet_anchor("old", path=self.db), 1)
        twitter_monitor.record_tweet_anchor("fresh", 2, path=self.db)
        self.assertIsNone(twitter_monitor.lookup_tweet_anchor("old", path=self.db))
        self.assertEqual(twitter_monitor.lookup_tweet_anchor("fresh", path=self.db), 2)

    def _rewind_to_pre_anchor_ledger(self):
        """把库退回「只有 deliveries、还没有锚点表」的升级前状态。"""
        with twitter_monitor._event_ledger_connect(self.db) as db:
            db.execute("DELETE FROM tweet_anchors")
            db.execute("PRAGMA user_version=0")

    def test_backfills_confirmed_deliveries_exactly_once(self):
        with patch.object(twitter_monitor, "_EVENT_DEDUP_EFFECTIVE_MODE", "observe"):
            for tid, message_id, state in (("100", 4242, "confirmed"),
                                           ("200", 4243, "ambiguous"),
                                           ("300", None, "failed_pre_send")):
                claim = twitter_monitor.claim_event_delivery(
                    {"id": tid, "text": "parent " + tid}, "vista8",
                    target_chat_id="g", target_thread_id=1145, path=self.db)
                twitter_monitor.finish_event_delivery(
                    claim, state, {"result": {"message_id": message_id}}, path=self.db)
        self._rewind_to_pre_anchor_ledger()

        def anchor(tid):
            return twitter_monitor.lookup_tweet_anchor(
                tid, target_chat_id="g", target_thread_id=1145, path=self.db)

        # 升级后首次连接即回填，历史父推立刻可接线程，不必空等一个 TTL 攒锚点
        self.assertEqual(anchor("100"), 4242)
        # 只认已确认送达：歧义/失败的投递没有可靠的 message_id
        self.assertIsNone(anchor("200"))
        self.assertIsNone(anchor("300"))
        # 回填只跑一次：TTL 清掉的锚点不该被下次连接反复捞回来
        with twitter_monitor._event_ledger_connect(self.db) as db:
            db.execute("DELETE FROM tweet_anchors")
        self.assertIsNone(anchor("100"))

    def test_backfill_survives_a_ledger_without_target_columns(self):
        # 目标列是后加的；回填若排在列迁移之前，老库会 no such column 直接炸库
        legacy = sqlite3.connect(self.db)
        legacy.executescript("""
            CREATE TABLE deliveries (
                delivery_key TEXT PRIMARY KEY, event_key TEXT NOT NULL,
                event_type TEXT NOT NULL DEFAULT '', facts_json TEXT NOT NULL DEFAULT '[]',
                tweet_id TEXT NOT NULL, username TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN
                    ('pending','confirmed','ambiguous','failed_pre_send')),
                claim_token TEXT NOT NULL, claimed_at TEXT NOT NULL,
                updated_at TEXT NOT NULL, message_id TEXT, send_method TEXT, detail TEXT);
            INSERT INTO deliveries VALUES
                ('k','v1:e','','[]','100','vista8','confirmed','tok',
                 '2026-05-12T00:00:00+00:00','2026-05-12T00:00:00+00:00','4242',
                 'sendRichMessage',NULL);
        """)
        legacy.commit()
        legacy.close()
        self.assertEqual(twitter_monitor.lookup_tweet_anchor("100", path=self.db), 4242)


class SelfReplyThreadSendTest(unittest.TestCase):
    """锚点一路带到各降级档，并在 process_user 里把评论接到父推消息下面。"""

    PARENT = {"id": "100", "text": "先说结论，Scaling Law 并没有撞墙，这里展开讲讲我的理由",
              "createdAt": "Tue May 12 00:20:00 +0000 2026",
              "user": {"screen_name": "vista8", "id_str": "42"}}
    REPLY = {"id": "101", "text": "补一张图，这是我说的那份实验数据的完整对照表",
             "createdAt": "Tue May 12 00:25:00 +0000 2026",
             "user": {"screen_name": "vista8", "id_str": "42"},
             "in_reply_to_status": {"id": "100", "screen_name": "vista8", "user_id": "42"}}

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "ledger.sqlite3")
        self.ledger = patch.object(twitter_monitor, "EVENT_LEDGER_PATH", self.db)
        self.ledger.start()

    def tearDown(self):
        self.ledger.stop()
        self.tmp.cleanup()

    def test_send_telegram_rich_sets_reply_parameters(self):
        seen = {}
        with patch.object(twitter_monitor, "_tg_post",
                          side_effect=lambda tok, payload, method="": seen.update(payload)
                          or {"ok": True}):
            twitter_monitor.send_telegram_rich("tok", "g", html="hi", reply_to_message_id=5754)
        # allow_sending_without_reply：父推消息被删时降级为普通消息，不是 400 丢推
        self.assertEqual(seen["reply_parameters"],
                         {"message_id": 5754, "allow_sending_without_reply": True})

    def test_no_anchor_leaves_reply_parameters_unset(self):
        seen = {}
        with patch.object(twitter_monitor, "_tg_post",
                          side_effect=lambda tok, payload, method="": seen.update(payload)
                          or {"ok": True}):
            twitter_monitor.send_telegram_rich("tok", "g", html="hi")
        self.assertNotIn("reply_parameters", seen)

    def test_anchor_survives_the_whole_fallback_ladder(self):
        # rich 被拒 → sendPhoto 被拒 → HTML：任何一档掉了锚点，评论都会脱离线程
        anchors = {}
        tweet = dict(self.REPLY, media=[{"type": "photo",
                                         "url": "https://pbs.twimg.com/media/x.png"}])

        def fake_rich(token, chat_id, markdown="", link="", *, html="", thread_id=None,
                      reply_to_message_id=None):
            anchors["rich"] = reply_to_message_id
            return {"ok": False, "rich_fallback": True}

        def fake_photo(token, chat_id, photo, caption="", link="", *, thread_id=None,
                       reply_to_message_id=None, button_text=""):
            anchors["photo"] = reply_to_message_id
            return {"ok": False, "photo_fallback": True}

        def fake_legacy(token, chat_id, text, link="", *, preview_url="", thread_id=None,
                        reply_to_message_id=None, button_text=""):
            anchors["html"] = reply_to_message_id
            return {"ok": True, "result": {"message_id": 9}}

        with patch.object(twitter_monitor, "send_telegram_rich", side_effect=fake_rich), \
             patch.object(twitter_monitor, "send_telegram_photo", side_effect=fake_photo), \
             patch.object(twitter_monitor, "send_telegram", side_effect=fake_legacy):
            twitter_monitor.send_tweet("tok", "g", "vista8", tweet, reply_to_message_id=5754)
        self.assertEqual(anchors, {"rich": 5754, "photo": 5754, "html": 5754})

    def _process(self, tweets, **overrides):
        args = argparse.Namespace(test=False, seed=False, dry_run=False,
                                  limit=20, max_push_age_minutes=45)
        sent = []
        message_ids = iter([5754, 5764, 5774])

        def fake_send_tweet(token, chat_id, username, t, ai=None, thread_id=None,
                            reply_to_message_id=None):
            sent.append((t["id"], reply_to_message_id))
            return {"ok": True, "result": {"message_id": next(message_ids)}}

        with patch.object(twitter_monitor, "datetime", FixedDatetime), \
             patch.object(twitter_monitor, "fetch_tweets", return_value=list(tweets)), \
             patch.object(twitter_monitor, "load_seen", return_value=({"old"}, None)), \
             patch.object(twitter_monitor, "save_seen", return_value=None), \
             patch.object(twitter_monitor, "send_tweet", side_effect=fake_send_tweet), \
             patch.object(twitter_monitor.time, "sleep", return_value=None):
            twitter_monitor.process_user(pool=None, ai=FakeAI(False), username="vista8",
                                         bot_token="b", chat_id="c", args=args,
                                         **overrides)
        return sent

    def test_same_run_thread_sends_parent_first_then_replies_under_it(self):
        # 时间线是倒序的；父推必须先落地才有 message_id 可锚
        sent = self._process([self.REPLY, self.PARENT])
        self.assertEqual(sent, [("100", None), ("101", 5754)])

    def test_reply_threads_onto_a_parent_pushed_in_an_earlier_run(self):
        twitter_monitor.record_tweet_anchor("100", 5754, username="vista8",
                                            target_chat_id="c", path=self.db)
        self.assertEqual(self._process([self.REPLY]), [("101", 5754)])

    def test_reply_without_a_telegram_anchor_stays_standalone(self):
        # 父推被过滤/超出保留期时无锚点可接，退化成独立消息而不是丢推
        self.assertEqual(self._process([self.REPLY]), [("101", None)])

    def test_anchor_lookup_follows_the_content_route(self):
        twitter_monitor.record_tweet_anchor("100", 5754, target_chat_id="group",
                                            target_thread_id=1145, path=self.db)
        sent = self._process([self.REPLY], content_chat_id="group", content_thread_id=1145)
        self.assertEqual(sent, [("101", 5754)])
        # 同一批锚点不属于默认 DM 目标
        self.assertEqual(self._process([self.REPLY]), [("101", None)])


class SemanticMediaOnlyRenderTest(unittest.TestCase):
    """无正文的纯媒体推：媒体块必须是真 <img> 标签，不能被削成裸文本。"""

    @staticmethod
    def tweet(url="https://pbs.twimg.com/media/HQE63QGaAAIFQfx.png"):
        media = [{"type": "photo", "url": url}]
        return {"id": "101", "text": "", "media": media, "_semantic_active": True,
                "semantic_bundle": {
                    "observation": {"outer_id": "101", "observed_via": "Khazix0918"},
                    "anchor": {"tweet_id": "101", "author": "Khazix0918", "text": "",
                               "media": media,
                               "source_url": "https://x.com/Khazix0918/status/101"},
                    "repost_path": [], "context_nodes": [], "assets": [],
                    "resolution": {"status": "complete"}}}

    def test_media_only_anchor_keeps_the_img_tag_intact(self):
        with patch.object(twitter_monitor, "_SEMANTIC_BUNDLE_ENABLED", True):
            _plain, rich, _link = twitter_monitor.format_message("Khazix0918", self.tweet())
        # 回归：曾用 str.lstrip("<br>") 削前缀，把 "<img" 的 "<" 一起吃掉，
        # 图片降级成裸文本 `img src="…"/>`
        self.assertIn('<img src="https://pbs.twimg.com/media/HQE63QGaAAIFQfx.png"/>', rich)
        self.assertNotRegex(rich, r'(?<!<)img src=')
        # 头部与媒体块之间恰好一层分隔，媒体块自带的前导 <br><br> 已按前缀删掉
        self.assertEqual(rich.count("<br>"), 2)


if __name__ == "__main__":
    unittest.main()
