"""Behavioral regressions for performance changes; no external traffic."""
import argparse
from contextlib import ExitStack, redirect_stdout
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import twitter_monitor as tm
from test_twitter_monitor import FakeAI, FixedDatetime


class LedgerPerformanceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / 'ledger.sqlite3')
        tm._event_ledger_connect(self.path).close()

    def test_ready_lookup_can_read_while_another_connection_owns_writer_lock(self):
        tm.record_tweet_anchor('10', 42, path=self.path)
        connect = sqlite3.connect

        class ShortTimeoutConnection(sqlite3.Connection):
            def execute(self, sql, *args, **kwargs):
                if sql == 'PRAGMA busy_timeout=10000':
                    sql = 'PRAGMA busy_timeout=1'
                return super().execute(sql, *args, **kwargs)

        writer = connect(self.path, isolation_level=None)
        try:
            writer.execute('BEGIN IMMEDIATE')
            writer.execute("UPDATE tweet_anchors SET message_id='99' WHERE tweet_id='10'")
            with patch.object(tm.sqlite3, 'connect', side_effect=lambda *a, **kw:
                              connect(*a, factory=ShortTimeoutConnection, **kw)), \
                 patch.object(tm.time, 'sleep', side_effect=AssertionError('reader waited on writer')):
                # Read committed data immediately; no timing assertion or writer release needed.
                self.assertEqual(tm.lookup_tweet_anchor('10', path=self.path), 42)
                self.assertEqual(tm.event_dedup_gate_report(self.path)['candidates'], 0)
        finally:
            writer.rollback()
            writer.close()

    def test_helpers_close_connections_even_when_duplicate_claim_returns_early(self):
        connect = tm._event_ledger_connect
        connections = []

        def tracked(path):
            db = connect(path)
            connections.append(db)
            return db

        with patch.object(tm, '_event_ledger_connect', side_effect=tracked):
            first = tm.claim_event_delivery({'id': '10', 'text': 'parent'}, 'u', path=self.path)
            duplicate = tm.claim_event_delivery({'id': '10', 'text': 'parent'}, 'u', path=self.path)
            tm.event_dedup_gate_report(self.path)
        self.assertTrue(first['claimed'])
        self.assertFalse(duplicate['claimed'])
        for db in connections:
            with self.assertRaises(sqlite3.ProgrammingError):
                db.execute('SELECT 1')

    def test_session_rolls_back_and_closes_on_exception(self):
        with self.assertRaisesRegex(RuntimeError, 'injected'):
            with tm._event_ledger_session(self.path) as db:
                db.execute('BEGIN IMMEDIATE')
                db.execute("INSERT INTO tweet_anchors VALUES ('','','10','42','','now')")
                raise RuntimeError('injected')
        with self.assertRaises(sqlite3.ProgrammingError):
            db.execute('SELECT 1')
        self.assertIsNone(tm.lookup_tweet_anchor('10', path=self.path))

    def test_backfill_and_version_marker_rollback_together(self):
        claim = tm.claim_event_delivery({'id': '10', 'text': 'parent'}, 'u', path=self.path)
        tm.finish_event_delivery(claim, 'confirmed', {'result': {'message_id': 42}}, path=self.path)
        with tm._event_ledger_session(self.path) as db:
            db.execute('PRAGMA user_version=0')
            db.execute('DROP INDEX event_observations_candidate_idx')
        connect = sqlite3.connect
        connections = []

        class FailVersionConnection(sqlite3.Connection):
            def execute(self, sql, *args, **kwargs):
                if sql.startswith('PRAGMA user_version='):
                    raise sqlite3.DatabaseError('injected version failure')
                return super().execute(sql, *args, **kwargs)

        def failing_connect(*args, **kwargs):
            db = connect(*args, factory=FailVersionConnection, **kwargs)
            connections.append(db)
            return db

        with patch.object(tm.sqlite3, 'connect', side_effect=failing_connect):
            with self.assertRaisesRegex(sqlite3.DatabaseError, 'injected version failure'):
                tm._event_ledger_connect(self.path)
        for db in connections:
            with self.assertRaises(sqlite3.ProgrammingError):
                db.execute('SELECT 1')
        db = connect(self.path)
        try:
            self.assertEqual(db.execute('PRAGMA user_version').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT count(*) FROM tweet_anchors').fetchone()[0], 0)
            self.assertEqual(db.execute('PRAGMA index_info(event_observations_candidate_idx)').fetchall(), [])
        finally:
            db.close()
        self.assertEqual(tm.lookup_tweet_anchor('10', path=self.path), 42)

    def test_nonunique_candidate_index_is_repaired_without_losing_review(self):
        with tm._event_ledger_session(self.path) as db:
            db.execute('DROP INDEX event_observations_candidate_idx')
            db.execute('CREATE INDEX event_observations_candidate_idx ON event_observations '
                       '(target_chat_id,target_thread_id,event_key,candidate_tweet_id,decision)')
            insert = """INSERT INTO event_observations
                (observed_at,event_key,candidate_tweet_id,candidate_username,decision,reviewed,false_positive)
                VALUES ('now','event','10','u','would_suppress',?,?)"""
            db.execute(insert, (0, None))
            db.execute(insert, (1, 0))
        report = tm.event_dedup_gate_report(self.path)
        self.assertEqual((report['candidates'], report['reviewed']), (1, 1))
        with tm._event_ledger_session(self.path) as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute(insert, (0, None))


class VideoProvenancePerformanceTest(unittest.TestCase):
    def test_confirmed_video_and_gif_keep_content_without_repeating_head(self):
        for kind in ('video', 'animated_gif'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
                root = Path(tmp)
                tweet = {'id': '10', 'text': 'A detailed public release with a video demonstration.',
                         'createdAt': 'Tue May 12 00:20:00 +0000 2026',
                         'media': [{'type': kind, 'url': 'https://pbs.twimg.com/media/demo.jpg',
                                    'variants': [{'url': 'https://video.twimg.com/demo.mp4', 'bitrate': 1000}],
                                    'duration_ms': 1000}]}
                for name, value in {
                    'datetime': FixedDatetime, 'EVENT_LEDGER_PATH': str(root / 'ledger.sqlite3'),
                    'SENT_CONTENT_LEDGER_PATH': str(root / 'sent.jsonl'),
                    '_RICH_VIDEO_ENABLED': True, '_SENT_CONTENT_LEDGER_ENABLED': True,
                    '_SEMANTIC_BUNDLE_ENABLED': False, '_CROSS_DEDUP_ENABLED': False,
                    '_EVENT_DEDUP_EFFECTIVE_MODE': 'off', '_ACCOUNT_CONFIG_BY_USERNAME': {},
                    '_ARTICLE_QUEUE_RUN_START': None,
                }.items():
                    stack.enter_context(patch.object(tm, name, value))
                stack.enter_context(patch.object(tm, 'fetch_tweets', return_value=[tweet]))
                stack.enter_context(patch.object(tm, 'load_seen', return_value=({'old'}, None)))
                stack.enter_context(patch.object(tm, 'load_push_retry', return_value=set()))
                stack.enter_context(patch.object(tm, 'save_seen'))
                stack.enter_context(patch.object(tm, 'save_push_retry'))
                stack.enter_context(patch.object(tm.time, 'sleep'))
                stack.enter_context(patch.object(tm.urllib.request, 'urlopen', side_effect=AssertionError('network')))
                heads = stack.enter_context(patch.object(tm, '_head_content_length', return_value=1000))
                sends = stack.enter_context(patch.object(tm, '_tg_post',
                    return_value={'ok': True, 'result': {'message_id': 42}}))
                expected = tm._html_to_plain(tm.format_message('u', tweet, embed_video=False)[0])
                args = argparse.Namespace(test=False, seed=False, dry_run=False, limit=20,
                                          test_count=3, max_push_age_minutes=45)
                with redirect_stdout(io.StringIO()):
                    result = tm.process_user(None, FakeAI(False), 'u', 'fake', '-1001', args)
                self.assertEqual(result, (1, 1, 0, 0))
                self.assertEqual(heads.call_count, 1)
                self.assertEqual(sends.call_count, 1)
                row = json.loads((root / 'sent.jsonl').read_text())
                self.assertEqual(row['content'], expected)
                self.assertEqual(row['delivery_state'], 'confirmed')


if __name__ == '__main__':
    unittest.main()
