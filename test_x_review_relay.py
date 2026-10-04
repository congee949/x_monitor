import copy
import json
from types import SimpleNamespace
import unittest
from unittest import mock
import x_review_relay as relay
from test_x_review_bot import ReviewTest


class RelayInputTest(unittest.TestCase):
    def test_source_command_quoted_and_newer_than_cursor(self):
        def run(argv, **kwargs):
            self.assertEqual(argv[-2], 'r4s')
            self.assertIn("'/root/path with space.py'", argv[-1])
            self.assertIn('--after 5', argv[-1])
            return SimpleNamespace(stdout='[{"update_id":6}]')
        self.assertEqual(relay.fetch_updates('r4s', '/root/path with space.py', '/state.db', 42, 5, run),
                         [{'update_id': 6}])

    def test_bad_host_and_unordered_batch_rejected(self):
        with self.assertRaises(ValueError):
            relay.fetch_updates('-oProxyCommand=bad', '/a.py', '/s.db', 42, 0)
        for rows in [[{'update_id': 6}, {'update_id': 5}], [{'update_id': 5}], [{}]]:
            with self.assertRaises(ValueError):
                relay.fetch_updates('r4s', '/a.py', '/s.db', 42, 5,
                    lambda *a, **k: SimpleNamespace(stdout=json.dumps(rows)))


class RelayStoreTest(ReviewTest):
    def test_ingest_never_polls_bot_api(self):
        relay.ingest(self.bot, [self.update()])
        self.assertEqual(self.gate(), (1, 1))
        self.assertEqual(self.store.offset(), 2)
        self.assertNotIn('getUpdates', [name for name, _ in self.api.calls])
        relay.ingest(self.bot, [self.update()])
        self.assertEqual(len(self.audit()), 1)

    def test_changed_payload_is_rejected_before_other_writes(self):
        relay.ingest(self.bot, [self.update()])
        changed = self.update(verdict='merge')
        with self.assertRaises(ValueError):
            relay.ingest(self.bot, [self.update(2), changed])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM updates').fetchone()[0], 1)

    def test_entire_batch_persisted_before_render_failure(self):
        self.api.fail_method = 'editMessageText'
        with self.assertRaises(Exception):
            relay.ingest(self.bot, [self.update(), self.update(2, 'merge')])
        self.assertEqual(self.store.offset(), 0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM updates').fetchone()[0], 2)
        self.api.fail_method = None
        relay.ingest(self.bot, [])
        self.assertEqual(self.store.offset(), 3)
        self.assertEqual(self.gate(), (1, 0))


if __name__ == '__main__':
    unittest.main()
