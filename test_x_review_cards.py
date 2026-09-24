#!/usr/bin/env python3
from pathlib import Path
import json
import tempfile
import unittest
from unittest import mock
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import x_review_cards as cards


def fixture():
    sides = {
        "prior": {"tweet_id": "100001", "username": "OpenAI",
                  "url": "https://x.com/OpenAI/status/100001", "caption": "old < &"},
        "candidate": {"tweet_id": "100002", "username": "OpenAI",
                      "url": "https://x.com/OpenAI/status/100002", "caption": "new"},
    }
    packet = {"cases": [{"case_id": "R01", "gate_eligible": True,
                         "machine_decision": "would_suppress", **sides}]}
    receipt = {"chat_id": 42, "receipts": [{"case_id": "R01", "message_id": 77, "ok": True}]}
    return packet, receipt


class ReviewCardsTests(unittest.TestCase):
    def test_binding_preserves_case_and_message_identity(self):
        packet, receipt = fixture()
        self.assertEqual(cards.bindings(packet, receipt, 42), {"R01": 77})
        with self.assertRaises(ValueError):
            cards.bindings(packet, {**receipt, "chat_id": 43}, 42)
        with self.assertRaises(ValueError):
            cards.bindings(packet, {"chat_id": 42, "receipts": [
                {"case_id": "R02", "message_id": 78, "ok": True}]}, 42)

    def test_keyboard_has_original_links_and_stable_callbacks(self):
        packet, _ = fixture()
        keyboard = cards.keyboard(packet["cases"][0], "merge")
        self.assertEqual(keyboard["inline_keyboard"][0][0]["url"],
                         "https://x.com/OpenAI/status/100001")
        self.assertEqual(keyboard["inline_keyboard"][1][1]["text"], "✓ 可以合并")
        self.assertEqual(keyboard["inline_keyboard"][1][1]["callback_data"],
                         "xreview:R01:merge")

    def test_media_renderer_filters_untrusted_hosts_and_restores_photo_video(self):
        rendered = cards.x_media_block([
            {"type": "photo", "url": "https://pbs.twimg.com/media/a.jpg"},
            {"type": "video", "url": "https://video.twimg.com/a.mp4",
             "video_url": "https://video.twimg.com/a.mp4",
             "variants": [{"url": "https://video.twimg.com/a.mp4", "bitrate": 1000}]},
            {"type": "photo", "url": "https://evil.example/a.jpg"},
        ])
        self.assertIn("<tg-collage>", rendered)
        self.assertIn("<img src=\"https://pbs.twimg.com/media/a.jpg\"/>", rendered)
        self.assertIn("<video src=\"https://video.twimg.com/a.mp4\"/>", rendered)
        self.assertNotIn("evil.example", rendered)

    def test_render_card_uses_snapshot_media_and_escapes_text(self):
        packet, receipt = fixture()
        media = {"entries": {
            "100001": {"bundle": {"observation": {"outer_id": "100001"},
                                  "anchor": {"tweet_id": "100001", "author": "OpenAI",
                                             "text": "old < &",
                                             "media": [{"type": "photo",
                                                        "url": "https://pbs.twimg.com/media/a.jpg"}]}}},
            "100002": {"bundle": {"observation": {"outer_id": "100002"},
                                  "anchor": {"tweet_id": "100002", "author": "OpenAI",
                                             "text": "new",
                                             "media": [{"type": "video",
                                                        "url": "https://video.twimg.com/v.jpg",
                                                        "video_url": "https://video.twimg.com/v.mp4",
                                                        "variants": [{"url": "https://video.twimg.com/v.mp4",
                                                                      "bitrate": 1000}]}]}}},
        }}
        payloads = cards.build_payloads(packet, receipt, media, 42)
        rich = payloads[0]["payload"]["rich_message"]
        self.assertIn("&lt; &amp;", rich["html"])
        self.assertIn("pbs.twimg.com", rich["html"])
        self.assertIn("video.twimg.com", rich["html"])
        self.assertEqual(payloads[0]["payload"]["message_id"], 77)
        self.assertEqual(payloads[0]["payload"]["reply_markup"]["inline_keyboard"][-1][0]["callback_data"],
                         "xreview:R01:keep")
        self.assertNotIn("token", json.dumps(payloads))

    def test_missing_snapshot_retains_text_and_marks_media_unavailable(self):
        packet, receipt = fixture()
        value = cards.build_payloads(packet, receipt, {"entries": {}}, 42)[0]
        self.assertIn("文本快照", value["payload"]["rich_message"]["html"])
        self.assertNotIn("rich_message.media", value["payload"])

    def test_read_selections_is_read_only(self):
        packet, receipt = fixture()
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "review.sqlite3"
            import sqlite3, hashlib
            db = sqlite3.connect(state)
            db.executescript("""CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE callbacks(seq INTEGER PRIMARY KEY,case_id TEXT,verdict TEXT,accepted INTEGER);""")
            mapping = cards.bindings(packet, receipt, 42)
            digest = hashlib.sha256(cards.json_text([packet, mapping]).encode()).hexdigest()
            db.executemany("INSERT INTO meta VALUES (?,?)",
                           [("owner", "42"), ("packet_fingerprint", digest)])
            db.execute("INSERT INTO callbacks VALUES (1,'R01','keep',1)")
            db.commit(); db.close()
            before = state.stat().st_mtime_ns
            self.assertEqual(cards.read_selections(state, packet, mapping, 42), {"R01": "keep"})
            self.assertEqual(state.stat().st_mtime_ns, before)

    def test_quote_media_and_long_text_are_preserved(self):
        packet, _ = fixture()
        anchor = {"tweet_id": "100001", "text": "outer", "author": "OpenAI"}
        quote = {"tweet_id": "100003", "author": "Quoted", "text": "q" * 2200,
                 "media": [{"type": "photo", "url": "https://pbs.twimg.com/media/q.jpg"}]}
        entry = {"bundle": {"observation": {"outer_id": "100001"}, "anchor": anchor,
                            "context_nodes": [quote]}}
        rich = cards.render_card(packet["cases"][0], {"entries": {"100001": entry}}, total=1)
        self.assertIn("q" * 2200, rich["html"])
        self.assertIn("引用 · @Quoted", rich["html"])
        self.assertIn("https://pbs.twimg.com/media/q.jpg", rich["html"])

    def test_file_id_reuse_keeps_media_owner(self):
        attachments = []
        node = {"tweet_id": "100001", "media": []}
        refs = [{"owner_tweet_id": "100001", "type": "photo", "file_id": "same-bot-file"},
                {"owner_tweet_id": "100002", "type": "video", "file_id": "other-tweet"}]
        value = cards.node_media(node, refs, attachments)
        self.assertIn("tg://photo?id=review_media_0", value)
        self.assertEqual(attachments, [{"id": "review_media_0",
                                       "media": {"type": "photo", "media": "same-bot-file"}}])

    def test_cross_tweet_snapshot_rejected(self):
        packet, _ = fixture()
        with self.assertRaisesRegex(ValueError, "identity"):
            cards.render_card(packet["cases"][0], {"entries": {
                "100001": {"bundle": {"observation": {"outer_id": "100099"},
                                      "anchor": {"tweet_id": "100099", "text": "wrong"}}}}}, total=1)

    def test_media_render_restores_monitor_globals_after_failure(self):
        prior = cards.monitor._RICH_VIDEO_ENABLED
        head = cards.monitor._head_content_length
        with mock.patch.object(cards.monitor, "_rich_media_block", side_effect=ValueError):
            with self.assertRaises(ValueError):
                cards.x_media_block([])
        self.assertEqual(cards.monitor._RICH_VIDEO_ENABLED, prior)
        self.assertIs(cards.monitor._head_content_length, head)

    def test_preview_makes_no_network_head_calls(self):
        with mock.patch.object(cards.monitor, "_head_content_length", side_effect=AssertionError):
            cards.x_media_block([{"type": "video", "url": "https://pbs.twimg.com/v.jpg",
                                  "video_url": "https://video.twimg.com/v.mp4"}])

    def test_progress_excludes_historical_and_uncertain_gate_labels(self):
        packet, _ = fixture()
        packet["cases"] += [{"case_id": "R02", "gate_eligible": False},
                            {"case_id": "R03", "gate_eligible": True}]
        self.assertEqual(cards.progress(packet, {"R01": "uncertain", "R02": "merge", "R03": "keep"}),
                         "已选择 3/3；门槛有效标注 1/2")

    def test_long_note_and_entity_link_are_used(self):
        node = {"text": "short", "note": {"text": "full https://t.co/link", "entities": {
            "urls": [{"url": "https://t.co/link", "expanded_url": "https://example.com/doc",
                      "display_url": "example.com/doc"}]}}}
        rendered = cards.node_text(node)
        self.assertIn('href="https://example.com/doc"', rendered)
        self.assertNotIn("short", rendered)

    def test_oversize_card_fails_without_silent_truncation(self):
        packet, _ = fixture()
        packet["cases"][0]["prior"]["caption"] = "a" * 31000
        with self.assertRaisesRegex(ValueError, "budget"):
            cards.render_card(packet["cases"][0], {"entries": {}}, total=1)


if __name__ == "__main__":
    unittest.main()

