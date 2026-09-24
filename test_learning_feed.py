import json
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

import learning_feed


def prose(words=900, keyword="medical evidence"):
    paragraph = (
        f"This {keyword} argument examines a difficult public question. "
        "However, the evidence suggests that the common assumption is incomplete. "
        "Researchers argue that policy should distinguish correlation from causation. "
        "Therefore, the conclusion depends on how the study was designed. "
    )
    return "\n\n".join([paragraph] * max(1, words // len(paragraph.split())))


class LearningFeedTests(unittest.TestCase):
    def test_eligible_english_longform(self):
        result = learning_feed.evaluate_article("A medical ethics debate", prose())
        self.assertTrue(result["eligible"])
        self.assertGreaterEqual(result["score"], learning_feed.MIN_SCORE)

    def test_rejects_short_non_english_and_promo(self):
        self.assertEqual(learning_feed.evaluate_article("Short", "hello world")["reason"], "too_short")
        chinese = ("这是一篇关于医学伦理和社会政策的长文。" * 800) + (" evidence" * 500)
        self.assertEqual(learning_feed.evaluate_article("医学文章", chinese)["reason"], "not_english_dominant")
        promo = prose(keyword="coupon giveaway")
        self.assertEqual(learning_feed.evaluate_article("Limited-time offer", promo)["reason"], "promotional")

    def test_publish_is_idempotent_and_rss_is_valid(self):
        entry = {
            "article_id": "123",
            "tweet_id": "456",
            "author": "researcher",
            "article_title": "Evidence and public health policy",
            "sent_at": "2026-08-06T00:00:00+00:00",
        }
        with tempfile.TemporaryDirectory() as tmp:
            kwargs = {"output_dir": tmp, "base_url": "https://example.test/secret"}
            first = learning_feed.publish_article("monitor", entry, prose(), **kwargs)
            second = learning_feed.publish_article("monitor", entry, prose(1200), **kwargs)
            self.assertTrue(first["published"])
            self.assertTrue(second["published"])
            items = json.loads((Path(tmp) / "articles.json").read_text())
            self.assertEqual(len(items), 1)
            self.assertTrue((Path(tmp) / "articles" / "123.html").exists())
            root = ET.parse(Path(tmp) / "feed.xml").getroot()
            self.assertEqual(root.findtext("./channel/item/guid"), "x-article:123")

    def test_empty_feed_is_subscribable(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = learning_feed.ensure_feed(
                output_dir=tmp,
                base_url="https://example.test/secret",
            )
            self.assertEqual(result, {"ready": True, "item_count": 0})
            self.assertEqual(json.loads((Path(tmp) / "articles.json").read_text()), [])
            root = ET.parse(Path(tmp) / "feed.xml").getroot()
            self.assertEqual(root.findtext("./channel/title"), "BWG X Articles · English Learning")
            self.assertIsNone(root.find("./channel/item"))

    def test_disabled_without_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp) / "config.json"
            cfg.write_text(json.dumps({"learning_feed_enabled": False}))
            result = learning_feed.publish_article("u", {"article_id": "1"}, prose(), config_path=cfg)
            self.assertEqual(result["reason"], "disabled")


if __name__ == "__main__":
    unittest.main()
