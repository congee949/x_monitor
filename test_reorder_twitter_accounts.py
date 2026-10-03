import json
import tempfile
import unittest
from pathlib import Path

import reorder_twitter_accounts as reorder


class ReorderTests(unittest.TestCase):
    def records(self):
        return [
            {"username": "vista8", "enabled": True, "topic": "ai_cn"},
            {"username": "ClaudeDevs", "enabled": True, "token_ref": "keep"},
            {"username": "other", "enabled": False, "extra": {"x": 1}},
            {"username": "claudeai", "enabled": True, "topic": "official"},
            {"username": "OpenAIDevs", "enabled": True},
            {"username": "OpenAI", "enabled": True},
            {"username": "tail", "enabled": True},
        ]

    def test_only_target_slots_are_reordered_and_objects_are_unchanged(self):
        before = self.records()
        after, changed = reorder.reorder(before)
        self.assertEqual([x["username"] for x in after],
                         ["vista8", "claudeai", "other", "ClaudeDevs",
                          "OpenAI", "OpenAIDevs", "tail"])
        self.assertEqual(after[2], before[2])
        self.assertEqual(after[0], before[0])
        self.assertEqual(changed, ["claudeai -> ClaudeDevs", "OpenAI -> OpenAIDevs"])

    def test_disabled_accounts_and_all_fields_are_preserved(self):
        before = self.records()
        after, _ = reorder.reorder(before)
        self.assertEqual(sorted(after, key=lambda x: x["username"]),
                         sorted(before, key=lambda x: x["username"]))
        by_name = {item["username"]: item for item in after}
        self.assertEqual(by_name["claudeai"]["topic"], "official")
        self.assertEqual(by_name["other"]["extra"], {"x": 1})

    def test_write_can_preview_separate_output_without_replacing_source(self):
        before = self.records()
        with tempfile.TemporaryDirectory() as directory:
            src = Path(directory) / "twitter_accounts.json"
            out = Path(directory) / "preview.json"
            src.write_text(json.dumps(before, ensure_ascii=False, indent=2) + "\n")
            updated, _ = reorder.reorder(before)
            reorder.write_json(out, updated)
            self.assertEqual(json.loads(out.read_text())[1]["username"], "claudeai")
            self.assertEqual(json.loads(src.read_text())[1]["username"], "ClaudeDevs")


if __name__ == "__main__":
    unittest.main()
