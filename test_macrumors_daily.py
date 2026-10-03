import contextlib
import io
import json
import unittest
from unittest.mock import patch

import macrumors_daily as md


class MergeJsonRegressionTests(unittest.TestCase):
    def test_mixed_array_skips_scalars_and_preserves_full_coverage(self):
        quality = {}
        groups = md.parse_merge_json(json.dumps([
            {"indices": [0, 2], "zh_title": "同一事件"},
            7,
            None,
            "noise",
            [1],
            {},
            {"indices": [1], "zh_title": "另一事件"},
        ]), 4, quality)

        self.assertEqual([g["indices"] for g in groups], [[0, 2], [1], [3]])
        self.assertEqual(quality["invalid_elements"], 5)
        self.assertEqual(quality["missing_indices"], 1)
        self.assertEqual(quality["status"], "degraded")

    def test_merge_logs_degradation_without_dropping_items(self):
        class AI:
            @staticmethod
            def complete(_prompt, max_tokens=4000):
                return ('[{"indices":[0]}, null, {"indices":[1]}, '
                        '{"indices":[2]}, {"indices":[3]}]', "test")

        items = [{"zh_title": f"title-{i}"} for i in range(4)]
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            result = md.merge_similar(AI(), items)

        self.assertEqual(result, items)
        metric = json.loads(stderr.getvalue().strip().split(" ", 2)[2])
        self.assertEqual(metric["status"], "degraded")
        self.assertEqual(metric["invalid_elements"], 1)


class TranslationQualityTests(unittest.TestCase):
    def test_parse_mixed_array_reports_missing_and_invalid_entries(self):
        quality = {}
        result = md.parse_ai_json(json.dumps([
            {"i": 0, "zh_title": "甲", "zh_summary": "摘要"},
            None,
            "noise",
            {"i": True, "zh_title": "坏编号"},
            {"i": 9, "zh_title": "越界"},
        ]), expected=2, quality=quality)

        self.assertEqual(result, {0: ("甲", "摘要")})
        self.assertEqual(quality["invalid_elements"], 2)
        self.assertEqual(quality["invalid_indices"], 2)
        self.assertEqual(quality["missing_indices"], 1)
        self.assertEqual(quality["status"], "degraded")

    @patch.object(md.time, "sleep", return_value=None)
    def test_translate_keeps_best_partial_retry_and_emits_metrics(self, _sleep):
        class AI:
            responses = iter([
                ('[{"i":0,"zh_title":"译题","zh_summary":"译摘"}]', "test"),
                ("not json", "test"),
                ("[]", "test"),
            ])

            @classmethod
            def complete(cls, _prompt, max_tokens=2500):
                return next(cls.responses)

        items = [{"title": "A", "desc": "a"}, {"title": "B", "desc": "b"}]
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            quality = md.translate(AI(), items, batch=2)

        self.assertEqual(items[0]["zh_title"], "译题")
        self.assertEqual(items[1]["zh_title"], "B")
        self.assertEqual(quality["translated_titles"], 1)
        self.assertEqual(quality["fallback_titles"], 1)
        lines = stderr.getvalue().strip().splitlines()
        self.assertEqual(len(lines), 3)
        self.assertTrue(all(line.startswith(
            "MACRUMORS_QUALITY translation_parse ") for line in lines))


if __name__ == "__main__":
    unittest.main()
