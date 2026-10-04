import copy
from contextlib import ExitStack
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import twitter_graphql as gql
import tools_x_review_media_fetch as media_fetch


def node(tid, *, quote=None, embedded=None):
    value = {"legacy": {"id_str": tid, "full_text": "text", "entities": {}},
             "core": {"user_results": {"result": {"legacy": {"screen_name": "author"}}}}}
    if quote:
        value["legacy"].update(is_quote_status=True, quoted_status_id_str=quote)
    if embedded:
        value["quoted_status_result"] = {"result": embedded}
    return value


class ReviewMediaFetchTests(unittest.TestCase):
    def collect(self, cached, targets, details, budget=1):
        requests = []
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            cache = Path(directory) / ".semantic_detail_cache.json"
            cache.write_text(json.dumps({"entries": {
                str(index): {"node": value} for index, value in enumerate(cached)}}))
            before = cache.read_bytes()
            stack.enter_context(patch.object(gql, "GUEST_TOKEN_CACHE", str(Path(directory) / "guest")))
            stack.enter_context(patch.object(gql, "_auth_headers", return_value={}))
            # collect replaces these globals; restore them after each offline run.
            for name in ("_curl", "_get_guest_token", "_gql_headers"):
                stack.enter_context(patch.object(gql, name, getattr(gql, name)))

            def curl(tid):
                requests.append(tid)
                return copy.deepcopy(details[tid])

            stack.enter_context(patch.object(gql, "_curl", side_effect=curl))
            stack.enter_context(patch.object(gql, "fetch_article_tweet",
                                             side_effect=lambda tid, **kw: gql._curl(tid)))
            result = media_fetch.collect(directory, targets, max_requests=budget)
            self.assertEqual(cache.read_bytes(), before)
        return result, requests

    def test_unrelated_cached_reference_does_not_consume_target_budget(self):
        result, requests = self.collect(
            [node("90000", quote="99999"), node("10000", quote="11111")],
            ["10000"], {"99999": node("99999"), "11111": node("11111")})
        self.assertEqual(requests, ["11111"])
        self.assertEqual(result["physical_requests"], 1)
        self.assertEqual(list(result["entries"]), ["10000"])
        bundle = result["entries"]["10000"]["bundle"]
        self.assertEqual(bundle["context_nodes"][0]["tweet_id"], "11111")

    def test_cached_target_reference_is_reused_and_its_missing_reference_fetched(self):
        result, requests = self.collect(
            [node("90000", quote="99999"), node("11111", quote="22222"),
             node("10000", quote="11111")],
            ["10000"], {"99999": node("99999"), "22222": node("22222")})
        self.assertEqual(requests, ["22222"])
        self.assertEqual(result["entries"]["10000"]["source"], "semantic_detail_cache")

    def test_embedded_target_nodes_remain_in_review_scope(self):
        result, requests = self.collect(
            [node("90000", quote="99999"),
             node("10000", quote="11111", embedded=node("11111", quote="22222"))],
            ["10000"], {"99999": node("99999"), "22222": node("22222")})
        self.assertEqual(requests, ["22222"])
        self.assertEqual(result["entries"]["10000"]["status"], "complete")

    def test_fetched_target_and_fetched_reference_expand_review_scope(self):
        result, requests = self.collect(
            [node("90000", quote="99999")], ["10000"],
            {"10000": node("10000", quote="11111"),
             "11111": node("11111", quote="22222"), "22222": node("22222"),
             "99999": node("99999")}, budget=3)
        self.assertEqual(requests, ["10000", "11111", "22222"])
        self.assertEqual(result["physical_requests"], 3)

    def test_embedded_repost_reference_remains_in_review_scope(self):
        target = node("10000")
        target["legacy"].update(full_text="RT @author: text",
                                retweeted_status_result={"result": node("11111", quote="22222")})
        result, requests = self.collect(
            [node("90000", quote="99999"), target], ["10000"],
            {"99999": node("99999"), "22222": node("22222")})
        self.assertEqual(requests, ["22222"])
        self.assertEqual(result["entries"]["10000"]["bundle"]["anchor"]["tweet_id"], "11111")

    def test_all_review_roots_are_included_and_duplicates_do_not_spend_requests(self):
        result, requests = self.collect(
            [node("90000", quote="99999"), node("10000", quote="11111"),
             node("20000", quote="22222")], ["10000", "20000", "10000"],
            {"99999": node("99999"), "11111": node("11111"), "22222": node("22222")},
            budget=2)
        self.assertEqual(requests, ["11111", "22222"])
        self.assertEqual(list(result["entries"]), ["10000", "20000"])


if __name__ == "__main__":
    unittest.main()
