"""Twitter GraphQL data source - uses guest token, no API key needed.

Replaces ai.6551.io, completely free.
Guest token from api.x.com/1.1/guest/activate.json,
then standard bearer token for GraphQL API calls.
"""

import json
import os
import re
import subprocess
import time
from urllib.parse import quote

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GUEST_TOKEN_CACHE = os.path.join(SCRIPT_DIR, ".guest_token_cache.json")
AUTH_COOKIE_CACHE = os.path.join(SCRIPT_DIR, ".auth_cookies.json")
USER_ID_CACHE = os.path.join(SCRIPT_DIR, ".user_id_cache.json")

# Twitter web app bearer token (public, embedded in JS)
BEARER = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs=1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"

# GraphQL query IDs (extracted from x.com JS, may change over time)
QUERY_USER_BY_SCREEN_NAME = "xmU6X_CKVnQ5lSrCbAmJsg"
QUERY_USER_TWEETS = "E3opETHurmVJflFsUBVuUQ"
QUERY_TWEET_BY_REST_ID = "SgZWKwvBiOKrSC0QeOGvXw"

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Guest token TTL: ~3 hours
GUEST_TOKEN_TTL = 10000

# Auth mode: "cookie" (auth_token+ct0) or "guest" (fallback)
AUTH_MODE = "cookie"

# Consecutive cookie-auth failures before the stale cookie file is renamed.
_COOKIE_FAILURE_THRESHOLD = 3
_cookie_fail_count = 0


class CurlError(Exception):
    """curl subprocess failed or returned a non-2xx HTTP status."""


def _curl(url, headers=None, method="GET", timeout=15):
    """Use curl to avoid Python httpx TLS fingerprint issues.

    Raises:
        CurlError: on curl non-zero exit or HTTP status >= 300.
    """
    headers = headers or {}
    write_format = "\n%{http_code}\n%{exitcode}"
    cmd = [
        "curl", "-s", "--connect-timeout", "5",
        "--max-time", str(timeout), "-w", write_format,
    ]
    if method == "POST":
        cmd += ["-X", "POST"]
    for k, v in headers.items():
        cmd += ["-H", f"{k}: {v}"]
    cmd.append(url)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 5)
    except FileNotFoundError:
        print("  [GraphQL] curl 未安装或不在 PATH")
        return ""
    except subprocess.TimeoutExpired:
        print(f"  [GraphQL] curl 超时 ({timeout}s): {url[:80]}")
        return ""

    stdout = result.stdout
    if stdout.endswith("\n"):
        stdout = stdout[:-1]
    parts = stdout.rsplit("\n", 2)
    if len(parts) >= 3:
        body = "\n".join(parts[:-2])
        http_code_str = parts[-2]
        exitcode_str = parts[-1]
    else:
        body = stdout
        http_code_str = "0"
        exitcode_str = "0"

    try:
        curl_exit = int(exitcode_str)
    except ValueError:
        curl_exit = -1

    try:
        http_code = int(http_code_str)
    except ValueError:
        http_code = 0

    if curl_exit != 0:
        stderr = result.stderr.strip() if result.stderr else ""
        raise CurlError(f"curl failed with exit code {curl_exit}: {stderr}")

    if http_code < 200 or http_code >= 300:
        stderr = result.stderr.strip() if result.stderr else ""
        raise CurlError(f"HTTP {http_code}: {stderr}")

    return body


def _gql_query_id_stale(errors):
    """Heuristic: GraphQL errors that often mean hardcoded queryId drift."""
    joined = " ".join(e.get("message", "") for e in (errors or [])).lower()
    return any(k in joined for k in (
        "unauthorized", "queryid", "query id", "persistedquery",
        "bad request", "graphql validation", "not found for query",
        "could not authenticate", "authorization",
    ))


def _account_gone(errors):
    """Return True if GraphQL errors indicate the cached rest_id is stale.

    Matches both human-readable messages and structured error codes.
    """
    msgs = " ".join(e.get("message", "") for e in (errors or [])).lower()
    if any(kw in msgs for kw in ("suspended", "not found", "deactivated", "unavailable")):
        return True
    for e in errors or []:
        code = e.get("code")
        if code in (50, "50", "NonExistent", "NotFound"):
            return True
    return False


def _get_guest_token():
    """Get guest token with local cache."""
    if os.path.exists(GUEST_TOKEN_CACHE):
        try:
            with open(GUEST_TOKEN_CACHE) as f:
                cache = json.load(f)
            if cache.get("token") and time.time() - cache.get("ts", 0) < GUEST_TOKEN_TTL:
                return cache["token"]
        except Exception:
            pass

    resp = _curl(
        "https://api.x.com/1.1/guest/activate.json",
        {"Authorization": f"Bearer {BEARER}"},
        method="POST",
    )
    try:
        data = json.loads(resp)
        token = data.get("guest_token")
        if token:
            tmp = GUEST_TOKEN_CACHE + ".tmp"
            with open(tmp, "w") as f:
                json.dump({"token": token, "ts": time.time()}, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, GUEST_TOKEN_CACHE)
            return token
    except Exception:
        pass
    return None


def _load_auth_cookies():
    """Load auth_token and ct0 from cache file."""
    if os.path.exists(AUTH_COOKIE_CACHE):
        try:
            with open(AUTH_COOKIE_CACHE) as f:
                data = json.load(f)
            if data.get("auth_token") and data.get("ct0"):
                return data
        except Exception:
            pass
    return None


def _clear_auth_cookies():
    """Rename stale auth cookies out of the way so we stop wasting authed calls."""
    try:
        if os.path.exists(AUTH_COOKIE_CACHE):
            backup = AUTH_COOKIE_CACHE + ".stale"
            if os.path.exists(backup):
                os.remove(backup)
            os.replace(AUTH_COOKIE_CACHE, backup)
            print(f"  [GraphQL] cookie 连续失败 {_COOKIE_FAILURE_THRESHOLD} 次，已重命名 {AUTH_COOKIE_CACHE}")
    except Exception:
        pass


def _save_auth_cookies(auth_token, ct0):
    """Save auth cookies to cache atomically.

    Currently a manual-maintenance hook (no auto-refresh caller).  Atomic
    tmp+replace write prevents half-written JSON on crash.
    """
    try:
        tmp = AUTH_COOKIE_CACHE + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"auth_token": auth_token, "ct0": ct0}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, AUTH_COOKIE_CACHE)
    except Exception:
        pass


def _load_user_id_cache():
    """Load the screen_name(lower) -> rest_id cache.

    An unreadable/corrupted cache file is treated as an empty cache so the
    cache layer can never break the caller.
    """
    if os.path.exists(USER_ID_CACHE):
        try:
            with open(USER_ID_CACHE) as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {}


def _save_user_id_cache(cache):
    """Save the user id cache atomically (tmp file + os.replace + fsync).

    A crash mid-write can never leave half-written JSON behind; write
    failures are non-fatal (next run simply re-resolves).
    """
    try:
        tmp = USER_ID_CACHE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(cache, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, USER_ID_CACHE)
    except Exception:
        pass


def invalidate_user_id(screen_name):
    """Drop the cached rest_id for screen_name so the next run re-resolves it
    via UserByScreenName (used when the cached id turns out to be stale)."""
    cache = _load_user_id_cache()
    if cache.pop(screen_name.lower(), None) is not None:
        _save_user_id_cache(cache)


def _gql_headers(guest_token):
    return {
        "Authorization": f"Bearer {BEARER}",
        "x-guest-token": guest_token,
        "User-Agent": USER_AGENT,
    }


def _auth_headers():
    """Headers with authenticated cookies (auth_token + ct0)."""
    cookies = _load_auth_cookies()
    if not cookies:
        return None
    return {
        "Authorization": f"Bearer {BEARER}",
        "Cookie": f"auth_token={cookies['auth_token']}; ct0={cookies['ct0']}",
        "User-Agent": USER_AGENT,
        "X-Twitter-Active-User": "yes",
        "X-Twitter-Client-Language": "en",
        "x-csrf-token": cookies["ct0"],
    }


def get_user_id(screen_name):
    """Get user ID by screen name, with persistent local cache.

    rest_id never changes for a given account, so resolving each screen name
    once and caching it on disk skips the UserByScreenName call on every
    later run (halves the GraphQL calls of the 30-min monitor loop).
    """
    cache = _load_user_id_cache()
    uid = cache.get(screen_name.lower())
    if uid:
        return uid

    # Try authenticated first
    ah = _auth_headers()
    # Build variables unconditionally: the guest fallback (ah == {}) must not hit a
    # NameError on `variables` used in the URL below.
    variables = json.dumps({"screen_name": screen_name, "withSafetyModeUserFields": True})
    features = json.dumps({
        "hidden_profile_subscriptions_enabled": True,
        "rweb_tipjar_consumption_enabled": True,
        "responsive_web_graphql_exclude_directive_enabled": True,
        "verified_phone_label_enabled": False,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
        "responsive_web_graphql_timeline_navigation_enabled": True,
    })
    url = "https://x.com/i/api/graphql/{}/UserByScreenName?variables={}&features={}".format(
        QUERY_USER_BY_SCREEN_NAME, quote(variables), quote(features)
    )

    if ah:
        resp = _curl(url, ah)
        try:
            data = json.loads(resp)
            uid = data.get("data", {}).get("user", {}).get("result", {}).get("rest_id")
            if uid:
                cache[screen_name.lower()] = uid
                _save_user_id_cache(cache)
                return uid
            errors = data.get("errors")
            if errors and _gql_query_id_stale(errors) and refresh_query_ids():
                url = "https://x.com/i/api/graphql/{}/UserByScreenName?variables={}&features={}".format(
                    QUERY_USER_BY_SCREEN_NAME, quote(variables), quote(features))
                resp = _curl(url, ah)
                try:
                    data = json.loads(resp)
                    uid = data.get("data", {}).get("user", {}).get("result", {}).get("rest_id")
                    if uid:
                        cache[screen_name.lower()] = uid
                        _save_user_id_cache(cache)
                        return uid
                except Exception:
                    pass
        except Exception:
            pass

    # Fallback to guest token
    gt = _get_guest_token()
    if not gt:
        return None
    resp = _curl(url, _gql_headers(gt))
    try:
        data = json.loads(resp)
        uid = data.get("data", {}).get("user", {}).get("result", {}).get("rest_id")
    except Exception:
        return None
    # Only cache successful resolutions; never write None/empty into the cache.
    if uid:
        cache[screen_name.lower()] = uid
        _save_user_id_cache(cache)
    return uid


def _build_media(raw_media: list) -> list:
    """把 GraphQL 的 media 原始节点转成便携 list：photo 用缩略图 url，video/gif 附最佳 mp4。

    同时用于推文本体与转推原推（转推媒体在 rt_result.legacy.extended_entities，壳里没有）。
    """
    media = []
    for m in raw_media or []:
        if not isinstance(m, dict):
            continue
        item = {
            "type": m.get("type"),  # photo / video / animated_gif
            "url": m.get("media_url_https") or m.get("media_url"),
            "width": None,
            "height": None,
            "duration_ms": None,
            "bitrate": None,
            "video_url": None,
        }
        orig = m.get("original_info") or {}
        if orig.get("width"):
            item["width"] = orig.get("width")
            item["height"] = orig.get("height")
        else:
            large = (m.get("sizes") or {}).get("large") or {}
            item["width"] = large.get("w")
            item["height"] = large.get("h")
        if item["type"] in ("video", "animated_gif"):
            vi = m.get("video_info") or {}
            item["duration_ms"] = vi.get("duration_millis")
            variants = vi.get("variants") or []
            mp4s = [v for v in variants
                    if v.get("content_type") == "video/mp4" and v.get("url")]
            if mp4s:
                best = max(mp4s, key=lambda v: v.get("bitrate") or 0)
                item["video_url"] = best.get("url")
                item["bitrate"] = best.get("bitrate")
                # 全部 mp4 档（按 bitrate 降序）：外链嵌入有 20MB 上限，消费方
                # 需要「能塞进上限的最大档」而不是恒选 best（bitrate 是峰值声明）。
                item["variants"] = sorted(
                    ({"url": v["url"], "bitrate": v.get("bitrate") or 0} for v in mp4s),
                    key=lambda v: v["bitrate"], reverse=True)
        media.append(item)
    return media


def fetch_tweets(username, limit=20):
    """Fetch user tweets, return normalized format list.

    Return format compatible with original 6551.io API (additive fields added):
    - id: str (tweet ID)
    - text: str (full text)
    - created_at: str (timestamp)
    - entities: dict (contains urls + basic media)
    - extended_entities: dict (raw, for full video variants)
    - media: list[dict]  # convenient processed list:
        each item: {
          "type": "photo"|"video"|"animated_gif",
          "url": str,           # photo or fallback
          "video_url": str|None,# best mp4 for video/gif
          "variants": list,     # all mp4s [{url, bitrate}] sorted desc (video/gif only)
          "width", "height",
          "duration_ms", "bitrate"
        }
    - note_tweet: dict (longform content, if present)
    - article: dict (Twitter Article, if present)
    - user: dict (user info)
    """
    # Try authenticated first (gets latest tweets + articles)
    ah = _auth_headers()
    use_auth = ah is not None

    if not use_auth:
        gt = _get_guest_token()
        if not gt:
            raise RuntimeError("Cannot get guest token")

    uid = get_user_id(username)
    if not uid:
        raise RuntimeError("Cannot find user @" + username)

    variables = json.dumps({
        "userId": uid,
        "count": min(limit, 40),
        "includePromotedContent": False,
        "withQuickPromoteEligibilityTweetFields": True,
        "withVoice": True,
        "withV2Timeline": True,
    })
    features = json.dumps({
        "rweb_tipjar_consumption_enabled": True,
        "responsive_web_graphql_exclude_directive_enabled": True,
        "verified_phone_label_enabled": False,
        "creator_subscriptions_tweet_preview_api_enabled": True,
        "responsive_web_graphql_timeline_navigation_enabled": True,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
        "communities_web_enable_tweet_community_results_featuring": True,
        "c9s_tweet_anatomy_moderator_badge_enabled": True,
        "articles_preview_enabled": True,
        "responsive_web_edit_tweet_api_enabled": True,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
        "view_counts_everywhere_api_enabled": True,
        "longform_notetweets_consumption_enabled": True,
        "responsive_web_twitter_article_tweet_consumption_enabled": True,
        "tweet_awards_web_tipping_enabled": False,
        "creator_subscriptions_quote_tweet_preview_enabled": False,
        "freedom_of_speech_not_reach_fetch_enabled": True,
        "standardized_nudges_misinfo": True,
        "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
        "rweb_video_timestamps_enabled": True,
        "longform_notetweets_rich_text_read_enabled": True,
        "longform_notetweets_inline_media_enabled": True,
        "responsive_web_enhance_cards_enabled": False,
    })
    url = "https://x.com/i/api/graphql/{}/UserTweets?variables={}&features={}".format(
        QUERY_USER_TWEETS, quote(variables), quote(features)
    )

    headers = ah if use_auth else _gql_headers(gt)

    def _do_request(req_headers):
        resp = _curl(url, req_headers, timeout=20)
        try:
            return json.loads(resp)
        except json.JSONDecodeError:
            raise RuntimeError("GraphQL response not JSON: " + resp[:200])

    def _retry_with_fresh_query(req_headers, label, current_errors):
        nonlocal url
        if _gql_query_id_stale(current_errors) and refresh_query_ids():
            print(f"  [GraphQL] query ID 可能过期，已从 x.com JS 刷新并重试 ({label})")
            url = "https://x.com/i/api/graphql/{}/UserTweets?variables={}&features={}".format(
                QUERY_USER_TWEETS, quote(variables), quote(features))
            data2 = _do_request(req_headers)
            if data2.get("errors"):
                raise RuntimeError(f"GraphQL errors ({label} after query refresh): " + str(data2["errors"]))
            return data2
        return None

    data = _do_request(headers)
    errors = data.get("errors")
    used_auth_for_success = use_auth

    if errors:
        global _cookie_fail_count
        error_msgs = [e.get("message", "") for e in errors]
        if _account_gone(errors):
            invalidate_user_id(username)

        if use_auth:
            used_auth_for_success = False
            print(f"  [GraphQL] cookie 认证失效，降级 guest 模式: {error_msgs}")
            _cookie_fail_count += 1
            if _cookie_fail_count >= _COOKIE_FAILURE_THRESHOLD:
                _clear_auth_cookies()
                _cookie_fail_count = 0
            if os.path.exists(GUEST_TOKEN_CACHE):
                os.remove(GUEST_TOKEN_CACHE)
            gt2 = _get_guest_token()
            if not gt2:
                raise RuntimeError("GraphQL auth failed and cannot get guest token: " + str(errors))
            data = _do_request(_gql_headers(gt2))
            errors = data.get("errors")
            if errors:
                refreshed = _retry_with_fresh_query(_gql_headers(gt2), "auth+guest fallback", errors)
                if refreshed is not None:
                    data = refreshed
                else:
                    raise RuntimeError("GraphQL errors (auth+guest fallback): " + str(errors))
        else:
            refreshed = _retry_with_fresh_query(headers, "guest", errors)
            if refreshed is not None:
                data = refreshed
            elif any("internal server error" in m.lower() for m in error_msgs):
                if os.path.exists(GUEST_TOKEN_CACHE):
                    os.remove(GUEST_TOKEN_CACHE)
                gt2 = _get_guest_token()
                if gt2:
                    data = _do_request(_gql_headers(gt2))
                    if data.get("errors"):
                        raise RuntimeError("GraphQL errors (after retry): " + str(data["errors"]))
                else:
                    raise RuntimeError("GraphQL errors: " + str(errors))
            else:
                raise RuntimeError("GraphQL errors: " + str(errors))

    # Reset cookie failure counter only when the authed request itself succeeds.
    if used_auth_for_success:
        _cookie_fail_count = 0

    # No errors but an empty user node: typical signature of a deleted account
    # when fetching by a (possibly cached) rest_id. Invalidate the cache so the
    # next run re-resolves the screen name; this run still returns [] as before.
    if not data.get("data", {}).get("user", {}).get("result"):
        invalidate_user_id(username)

    instructions = (
        data.get("data", {})
        .get("user", {})
        .get("result", {})
        .get("timeline_v2", {})
        .get("timeline", {})
        .get("instructions", [])
    )

    tweets = []
    for inst in instructions:
        for entry in inst.get("entries", []):
            tweet_result = (
                entry.get("content", {})
                .get("itemContent", {})
                .get("tweet_results", {})
                .get("result", {})
            )
            # Unwrap TweetWithVisibilityResults: the real tweet (and its legacy
            # node) lives under ["tweet"]; otherwise these tweets are silently dropped.
            if tweet_result.get("__typename") == "TweetWithVisibilityResults":
                tweet_result = tweet_result.get("tweet", {})

            legacy = tweet_result.get("legacy")
            if not legacy:
                continue

            tid = legacy.get("id_str", "")
            text = legacy.get("full_text", "")

            # RT: article 节点挂在内层原推上，转推壳本体没有
            rt_result = (legacy.get("retweeted_status_result") or {}).get("result") or {}
            if rt_result.get("__typename") == "TweetWithVisibilityResults":
                rt_result = rt_result.get("tweet") or {}
            rt_legacy = rt_result.get("legacy") or {}
            rt_user = ((rt_result.get("core") or {}).get("user_results") or {}).get("result") or {}
            rt_screen = ((rt_user.get("legacy") or {}).get("screen_name")
                         or (rt_user.get("core") or {}).get("screen_name") or "")

            # Quote: quoted_status_result 在 tweet_result 顶层（非 legacy 下），形状同推文
            quoted_result = (tweet_result.get("quoted_status_result") or {}).get("result") or {}
            if quoted_result.get("__typename") == "TweetWithVisibilityResults":
                quoted_result = quoted_result.get("tweet") or {}
            # 引用作者无法解析（被引推文删除/封禁/隐藏，legacy 在但 core 被剥）时，
            # 既不取其 article 也不设 quoted_status，退化为普通推文——否则空 screen_name
            # 会把引用文章错署到本博主并拼出坏 fetch URL（x.com/本博主/status/被引id）。
            q_legacy = quoted_result.get("legacy") or {}
            q_user = ((quoted_result.get("core") or {}).get("user_results") or {}).get("result") or {}
            q_screen = ((q_user.get("legacy") or {}).get("screen_name")
                        or (q_user.get("core") or {}).get("screen_name") or "")
            if not (q_legacy.get("id_str") and q_screen):
                quoted_result = {}

            # Extract note_tweet (longform)
            note_data = tweet_result.get("note_tweet", {})
            note_results = note_data.get("note_tweet_results", {}).get("result", {})
            note_text = note_results.get("text", "")

            # Extract article (Twitter Article format)；转推/引用时读原推的 article
            # 优先级：转推 > 引用 > 本体
            article_data = (rt_result or quoted_result or tweet_result).get("article", {})
            article_result = article_data.get("article_results", {}).get("result", {})
            article_title = article_result.get("title", "")
            article_preview = article_result.get("preview_text", "")
            article_rest_id = article_result.get("rest_id", "")

            entities = legacy.get("entities", {}) or {}
            extended_entities = legacy.get("extended_entities", {}) or {}

            # ========== Media extraction (photos, videos, animated_gif) ==========
            # 推文本体媒体在 entities.media / (更全) extended_entities.media。
            media = _build_media(extended_entities.get("media") or entities.get("media") or [])

            # 转推重建：Twitter 把转推壳的 full_text 砍到 140、壳无 note_tweet/media；
            # 原推正文与媒体都在 rt_result 里。非 article 转推时用原推重建全文
            # + note_tweet（长推 → format_message 走平铺全文）+ 媒体（配图），
            # 使转推与本博主自己发长推/带图推同款展示；article 转推仍走摘要队列不重建。
            if rt_result and rt_screen and not article_rest_id:
                rt_note = ((rt_result.get("note_tweet") or {})
                           .get("note_tweet_results") or {}).get("result", {}).get("text", "")
                rt_full = rt_legacy.get("full_text", "")
                if rt_note or rt_full:
                    text = f"RT @{rt_screen}: {rt_note or rt_full}"
                    if rt_note:
                        note_text = f"RT @{rt_screen}: {rt_note}"
                if not media:
                    rt_ext = rt_legacy.get("extended_entities") or {}
                    rt_ent = rt_legacy.get("entities") or {}
                    media = _build_media(rt_ext.get("media") or rt_ent.get("media") or [])
                    # media 换成原推的之后，entities/extended_entities（t.co 短链匹配用）也要
                    # 同步换成原推的——否则 normalized["text"] 已是原推全文，但 entities 仍是
                    # 壳的，_strip_media_tco 会找不到对应短链、漏剥离。
                    extended_entities = rt_ext
                    entities = rt_ent

            normalized = {
                "id": tid,
                "text": text,
                "created_at": legacy.get("created_at", ""),
                "entities": entities,
                "extended_entities": extended_entities,
                "media": media,  # new: easy-to-use list with direct media URLs
                "user": {
                    "screen_name": username,
                    "id_str": uid,
                },
                "conversation_id_str": legacy.get("conversation_id_str", tid),
                "favorite_count": legacy.get("favorite_count", 0),
                "retweet_count": legacy.get("retweet_count", 0),
                "reply_count": legacy.get("reply_count", 0),
            }

            if note_text:
                normalized["note_tweet"] = {"text": note_text}

            # rest_id 是去重/缓存/抓取键：无 id 的 article 节点既不可入队也无法 fetch。
            # 不挂节点 → 与 process_user 的节点兜底（按 rest_id）和 format_message 的
            # t["article"] 判定一致，避免「有标题无 rest_id」漏检后裸推误署名。
            if article_rest_id and (article_title or article_preview):
                normalized["article"] = {
                    "title": article_title,
                    "preview_text": article_preview,
                    "rest_id": article_rest_id,
                }

            if rt_result and rt_legacy.get("id_str"):
                normalized["retweeted_status"] = {"id": rt_legacy["id_str"],
                                                  "screen_name": rt_screen}

            # 引用：作者可解析时才设 quoted_status（quoted_result 已在上方按可用性归零，
            # 非空即代表 id 与 screen_name 均有效；被引推文删除/作者不可解析时为空 → 不设）。
            if quoted_result:
                normalized["quoted_status"] = {"id": q_legacy["id_str"],
                                               "screen_name": q_screen}

            tweets.append(normalized)

    return tweets


def fetch_article_tweet(tweet_id: str):
    """Fetch a single tweet by ID via TweetResultByRestId. Returns raw tweet result or None."""
    gt = _get_guest_token()
    if not gt:
        return None

    variables = json.dumps({
        "tweetId": str(tweet_id),
        "withCommunity": False,
        "includePromotedContent": False,
        "withVoice": True,
        "withArticleRichContent": True,
        "withArticlePlainText": True,
    })
    features = json.dumps({
        "creator_subscriptions_tweet_preview_api_enabled": True,
        "communities_web_enable_tweet_community_results_featuring": True,
        "c9s_tweet_anatomy_moderator_badge_enabled": True,
        "articles_preview_enabled": True,
        "responsive_web_edit_tweet_api_enabled": True,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
        "view_counts_everywhere_api_enabled": True,
        "longform_notetweets_consumption_enabled": True,
        "responsive_web_twitter_article_tweet_consumption_enabled": True,
        "longform_notetweets_rich_text_read_enabled": True,
        "longform_notetweets_inline_media_enabled": True,
        "responsive_web_enhance_cards_enabled": False,
    })
    url = "https://x.com/i/api/graphql/{}/TweetResultByRestId?variables={}&features={}".format(
        QUERY_TWEET_BY_REST_ID, quote(variables), quote(features)
    )

    try:
        resp = _curl(url, _gql_headers(gt), timeout=15)
    except CurlError:
        return None
    try:
        data = json.loads(resp)
    except json.JSONDecodeError:
        return None

    if data.get("errors"):
        return None

    return data.get("data", {}).get("tweetResult", {}).get("result", {})


def refresh_query_ids():
    """Try to refresh GraphQL query IDs from x.com JS bundle.

    Call this if current query IDs return errors.
    Returns True if update succeeded.
    """
    global QUERY_USER_BY_SCREEN_NAME, QUERY_USER_TWEETS

    gt = _get_guest_token()
    if not gt:
        return False

    try:
        resp = _curl("https://x.com", {"User-Agent": USER_AGENT}, timeout=10)
    except CurlError:
        return False
    js_match = re.search(r'(https://abs\.twimg\.com/responsive-web/client-web[^"]+\.js)', resp)
    if not js_match:
        return False

    js_url = js_match.group(1)
    try:
        js_content = _curl(js_url, {"User-Agent": USER_AGENT}, timeout=20)
    except CurlError:
        return False

    patterns = {
        "UserByScreenName": r'"UserByScreenName",\s*queryId:"([^"]+)"',
        "UserTweets": r'"UserTweets",\s*queryId:"([^"]+)"',
        "TweetResultByRestId": r'"TweetResultByRestId",\s*queryId:"([^"]+)"',
    }
    found = {}
    for name, pattern in patterns.items():
        m = re.search(pattern, js_content)
        if m:
            found[name] = m.group(1)

    if "UserByScreenName" in found:
        QUERY_USER_BY_SCREEN_NAME = found["UserByScreenName"]
    if "UserTweets" in found:
        QUERY_USER_TWEETS = found["UserTweets"]
    if "TweetResultByRestId" in found:
        global QUERY_TWEET_BY_REST_ID
        QUERY_TWEET_BY_REST_ID = found["TweetResultByRestId"]

    return bool(found)
