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


def _curl(url, headers, method="GET", timeout=15):
    """Use curl to avoid Python httpx TLS fingerprint issues."""
    cmd = ["curl", "-s", "--max-time", str(timeout)]
    if method == "POST":
        cmd += ["-X", "POST"]
    for k, v in headers.items():
        cmd += ["-H", f"{k}: {v}"]
    cmd.append(url)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 5)
    return result.stdout


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
            with open(GUEST_TOKEN_CACHE, "w") as f:
                json.dump({"token": token, "ts": time.time()}, f)
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


def _save_auth_cookies(auth_token, ct0):
    """Save auth cookies to cache."""
    with open(AUTH_COOKIE_CACHE, "w") as f:
        json.dump({"auth_token": auth_token, "ct0": ct0}, f)


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
    """Get user ID by screen name."""
    # Try authenticated first
    ah = _auth_headers()
    if ah:
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

    resp = _curl(url, ah)
    try:
        data = json.loads(resp)
        uid = data.get("data", {}).get("user", {}).get("result", {}).get("rest_id")
        if uid:
            return uid
    except Exception:
        pass

    # Fallback to guest token
    gt = _get_guest_token()
    if not gt:
        return None
    resp = _curl(url, _gql_headers(gt))
    try:
        data = json.loads(resp)
        return data.get("data", {}).get("user", {}).get("result", {}).get("rest_id")
    except Exception:
        return None


def fetch_tweets(username, limit=20):
    """Fetch user tweets, return normalized format list.

    Return format compatible with original 6551.io API:
    - id: str (tweet ID)
    - text: str (full text)
    - created_at: str (timestamp)
    - entities: dict (contains urls)
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
    resp = _curl(url, headers, timeout=20)
    try:
        data = json.loads(resp)
    except json.JSONDecodeError:
        raise RuntimeError("GraphQL response not JSON: " + resp[:200])

    errors = data.get("errors")
    if errors:
        # Retry once with fresh guest token on server errors
        error_msgs = [e.get("message", "") for e in errors]
        if any("Internal server error" in m for m in error_msgs) and not use_auth:
            if os.path.exists(GUEST_TOKEN_CACHE):
                os.remove(GUEST_TOKEN_CACHE)
            gt2 = _get_guest_token()
            if gt2:
                resp = _curl(url, _gql_headers(gt2), timeout=20)
                try:
                    data = json.loads(resp)
                    errors = data.get("errors")
                    if not errors:
                        pass  # Retry succeeded
                    else:
                        raise RuntimeError("GraphQL errors (after retry): " + str(errors))
                except json.JSONDecodeError:
                    raise RuntimeError("GraphQL response not JSON (after retry): " + resp[:200])
            else:
                raise RuntimeError("GraphQL errors: " + str(errors))
        else:
            raise RuntimeError("GraphQL errors: " + str(errors))

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
            legacy = tweet_result.get("legacy")
            if not legacy:
                continue

            tid = legacy.get("id_str", "")
            text = legacy.get("full_text", "")

            # Extract note_tweet (longform)
            note_data = tweet_result.get("note_tweet", {})
            note_results = note_data.get("note_tweet_results", {}).get("result", {})
            note_text = note_results.get("text", "")

            # Extract article (Twitter Article format)
            article_data = tweet_result.get("article", {})
            article_result = article_data.get("article_results", {}).get("result", {})
            article_title = article_result.get("title", "")
            article_preview = article_result.get("preview_text", "")
            article_rest_id = article_result.get("rest_id", "")

            entities = legacy.get("entities", {})

            normalized = {
                "id": tid,
                "text": text,
                "created_at": legacy.get("created_at", ""),
                "entities": entities,
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

            if article_title or article_preview:
                normalized["article"] = {
                    "title": article_title,
                    "preview_text": article_preview,
                    "rest_id": article_rest_id,
                }

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

    resp = _curl(url, _gql_headers(gt), timeout=15)
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

    resp = _curl("https://x.com", {"User-Agent": USER_AGENT}, timeout=10)
    js_match = re.search(r'(https://abs\.twimg\.com/responsive-web/client-web[^"]+\.js)', resp)
    if not js_match:
        return False

    js_url = js_match.group(1)
    js_content = _curl(js_url, {"User-Agent": USER_AGENT}, timeout=20)

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
