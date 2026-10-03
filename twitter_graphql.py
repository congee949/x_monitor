"""Twitter GraphQL data source - uses guest token, no API key needed.

Replaces ai.6551.io, completely free.
Guest token from api.x.com/1.1/guest/activate.json,
then standard bearer token for GraphQL API calls.
"""

import html
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GUEST_TOKEN_CACHE = os.path.join(SCRIPT_DIR, ".guest_token_cache.json")
AUTH_COOKIE_CACHE = os.path.join(SCRIPT_DIR, ".auth_cookies.json")
USER_ID_CACHE = os.path.join(SCRIPT_DIR, ".user_id_cache.json")
SEMANTIC_DETAIL_CACHE_PATH = os.path.join(SCRIPT_DIR, ".semantic_detail_cache.json")

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

# Per-process auth health, for the run-level cookie watchdog in twitter_monitor.
# Each cron tick is a fresh process, so these start at 0 every run — no reset
# needed in production. auth_health_summary() reads them after the run to decide
# whether this whole tick failed to reach authenticated access (silent guest
# degradation), which per-account failure tracking cannot see.
_cookies_loaded_this_run = False   # a valid auth_token+ct0 was ever loaded
_authed_success_count = 0          # authed request that actually succeeded
_cookie_degrade_count = 0          # authed request rejected → fell back to guest


def normalize_x_text(value):
    """Decode X HTML entities once at the source boundary."""
    return html.unescape(value) if isinstance(value, str) else ""


def _reset_auth_health():
    """Reset per-process auth-health counters (tests exercise many runs in one process)."""
    global _cookies_loaded_this_run, _authed_success_count, _cookie_degrade_count
    _cookies_loaded_this_run = False
    _authed_success_count = 0
    _cookie_degrade_count = 0


def auth_health_summary():
    """Snapshot of this process's auth health for the run-level watchdog.

    degraded == the run never achieved authenticated access: either no cookies
    were loadable (missing / renamed to .stale), or cookies loaded but every
    authed request was rejected and fell back to guest.
    """
    degraded = (not _cookies_loaded_this_run) or (_authed_success_count == 0)
    return {
        "cookies_loaded": _cookies_loaded_this_run,
        "authed_success": _authed_success_count,
        "degrade_events": _cookie_degrade_count,
        "degraded": degraded,
    }


class CurlError(Exception):
    """curl subprocess failed or returned a non-2xx HTTP status."""

    def __init__(self, message, *, status_code=0, retry_after=0):
        super().__init__(message)
        self.status_code = int(status_code or 0)
        self.retry_after = min(max(int(retry_after or 0), 0), 30)


def _curl(url, headers=None, method="GET", timeout=15):
    """Use curl to avoid Python httpx TLS fingerprint issues.

    Raises:
        CurlError: on curl non-zero exit or HTTP status >= 300.
    """
    headers = headers or {}
    write_format = "\n%{http_code}\n%{exitcode}"
    header_fd, header_path = tempfile.mkstemp(prefix="x-gql-headers-")
    os.close(header_fd)
    cmd = [
        "curl", "-s", "--connect-timeout", "5",
        "--max-time", str(timeout), "-D", header_path, "-w", write_format,
    ]
    if method == "POST":
        cmd += ["-X", "POST"]
    for k, v in headers.items():
        cmd += ["-H", f"{k}: {v}"]
    cmd.append(url)
    response_headers = ""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 5)
    except FileNotFoundError:
        print("  [GraphQL] curl 未安装或不在 PATH")
        return ""
    except subprocess.TimeoutExpired:
        print(f"  [GraphQL] curl 超时 ({timeout}s): {url[:80]}")
        return ""
    finally:
        try:
            with open(header_path, encoding="iso-8859-1") as header_file:
                response_headers = header_file.read()
        except OSError:
            pass
        try:
            os.unlink(header_path)
        except OSError:
            pass

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
        retry_after = 0
        if http_code == 429:
            matches = re.findall(r"(?im)^retry-after:\s*([^\r\n]+)", response_headers)
            if matches:
                raw_retry_after = matches[-1].strip()
                try:
                    retry_after = int(raw_retry_after)
                except ValueError:
                    try:
                        retry_at = parsedate_to_datetime(raw_retry_after)
                        if retry_at.tzinfo is None:
                            retry_at = retry_at.replace(tzinfo=timezone.utc)
                        retry_after = max(0, int((retry_at - datetime.now(timezone.utc)).total_seconds()))
                    except (TypeError, ValueError, OverflowError):
                        retry_after = 1
        raise CurlError(f"HTTP {http_code}: {stderr}", status_code=http_code,
                        retry_after=retry_after or (1 if http_code == 429 else 0))

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
                global _cookies_loaded_this_run
                _cookies_loaded_this_run = True
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


SEMANTIC_BUNDLE_SCHEMA_VERSION = 1
SEMANTIC_BUNDLE_RESOLVER_VERSION = "embedded-v1"
SEMANTIC_MAX_DEPTH = 3
SEMANTIC_MAX_NODES = 5
SEMANTIC_DETAIL_PER_BUNDLE = 2
SEMANTIC_DETAIL_PER_RUN = 12
SEMANTIC_RESOLVER_DEADLINE_SECONDS = 90
SEMANTIC_CACHE_COMPLETE_TTL = 24 * 3600
SEMANTIC_CACHE_PARTIAL_TTL = 15 * 60
SEMANTIC_CACHE_TRANSIENT_TTL = 5 * 60
SEMANTIC_CACHE_TERMINAL_TTL = 24 * 3600
_TWEET_ID_RE = re.compile(r"^\d{1,24}$")
_semantic_detail_cache: dict[str, dict] = {}
_semantic_detail_inflight: set[str] = set()
_semantic_run_requests = 0
_semantic_run_started = time.monotonic()
_semantic_cache_loaded = False
_semantic_cooldown_until = 0.0


def _load_semantic_cache() -> None:
    global _semantic_cache_loaded
    if _semantic_cache_loaded:
        return
    _semantic_cache_loaded = True
    try:
        with open(SEMANTIC_DETAIL_CACHE_PATH) as stream:
            raw = json.load(stream)
        entries = raw.get("entries") or {}
        if isinstance(entries, dict):
            _semantic_detail_cache.update({str(k): v for k, v in entries.items()
                                           if isinstance(v, dict)})
    except Exception:
        pass


def _save_semantic_cache(now=None) -> None:
    now = time.time() if now is None else now
    entries = {k: v for k, v in _semantic_detail_cache.items()
               if isinstance(v, dict) and now - float(v.get("ts") or 0) <= float(v.get("ttl") or 0)}
    if len(entries) > 500:
        entries = dict(sorted(entries.items(), key=lambda item: float(item[1].get("ts") or 0),
                              reverse=True)[:500])
    tmp = SEMANTIC_DETAIL_CACHE_PATH + f".tmp.{os.getpid()}"
    try:
        with open(tmp, "w", encoding="utf-8") as stream:
            json.dump({"version": 1, "entries": entries}, stream,
                      ensure_ascii=False, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, SEMANTIC_DETAIL_CACHE_PATH)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass


def reset_semantic_resolver_run() -> None:
    global _semantic_run_requests, _semantic_run_started, _semantic_cooldown_until
    _semantic_run_requests = 0
    _semantic_run_started = time.monotonic()
    _semantic_cooldown_until = 0.0


def _relation_missing_ids(result: dict) -> list[tuple[str, str]]:
    """Return typed missing embedded edges without interpreting absence as no edge."""
    result = _unwrap_tweet_result(result)
    legacy = result.get("legacy") or {}
    out = []
    rt_box = legacy.get("retweeted_status_result") or {}
    rt = _unwrap_tweet_result(rt_box.get("result") if isinstance(rt_box, dict) else {})
    if re.match(r"^\s*RT\s+@", normalize_x_text(legacy.get("full_text", "")), re.I) and not rt:
        rid = str(legacy.get("retweeted_status_id_str") or "")
        if _TWEET_ID_RE.fullmatch(rid):
            out.append(("repost", rid))
    if rt:
        # RT shells mirror the original quote id/permalink but the authoritative
        # quote edge lives on the embedded original. Resolving it here wastes a
        # request and incorrectly makes quote context a sibling of the anchor.
        return out
    q_box = result.get("quoted_status_result") or {}
    q = _unwrap_tweet_result(q_box.get("result") if isinstance(q_box, dict) else {})
    qid = str(legacy.get("quoted_status_id_str") or "")
    if (legacy.get("is_quote_status") or qid) and not q and _TWEET_ID_RE.fullmatch(qid):
        out.append(("quote", qid))
    return out


def _inject_relation(result: dict, relation: str, child: dict) -> None:
    result = _unwrap_tweet_result(result)
    if relation == "repost":
        (result.setdefault("legacy", {}))["retweeted_status_result"] = {"result": child}
    else:
        result["quoted_status_result"] = {"result": child}


def _semantic_detail(tweet_id: str, fetcher=None, now=None) -> dict:
    """Typed, cached, single-flight detail read; never logs auth headers or URLs."""
    global _semantic_run_requests, _semantic_cooldown_until
    now = time.time() if now is None else now
    _load_semantic_cache()
    cached = _semantic_detail_cache.get(tweet_id)
    if cached and now - cached["ts"] <= cached["ttl"]:
        return dict(cached, cache_hit=True, physical_attempts=0)
    if tweet_id in _semantic_detail_inflight:
        return {"status": "transient", "reason": "single_flight_busy", "cache_hit": False,
                "physical_attempts": 0}
    if time.monotonic() < _semantic_cooldown_until:
        return {"status": "transient", "reason": "rate_limit_cooldown", "cache_hit": False,
                "physical_attempts": 0, "cooldown_remaining":
                max(0.0, _semantic_cooldown_until - time.monotonic())}
    if (_semantic_run_requests >= SEMANTIC_DETAIL_PER_RUN
            or time.monotonic() - _semantic_run_started >= SEMANTIC_RESOLVER_DEADLINE_SECONDS):
        return {"status": "transient", "reason": "run_budget", "cache_hit": False,
                "physical_attempts": 0}
    _semantic_detail_inflight.add(tweet_id)
    physical_attempts = 0
    source_mode = "custom_detail" if fetcher else "graphql_guest_detail"

    def attempt():
        nonlocal physical_attempts
        global _semantic_run_requests
        if (_semantic_run_requests >= SEMANTIC_DETAIL_PER_RUN
                or time.monotonic() - _semantic_run_started >= SEMANTIC_RESOLVER_DEADLINE_SECONDS):
            raise RuntimeError("semantic_run_budget")
        _semantic_run_requests += 1
        physical_attempts += 1
        return (fetcher or (lambda tid: fetch_article_tweet(tid, raise_errors=True)))(tweet_id)

    try:
        try:
            value = attempt()
        except CurlError as exc:
            if exc.status_code == 429:
                delay = min(max(exc.retry_after or 1, 1), 30)
                _semantic_cooldown_until = time.monotonic() + delay
                time.sleep(delay)
                try:
                    # The retry is a second physical request: re-check both the
                    # 90-second deadline and the run-wide physical-attempt budget.
                    value = attempt()
                except CurlError as retry_exc:
                    if retry_exc.status_code == 429:
                        retry_delay = min(max(retry_exc.retry_after or 1, 1), 30)
                        _semantic_cooldown_until = time.monotonic() + retry_delay
                    result = {"status": "transient", "reason": "rate_limited",
                              "node": None, "ts": now, "ttl": SEMANTIC_CACHE_TRANSIENT_TTL}
                except RuntimeError as retry_exc:
                    result = {"status": "transient",
                              "reason": "run_budget" if str(retry_exc) == "semantic_run_budget"
                              else type(retry_exc).__name__,
                              "node": None, "ts": now, "ttl": SEMANTIC_CACHE_TRANSIENT_TTL}
                except Exception as retry_exc:
                    result = {"status": "transient", "reason": type(retry_exc).__name__,
                              "node": None, "ts": now, "ttl": SEMANTIC_CACHE_TRANSIENT_TTL}
                else:
                    node = _unwrap_tweet_result(value or {})
                    result = {"status": "complete" if node.get("legacy") else "transient",
                              "reason": "" if node.get("legacy") else "empty_after_429",
                              "node": node if node.get("legacy") else None, "ts": now,
                              "ttl": SEMANTIC_CACHE_COMPLETE_TTL if node.get("legacy")
                              else SEMANTIC_CACHE_TRANSIENT_TTL}
                result.update({"fetch_mode": source_mode, "physical_attempts": physical_attempts})
                _semantic_detail_cache[tweet_id] = result
                if fetcher is None:
                    _save_semantic_cache(now)
                return dict(result, cache_hit=False)
            result = {"status": "transient", "reason": f"http_{exc.status_code or 0}",
                      "node": None, "ts": now, "ttl": SEMANTIC_CACHE_TRANSIENT_TTL}
        except RuntimeError as exc:
            result = {"status": "transient",
                      "reason": "run_budget" if str(exc) == "semantic_run_budget"
                      else type(exc).__name__,
                      "node": None, "ts": now, "ttl": SEMANTIC_CACHE_TRANSIENT_TTL}
        except Exception as exc:
            result = {"status": "transient", "reason": type(exc).__name__,
                      "node": None, "ts": now, "ttl": SEMANTIC_CACHE_TRANSIENT_TTL}
        else:
            node = _unwrap_tweet_result(value or {})
            typename = str((value or {}).get("__typename") or "") if isinstance(value, dict) else ""
            if node.get("legacy"):
                result = {"status": "complete", "reason": "", "node": node,
                          "ts": now, "ttl": SEMANTIC_CACHE_COMPLETE_TTL}
            elif typename in ("TweetTombstone", "TweetUnavailable", "TweetNotFound"):
                result = {"status": "terminal", "reason": typename, "node": None,
                          "ts": now, "ttl": SEMANTIC_CACHE_TERMINAL_TTL}
            else:
                result = {"status": "transient", "reason": "empty_or_schema_drift",
                          "node": None, "ts": now, "ttl": SEMANTIC_CACHE_TRANSIENT_TTL}
        result.update({"fetch_mode": source_mode, "physical_attempts": physical_attempts})
        _semantic_detail_cache[tweet_id] = result
        if fetcher is None:
            _save_semantic_cache(now)
        return dict(result, cache_hit=False)
    finally:
        _semantic_detail_inflight.discard(tweet_id)


def resolve_semantic_bundle(tweet_result: dict, observed_via: str, *, fetch_mode="graphql",
                            fetcher=None) -> dict:
    """Resolve missing embedded relations under per-bundle and per-run hard budgets."""
    outer = _unwrap_tweet_result(tweet_result)
    requests = 0
    cache_hits = 0
    detail_fetch_modes = []
    terminal = []
    frontier = [outer]
    visited_nodes = set()
    while frontier and requests < SEMANTIC_DETAIL_PER_BUNDLE:
        parent = frontier.pop(0)
        pid = str((parent.get("legacy") or {}).get("id_str") or parent.get("rest_id") or "")
        if pid in visited_nodes:
            continue
        visited_nodes.add(pid)
        for relation, missing_id in _relation_missing_ids(parent):
            if requests >= SEMANTIC_DETAIL_PER_BUNDLE:
                break
            detail = _semantic_detail(missing_id, fetcher=fetcher)
            if detail.get("fetch_mode"):
                detail_fetch_modes.append(str(detail["fetch_mode"]))
            if detail.get("cache_hit"):
                cache_hits += 1
            requests += int(detail.get("physical_attempts") or 0)
            if detail.get("status") == "complete":
                child = detail["node"]
                _inject_relation(parent, relation, child)
                frontier.append(child)
            elif detail.get("status") == "terminal":
                terminal.append(f"{relation}:{missing_id}:{detail.get('reason')}")
            else:
                frontier = []
                break
        for child in (
            ((parent.get("legacy") or {}).get("retweeted_status_result") or {}).get("result"),
            (parent.get("quoted_status_result") or {}).get("result"),
        ):
            child = _unwrap_tweet_result(child or {})
            if child:
                frontier.append(child)
    bundle = build_semantic_bundle(outer, observed_via, fetch_mode=fetch_mode)
    resolution = bundle["resolution"]
    prior_status = resolution.get("status")
    resolution["request_count"] = requests
    resolution["cache_hits"] = cache_hits
    resolution["fetch_modes"] = list(dict.fromkeys(
        [str(fetch_mode)] + detail_fetch_modes))
    if terminal:
        resolution["reasons"] = [r for r in resolution.get("reasons", [])
                                 if not r.startswith("context_unresolved_transient")]
        resolution["reasons"].extend("context_unavailable_terminal:" + x for x in terminal)
        if prior_status == "degraded_optional":
            resolution["status"] = "degraded_optional"
            resolution["required_context_complete"] = True
        else:
            resolution["status"] = "context_unavailable_terminal"
            resolution["required_context_complete"] = False
    # A terminal relation is stronger information than transport equivalence.
    # Keep terminal/degraded-terminal status for the state machine and record the
    # guest provenance separately; process_user still fail-closes gray on modes.
    if (not terminal and any("guest" in mode for mode in resolution["fetch_modes"])):
        resolution["status"] = "auth_degraded"
        resolution["required_context_complete"] = False
        resolution.setdefault("reasons", []).append("auth_degraded:guest_detail")
    return bundle


def _unwrap_tweet_result(result: dict) -> dict:
    """Return the concrete Tweet node behind visibility wrappers."""
    result = result if isinstance(result, dict) else {}
    if result.get("__typename") == "TweetWithVisibilityResults":
        result = result.get("tweet") or {}
    return result if isinstance(result, dict) else {}


def _tweet_result_author(result: dict) -> str:
    result = _unwrap_tweet_result(result)
    user = ((result.get("core") or {}).get("user_results") or {}).get("result") or {}
    return str(((user.get("legacy") or {}).get("screen_name")
                or (user.get("core") or {}).get("screen_name") or ""))


def _tweet_result_note(result: dict) -> tuple[str, dict]:
    result = _unwrap_tweet_result(result)
    note_result = (((result.get("note_tweet") or {}).get("note_tweet_results") or {})
                   .get("result") or {})
    return (normalize_x_text(note_result.get("text", "")),
            note_result.get("entity_set") or note_result.get("entities") or {})


def _tweet_result_reply_parent(result: dict) -> dict:
    """Reply linkage as {id, screen_name, user_id}; {} when the tweet is not a reply.

    X 把自回复串包成 TimelineTimelineModule，抓取端已刻意摊平成独立推文，串关系
    只剩 legacy 里这三个字段。screen_name 在被回复方改名/不可解析时可能缺失，
    所以 user_id 一并保留，供调用方按数字 id 判定「是否回复自己」。
    """
    legacy = _unwrap_tweet_result(result).get("legacy") or {}
    parent_id = str(legacy.get("in_reply_to_status_id_str") or "")
    if not parent_id:
        return {}
    return {"id": parent_id,
            "screen_name": str(legacy.get("in_reply_to_screen_name") or ""),
            "user_id": str(legacy.get("in_reply_to_user_id_str") or "")}


def _tweet_result_snapshot(result: dict, *, fallback_author: str = "") -> dict:
    """Normalize one embedded tweet without flattening its relationships."""
    result = _unwrap_tweet_result(result)
    legacy = result.get("legacy") or {}
    tid = str(legacy.get("id_str") or result.get("rest_id") or "")
    author = _tweet_result_author(result) or fallback_author
    note_text, note_entities = _tweet_result_note(result)
    entities = legacy.get("entities") or {}
    extended = legacy.get("extended_entities") or {}
    article_result = ((((result.get("article") or {}).get("article_results") or {})
                       .get("result") or {}))
    article = None
    article_id = str(article_result.get("rest_id") or "")
    if article_id and (article_result.get("title") or article_result.get("preview_text")):
        article = {
            "article_id": article_id,
            "title": normalize_x_text(article_result.get("title", "")),
            "preview_text": normalize_x_text(article_result.get("preview_text", "")),
        }
    return {
        "tweet_id": tid,
        "author": author,
        "text": normalize_x_text(legacy.get("full_text", "")),
        "note": {"text": note_text, "entities": note_entities} if note_text else None,
        "source_url": f"https://x.com/{quote(author)}/status/{quote(tid)}"
                      if author and tid else "",
        "created_at": legacy.get("created_at", ""),
        "entities": entities,
        "extended_entities": extended,
        "media": _build_media(extended.get("media") or entities.get("media") or []),
        "article": article,
        "metrics": {
            "favorite_count": legacy.get("favorite_count", 0),
            "retweet_count": legacy.get("retweet_count", 0),
            "reply_count": legacy.get("reply_count", 0),
            "fetched_at": int(time.time()),
        },
    }


def build_semantic_bundle(tweet_result: dict, observed_via: str,
                          *, fetch_mode: str = "graphql") -> dict:
    """Build an embedded-first observation/anchor/context bundle.

    This resolver deliberately performs no network I/O. Missing required embedded
    nodes are typed as transient so the delivery layer can defer without advancing
    source seen. A later detail resolver can fill them under a bounded budget.
    """
    outer = _unwrap_tweet_result(tweet_result)
    outer_snap = _tweet_result_snapshot(outer, fallback_author=observed_via)
    outer_id = outer_snap["tweet_id"]
    reasons: list[str] = []
    visited: set[str] = set()
    repost_path: list[dict] = []
    context_nodes: list[dict] = []
    assets: list[dict] = []
    article_refs: list[dict] = []
    node_count = 0
    max_depth_seen = 0

    def register(result: dict, *, fallback_author: str = "") -> tuple:
        nonlocal node_count
        snap = _tweet_result_snapshot(result, fallback_author=fallback_author)
        tid = snap.get("tweet_id") or ""
        if not _TWEET_ID_RE.fullmatch(str(tid)):
            reasons.append("schema_drift:invalid_tweet_id")
            return None, False
        if not snap.get("author"):
            reasons.append("schema_drift:missing_author")
            return snap, False
        if tid in visited:
            reasons.append("cycle_detected")
            return snap, False
        if node_count >= SEMANTIC_MAX_NODES:
            reasons.append("truncated_budget:nodes")
            return snap, False
        visited.add(tid)
        node_count += 1
        return snap, True

    current = outer
    current_snap, ok = register(current, fallback_author=observed_via)
    depth = 0
    if not ok or current_snap is None:
        return {
            "schema_version": SEMANTIC_BUNDLE_SCHEMA_VERSION,
            "resolver_version": SEMANTIC_BUNDLE_RESOLVER_VERSION,
            "observation": {"outer_id": outer_id, "observed_via": observed_via,
                            "source_url": outer_snap.get("source_url", ""),
                            "fetch_mode": fetch_mode},
            "anchor": None, "repost_path": [], "context_nodes": [], "assets": [],
            "article_refs": [],
            "identity": {"bundle_key": "", "alias_keys": [], "context_keys": [],
                         "article_keys": []},
            "resolution": {"status": "schema_drift", "required_context_complete": False,
                           "depth": 0, "node_count": node_count, "request_count": 0,
                           "reasons": reasons or ["schema_drift"]},
        }

    # Reposts are lineage only. Follow consecutive embedded reposts to the first
    # non-repost node, which becomes the immutable content anchor.
    while True:
        legacy = current.get("legacy") or {}
        rt_box = legacy.get("retweeted_status_result") or {}
        rt_raw = rt_box.get("result") if isinstance(rt_box, dict) else None
        rt = _unwrap_tweet_result(rt_raw or {})
        has_rt_signal = bool(rt_raw or re.match(r"^\s*RT\s+@", current_snap.get("text") or "", re.I))
        if not has_rt_signal:
            break
        repost_path.append({"tweet_id": current_snap["tweet_id"],
                            "author": current_snap.get("author") or ""})
        if not rt:
            reasons.append("context_unresolved_transient:missing_repost_node")
            break
        if depth >= SEMANTIC_MAX_DEPTH:
            reasons.append("truncated_budget:depth")
            break
        depth += 1
        max_depth_seen = max(max_depth_seen, depth)
        rt_snap, added = register(rt)
        if not added or rt_snap is None:
            current_snap = rt_snap or current_snap
            break
        current, current_snap = rt, rt_snap

    anchor_result = current
    anchor = current_snap

    def collect_owned(snap: dict) -> None:
        owner = snap.get("tweet_id") or ""
        for index, media in enumerate(snap.get("media") or []):
            url = str(media.get("url") or media.get("video_url") or "")
            digest = hashlib.sha256(url.encode()).hexdigest()[:20] if url else f"slot-{index}"
            asset = dict(media)
            asset.update({"asset_id": f"m:{digest}", "owner_tweet_id": owner,
                          "kind": media.get("type"), "completeness": "complete"})
            assets.append(asset)
        article = snap.get("article") or {}
        if article.get("article_id"):
            article_refs.append({"article_id": article["article_id"],
                                 "owner_tweet_id": owner,
                                 "title": article.get("title", ""),
                                 "preview_text": article.get("preview_text", ""),
                                 "body_ref": None, "completeness": "preview"})

    collect_owned(anchor)

    def walk_quote(parent_result: dict, parent_snap: dict, edge_depth: int) -> None:
        nonlocal max_depth_seen
        q_box = parent_result.get("quoted_status_result") or {}
        q_raw = q_box.get("result") if isinstance(q_box, dict) else None
        legacy = parent_result.get("legacy") or {}
        has_quote_signal = bool(q_raw or legacy.get("is_quote_status")
                                or legacy.get("quoted_status_id_str"))
        if not has_quote_signal:
            return
        if not q_raw:
            reasons.append("context_unresolved_transient:missing_quote_node")
            return
        if edge_depth > SEMANTIC_MAX_DEPTH:
            reasons.append("truncated_budget:depth")
            return
        q_result = _unwrap_tweet_result(q_raw)
        q_snap, added = register(q_result)
        max_depth_seen = max(max_depth_seen, edge_depth)
        if not added or q_snap is None:
            return
        # A quoted context may itself be a repost shell. It remains context (never
        # changes the root anchor), but the visible context is its first non-repost
        # descendant and assets stay with that real owner.
        context_depth = edge_depth
        while True:
            q_legacy = q_result.get("legacy") or {}
            rt_box = q_legacy.get("retweeted_status_result") or {}
            rt_raw = rt_box.get("result") if isinstance(rt_box, dict) else None
            if not rt_raw:
                break
            if context_depth >= SEMANTIC_MAX_DEPTH:
                reasons.append("truncated_budget:depth")
                return
            context_depth += 1
            max_depth_seen = max(max_depth_seen, context_depth)
            q_result = _unwrap_tweet_result(rt_raw)
            q_snap, added = register(q_result)
            if not added or q_snap is None:
                return
        q_node = dict(q_snap)
        q_node.update({"relation": "quote", "parent_id": parent_snap["tweet_id"],
                       "completeness": "complete"})
        context_nodes.append(q_node)
        collect_owned(q_snap)
        walk_quote(q_result, q_snap, context_depth + 1)

    walk_quote(anchor_result, anchor, depth + 1)
    aliases = ["t:" + item["tweet_id"] for item in repost_path
               if item.get("tweet_id") and item["tweet_id"] != anchor.get("tweet_id")]
    context_keys = ["t:" + item["tweet_id"] for item in context_nodes]
    article_keys = ["a:" + item["article_id"] for item in article_refs]
    transient = any(reason.startswith("context_unresolved_transient") for reason in reasons)
    anchor_body = ((anchor.get("note") or {}).get("text") or anchor.get("text") or "").strip()
    anchor_signal = re.sub(r"https?://\S+", "", anchor_body).strip().casefold()
    dependent_context = (len(anchor_signal) < 24 or bool(re.search(
        r"^(?:this|that|these|look|watch|exactly|yes|草|确实|太传神|看这个|这个|这图)\b",
        anchor_signal, re.I)))
    missing_repost = any("missing_repost_node" in reason for reason in reasons)
    required_transient = transient and (missing_repost or dependent_context)
    if required_transient:
        status = "context_unresolved_transient"
    elif transient:
        status = "degraded_optional"
    elif "cycle_detected" in reasons:
        status = "cycle_detected"
    elif any(r.startswith("truncated_budget") for r in reasons):
        status = "truncated_budget"
    elif any(r.startswith("schema_drift") for r in reasons):
        status = "schema_drift"
    else:
        status = "auth_degraded" if "guest" in fetch_mode else "complete"
    return {
        "schema_version": SEMANTIC_BUNDLE_SCHEMA_VERSION,
        "resolver_version": SEMANTIC_BUNDLE_RESOLVER_VERSION,
        "observation": {"outer_id": outer_id, "observed_via": observed_via,
                        "observed_at": outer_snap.get("created_at", ""),
                        "source_url": outer_snap.get("source_url", ""),
                        "fetch_mode": fetch_mode},
        "anchor": anchor,
        "repost_path": repost_path,
        "context_nodes": context_nodes,
        "assets": assets,
        "article_refs": article_refs,
        "identity": {"bundle_key": "t:" + str(anchor.get("tweet_id") or ""),
                     "alias_keys": aliases, "context_keys": context_keys,
                     "article_keys": article_keys},
        "resolution": {"status": status, "required_context_complete": not required_transient,
                       "depth": max_depth_seen, "node_count": node_count,
                       "request_count": 0, "reasons": reasons},
    }


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
        global _cookie_fail_count, _cookie_degrade_count
        error_msgs = [e.get("message", "") for e in errors]
        if _account_gone(errors):
            invalidate_user_id(username)

        if use_auth:
            used_auth_for_success = False
            print(f"  [GraphQL] cookie 认证失效，降级 guest 模式: {error_msgs}")
            _cookie_fail_count += 1
            _cookie_degrade_count += 1
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
        global _authed_success_count
        _cookie_fail_count = 0
        _authed_success_count += 1

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
        flat_entries = []
        for entry in inst.get("entries", []):
            content = entry.get("content", {}) or {}
            # X groups self-reply threads into TimelineTimelineModule entries. Some
            # accounts (notably @claudeai) currently return *only* modules, so reading
            # entry.content.itemContent alone makes a healthy timeline look empty.
            # Flatten both shapes; non-tweet module items (for example who-to-follow)
            # naturally fall through because they have no tweet_results.result.
            if content.get("itemContent"):
                flat_entries.append(entry)
            for module_item in content.get("items", []) or []:
                item = module_item.get("item", {}) or {}
                if item.get("itemContent"):
                    flat_entries.append({"content": {"itemContent": item["itemContent"]}})
        for entry in flat_entries:
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
            text = normalize_x_text(legacy.get("full_text", ""))

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
            note_text = normalize_x_text(note_results.get("text", ""))
            note_entities = (note_results.get("entity_set")
                             or note_results.get("entities") or {})

            # Extract article (Twitter Article format)；转推/引用时读原推的 article
            # 优先级：转推 > 引用 > 本体
            article_data = (rt_result or quoted_result or tweet_result).get("article", {})
            article_result = article_data.get("article_results", {}).get("result", {})
            article_title = normalize_x_text(article_result.get("title", ""))
            article_preview = normalize_x_text(article_result.get("preview_text", ""))
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
                rt_note_result = ((rt_result.get("note_tweet") or {})
                                  .get("note_tweet_results") or {}).get("result", {})
                rt_note = normalize_x_text(rt_note_result.get("text", ""))
                rt_full = normalize_x_text(rt_legacy.get("full_text", ""))
                if rt_note or rt_full:
                    text = f"RT @{rt_screen}: {rt_note or rt_full}"
                    if rt_note:
                        note_text = f"RT @{rt_screen}: {rt_note}"
                        note_entities = (rt_note_result.get("entity_set")
                                         or rt_note_result.get("entities") or {})
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

            # Additive semantic representation. Legacy flat fields stay intact for
            # rollback; bundle-aware consumers can separate observation, anchor and
            # quoted context without re-reading provider-specific GraphQL shapes.
            try:
                normalized["semantic_bundle"] = build_semantic_bundle(
                    tweet_result, username,
                    fetch_mode="graphql_auth" if used_auth_for_success else "graphql_guest")
                normalized["_semantic_raw"] = tweet_result
            except Exception as exc:
                # Additive shadow metadata must never take down the established flat
                # provider path while the rollout flag is off.
                normalized["semantic_bundle_error"] = type(exc).__name__

            if note_text:
                normalized["note_tweet"] = {"text": note_text,
                                            "entities": note_entities}

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

            # 自回复串的父推 id：投递层据此把评论接到父推那条 Telegram 消息下面。
            reply_parent = _tweet_result_reply_parent(tweet_result)
            if reply_parent:
                normalized["in_reply_to_status"] = reply_parent

            # 引用：作者可解析时才设 quoted_status（quoted_result 已在上方按可用性归零，
            # 非空即代表 id 与 screen_name 均有效；被引推文删除/作者不可解析时为空 → 不设）。
            if quoted_result:
                normalized["quoted_status"] = {"id": q_legacy["id_str"],
                                               "screen_name": q_screen}

            tweets.append(normalized)

    return tweets[:limit]


def fetch_article_tweet(tweet_id: str, raise_errors: bool = False):
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
        if raise_errors:
            raise
        return None
    try:
        data = json.loads(resp)
    except json.JSONDecodeError:
        return None

    errors = data.get("errors") or []
    if errors:
        codes = {str(error.get("code", "")) for error in errors if isinstance(error, dict)}
        messages = " ".join(str(error.get("message") or "") for error in errors
                            if isinstance(error, dict)).casefold()
        if codes & {"88", "RateLimitExceeded"} or "rate limit" in messages:
            if raise_errors:
                raise CurlError("GraphQL rate limited", status_code=429, retry_after=1)
            return None
        if codes & {"144", "34", "50", "NotFound", "NonExistent"} or any(
                token in messages for token in ("does not exist", "not found", "deleted")):
            return {"__typename": "TweetNotFound"}
        if codes & {"179", "63"} or any(token in messages for token in (
                "not authorized", "protected", "suspended", "unavailable")):
            return {"__typename": "TweetUnavailable"}
        return None

    return data.get("data", {}).get("tweetResult", {}).get("result", {})


def fetch_semantic_tweet(tweet_id: str, observed_via: str):
    """Recover a retry observation after it falls out of the user timeline."""
    if not _TWEET_ID_RE.fullmatch(str(tweet_id)):
        return None
    detail = _semantic_detail(str(tweet_id))
    if detail.get("status") != "complete":
        return {"id": str(tweet_id), "text": "", "semantic_bundle": {
            "schema_version": SEMANTIC_BUNDLE_SCHEMA_VERSION,
            "resolver_version": SEMANTIC_BUNDLE_RESOLVER_VERSION,
            "observation": {"outer_id": str(tweet_id), "observed_via": observed_via,
                            "fetch_mode": "detail_retry"},
            "anchor": {"tweet_id": str(tweet_id), "author": observed_via, "text": ""},
            "repost_path": [], "context_nodes": [], "assets": [], "article_refs": [],
            "identity": {"bundle_key": "", "alias_keys": [], "context_keys": [],
                         "article_keys": []},
            "resolution": {"status": "context_unavailable_terminal"
                           if detail.get("status") == "terminal"
                           else "context_unresolved_transient",
                           "required_context_complete": False, "depth": 0, "node_count": 0,
                           "request_count": 0 if detail.get("cache_hit") else 1,
                           "cache_hits": 1 if detail.get("cache_hit") else 0,
                           "reasons": [str(detail.get("reason") or detail.get("status"))]},
        }}
    raw = detail["node"]
    bundle = resolve_semantic_bundle(raw, observed_via, fetch_mode="detail_retry")
    snap = _tweet_result_snapshot(raw, fallback_author=observed_via)
    recovered = {"id": str(tweet_id), "text": snap.get("text", ""),
                 "created_at": snap.get("created_at", ""),
                 "entities": snap.get("entities", {}),
                 "extended_entities": snap.get("extended_entities", {}),
                 "media": snap.get("media", []), "semantic_bundle": bundle}
    reply_parent = _tweet_result_reply_parent(raw)
    if reply_parent:
        recovered["in_reply_to_status"] = reply_parent
    return recovered


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
