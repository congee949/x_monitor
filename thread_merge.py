"""Coalesce selected official text threads without advancing seen."""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# Only release/API announcement threads benefit from a short idle window.
# Entitlement and quota notices must be delivered on their first observation.
IDLE_WINDOW_EVENT_TYPES = frozenset({
    "dev_release", "model_api", "model_launch", "major_product_launch",
})


def timestamp(tweet):
    value = tweet.get('createdAt') or tweet.get('created_at') or ''
    try:
        stamp = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except (ValueError, TypeError):
        try:
            stamp = parsedate_to_datetime(str(value))
        except (ValueError, TypeError, OverflowError):
            return None
    return stamp.astimezone(timezone.utc) if stamp.tzinfo is not None else None


def has_context_or_media(tweet):
    bundle = tweet.get('semantic_bundle') or {}
    return bool(tweet.get('media') or (tweet.get('entities') or {}).get('media')
                or (tweet.get('extended_entities') or {}).get('media')
                or tweet.get('quoted_status') or tweet.get('retweeted_status')
                or tweet.get('article') or bundle.get('assets') or bundle.get('context_nodes'))


def body(tweet):
    return (tweet.get('note_tweet') or {}).get('text') or tweet.get('text') or ''


def merge_ready(items, username, *, enabled, now=None, idle_seconds=90, deferred=None):
    if not enabled:
        return items
    now = now or datetime.now(timezone.utc)
    result = []
    for tweet, reason in items:
        parent = tweet.get('in_reply_to_status') or {}
        author = str(parent.get('screen_name') or '').lstrip('@').casefold()
        conversation = str(tweet.get('conversation_id_str') or '')
        prior = result[-1][0] if result else {}
        members = prior.get('_official_thread_members') or [prior]
        ids = {str(t.get('id')) for t in members}
        if (tweet.get("_push_event_type") in IDLE_WINDOW_EVENT_TYPES
                and prior.get("_push_event_type") in IDLE_WINDOW_EVENT_TYPES
                and conversation and conversation == str(prior.get('conversation_id_str') or prior.get('id') or '')
                and str(parent.get('id') or '') in ids and author == username.lstrip('@').casefold()
                and timestamp(tweet) is not None and all(timestamp(t) is not None for t in members)
                and not has_context_or_media(tweet) and not has_context_or_media(prior)
                and len(body(tweet)) + len(body(prior)) < 3000):
            combined = dict(prior)
            merged_body = body(prior) + '\n\n' + body(tweet)
            entities = {}
            for member in members + [tweet]:
                for source in (member.get('entities') or {},
                               (member.get('note_tweet') or {}).get('entities') or {}):
                    for key, values in source.items():
                        if isinstance(values, list):
                            entities.setdefault(key, []).extend(values)
            combined.pop('semantic_bundle', None)
            combined.update(text=merged_body, entities=entities,
                            note_tweet={'text': merged_body, 'entities': entities},
                            _official_thread_members=members + [tweet], _semantic_active=False)
            result[-1] = (combined, reason)
        else:
            result.append((tweet, reason))
    ready = []
    for tweet, reason in result:
        members = tweet.get('_official_thread_members') or [tweet]
        stamps = [timestamp(member) for member in members]
        # Callers without a durable retry queue get immediate separate cards.
        # A caller with deferred persists every original member before exiting.
        active = (all(member.get("_push_event_type") in IDLE_WINDOW_EVENT_TYPES
                      for member in members)
                  and not has_context_or_media(tweet) and all(stamps)
                  and 0 <= (now - max(stamps)).total_seconds() < idle_seconds)
        if active:
            target = deferred if deferred is not None else ready
            target.extend((member, reason) for member in members)
        else:
            ready.append((tweet, reason))
    return ready
