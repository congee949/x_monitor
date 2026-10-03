"""Target-scoped folding for confirmed source translations and official quotes."""
import json
import sqlite3
from datetime import datetime, timedelta, timezone


def normalize_groups(value):
    if not isinstance(value, dict):
        return []
    return [frozenset(str(name).lstrip('@').casefold() for name in names)
            for names in value.values() if isinstance(names, list) and len(names) > 1]


def confirmed_source(path, tweet_id, chat_id, thread_id):
    """Read only. Pending, ambiguous, other-target and expired rows never anchor."""
    try:
        db = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True)
        db.row_factory = sqlite3.Row
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
            row = db.execute('''SELECT d.message_id,d.facts_json,d.username,d.tweet_id
                FROM deliveries d WHERE d.target_chat_id=? AND d.target_thread_id=?
                AND d.state='confirmed' AND d.updated_at>=? AND d.message_id IS NOT NULL
                AND (d.tweet_id=? OR d.message_id IN (
                    SELECT a.message_id FROM tweet_anchors a WHERE a.target_chat_id=?
                    AND a.target_thread_id=? AND a.tweet_id=? AND a.updated_at>=?))
                ORDER BY d.updated_at ASC LIMIT 1''',
                (str(chat_id), '' if thread_id is None else str(thread_id), cutoff,
                 str(tweet_id), str(chat_id), '' if thread_id is None else str(thread_id),
                 str(tweet_id), cutoff)).fetchone()
            if row is None or int(row['message_id']) <= 0:
                return None
            facts = json.loads(row['facts_json'])
            if not isinstance(facts, list):
                return None
            return dict(row, message_id=int(row['message_id']), facts=facts)
        finally:
            db.close()
    except (OSError, sqlite3.Error, ValueError, TypeError):
        return None


def _plan(tweet, username, *, chat_id, thread_id, ledger_path,
          translation_reply, groups, facts):
    """Return deliver/reply/skip; source identity and bundle identity stay distinct."""
    result = {'action': 'deliver', 'reason': 'no_confirmed_source'}
    if not translation_reply and not groups:
        return result
    bundle = tweet.get('semantic_bundle') or {}
    if not isinstance(bundle, dict):
        return result
    if bundle and (bundle.get('resolution') or {}).get('status') != 'complete':
        return result
    contexts = bundle.get('context_nodes') or []
    source = contexts[0] if contexts and isinstance(contexts[0], dict) else tweet.get('quoted_status') or {}
    if not isinstance(source, dict):
        return result
    source_id = str(source.get('tweet_id') or source.get('id') or '')
    translation = tweet.get('_quote_translation') or {}
    is_translation = translation_reply and translation.get('action') == 'source_only'
    if is_translation:
        source_id = str(translation.get('source_id') or '')
    author = source.get('author') or source.get('screen_name') or ''
    if isinstance(author, dict):
        author = author.get('screen_name') or author.get('username') or ''
    candidate_author = (bundle.get('anchor') or {}).get('author') or username
    official = any(str(candidate_author).lstrip('@').casefold() in group
                   and str(username).lstrip('@').casefold() in group
                   and str(author).lstrip('@').casefold() in group for group in groups)
    if not source_id or not (is_translation or official):
        return result
    prior = confirmed_source(ledger_path, source_id, chat_id, thread_id)
    if prior is None:
        return result
    result.update(source_id=source_id, message_id=prior['message_id'], action='reply',
                  target_chat_id=str(chat_id), target_thread_id=thread_id)
    if is_translation:
        result['reason'] = 'translation_source_delivered'
        return result
    # An image may carry facts absent from its caption. Preserve the whole card as reply.
    anchor = bundle.get('anchor') or tweet
    own_media = anchor.get('media') or (anchor.get('entities') or {}).get('media') or (anchor.get('extended_entities') or {}).get('media')
    result['reason'] = 'official_quote_update'
    if prior['facts'] and set(facts) <= set(prior['facts']) and not own_media:
        result.update(action='skip', reason='official_quote_no_new_facts')
    return result

def plan(tweet, username, *, chat_id, thread_id, ledger_path,
         translation_reply, groups, facts):
    """Malformed optional metadata never blocks the normal delivery path."""
    try:
        return _plan(tweet, username, chat_id=chat_id, thread_id=thread_id,
                     ledger_path=ledger_path, translation_reply=translation_reply,
                     groups=groups, facts=facts)
    except (AttributeError, IndexError, KeyError, TypeError, ValueError, OverflowError):
        return {'action': 'deliver', 'reason': 'invalid_fold_metadata'}
