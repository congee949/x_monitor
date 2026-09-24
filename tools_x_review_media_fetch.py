#!/usr/bin/env python3
"""Export bounded X details to stdout; never change the monitor's runtime files.

Remote (IDS is a JSON array copied from the fixed review packet):
  ssh bwg python3 -B - /root/x_monitor "$IDS" < media_fetch.py > media-cache.json
"""
import copy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import re
import sys
import time


def collect(repo, ids, *, max_requests=48, deadline_seconds=240):
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(Path(repo).resolve()))
    import twitter_graphql as gql
    ids = list(dict.fromkeys(str(value) for value in ids))
    if not ids or len(ids) > 40 or any(not re.fullmatch(r'[0-9]{5,25}', tid) for tid in ids):
        raise ValueError('expected at most 40 tweet ids')
    start = time.monotonic()
    count = 0
    stopped = None
    original_curl = gql._curl

    def bounded_curl(*args, **kwargs):
        nonlocal count
        if count >= max_requests or time.monotonic() - start > deadline_seconds:
            raise RuntimeError('request_budget')
        count += 1
        with redirect_stdout(io.StringIO()):
            return original_curl(*args, **kwargs)

    gql._curl = bounded_curl
    guest = None
    try:
        cached = json.loads(Path(gql.GUEST_TOKEN_CACHE).read_text())
        if time.time() - cached.get('ts', 0) < gql.GUEST_TOKEN_TTL:
            guest = cached.get('token')
    except (OSError, ValueError, TypeError):
        pass

    def guest_memory_only():
        nonlocal guest
        if not guest:
            value = json.loads(bounded_curl('https://api.x.com/1.1/guest/activate.json',
                               {'Authorization': f'Bearer {gql.BEARER}'}, method='POST'))
            guest = value.get('guest_token')
        return guest

    auth_headers = gql._auth_headers()
    if auth_headers:
        gql._get_guest_token = lambda: 'authenticated'
        gql._gql_headers = lambda _token: auth_headers
    else:
        gql._get_guest_token = guest_memory_only
    nodes, statuses, provenance = {}, {}, {}

    def register(raw, source):
        node = gql._unwrap_tweet_result(raw)
        tid = str((node.get('legacy') or {}).get('id_str') or node.get('rest_id') or '')
        if not re.fullmatch(r'[0-9]{5,25}', tid) or tid in nodes:
            return
        nodes[tid] = copy.deepcopy(node)
        provenance[tid] = source
        statuses[tid] = 'complete'
        register(((node.get('legacy') or {}).get('retweeted_status_result') or {}).get('result'), source)
        register((node.get('quoted_status_result') or {}).get('result'), source)

    try:
        saved = json.loads((Path(repo) / '.semantic_detail_cache.json').read_text())
        for value in saved.get('entries', {}).values():
            if isinstance(value, dict) and value.get('node'):
                # Historical media are useful even after the resolver TTL has expired.
                register(value['node'], 'semantic_detail_cache')
    except (OSError, ValueError, TypeError):
        pass

    def fetch(tid):
        nonlocal stopped
        if tid in nodes or tid in statuses:
            return
        if stopped:
            statuses[tid] = stopped
            return
        try:
            raw = gql.fetch_article_tweet(tid, raise_errors=True)
            register(raw, 'graphql_detail')
            statuses[tid] = 'complete' if tid in nodes else 'unavailable'
        except Exception as exc:
            code = getattr(exc, 'status_code', None)
            reason = ('rate_limited' if code == 429 else f'http_{code}' if code else 'request_budget'
                      if isinstance(exc, RuntimeError) else type(exc).__name__)
            statuses[tid] = reason
            if code in (401, 403, 429) or isinstance(exc, RuntimeError):
                stopped = reason

    for tid in ids:
        fetch(tid)
    for _ in range(gql.SEMANTIC_MAX_DEPTH):
        missing = [(node, relation, tid) for node in list(nodes.values())
                   for relation, tid in gql._relation_missing_ids(node)]
        if not missing:
            break
        changed = False
        for node, relation, tid in missing:
            fetch(tid)
            if tid in nodes:
                gql._inject_relation(node, relation, copy.deepcopy(nodes[tid]))
                changed = True
        if not changed:
            break
    entries = {}
    for tid in ids:
        entries[tid] = {'status': statuses.get(tid, 'unavailable'),
                        'source': provenance.get(tid, 'graphql_detail')}
        if tid in nodes:
            entries[tid]['bundle'] = gql.build_semantic_bundle(nodes[tid], '', fetch_mode='review_export')
    return {'schema': 'x-review-media-v1', 'fetched_at': time.time(),
            'physical_requests': count, 'max_requests': max_requests,
            'elapsed_seconds': round(time.monotonic() - start, 2), 'entries': entries}


if __name__ == '__main__':
    try:
        print(json.dumps(collect(sys.argv[1], json.loads(sys.argv[2])), ensure_ascii=False))
    except Exception as exc:
        # URLs and exception strings can contain credentials.
        print(json.dumps({'error': type(exc).__name__}), file=sys.stderr)
        sys.exit(1)
