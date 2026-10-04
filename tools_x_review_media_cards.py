#!/usr/bin/env python3
"""Preview or edit the fixed review messages; config supplies the Telegram token.

preview performs no Telegram requests. apply takes the consumer lock and requires
the bound review SQLite sidecar, so the service must be stopped for that operation.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import html
import json
import os
from pathlib import Path
import sys


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    temp.chmod(0o600)
    temp.replace(path)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['preview', 'apply'])
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--packet', type=Path, required=True)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--media', type=Path, required=True)
    parser.add_argument('--state', type=Path)
    parser.add_argument('--config', type=Path)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--probe-video', action='store_true')
    args = parser.parse_args(argv)
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(args.repo.resolve()))
    import x_review_cards as cards
    from x_review_bot import Telegram, BotError, consumer_lock, positive_id
    packet = json.loads(args.packet.read_text())
    receipt = json.loads(args.receipt.read_text())
    media = json.loads(args.media.read_text())
    owner = positive_id(receipt['chat_id'])
    mapping = cards.bindings(packet, receipt, owner)
    digest = hashlib.sha256(json.dumps([packet, receipt, media], sort_keys=True).encode()).hexdigest()

    def build():
        selections = cards.read_selections(args.state, packet, mapping, owner, require_drained=args.command == 'apply') if args.state else {}
        values = cards.build_payloads(packet, receipt, media, owner, selections, probe_video=args.probe_video)
        return values, cards.progress(packet, selections)

    if args.command == 'preview':
        values, progress = build()
        save(args.out, {'input_sha256': digest, 'progress': progress, 'cards': values})
        sections = []
        for item in values:
            body = item['payload']['rich_message']['html']
            # Rich HTML video tags are self-closing; regular browsers need an end tag.
            import re
            body = re.sub(r'<video src="([^"]+)"/>', r'<video controls preload="metadata" src="\1"></video>', body)
            sections.append('<article>' + body + '</article>')
        args.out.with_suffix('.html').write_text('<!doctype html><meta charset="utf-8"><title>X review media preview</title>'
            '<style>body{max-width:850px;margin:auto;font:16px/1.6 system-ui;background:#eee}article{background:white;padding:28px;margin:25px 0}img,video{max-width:100%;max-height:600px}tg-collage{display:grid;grid-template-columns:1fr 1fr;gap:8px}</style>'
            '<p>' + html.escape(progress) + '</p>' + ''.join(sections))
        print(json.dumps({'cards': len(values), 'input_sha256': digest, 'progress': progress,
                          'photos': sum(v['payload']['rich_message']['html'].count('<img ') for v in values),
                          'videos': sum(v['payload']['rich_message']['html'].count('<video ') for v in values)}, ensure_ascii=False))
        return 0
    if not args.state or not args.config:
        parser.error('apply requires --state and --config')
    config = json.loads(args.config.read_text())
    if positive_id(config['telegram_chat_id']) != owner:
        raise ValueError('config owner mismatch')
    api = Telegram(config['telegram_bot_token'])
    # Do not race a human verdict: the live consumer owns this same lock.
    with consumer_lock(args.state):
        values, progress = build()
        audit = {'input_sha256': digest, 'started_at': datetime.now(timezone.utc).isoformat(),
                 'progress': progress, 'method': 'editMessageText', 'receipts': []}
        for item in values:
            payload = item['payload']
            entry = {'case_id': item['case_id'], 'message_id': payload['message_id']}
            try:
                result = api.call('editMessageText', payload)
                if not isinstance(result, dict) or result.get('message_id') != payload['message_id'] or str((result.get('chat') or {}).get('id')) != str(owner):
                    raise ValueError('response message identity mismatch')
                entry.update(ok=True, rich_message=result.get('rich_message'),
                             reply_markup=result.get('reply_markup'))
            except BotError as exc:
                if exc.code == 400 and 'message is not modified' in exc.description.lower():
                    entry.update(ok=True, unchanged=True)
                else:
                    entry.update(ok=False, error_code=exc.code)
            except Exception as exc:
                entry.update(ok=False, error_type=type(exc).__name__)
            audit['receipts'].append(entry)
            save(args.out, audit)
            if not entry['ok']:
                print(json.dumps({'failed_case': item['case_id'], 'audit': str(args.out)}))
                return 1
        print(json.dumps({'ok': len(audit['receipts']), 'input_sha256': digest, 'progress': progress}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    os.umask(0o077)
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({'error_type': type(exc).__name__}), file=sys.stderr)
        raise SystemExit(1)

