#!/usr/bin/env python3
"""Review X Monitor event-dedup observations without editing SQLite by hand."""

import argparse
import json

import twitter_monitor as tm


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=tm.EVENT_LEDGER_PATH)
    sub = parser.add_subparsers(dest="command", required=True)
    listing = sub.add_parser("list", help="list would-suppress candidates")
    listing.add_argument("--limit", type=int, default=100)
    label = sub.add_parser("label", help="label one candidate after human review")
    label.add_argument("id", type=int)
    verdict = label.add_mutually_exclusive_group(required=True)
    verdict.add_argument("--valid-suppression", action="store_true")
    verdict.add_argument("--false-positive", action="store_true")
    label.add_argument("--note", default="")
    sub.add_parser("report", help="print the enforce Go/No-Go report")
    args = parser.parse_args()

    if args.command == "list":
        payload = tm.event_review_rows(args.ledger, args.limit)
    elif args.command == "label":
        updated = tm.review_event_observation(
            args.id, args.false_positive, args.note, path=args.ledger)
        if not updated:
            parser.error(f"would-suppress observation {args.id} was not found")
        payload = {"updated": args.id, "false_positive": args.false_positive,
                   "report": tm.event_dedup_gate_report(args.ledger)}
    else:
        payload = tm.event_dedup_gate_report(args.ledger)
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
