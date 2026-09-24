#!/usr/bin/env python3
"""Safely move product accounts before their developer accounts.

The command preserves every account object and the relative order of all accounts
outside the requested precedence groups. It prints a plan by default; --apply is
required before changing a file.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from pathlib import Path

DEFAULT_GROUPS = (
    ("claudeai", "ClaudeDevs"),
    ("OpenAI", "OpenAIDevs", "thsottiaux"),
)


def canonical(value: object) -> str:
    return str(value or "").strip().lstrip("@").casefold()


def reorder(records: list[dict], groups=DEFAULT_GROUPS) -> tuple[list[dict], list[str]]:
    seen: dict[str, int] = {}
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"account at index {index} is not an object")
        name = canonical(record.get("username"))
        if not name:
            raise ValueError(f"account at index {index} has no username")
        if name in seen:
            raise ValueError(f"duplicate account username: {record.get('username')}")
        seen[name] = index

    result = list(records)
    changed: list[str] = []
    for group in groups:
        names = [canonical(name) for name in group]
        positions = sorted(seen[name] for name in names if name in seen)
        ordered = [records[seen[name]] for name in names if name in seen]
        if len(positions) < 2:
            continue
        if [result[index] for index in positions] != ordered:
            for index, record in zip(positions, ordered):
                result[index] = record
            changed.append(" -> ".join(record["username"] for record in ordered))
    return result, changed


def write_json(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(records, ensure_ascii=False, indent=2) + "\n"
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent,
                                    prefix=f".{path.name}.", delete=False) as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
        temporary = Path(stream.name)
    if path.exists():
        os.chmod(temporary, path.stat().st_mode & 0o777)
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="twitter_accounts.json")
    parser.add_argument("--output", type=Path, help="write to another path instead of path")
    parser.add_argument("--apply", action="store_true", help="write the planned order")
    parser.add_argument("--backup", action="store_true", help="with --apply, save path.bak first")
    args = parser.parse_args()
    source = args.path
    target = args.output or source
    records = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise SystemExit("twitter_accounts.json must contain a JSON array")
    updated, changed = reorder(records)
    before = [record.get("username") for record in records]
    after = [record.get("username") for record in updated]
    if not changed:
        print("No configured precedence group needs reordering.")
        return 0
    print("Planned account order:")
    print("  " + " -> ".join(map(str, before)))
    print("  " + " -> ".join(map(str, after)))
    print("Changed groups: " + ", ".join(changed))
    if not args.apply:
        print("Dry run only; pass --apply to write the file.")
        return 0
    if args.backup:
        backup = source.with_name(source.name + ".bak")
        if backup.exists():
            raise SystemExit(f"Backup already exists; preserve it and choose a separate backup: {backup}")
        shutil.copy2(source, backup)
        print(f"Backup: {backup}")
    write_json(target, updated)
    print(f"Wrote: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
