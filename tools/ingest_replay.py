#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Dead-letter replay / recovery CLI.

Usage:
  python tools/ingest_replay.py --list
  python tools/ingest_replay.py --replay <envelope_uuid>
  python tools/ingest_replay.py --replay-all
  python tools/ingest_replay.py --purge <envelope_uuid>

DLQ entries persist in Redis (``ingest:dlq``) when available and in
memory otherwise; ``--list`` without Redis reports the empty in-process
queue honestly instead of inventing entries.
"""
import argparse
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "bin"))

from lib.ingest import DeadLetterQueue, IngestionPipeline  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest DLQ replay")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list", action="store_true")
    group.add_argument("--replay", metavar="UUID")
    group.add_argument("--replay-all", action="store_true")
    group.add_argument("--purge", metavar="UUID")
    args = parser.parse_args()

    dlq = DeadLetterQueue()
    if args.list:
        entries = dlq.list_entries()
        print(f"dlq_depth={len(entries)}")
        for entry in entries:
            env = entry.envelope
            print(f"{env.get('envelope_uuid')} stage={entry.failure_stage} "
                  f"class={entry.error_class} attempts={entry.attempts} error={entry.error}")
        return 0
    if args.purge:
        removed = dlq.pop(args.purge)
        print(f"purged={bool(removed)} uuid={args.purge}")
        return 0
    pipeline = IngestionPipeline()
    pipeline.dlq = dlq
    pipeline.gate.dlq = dlq
    count = pipeline.replay_dlq()
    print(f"replayed={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
