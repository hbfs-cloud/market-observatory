#!/usr/bin/env python3
"""Strict append-only bitemporal event selection; never invent historical dates."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from release_transport import timestamp
from vendor.immutable_cache_release import canonical_bytes, is_hex64, need, Refusal, sha256_file


def validate(record):
    required = {"dataset", "entity_id", "event_id", "revision", "known_from", "observed_at",
                "valid_from", "valid_to", "status", "source_document_sha256", "payload"}
    need(isinstance(record, dict) and set(record) == required, "PIT record fields differ")
    need(all(isinstance(record[k], str) and record[k] for k in ("dataset", "entity_id", "event_id")), "stable PIT IDs required")
    need(type(record["revision"]) is int and record["revision"] >= 0, "invalid revision")
    need(record["status"] in ("active", "retracted") and isinstance(record["payload"], dict), "invalid PIT event")
    need(is_hex64(record["source_document_sha256"]), "source-document pin required")
    known, observed = timestamp(record["known_from"]), timestamp(record["observed_at"])
    need(known <= observed, "historical knowledge cannot follow actual observation")
    begin = timestamp(record["valid_from"])
    if record["valid_to"] is not None:
        need(begin < timestamp(record["valid_to"]), "empty/reversed validity interval")
    return record


def select(records, effective_at, known_at, *, knowledge_policy="observed"):
    need(knowledge_policy in ("observed", "public"), "explicit knowledge policy required")
    effective, knowledge = timestamp(effective_at), timestamp(known_at)
    events, seen = {}, set()
    for row in records:
        validate(row)
        key = (row["dataset"], row["entity_id"], row["event_id"])
        identity = (*key, row["revision"])
        need(identity not in seen, "duplicate PIT event revision")
        seen.add(identity)
        events.setdefault(key, []).append(row)
    selected = []
    for revisions in events.values():
        revisions.sort(key=lambda r: (timestamp(r["known_from"]), r["revision"]))
        need(all(a["revision"] < b["revision"] and timestamp(a["known_from"]) < timestamp(b["known_from"])
                 for a, b in zip(revisions, revisions[1:])), "ambiguous/nonmonotonic revision history")
        visible = [r for r in revisions if timestamp(r["known_from"]) <= knowledge
                   and (knowledge_policy == "public" or timestamp(r["observed_at"]) <= knowledge)]
        if not visible:
            continue
        # Select the correction BEFORE filtering validity. Otherwise a correction
        # moving an effective date can accidentally resurrect the superseded fact.
        row = visible[-1]
        if row["status"] == "active" and timestamp(row["valid_from"]) <= effective:
            if row["valid_to"] is None or effective < timestamp(row["valid_to"]):
                selected.append(row)
    return sorted(selected, key=lambda r: (r["dataset"], r["entity_id"], r["event_id"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="Pinned, restored normalized JSONL")
    parser.add_argument("--input-sha256", required=True)
    parser.add_argument("--effective-at", required=True)
    parser.add_argument("--known-at", required=True)
    parser.add_argument("--knowledge-policy", choices=("observed", "public"), default="observed")
    args = parser.parse_args()
    try:
        need(is_hex64(args.input_sha256) and sha256_file(args.input) == args.input_sha256, "PIT input pin differs")
        with args.input.open() as handle:
            result = select((json.loads(line) for line in handle if line.strip()), args.effective_at, args.known_at,
                            knowledge_policy=args.knowledge_policy)
        need(sha256_file(args.input) == args.input_sha256, "PIT input changed during selection")
        print(canonical_bytes({"records": result, "knowledge_policy": args.knowledge_policy,
                              "pit_source_certification": "external gate required"}).decode(), end="")
        return 0
    except (Refusal, ValueError, OSError, KeyError) as exc:
        parser.exit(1, f"PIT selection refused: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
