#!/usr/bin/env python3
"""Normalize pinned observations without inventing announcement or membership dates."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import gzip
import io
import json
from pathlib import Path
import shutil
import tempfile

import yaml

from cache_common import publish_directory, put_immutable, real_directory
from incremental_collector import validate_payload
from pit_records import select, validate
from release_transport import timestamp
from vendor.immutable_cache_release import (
    Refusal, canonical_bytes, inventory_from_roots, is_hex64, need, sha256_bytes,
)


def utc(value):
    need(type(value) in (int, float), "numeric observation timestamp required")
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def decimal_value(value, *, positive=False):
    need(not isinstance(value, bool), "boolean amount refused")
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        raise Refusal("invalid numeric amount") from None
    need(number.is_finite() and (number > 0 if positive else number >= 0), "invalid numeric amount")
    return str(number.normalize())


def chart_actions(receipt, source_pin):
    request = receipt["request"]
    need(receipt.get("schema_version") == 1 and receipt.get("coverage") == "observations_only",
         "native observation receipt required")
    validate_payload(receipt["payload"], request)
    observed = utc(receipt["observed_at"])
    need(timestamp(observed) <= datetime.now(timezone.utc), "future observation refused")
    data = receipt["payload"]["chart"]["result"][0]
    entity = f"{request['provider']}:{request['symbol']}"
    result = []
    for category, actions in data.get("events", {}).items():
        need(category in ("dividends", "splits", "capitalGains") and isinstance(actions, dict),
             "unknown corporate action structure")
        for provider_id, event in sorted(actions.items()):
            need(isinstance(event, dict) and type(event.get("date")) is int, "effective timestamp required")
            effective = utc(event["date"])
            values = {"numerator": decimal_value(event["numerator"], positive=True),
                      "denominator": decimal_value(event["denominator"], positive=True)} if category == "splits" else {
                          "amount": decimal_value(event["amount"])}
            payload = {"action_type": category, "provider_event_id": provider_id, **values,
                       "provider_symbol": request["symbol"], "instrument_id": None,
                       "announcement_at": None, "availability_basis": "local_observation_only",
                       "currency": data["meta"].get("currency"), "currency_basis": "current_response_metadata",
                       "classification": "corporate_action_observation_non_certified", "raw_event": event}
            result.append(validate({"dataset": "corporate_action_observations", "entity_id": entity,
                          "event_id": f"{category}:{provider_id}", "revision": 0,
                          "known_from": observed, "observed_at": observed, "valid_from": effective,
                          "valid_to": None, "status": "active", "source_document_sha256": source_pin,
                          "payload": payload}))
    return result


def index_snapshot(raw, source_pin, index_id, snapshot_at, observed_at):
    instant, observed = timestamp(snapshot_at), timestamp(observed_at)
    need(instant <= observed <= datetime.now(timezone.utc), "snapshot cannot predate observation or be observed in future")
    rows = csv.DictReader(io.StringIO(raw.decode("utf-8-sig"), newline=""))
    need(rows.fieldnames and "ticker" in rows.fieldnames and len(rows.fieldnames) == len(set(rows.fieldnames)),
         "index CSV requires unique columns and ticker")
    members, seen = [], set()
    for row in rows:
        need(None not in row and all(v is not None for v in row.values()), "ragged index CSV refused")
        symbol = row["ticker"].strip()
        need(symbol and symbol not in seen, "empty/duplicate index constituent")
        seen.add(symbol)
        weight = row.get("weight", "").strip()
        members.append({"provider_symbol": row.get("yahoo", symbol), "ticker": symbol,
                        "instrument_id": None, "weight_unscaled": decimal_value(weight) if weight else None,
                        "raw_fields": row})
    # A complete snapshot, including an empty set, is one event. Never infer
    # membership additions/removals between two sparsely sampled dates.
    return [validate({"dataset": "index_membership_snapshots", "entity_id": index_id,
                      "event_id": snapshot_at, "revision": 0, "known_from": observed_at,
                      "observed_at": observed_at, "valid_from": snapshot_at,
                      "valid_to": (instant + timedelta(seconds=1)).isoformat(), "status": "active",
                      "source_document_sha256": source_pin,
                      "payload": {"index_id": index_id, "snapshot_at": snapshot_at, "members": members,
                                  "announced_at": None, "effective_from": None, "effective_to": None,
                                  "classification": "legacy_index_snapshot_non_certified",
                                  "availability_basis": "local_observation_only", "forward_fill_allowed": False}})]


def revisions(rows, previous=()):
    events, output = {}, []
    previous = list(previous)
    now = datetime.now(timezone.utc).isoformat()
    select(previous, now, now)
    for row in sorted(previous, key=lambda r: timestamp(r["known_from"])):
        events[row["dataset"], row["entity_id"], row["event_id"]] = row
    for row in sorted(rows, key=lambda r: (timestamp(r["observed_at"]), r["entity_id"], r["event_id"])):
        key = row["dataset"], row["entity_id"], row["event_id"]
        previous = events.get(key)
        if previous:
            content = lambda r: (r["payload"], r["valid_from"], r["valid_to"], r["status"])
            if content(previous) == content(row):
                continue
            need(timestamp(previous["known_from"]) < timestamp(row["known_from"]),
                 "conflicting observations at the same timestamp")
            row = {**row, "revision": previous["revision"] + 1}
        events[key] = row
        output.append(row)
    return output


def normalize(args, config):
    need(not args.input.is_symlink() and args.input.is_file(), "regular pinned source required")
    need(args.input.stat().st_size <= config["max_input_bytes"], "source exceeds configured bound")
    raw = args.input.read_bytes()
    need(len(raw) <= config["max_input_bytes"], "source grew beyond configured bound")
    need(is_hex64(args.input_sha256) and sha256_bytes(raw) == args.input_sha256, "source SHA differs")
    history, history_pins = [], []
    for name, pin in getattr(args, "history", None) or []:
        path = Path(name)
        need(not path.is_symlink() and path.stat().st_size <= config["max_input_bytes"], "invalid history file")
        data = path.read_bytes()
        need(is_hex64(pin) and sha256_bytes(data) == pin, "history SHA differs")
        history.extend(json.loads(line) for line in data.splitlines() if line.strip())
        need(len(history) <= config["max_records"], "history record budget exceeded")
        history_pins.append(pin)
    if args.kind == "index-snapshot":
        need(args.index_id and args.snapshot_at and args.observed_at, "index ID/snapshot/observation required")
        rows = index_snapshot(raw, args.input_sha256, args.index_id, args.snapshot_at, args.observed_at)
    else:
        rows = []
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
            receipts = 0
            while line := stream.readline(config["max_line_bytes"] + 1):
                need(len(line) <= config["max_line_bytes"], "receipt line exceeds bound")
                receipts += 1
                need(receipts <= config["max_records"], "receipt budget exceeded")
                rows.extend(chart_actions(json.loads(line), args.input_sha256))
                need(len(rows) <= config["max_records"], "event budget exceeded")
    need(sum(len(r["payload"].get("members", [])) or 1 for r in rows) <= config["max_records"],
         "normalized row budget exceeded")
    rows = revisions(rows, history)
    destination = args.destination.absolute()
    need(not destination.exists() and not destination.is_symlink(), "destination already exists")
    real_directory(destination.parent)
    stage = Path(tempfile.mkdtemp(prefix=".pit-normalize-", dir=destination.parent))
    try:
        put_immutable(stage / "root" / "sources" / args.input_sha256, raw)
        normalized = b"".join(canonical_bytes(row) for row in rows)
        put_immutable(stage / "root" / "normalized" / f"{sha256_bytes(normalized)}.jsonl", normalized)
        temporary = stage / "temporary-inventory.json"
        inventory = inventory_from_roots([f"root={stage / 'root'}"], temporary)
        inventory["source_roots"] = {"root": str(destination / "root")}
        for item in inventory["files"]:
            item["path"] = str(destination / Path(item["path"]).relative_to(stage))
        put_immutable(stage / "inventory.json", canonical_bytes(inventory))
        temporary.unlink()
        report = {"rows": len(rows), "kind": args.kind, "source_sha256": args.input_sha256,
                  "history_sha256": history_pins, "output_semantics": "append_only_delta",
                  "pit_complete": False, "historical_announcement_certified": False,
                  "inventory_sha256": sha256_bytes(canonical_bytes(inventory))}
        put_immutable(stage / "normalization.json", canonical_bytes(report))
        publish_directory(stage, destination)
        return report
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("chart-actions", "index-snapshot"))
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--input-sha256", required=True)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--index-id")
    parser.add_argument("--snapshot-at")
    parser.add_argument("--observed-at")
    history = parser.add_mutually_exclusive_group(required=True)
    history.add_argument("--bootstrap", action="store_true", help="Explicit first normalized lot")
    history.add_argument("--history", action="append", nargs=2, metavar=("JSONL", "SHA256"),
                         help="All pinned earlier normalized deltas, not a mutable latest view")
    args = parser.parse_args()
    try:
        config = yaml.safe_load(args.config.read_text())
        need(config["schema_version"] == 1, "unknown normalization schema")
        print(json.dumps(normalize(args, config), sort_keys=True))
        return 0
    except (Refusal, ValueError, OSError, KeyError, TypeError) as exc:
        parser.exit(1, f"normalization refused: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
