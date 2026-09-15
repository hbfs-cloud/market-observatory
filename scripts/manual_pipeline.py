#!/usr/bin/env python3
"""One explicit, resumable acquisition run. No cron, implicit publication or GC."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time

import yaml

from cache_common import put_immutable, real_directory, writer_lock
from incremental_collector import Collector, fetch_chart, load_universe, utc_seconds
from object_archive import Archive
from release_transport import GitHub, Transport
from vendor.immutable_cache_release import canonical_bytes, need, Refusal, sha256_bytes


def run_once(config, work, run_id, universe, since, until, intervals, key_file, *,
             parent=None, transport=None, publish=False, inventories=(), observations=(),
             fetcher=fetch_chart, clock=time.time):
    need(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", run_id), "invalid run ID")
    need(config["automatic_scheduling"] is False, "manual configuration required")
    need(not publish or transport is not None, "publication requires transport")
    need(since < until <= clock(), "invalid run bounds")
    rows = load_universe(universe)
    work = real_directory(work)
    with writer_lock(work):
        run = real_directory(work / "runs" / run_id)
        intent = {"format": "marketdata-run-v2", "run_id": run_id, "since": since, "until": until,
                  "intervals": sorted(set(intervals)), "universe_sha256": sha256_bytes(universe.read_bytes()),
                  "config_sha256": sha256_bytes(canonical_bytes(config)), "parent": parent,
                  "inventories": [{"path": str(p.absolute()), "sha256": sha256_bytes(p.read_bytes())} for p in inventories],
                  "observations": [{"path": str(p.absolute()), "sha256": sha256_bytes(p.read_bytes())} for p in observations]}
        intent_path = run / "intent.json"
        if intent_path.exists():
            stored = json.loads(intent_path.read_text())
            need(stored.get("format") in ("marketdata-run-v1", "marketdata-run-v2"), "unsupported run intent")
            intent["format"] = stored["format"]
            if intent["format"] == "marketdata-run-v2":
                intent["archive_as_of"] = stored["archive_as_of"]
        else:
            # The archive vintage is not the requested data horizon: a late
            # backfill must not move publication backwards behind its parent.
            intent["archive_as_of"] = datetime.fromtimestamp(clock(), timezone.utc).isoformat()
        put_immutable(intent_path, canonical_bytes(intent))
        index, previous = None, None
        if parent:
            need(transport is not None, "parent requires pinned transport")
            archive, ready, index = transport.mount(parent["tag"], parent["ready_pin"], work / "archive", key_file)
            previous = ready["catalog_pin"]
        else:
            archive = Archive(work / "archive", key_file, create=True,
                              pack_bytes=config["archive"]["pack_bytes"],
                              compression_level=config["archive"]["compression_level"])
        archive.pack_bytes = config["archive"]["pack_bytes"]
        collector = Collector(work / "collector", config["collector"], fetcher=fetcher, clock=clock)
        with closing(collector.connect()) as db:
            fresh = db.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0
            fresh = fresh and db.execute("SELECT count(*) FROM coverage").fetchone()[0] == 0
            bound = db.execute("SELECT value FROM meta WHERE key='archive_parent'").fetchone()
            need(not bound or bound[0] == previous, "producer ledger belongs to another parent; use a fresh work directory")
        # A reference-only backfill has no acquisition ledger. It must not claim
        # raw-bar coverage, but it is a valid parent for the first collection.
        has_checkpoint = parent and "root/_producer/checkpoint.json" in archive.catalog(previous)["files"]
        if has_checkpoint and fresh:
            checkpoint_root = run / "parent-checkpoint"
            if checkpoint_root.exists():
                from vendor.immutable_cache_release import verify_directory
                verify_directory(checkpoint_root / ".marketdata-receipt/snapshot.json", checkpoint_root)
            else:
                archive.restore(previous, checkpoint_root, work / "client-cache", paths=["root/_producer/checkpoint.json"])
            collector.restore_checkpoint(checkpoint_root / "root/_producer/checkpoint.json")
        with closing(collector.connect()) as db, db:
            if previous:
                db.execute("INSERT OR IGNORE INTO meta VALUES ('archive_parent',?)", (previous,))
        prepared_path = run / "prepared.json"
        if prepared_path.exists():
            prepared = json.loads(prepared_path.read_text())
            archive.catalog(prepared["catalog_pin"])
        else:
            export = run / "closed"
            if export.exists():
                from vendor.immutable_cache_release import portable_snapshot
                portable_snapshot(export / "inventory.json")
                status = json.loads((export / "collection.json").read_text())
            else:
                for inventory in observations:
                    collector.import_observations(inventory)
                for interval in sorted(intent["intervals"], key=lambda i: config["collector"]["window_seconds"][i]):
                    step = config["collector"]["window_seconds"][interval]
                    stop = min(until, int(clock()) - config["collector"]["publication_lag_seconds"]) // step * step
                    start = since // step * step
                    span = config["collector"]["max_windows_per_plan"] * step
                    budget = config["collector"]["plan_jobs_by_interval"][interval]
                    with closing(collector.connect()) as db:
                        backlog = db.execute("""SELECT count(*) FROM jobs WHERE state NOT IN ('observed','unavailable')
                            AND json_extract(request,'$.interval')=?""", (interval,)).fetchone()[0]
                    budget = max(0, budget - backlog)
                    for right in range(stop, start, -span):
                        if budget <= 0:
                            break
                        budget -= collector.plan(rows, max(start, right - span), right, interval, max_new_jobs=budget)
                # A closed run never fetches again after an interrupted preparation.
                status = collector.run()
                collector.export(export, include_checkpoint=True)
            for inventory in [*inventories, export / "inventory.json"]:
                prepared = archive.prepare(inventory, parent=previous)
                previous = prepared["catalog_pin"]
            prepared.update(collection=status, run_id=run_id)
            prepared["parent_checkpoint_present"] = bool(has_checkpoint)
            put_immutable(prepared_path, canonical_bytes(prepared))
        if publish:
            result = transport.publish(archive, prepared["catalog_pin"],
                                       intent.get("archive_as_of", datetime.fromtimestamp(until, timezone.utc).isoformat()),
                                       parent=index)
            put_immutable(run / "published.json", canonical_bytes({k: result[k] for k in ("tag", "ready_pin", "catalog_pin")}))
            return {**prepared, **result, "published": True, "automatic_scheduling": False}
        return {**prepared, "published": False, "automatic_scheduling": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--transport-config", required=True, type=Path)
    parser.add_argument("--key-file", type=Path)
    parser.add_argument("--work", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--universe", required=True, type=Path)
    parser.add_argument("--since", required=True)
    parser.add_argument("--until", required=True)
    parser.add_argument("--interval", action="append", choices=("1d", "1m"), required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--bootstrap", action="store_true", help="Explicit first run; never falls back after a remote error")
    source.add_argument("--parent", type=Path, help="Pinned resolve receipt with tag and ready_pin")
    parser.add_argument("--inventory", type=Path, action="append", default=[], help="Closed legacy/reference datasets to append")
    parser.add_argument("--observations", type=Path, action="append", default=[], help="Compatible closed bar receipts reused before fetch")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--publish", action="store_true", help="Separate explicit authorization for remote writes")
    args = parser.parse_args()
    try:
        config = yaml.safe_load(args.config.read_text())
        transport_config = yaml.safe_load(args.transport_config.read_text())
        need(config["schema_version"] == transport_config["schema_version"] == 1
             and transport_config["automatic_scheduling"] is False, "manual versioned policy required")
        parent = json.loads(args.parent.read_text()) if args.parent else None
        need(not args.publish or args.execute, "--publish also requires --execute")
        if not args.execute:
            print(json.dumps({"executed": False, "symbols": len(load_universe(args.universe)),
                              "since": args.since, "until": args.until, "intervals": args.interval,
                              "automatic_scheduling": False}))
            return 0
        transport = Transport(GitHub(transport_config), transport_config)
        result = run_once(config, args.work, args.run_id, args.universe, utc_seconds(args.since),
                          utc_seconds(args.until), args.interval, args.key_file or Path(config["archive"]["key_file"]),
                          parent=parent, transport=transport, publish=args.publish, inventories=args.inventory,
                          observations=args.observations)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (Refusal, OSError, ValueError, KeyError) as exc:
        parser.exit(1, f"run refused: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
