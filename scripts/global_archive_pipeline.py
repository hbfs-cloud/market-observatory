#!/usr/bin/env python3
"""Manual global accumulator to pinned private releases, with a durable outbox."""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time

import yaml

from cache_common import put_immutable, real_directory, writer_lock
from collect_global import run as collect
from incremental_collector import Collector, fetch_chart, utc_seconds
from object_archive import Archive
from producer_checkpoint import restore_from_archive
from release_transport import GitHub, Transport
from vendor.immutable_cache_release import canonical_bytes, need, portable_snapshot, sha256_bytes


def run(config, base, work, run_id, until, key_file, *, parent=None, transport=None,
        publish=False, max_batches=None, fetcher=fetch_chart, clock=time.time):
    need(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", run_id), "invalid run ID")
    need(config["automatic_scheduling"] is False, "manual configuration required")
    need(not (publish or parent) or transport is not None, "pinned transport required")
    work = real_directory(work)
    outbox = real_directory(work / "publication")
    with writer_lock(outbox):
        directory = real_directory(outbox / "runs" / run_id)
        intent_path = directory / "intent.json"
        if not intent_path.exists():
            published, superseded = {}, set()
            for prior in sorted((outbox / "runs").glob("*/intent.json")):
                receipt = prior.parent / "published.json"
                need(receipt.exists(), "resume the unfinished outbox before starting a new run")
                completed = json.loads(receipt.read_text())
                published[completed["tag"]] = completed["ready_pin"]
                ancestor = json.loads(prior.read_text())["parent"]
                if ancestor:
                    superseded.add(ancestor["tag"])
            leaves = set(published) - superseded
            need(len(leaves) <= 1, "outbox publication chain diverged")
            if leaves:
                expected = leaves.pop()
                need(parent and parent["tag"] == expected and parent["ready_pin"] == published[expected],
                     "new run must continue the previous published outbox pin")
        intent = {"format": "marketdata-global-publication-v1", "run_id": run_id, "until": until,
                  "parent": parent, "config_sha256": sha256_bytes(canonical_bytes(config)),
                  "universe_sha256": sha256_bytes((base / config["universe"]).read_bytes()),
                  "max_batches": max_batches}
        intent["archive_as_of"] = (json.loads(intent_path.read_text())["archive_as_of"] if intent_path.exists()
                                   else datetime.fromtimestamp(clock(), timezone.utc).isoformat())
        put_immutable(intent_path, canonical_bytes(intent))
        if (directory / "published.json").exists():
            return json.loads((directory / "published.json").read_text())
        ancestor_index, previous = None, None
        if parent:
            archive, ready, ancestor_index = transport.mount(parent["tag"], parent["ready_pin"], work / "archive", key_file)
            previous = ready["catalog_pin"]
        else:
            archive = Archive(work / "archive", key_file, create=True, pack_bytes=config["archive"]["pack_bytes"],
                              compression_level=config["archive"]["compression_level"])
        archive.pack_bytes = config["archive"]["pack_bytes"]
        collector = Collector(work / "collector", config["collector"], fetcher=fetcher, clock=clock)
        with closing(collector.connect()) as db:
            fresh = not db.execute("SELECT 1 FROM jobs LIMIT 1").fetchone() and not db.execute("SELECT 1 FROM coverage LIMIT 1").fetchone()
        if fresh and previous and "root/_producer/checkpoint.json" in archive.catalog(previous)["files"]:
            restore_from_archive(collector, archive, previous, directory / "parent-checkpoint", work / "client-cache")
        prepared_path = directory / "prepared.json"
        if prepared_path.exists():
            prepared = json.loads(prepared_path.read_text())
            archive.catalog(prepared["catalog_pin"])
        else:
            closed = directory / "closed"
            if not closed.exists():
                status = collect(config, base, work, until, execute=True, max_batches=max_batches, fetcher=fetcher, clock=clock)
                exported = collector.export(closed, include_checkpoint=True, only_unexported=True)
                status = {**status, "export": exported}
            else:
                portable_snapshot(closed / "inventory.json")
                status = json.loads((closed / "collection.json").read_text())
            # This also repairs a crash after closing files but before SQLite's
            # export acknowledgement committed. Closed sources are never refetched.
            collector.acknowledge_export(closed / "inventory.json")
            prepared = archive.prepare(closed / "inventory.json", parent=previous)
            prepared.update(collection=status, run_id=run_id, automatic_scheduling=False, pit_complete=False)
            put_immutable(prepared_path, canonical_bytes(prepared))
        if publish:
            result = {**prepared, **transport.publish(archive, prepared["catalog_pin"], intent["archive_as_of"], parent=ancestor_index),
                      "published": True}
            put_immutable(directory / "published.json", canonical_bytes(result))
            return result
        return {**prepared, "published": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--transport-config", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--until", required=True)
    parser.add_argument("--key-file", type=Path)
    parser.add_argument("--max-batches", type=int)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--parent", type=Path)
    source.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    config = yaml.safe_load(args.config.read_text())
    policy = yaml.safe_load(args.transport_config.read_text())
    need(config["automatic_scheduling"] is False and policy["automatic_scheduling"] is False, "manual execution required")
    need(not args.publish or args.execute, "publication also requires --execute")
    if not args.execute:
        print(json.dumps({"executed": False, "automatic_scheduling": False}))
        return 0
    result = run(config, Path(__file__).resolve().parents[1], args.work, args.run_id, utc_seconds(args.until),
                 args.key_file or Path(config["archive"]["key_file"]),
                 parent=json.loads(args.parent.read_text()) if args.parent else None,
                 transport=Transport(GitHub(policy), policy), publish=args.publish, max_batches=args.max_batches)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
