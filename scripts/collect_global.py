#!/usr/bin/env python3
"""Explicit full-universe backfill, with a durable ledger and bounded execution."""
import argparse
from contextlib import closing
import json
from pathlib import Path
import shutil
import time

import yaml

from cache_common import put_immutable, real_directory, writer_lock
from incremental_collector import Collector, load_universe, utc_seconds
from vendor.immutable_cache_release import canonical_bytes, need, sha256_bytes


def run(config, base, work, until, *, execute=False, max_batches=None, fetcher=None, clock=time.time):
    need(config["automatic_scheduling"] is False, "manual execution required")
    universe = base / config["universe"]
    raw = universe.read_bytes()
    inventory = json.loads((base / config["universe_inventory"]).read_text())
    need(sha256_bytes(raw) == inventory["universe_sha256"], "universe pin differs")
    rows = load_universe(universe)
    need(len(rows) == inventory["rows"], "universe count differs")
    need(max_batches is None or type(max_batches) is int and max_batches > 0, "invalid batch budget")
    need(until <= int(clock()) - config["collector"]["publication_lag_seconds"], "explicit closed horizon required")
    work = real_directory(work)
    with writer_lock(work):
        kwargs = {"clock": clock}
        if fetcher is not None:
            kwargs["fetcher"] = fetcher
        collector = Collector(work / "collector", config["collector"], **kwargs)
        intervals = config["intraday_intervals"]
        need(isinstance(intervals, list) and intervals and len(set(intervals)) == len(intervals)
             and set(intervals) <= {"1m", "15m", "1h"}, "invalid intraday resolutions")
        anchor = utc_seconds(config["intraday_start"])
        policy = {"format": "marketdata-global-policy-v2", "universe_sha256": sha256_bytes(raw),
                  "daily_start": config["daily_start"], "intraday_start": config["intraday_start"],
                  "intraday_intervals": intervals, "price_contract": config["collector"]["price_contract"]}
        need(not (work / "intent.json").exists(), "legacy backfill directory; use a new accumulator")
        put_immutable(work / "policy.json", canonical_bytes(policy))
        windows = {"1d": (utc_seconds(config["daily_start"]), until // 86400 * 86400)}
        for interval in intervals:
            step = config["collector"]["window_seconds"][interval]
            duration = config["collector"]["bar_seconds"][interval]
            need(anchor % step == 0 and duration > 0, "invalid intraday start or bar duration")
            # A session may anchor hourly bars at :30 or :15. Reserve a full
            # duration beyond the window of opening timestamps before fetching.
            windows[interval] = (anchor, (until - duration) // step * step)
        intent = {"format": "marketdata-global-run-v2", "universe_sha256": sha256_bytes(raw),
                  "config_sha256": sha256_bytes(canonical_bytes(config)), "symbols": len(rows),
                  "until": until, "windows": windows, "pit_complete": False}
        intent_path = work / "runs" / sha256_bytes(canonical_bytes(intent)) / "intent.json"
        put_immutable(intent_path, canonical_bytes(intent))
        for interval, (start, end) in windows.items():
            if start < end:
                collector.plan_recent_revisions(rows, start, end, interval)
                collector.plan(rows, start, end, interval)
        if execute:
            started = time.monotonic()
            batches = 0
            while time.monotonic() - started < config["max_run_seconds"]:
                need(shutil.disk_usage(work).free >= config["minimum_free_bytes"], "disk reserve reached")
                before = clock()
                status = collector.run()
                batches += 1
                print(json.dumps({"progress": status, "symbols": len(rows)}, sort_keys=True), flush=True)
                with closing(collector.connect()) as db:
                    meta = dict(db.execute("SELECT key,value FROM meta"))
                    due = db.execute("SELECT count(*) FROM jobs WHERE state IN ('pending','retry') AND retry_at<=?",
                                     (before,)).fetchone()[0]
                if not due or meta.get("blocked") == "true" or float(meta.get("cooldown", "0")) > clock():
                    break
                if max_batches is not None and batches >= max_batches:
                    break
        with closing(collector.connect()) as db:
            status = collector.summary(db)
            counts = [dict(row) for row in db.execute("""SELECT json_extract(request,'$.interval') interval,
                state,count(*) windows,count(DISTINCT json_extract(request,'$.symbol')) symbols
                FROM jobs GROUP BY interval,state ORDER BY interval,state""")]
            known = {}
            for row in db.execute("""SELECT json_extract(request,'$.symbol'),json_extract(request,'$.interval') FROM jobs
                UNION SELECT json_extract(contract,'$.symbol'),json_extract(contract,'$.interval') FROM coverage"""):
                known.setdefault(row[1], set()).add(row[0])
            required = {row["provider_symbol"] for row in rows}
            planned = len(set().union(*known.values()) & required) if known else 0
            for interval, (start, end) in windows.items():
                need(start >= end or required <= known.get(interval, set()),
                     "not all universe instruments were planned per interval")
        report = {**intent, **status, "executed": execute, "coverage": counts, "planned_symbols": planned}
        output = work / "reports" / (sha256_bytes(canonical_bytes(report)) + ".json")
        put_immutable(output, canonical_bytes(report))
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--until", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--max-batches", type=int, help="Optional bounded acceptance run; remaining work stays queued")
    args = parser.parse_args()
    result = run(yaml.safe_load(args.config.read_text()), Path(__file__).resolve().parents[1],
                 args.work, utc_seconds(args.until), execute=args.execute, max_batches=args.max_batches)
    print(json.dumps(result, sort_keys=True))
    return 0 if not args.execute or result["all_windows_observed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
