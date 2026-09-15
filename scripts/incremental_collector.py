#!/usr/bin/env python3
"""Manual durable window collection. Observations are not PIT completeness."""
from __future__ import annotations

import argparse
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import datetime
from email.utils import parsedate_to_datetime
import gzip
import io
import json
import math
import os
from pathlib import Path
import sqlite3
import shutil
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import yaml

from cache_common import checked_blob, publish_directory, put_immutable, real_directory, writer_lock
from vendor.immutable_cache_release import (
    Refusal, canonical_bytes, inventory_from_roots, need, portable_parts,
    portable_snapshot, sha256_bytes, sha256_file, source_path,
)


def utc_seconds(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    need(parsed.tzinfo is not None and parsed.microsecond == 0, "explicit UTC offset and whole seconds required")
    return int(parsed.timestamp())


def load_universe(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        need(set(reader.fieldnames or []) == {"symbol", "provider_symbol", "asset_class", "region", "provider"},
             "unexpected universe columns")
        rows = list(reader)
    need(rows, "empty universe")
    seen = {}
    for row in rows:
        need(None not in row and all(isinstance(v, str) for v in row.values()), "invalid CSV row")
        symbol = row["provider_symbol"]
        need(row["provider"] == "yahoo" and symbol and not any(c.isspace() for c in symbol), "unsupported provider/symbol")
        need("/" not in symbol and "\\" not in symbol, "untranslated exchange mapping; use a validated subset")
        need(symbol not in seen or seen[symbol] == row, "conflicting duplicate symbol")
        seen[symbol] = row
    return list(seen.values())


def validate_payload(payload: dict, request: dict) -> int:
    try:
        chart = payload["chart"]
        need(chart.get("error") is None, "provider application error")
        need(isinstance(chart["result"], list) and len(chart["result"]) == 1, "missing chart result")
        data = chart["result"][0]
        need(data["meta"]["symbol"].upper() == request["symbol"].upper(), "provider symbol differs")
        need(data["meta"].get("dataGranularity") == request["interval"], "provider interval differs")
        timestamps = data.get("timestamp", [])
        if not timestamps:
            return 0
        need(all(type(t) is int for t in timestamps) and timestamps == sorted(set(timestamps)), "invalid timestamps")
        series = data["indicators"]["quote"]
        need(len(series) == 1, "unexpected quote series")
        quote = series[0]
        need(all(len(quote[k]) == len(timestamps) for k in ("open", "high", "low", "close", "volume")), "quote lengths differ")
        count = 0
        for i, t in enumerate(timestamps):
            if not request["start"] <= t < request["end"]:
                continue
            values = [quote[k][i] for k in ("open", "high", "low", "close", "volume")]
            need(all(type(v) in (int, float) and math.isfinite(v) for v in values), "invalid OHLCV")
            op, high, low, close, volume = values
            need(0 < low <= min(op, close) <= max(op, close) <= high and volume >= 0, "impossible OHLCV")
            need(request["interval"] != "1m" or t % 60 == 0, "unaligned minute")
            count += 1
        return count
    except (KeyError, IndexError, TypeError, AttributeError) as exc:
        raise Refusal("invalid provider response structure") from exc


def fetch_chart(request: dict, config: dict) -> dict:
    query = urllib.parse.urlencode({"period1": request["start"], "period2": request["end"],
                                   "interval": request["interval"], "events": "div,splits,capitalGains",
                                   "includePrePost": str(request["include_prepost"]).lower()})
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(request['symbol'], safe='')}?{query}"
    req = urllib.request.Request(url, headers={"User-Agent": "market-observatory/1"})
    with urllib.request.urlopen(req, timeout=config["timeout_seconds"]) as response:
        raw = response.read(config["response_max_bytes"] + 1)
    need(len(raw) <= config["response_max_bytes"], "response exceeds budget")
    return json.loads(raw)


def retry_delay(headers, attempt: int, config: dict, now: float) -> float:
    value = headers.get("Retry-After") if headers else None
    if value:
        try:
            delay = float(value)
        except ValueError:
            try:
                delay = parsedate_to_datetime(value).timestamp() - now
            except (TypeError, ValueError, OverflowError):
                delay = 0
        if math.isfinite(delay) and delay > 0:
            return delay
    return min(config["retry_max_seconds"], config["retry_base_seconds"] * 2 ** min(attempt - 1, 16))


class Collector:
    def __init__(self, root: Path, config: dict, *, fetcher=fetch_chart, clock=time.time):
        self.root = real_directory(root)
        self.config = config
        need(1 <= config["concurrency"] <= 30, "concurrency must be in 1..30")
        for name in ("max_jobs_per_run", "max_attempts", "timeout_seconds", "response_max_bytes",
                     "retry_base_seconds", "retry_max_seconds", "max_windows_per_plan"):
            need(config[name] > 0, f"invalid {name}")
        need(config["publication_lag_seconds"] >= 0, "negative publication lag")
        self.fetcher, self.clock = fetcher, clock
        self.gate = threading.Lock()
        self.cooldown, self.blocked = 0.0, False

    def connect(self):
        path = self.root / "ledger.sqlite"
        need(not path.is_symlink(), "symlink ledger refused")
        db = sqlite3.connect(path)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("""CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY, request TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0,
            object TEXT, reason TEXT)""")
        db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        db.execute("""CREATE INDEX IF NOT EXISTS jobs_symbol_interval ON jobs
            (json_extract(request,'$.symbol'), json_extract(request,'$.interval'))""")
        db.execute("""CREATE TABLE IF NOT EXISTS coverage (
            contract TEXT NOT NULL, start INTEGER NOT NULL, end INTEGER NOT NULL, state TEXT NOT NULL,
            PRIMARY KEY(contract,start,end,state))""")
        return db

    def plan(self, rows: list[dict], start: int, end: int, interval: str, revision: str = "initial", *,
             max_new_jobs: int | None = None) -> int:
        step = self.config["window_seconds"][interval]
        need(step > 0 and start < end and start % step == end % step == 0, "bounds must align with UTC windows")
        need(end <= self.clock() - self.config["publication_lag_seconds"], "window not closed with provider lag")
        need((end - start) // step <= self.config["max_windows_per_plan"], "window budget exceeded")
        need(revision, "revision required")
        need(max_new_jobs is None or type(max_new_jobs) is int and max_new_jobs > 0, "invalid planning budget")
        with writer_lock(self.root), closing(self.connect()) as db, db:
            before = db.total_changes
            for row in rows:
                contract_value = {"version": 1, "provider": "yahoo", "symbol": row["provider_symbol"],
                                  "interval": interval, "include_prepost": self.config["include_prepost"],
                                  "adjustment": "provider_unspecified", "revision": revision}
                contract = canonical_bytes(contract_value).decode()
                covered = [(r[0], r[1]) for r in db.execute("SELECT start,end FROM coverage WHERE contract=?", (contract,))]
                for existing in db.execute("""SELECT request FROM jobs WHERE json_extract(request,'$.symbol')=?
                    AND json_extract(request,'$.interval')=?""", (row["provider_symbol"], interval)):
                    prior = json.loads(existing[0])
                    if {k: v for k, v in prior.items() if k not in ("start", "end")} == contract_value:
                        covered.append((prior["start"], prior["end"]))
                # Subtract recorded/planned spans first, then coalesce only genuinely
                # new contiguous gaps. A changed request size cannot duplicate history.
                gaps, cursor = [], start
                for left, right in sorted(covered):
                    if right <= cursor or left >= end:
                        continue
                    if cursor < left:
                        gaps.append((cursor, min(left, end)))
                    cursor = max(cursor, right)
                if cursor < end:
                    gaps.append((cursor, end))
                window_count = self.config.get("request_windows", {}).get(interval, 1)
                need(type(window_count) is int and window_count > 0, "invalid request window count")
                for left, right in gaps:
                    for begin in range(left, right, step * window_count):
                        if max_new_jobs is not None and db.total_changes - before >= max_new_jobs:
                            return db.total_changes - before
                        request = {**contract_value, "start": begin, "end": min(right, begin + step * window_count)}
                        encoded = canonical_bytes(request)
                        db.execute("INSERT INTO jobs(id,request) VALUES (?,?) ON CONFLICT(id) DO NOTHING",
                                   (sha256_bytes(encoded), encoded.decode()))
            return db.total_changes - before

    def attempt(self, row: dict) -> dict:
        now = self.clock()
        with self.gate:
            if self.blocked or self.cooldown > now:
                return {"state": "retry", "retry_at": self.cooldown, "reason": "source_cooldown", "attempted": False}
        request = json.loads(row["request"])
        try:
            payload = self.fetcher(request, self.config)
            count = validate_payload(payload, request)
            record = {"schema_version": 1, "request": request, "observed_at": self.clock(),
                      "classification": "provider_current_non_pit", "coverage": "observations_only",
                      "rows_in_window": count, "payload": payload}
            compressed = gzip.compress(canonical_bytes(record), mtime=0)
            digest = sha256_bytes(compressed)
            put_immutable(self.root / "objects" / digest[:2] / digest, compressed)
            return {"state": "observed" if count else "unavailable", "object": digest,
                    "reason": None if count else "no_bars_in_window", "attempted": True}
        except urllib.error.HTTPError as exc:
            exc.close()
            if exc.code in (401, 403):
                with self.gate:
                    self.blocked = True
                return {"state": "blocked", "reason": f"http_{exc.code}", "attempted": True}
            if exc.code in (429, 408) or exc.code >= 500:
                delay = retry_delay(exc.headers, row["attempts"] + 1, self.config, now)
                if exc.code == 429:
                    with self.gate:
                        self.cooldown = max(self.cooldown, now + delay)
                return {"state": "retry", "retry_at": now + delay, "reason": f"http_{exc.code}", "attempted": True}
            return {"state": "unavailable" if exc.code == 404 else "quarantined",
                    "reason": f"http_{exc.code}", "attempted": True}
        except (Refusal, ValueError):
            return {"state": "quarantined", "reason": "invalid_provider_response", "attempted": True}
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            delay = retry_delay(None, row["attempts"] + 1, self.config, now)
            return {"state": "retry", "retry_at": now + delay, "reason": "network_error", "attempted": True}

    def run(self) -> dict:
        with writer_lock(self.root), closing(self.connect()) as db, db:
            db.execute("UPDATE jobs SET state='pending' WHERE state='running'")
            meta = dict(db.execute("SELECT key,value FROM meta").fetchall())
            self.cooldown = float(meta.get("cooldown", "0"))
            self.blocked = meta.get("blocked") == "true"
            if self.blocked or self.cooldown > self.clock():
                return self.summary(db)
            due = db.execute("""SELECT * FROM jobs WHERE state IN ('pending','retry') AND retry_at<=?
                ORDER BY json_extract(request,'$.start') DESC, id LIMIT ?""",
                             (self.clock(), self.config["max_jobs_per_run"])).fetchall()
            db.executemany("UPDATE jobs SET state='running' WHERE id=?", [(r["id"],) for r in due])
            db.commit()
            with ThreadPoolExecutor(max_workers=self.config["concurrency"]) as pool:
                pending = {pool.submit(self.attempt, dict(row)): row for row in due}
                for future in as_completed(pending):
                    row, result = pending[future], future.result()
                    attempts = row["attempts"] + int(result["attempted"])
                    state = result["state"]
                    if state == "retry" and attempts >= self.config["max_attempts"]:
                        state = "failed"
                    db.execute("UPDATE jobs SET state=?,attempts=?,retry_at=?,object=?,reason=? WHERE id=?",
                               (state, attempts, result.get("retry_at", 0), result.get("object"), result["reason"], row["id"]))
                    with self.gate:
                        db.execute("INSERT OR REPLACE INTO meta VALUES ('cooldown', ?)", (str(self.cooldown),))
                        db.execute("INSERT OR REPLACE INTO meta VALUES ('blocked', ?)", (json.dumps(self.blocked),))
                    db.commit()
            return self.summary(db)

    @staticmethod
    def summary(db) -> dict:
        states = dict(db.execute("SELECT state,count(*) FROM jobs GROUP BY state").fetchall())
        return {"states": states, "all_windows_observed": bool(states) and set(states) == {"observed"},
                "archived_coverage_spans": db.execute("SELECT count(*) FROM coverage").fetchone()[0],
                "pit_complete": False, "automatic_scheduling": False}

    def checkpoint(self, db) -> dict:
        """Compact terminal windows, retaining holes and retry state, never payloads."""
        groups, pending = {}, []
        for row in db.execute("SELECT * FROM coverage"):
            groups.setdefault((row["contract"], row["state"]), []).append((row["start"], row["end"]))
        for row in db.execute("SELECT * FROM jobs ORDER BY id"):
            request = json.loads(row["request"])
            if row["state"] in ("observed", "unavailable"):
                contract = canonical_bytes({k: v for k, v in request.items() if k not in ("start", "end")}).decode()
                groups.setdefault((contract, row["state"]), []).append((request["start"], request["end"]))
            else:
                pending.append({k: row[k] for k in ("request", "state", "attempts", "retry_at", "reason")})
        spans = []
        for (contract, state), windows in sorted(groups.items()):
            merged = []
            for left, right in sorted(windows):
                if merged and left <= merged[-1][1]:
                    merged[-1][1] = max(right, merged[-1][1])
                else:
                    merged.append([left, right])
            spans.append({"contract": json.loads(contract), "state": state, "ranges": merged})
        return {"format": "marketdata-checkpoint-v1", "coverage": spans, "pending": pending,
                "meta": dict(db.execute("SELECT key,value FROM meta")), "pit_complete": False}

    def restore_checkpoint(self, path: Path) -> dict:
        need(path.is_file() and not path.is_symlink() and path.stat().st_size <= 64 * 1024 * 1024,
             "invalid checkpoint file")
        raw = path.read_bytes()
        value = json.loads(raw)
        need(value.get("format") == "marketdata-checkpoint-v1" and value.get("pit_complete") is False,
             "unsupported checkpoint")
        digest = sha256_bytes(raw)
        with writer_lock(self.root), closing(self.connect()) as db, db:
            saved = db.execute("SELECT value FROM meta WHERE key='checkpoint'").fetchone()
            if saved:
                need(saved[0] == digest, "producer already bootstrapped from another checkpoint")
                return self.summary(db)
            need(db.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0
                 and db.execute("SELECT count(*) FROM coverage").fetchone()[0] == 0,
                 "checkpoint restoration requires a fresh producer ledger")
            for group in value["coverage"]:
                contract = group["contract"]
                need(set(contract) == {"version", "provider", "symbol", "interval", "include_prepost", "adjustment", "revision"}
                     and contract["provider"] == "yahoo" and contract["version"] == 1,
                     "unsupported checkpoint contract")
                step = self.config["window_seconds"][contract["interval"]]
                need(group["state"] in ("observed", "unavailable"), "invalid coverage state")
                for left, right in group["ranges"]:
                    need(type(left) is int and type(right) is int and left < right
                         and left % step == right % step == 0, "invalid coverage interval")
                    db.execute("INSERT INTO coverage VALUES (?,?,?,?)",
                               (canonical_bytes(contract).decode(), left, right, group["state"]))
            for row in value["pending"]:
                request = json.loads(row["request"])
                need(row["state"] in ("pending", "running", "retry", "blocked", "quarantined", "failed"),
                     "invalid pending state")
                need(type(row["attempts"]) is int and row["attempts"] >= 0
                     and type(row["retry_at"]) in (int, float) and math.isfinite(row["retry_at"]), "invalid retry state")
                encoded = canonical_bytes(request)
                need(request["provider"] == "yahoo" and request["interval"] in self.config["window_seconds"]
                     and request["start"] < request["end"], "invalid pending request")
                db.execute("INSERT INTO jobs(id,request,state,attempts,retry_at,reason) VALUES (?,?,?,?,?,?)",
                           (sha256_bytes(encoded), encoded.decode(), "pending" if row["state"] == "running" else row["state"],
                            row["attempts"], row["retry_at"], row["reason"]))
            for name in ("cooldown", "blocked"):
                if name in value["meta"]:
                    db.execute("INSERT INTO meta VALUES (?,?)", (name, value["meta"][name]))
            db.execute("INSERT INTO meta VALUES ('checkpoint',?)", (digest,))
            return self.summary(db)

    def import_observations(self, inventory: Path) -> dict:
        """Reuse closed native receipts; other data formats require an explicit adapter."""
        with writer_lock(self.root), closing(self.connect()) as db, db:
            snapshot, roots = portable_snapshot(inventory)
            imported = 0
            for item in snapshot["files"]:
                parts = portable_parts(item["path"])
                path = source_path(roots[parts[0]], Path(*parts[1:]))
                need(item["bytes"] <= self.config["response_max_bytes"], "imported object exceeds budget")
                raw = path.read_bytes()
                need(sha256_bytes(raw) == item["sha256"], "import changed after inventory validation")
                with gzip.GzipFile(fileobj=io.BytesIO(raw)) as handle:
                    decoded = handle.read(self.config["response_max_bytes"] + 1)
                need(len(decoded) <= self.config["response_max_bytes"], "imported receipt exceeds budget")
                record = json.loads(decoded)
                need(record.get("schema_version") == 1
                     and record.get("classification") == "provider_current_non_pit"
                     and record.get("coverage") == "observations_only", "unsupported receipt provenance")
                request = record["request"]
                need(set(request) == {"version", "provider", "symbol", "interval", "start", "end",
                                     "include_prepost", "adjustment", "revision"}, "unsupported request structure")
                need(request["version"] == 1 and request["provider"] == "yahoo"
                     and request["interval"] in self.config["window_seconds"]
                     and request["adjustment"] == "provider_unspecified", "unsupported receipt contract")
                need(isinstance(request["symbol"], str) and request["symbol"]
                     and not any(c.isspace() or c in "/\\" for c in request["symbol"]), "invalid receipt symbol")
                need(type(request["include_prepost"]) is bool and isinstance(request["revision"], str)
                     and request["revision"], "invalid request flags")
                step = self.config["window_seconds"][request["interval"]]
                need(type(request["start"]) is int and type(request["end"]) is int
                     and request["start"] % step == request["end"] % step == 0
                     and 0 < request["end"] - request["start"] <= step * self.config["max_windows_per_plan"],
                     "incompatible receipt window")
                need(type(record.get("observed_at")) in (int, float)
                     and request["end"] <= record["observed_at"] <= self.clock(), "invalid observation time")
                count = validate_payload(record["payload"], request)
                need(count > 0 and count == record["rows_in_window"], "receipt does not contain observed bars")
                encoded = canonical_bytes(request)
                job_id = sha256_bytes(encoded)
                existing = db.execute("SELECT object FROM jobs WHERE id=?", (job_id,)).fetchone()
                need(not existing or existing["object"] in (None, item["sha256"]), "conflicting observation; explicit revision required")
                put_immutable(self.root / "objects" / item["sha256"][:2] / item["sha256"], raw)
                db.execute("""INSERT INTO jobs(id,request,state,object) VALUES (?,?,'observed',?)
                    ON CONFLICT(id) DO UPDATE SET state='observed',object=excluded.object,reason=NULL,retry_at=0""",
                           (job_id, encoded.decode(), item["sha256"]))
                imported += int(not existing or existing["object"] is None)
            return {**self.summary(db), "imported_windows": imported,
                    "source_inventory_sha256": snapshot["source_inventory_sha256"]}

    def export(self, destination: Path, *, include_checkpoint: bool = False) -> dict:
        destination = destination.absolute()
        need(not destination.exists() and not destination.is_symlink(), "export already exists")
        need(not destination.is_relative_to(self.root) and not self.root.is_relative_to(destination),
             "export and collector state must not overlap")
        with writer_lock(self.root), closing(self.connect()) as db, db:
            rows = db.execute("SELECT * FROM jobs WHERE state='observed' ORDER BY id").fetchall()
            need(rows or include_checkpoint, "no observed windows to export")
            real_directory(destination.parent)
            staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.exporting-", dir=destination.parent))
            try:
                root = real_directory(staging / "root")
                if include_checkpoint:
                    checkpoint = canonical_bytes(self.checkpoint(db))
                    need(len(checkpoint) <= 64 * 1024 * 1024, "checkpoint exceeds format bound; partition the producer")
                    put_immutable(root / "_producer" / "checkpoint.json", checkpoint)
                batched = include_checkpoint and self.config.get("checkpoint_export_layout") == "interval_jsonl_batches"
                if batched:
                    for interval in sorted({json.loads(r["request"])["interval"] for r in rows}):
                        directory = real_directory(root / interval)
                        temporary = directory / ".batch.jsonl.gz"
                        with temporary.open("xb") as output:
                            with gzip.GzipFile(fileobj=output, mode="wb", mtime=0, filename="") as compressed:
                                for row in rows:
                                    if json.loads(row["request"])["interval"] != interval:
                                        continue
                                    raw = checked_blob(self.root / "objects", row["object"])
                                    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as source:
                                        decoded = source.read(self.config["response_max_bytes"] + 1)
                                    need(len(decoded) <= self.config["response_max_bytes"], "observation exceeds export bound")
                                    compressed.write(canonical_bytes(json.loads(decoded)))
                            output.flush()
                            os.fsync(output.fileno())
                        temporary.rename(directory / f"batch-{sha256_file(temporary)}.jsonl.gz")
                else:
                    for row in rows:
                        request = json.loads(row["request"])
                        raw = checked_blob(self.root / "objects", row["object"])
                        put_immutable(root / request["interval"] / f"{row['id']}.json.gz", raw)
                temporary_manifest = staging / "temporary-inventory.json"
                document = inventory_from_roots([f"root={root}"], temporary_manifest)
                document["source_roots"] = {"root": str(destination / "root")}
                for item in document["files"]:
                    item["path"] = str(destination / Path(item["path"]).relative_to(staging))
                put_immutable(staging / "inventory.json", canonical_bytes(document))
                temporary_manifest.unlink()
                result = {**self.summary(db), "files": len(document["files"]) - int(include_checkpoint),
                          "observation_requests": len(rows), "layout": "interval_jsonl_batches" if batched else "native_receipts",
                          "inventory": str(destination / "inventory.json")}
                put_immutable(staging / "collection.json", canonical_bytes(result))
                publish_directory(staging, destination)
                return result
            except BaseException:
                shutil.rmtree(staging, ignore_errors=True)
                raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--store", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    collect = commands.add_parser("collect", help="Plan windows; fetch only with --execute")
    collect.add_argument("--universe", type=Path, required=True)
    collect.add_argument("--start", required=True)
    collect.add_argument("--end", required=True)
    collect.add_argument("--interval", choices=("1d", "1m"), required=True)
    collect.add_argument("--revision", default="initial")
    collect.add_argument("--execute", action="store_true")
    exported = commands.add_parser("export")
    exported.add_argument("--destination", type=Path, required=True)
    imported = commands.add_parser("import-observations", help="Import closed native receipts before planning gaps")
    imported.add_argument("--inventory", type=Path, required=True)
    args = parser.parse_args()
    try:
        config = yaml.safe_load(args.config.read_text())
        need(config["schema_version"] == 1 and config["automatic_scheduling"] is False, "manual policy required")
        collector = Collector(args.store, config["collector"])
        if args.command == "collect":
            added = collector.plan(load_universe(args.universe), utc_seconds(args.start), utc_seconds(args.end),
                                   args.interval, args.revision)
            result = collector.run() if args.execute else {"planned_new_windows": added, "executed": False}
        elif args.command == "export":
            result = collector.export(args.destination)
        else:
            result = collector.import_observations(args.inventory)
        print(json.dumps(result, sort_keys=True))
        return int(result.get("all_windows_observed") is False)
    except (Refusal, OSError, ValueError, KeyError) as exc:
        parser.exit(1, f"collection refused: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
