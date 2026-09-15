#!/usr/bin/env python3
"""Backfill existing SEC rows over SSH into closed Parquet/Zstd bronze partitions."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from cache_common import publish_directory, put_immutable, real_directory
from sec_readonly_stream import DATE_COLUMNS, TABLES
from vendor.immutable_cache_release import canonical_bytes, inventory_from_roots, need, Refusal, sha256_bytes


def convert_stream(source, destination, policy, expected):
    digest, count, batch, partitions = hashlib.sha256(), 0, [], []
    schema = pa.schema([("row_id", pa.int64()), ("filing_date", pa.string()), ("record_json", pa.large_string())])
    def flush():
        if not batch:
            return
        path = destination / f"part-{len(partitions):05d}.parquet"
        pq.write_table(pa.Table.from_pylist(batch, schema=schema), path, compression="zstd",
                       compression_level=policy["compression_level"], write_page_checksum=True)
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
        need(pq.ParquetFile(path).metadata.num_rows == len(batch), "Parquet row count differs")
        partitions.append({"file": path.name, "rows": len(batch)})
        batch.clear()
    real_directory(destination)
    with gzip.open(source, "rb") as stream:
        def line():
            raw = stream.readline(policy["max_line_bytes"] + 1)
            need(len(raw) <= policy["max_line_bytes"] and (not raw or raw.endswith(b"\n")), "SEC line exceeds bound")
            return raw
        header = json.loads(line())["header"]
        need(header["format"] == "marketdata-sec-stream-v1" and header["pit_complete"] is False
             and header["read_consistent"] is True, "unsupported SEC export")
        need(all(header[k] == expected[k] for k in ("table", "start", "end")), "export selection differs")
        selection_column = header.get("selection_date_column", "filing_date")
        need(header["table"] in TABLES and selection_column == DATE_COLUMNS.get(header["table"], "filing_date"),
             "unexpected SEC selection date column")
        previous_id = -1
        while True:
            raw = line()
            need(raw, "SEC stream incomplete: footer missing")
            value = json.loads(raw)
            if "footer" in value:
                footer = value["footer"]
                need(footer == {"rows": count, "rows_sha256": digest.hexdigest()} and not line(), "SEC footer differs")
                break
            row = value["row"]
            count += 1
            need(count <= policy["max_rows"] and type(row["id"]) is int and row["id"] > previous_id,
                 "SEC row budget/order differs")
            need(isinstance(row.get(selection_column), str)
                 and expected["start"] <= row[selection_column] < expected["end"], "SEC row outside selection")
            previous_id = row["id"]
            digest.update(raw)
            batch.append({"row_id": row["id"], "filing_date": row["filing_date"],
                          "record_json": canonical_bytes(row).decode()})
            if len(batch) >= policy["rows_per_partition"]:
                flush()
        flush()
    provenance = {**header, **footer, "partitions": partitions, "classification": "legacy_sec_candidate_non_certified",
                  "availability_policy": "use original accepted_at/available_at/reported_at plus certified ingest ledger; export time is not historical availability"}
    put_immutable(destination / "provenance.json", canonical_bytes(provenance))
    return provenance


def backfill(config, start, end, destination):
    need(config["automatic_scheduling"] is False, "manual policy required")
    destination = destination.absolute()
    need(not destination.exists() and not destination.is_symlink(), "backfill destination already exists")
    real_directory(destination.parent)
    stage = Path(tempfile.mkdtemp(prefix=".sec-export-", dir=destination.parent))
    policy, ssh = config["sec"], config["ssh"]
    helper = Path(__file__).with_name("sec_readonly_stream.py").read_bytes()
    reports = []
    try:
        for table in policy["tables"]:
            need(table in TABLES, "SEC table not allowlisted")
            command = ["python3", "-", "--database", policy["database"], "--table", table,
                       "--start", start, "--end", end, "--max-rows", str(policy["max_rows"]),
                       "--max-seconds", str(policy["max_seconds"])]
            args = ["ssh", "-T", "-i", str(Path(ssh["identity_file"]).expanduser()), "-o", "BatchMode=yes",
                    "-o", f"ConnectTimeout={ssh['connect_timeout_seconds']}", ssh["host"], shlex.join(command)]
            source = stage / f"{table}.jsonl.gz"
            with source.open("xb") as output:
                # RLIMIT_FSIZE bounds the SSH output file as well as a failed remote export.
                def limits():
                    import resource
                    resource.setrlimit(resource.RLIMIT_FSIZE, (policy["max_compressed_bytes"], policy["max_compressed_bytes"]))
                result = subprocess.run(args, input=helper, stdout=output, stderr=subprocess.PIPE,
                                        timeout=policy["max_seconds"] + ssh["connect_timeout_seconds"] + 30, preexec_fn=limits)
                output.flush()
                os.fsync(output.fileno())
            need(result.returncode == 0 and source.stat().st_size <= policy["max_compressed_bytes"],
                 "read-only SEC export failed or exceeded budget; remote stderr suppressed")
            report = convert_stream(source, stage / "root" / "sec" / table / f"{start}_{end}", policy,
                                    {"table": table, "start": start, "end": end})
            report["compressed_transport_bytes"] = source.stat().st_size
            reports.append(report)
            source.unlink()
        temporary = stage / "temporary-inventory.json"
        inventory = inventory_from_roots([f"root={stage / 'root'}"], temporary)
        inventory["source_roots"] = {"root": str(destination / "root")}
        for item in inventory["files"]:
            item["path"] = str(destination / Path(item["path"]).relative_to(stage))
        put_immutable(stage / "inventory.json", canonical_bytes(inventory))
        temporary.unlink()
        receipt = {"source": "marketdata-mcp/ace/sec", "start": start, "end": end,
                   "inventory_sha256": sha256_bytes(canonical_bytes(inventory)), "tables": reports,
                   "remote_writes": False, "pit_complete": False}
        put_immutable(stage / "backfill.json", canonical_bytes(receipt))
        publish_directory(stage, destination)
        return {"destination": str(destination), "tables": len(reports), "rows": sum(r["rows"] for r in reports),
                "inventory_sha256": receipt["inventory_sha256"], "remote_writes": False, "pit_complete": False}
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    try:
        config = yaml.safe_load(args.config.read_text())
        need(config["schema_version"] == 1, "unsupported configuration")
        result = backfill(config, args.start, args.end, args.destination) if args.execute else {
            "executed": False, "tables": config["sec"]["tables"], "start": args.start, "end": args.end}
        print(json.dumps(result, sort_keys=True))
        return 0
    except (Refusal, ValueError, OSError, KeyError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"backfill refused: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
