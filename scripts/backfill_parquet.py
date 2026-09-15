#!/usr/bin/env python3
"""Verified dated-cache transfer via rsync; copy-dest reuses matching local bytes."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile

import yaml

from cache_common import publish_directory, put_immutable, real_directory
from vendor.immutable_cache_release import canonical_bytes, inventory_from_roots, need, portable_parts, Refusal, sha256_bytes, sha256_file, signature


def backfill(config, destination, *, execute=False, reuse=()):
    need(config["schema_version"] == 1 and config["automatic_scheduling"] is False, "manual policy required")
    ssh, policy = config["ssh"], config["parquet"]
    need(re.fullmatch(r"[A-Za-z0-9_-]+", policy["dataset_id"]), "invalid legacy dataset ID")
    need(re.fullmatch(r"[A-Za-z0-9_@.:-]+", ssh["host"]) and not ssh["host"].startswith("-"), "invalid SSH host")
    need(re.fullmatch(r"/[A-Za-z0-9_./-]+", policy["root"]) and "/../" not in policy["root"], "invalid cache path")
    transport = ["ssh", "-T", "-i", str(Path(ssh["identity_file"]).expanduser()), "-o", "BatchMode=yes",
                 "-o", f"ConnectTimeout={ssh['connect_timeout_seconds']}"]
    helper = Path(__file__).with_name("parquet_readonly_inventory.py").read_bytes()
    command = ["nice", "-n", "19", "python3", "-", "--root", policy["root"],
               "--max-files", str(policy["max_files"]), "--max-bytes", str(policy["max_bytes"]),
               "--max-seconds", str(policy["max_seconds"])]
    result = subprocess.run([*transport, ssh["host"], shlex.join(command)], input=helper,
                            capture_output=True, timeout=policy["max_seconds"] + 30)
    need(result.returncode == 0 and len(result.stdout) <= 64 * 1024 * 1024, "remote cache inventory failed; stderr suppressed")
    source = json.loads(result.stdout)
    need(source["format"] == "marketdata-parquet-inventory-v1" and source["files"], "invalid cache inventory")
    names = []
    for row in source["files"]:
        name = row["path"]
        portable_parts("root/" + name)
        need(not name.startswith("/") and "\n" not in name and "\r" not in name and name.endswith(".parquet"), "invalid Parquet path")
        names.append(name)
    need(len(names) == len(set(names)) <= policy["max_files"], "duplicate/excess cache files")
    need(sum(r["bytes"] for r in source["files"]) == source["total_bytes"] <= policy["max_bytes"], "cache bytes differ")
    if not execute:
        return {"executed": False, "files": len(names), "bytes": source["total_bytes"], "remote_writes": False}
    destination = destination.absolute()
    need(not destination.exists() and not destination.is_symlink(), "destination already exists")
    real_directory(destination.parent)
    stage = Path(tempfile.mkdtemp(prefix=".parquet-backfill-", dir=destination.parent))
    try:
        root = real_directory(stage / "root" / "legacy" / policy["dataset_id"])
        file_list = stage / "file-list.txt"
        put_immutable(file_list, ("\n".join(names) + "\n").encode())
        args = ["rsync", "--recursive", "--checksum", "--times", "--stats", "--files-from", str(file_list),
                "-e", shlex.join(transport)]
        for candidate in reuse:
            need(candidate.is_dir() and not candidate.is_symlink(), "invalid local reuse directory")
            args.append("--copy-dest=" + str(candidate.absolute()))
        args += [ssh["host"] + ":" + policy["root"].rstrip("/") + "/", str(root) + "/"]
        copied = subprocess.run(args, capture_output=True, timeout=policy["transfer_timeout_seconds"])
        need(copied.returncode == 0, "rsync incomplete; no destination published")
        for row in source["files"]:
            path = root / row["path"]
            need(path.is_file() and not path.is_symlink(), "transferred Parquet missing")
            before = signature(path)
            digest = sha256_file(path)
            need(before == signature(path) and path.stat().st_size == row["bytes"] and digest == row["sha256"],
                 "cache changed during transfer; no destination published")
            with path.open("rb") as handle:
                import os
                os.fsync(handle.fileno())
        put_immutable(root / "provenance.json", canonical_bytes({**source, "source_root": policy["root"],
                      "source_host": ssh["host"], "pit_complete": False, "raw_bars_proven": False}))
        file_list.unlink()
        temporary = stage / "temporary-inventory.json"
        document = inventory_from_roots([f"root={stage / 'root'}"], temporary)
        document["source_roots"] = {"root": str(destination / "root")}
        for row in document["files"]:
            row["path"] = str(destination / Path(row["path"]).relative_to(stage))
        temporary.unlink()
        put_immutable(stage / "inventory.json", canonical_bytes(document))
        receipt = {"files": len(names), "bytes": source["total_bytes"], "remote_writes": False,
                   "pit_complete": False, "inventory_sha256": sha256_bytes(canonical_bytes(document)),
                   "rsync_statistics": copied.stdout.decode(errors="replace")}
        put_immutable(stage / "backfill.json", canonical_bytes(receipt))
        publish_directory(stage, destination)
        return {k: v for k, v in receipt.items() if k != "rsync_statistics"}
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--reuse", type=Path, action="append", default=[])
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(backfill(yaml.safe_load(args.config.read_text()), args.destination,
                                 execute=args.execute, reuse=args.reuse), sort_keys=True))
        return 0
    except (Refusal, ValueError, OSError, KeyError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"backfill refused: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
