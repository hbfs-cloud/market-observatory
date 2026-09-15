#!/usr/bin/env python3
"""Inventory a dated, already existing Parquet cache without writing to its host."""
import argparse
import hashlib
import json
from pathlib import Path
import time


def inventory(root, max_files, max_bytes, max_seconds):
    root = Path(root).absolute()
    if any(p.is_symlink() for p in [*root.parents, root]) or not root.is_dir():
        raise ValueError("real cache directory required")
    started = time.monotonic()
    paths = sorted(root.rglob("*.parquet"))
    if not paths or len(paths) > max_files:
        raise ValueError("empty inventory or file budget exceeded")
    rows, total = [], 0
    for path in paths:
        if any(p.is_symlink() for p in [*path.parents, path]) or not path.is_file():
            raise ValueError("symlink/non-regular cache file refused")
        before = path.stat()
        total += before.st_size
        if total > max_bytes:
            raise ValueError("cache byte budget exceeded")
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
                if time.monotonic() - started > max_seconds:
                    raise ValueError("inventory time budget exceeded")
        after = path.stat()
        if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValueError("cache file changed during inventory")
        rows.append({"path": str(path.relative_to(root)), "bytes": before.st_size, "sha256": digest.hexdigest()})
    if paths != sorted(root.rglob("*.parquet")):
        raise ValueError("cache file list changed during inventory")
    return {"format": "marketdata-parquet-inventory-v1", "files": rows, "total_bytes": total,
            "classification": "legacy_adjustment_unknown_non_pit", "observed_at": time.time()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--max-files", type=int, required=True)
    parser.add_argument("--max-bytes", type=int, required=True)
    parser.add_argument("--max-seconds", type=int, required=True)
    args = parser.parse_args()
    print(json.dumps(inventory(args.root, args.max_files, args.max_bytes, args.max_seconds), sort_keys=True))


if __name__ == "__main__":
    main()
