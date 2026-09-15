"""Content-addressed producer shards; v1 checkpoints remain readable."""
import gzip
import io
import json
from pathlib import Path
import re

from cache_common import put_immutable
from vendor.immutable_cache_release import canonical_bytes, need, sha256_bytes

FORMAT_BOUND = 64 * 1024 * 1024


def read_index(path):
    path = Path(path)
    need(path.is_file() and not path.is_symlink() and path.stat().st_size <= FORMAT_BOUND,
         "invalid checkpoint file")
    raw = path.read_bytes()
    value = json.loads(raw)
    need(value.get("format") in ("marketdata-checkpoint-v1", "marketdata-checkpoint-v2")
         and value.get("pit_complete") is False, "unsupported checkpoint")
    if value["format"] == "marketdata-checkpoint-v2":
        need(isinstance(value.get("parts"), list), "invalid checkpoint shards")
        seen = set()
        for part in value["parts"]:
            name = part.get("path", "")
            need(re.fullmatch(r"checkpoints/[0-9a-f]{64}\.json\.gz", name) is not None,
                 "invalid checkpoint shard path")
            need(name not in seen and part.get("sha256") == Path(name).name[:-8],
                 "duplicate or unpinned checkpoint shard")
            need(all(type(part.get(k)) is int and 0 < part[k] <= FORMAT_BOUND
                     for k in ("bytes", "decoded_bytes")), "invalid checkpoint shard bounds")
            seen.add(name)
    return value, sha256_bytes(raw)


def write_checkpoint(value, path, config):
    size = config.get("checkpoint_shard_bytes")
    if size is None:
        raw = canonical_bytes(value)
        need(len(raw) <= FORMAT_BOUND, "checkpoint exceeds v1 bound; enable checkpoint shards")
        put_immutable(path, raw)
        return 1
    need(type(size) is int and 0 < size <= FORMAT_BOUND, "invalid checkpoint shard size")
    budget = config["checkpoint_total_bytes"]
    need(type(budget) is int and budget > 0, "invalid checkpoint total budget")
    buckets = {}
    for kind in ("coverage", "pending"):
        for row in value[kind]:
            contract = row["contract"] if kind == "coverage" else json.loads(row["request"])
            identity = {k: v for k, v in contract.items() if k not in ("start", "end")}
            bucket = sha256_bytes(canonical_bytes(identity))[:2]
            buckets.setdefault(bucket, []).append((kind, row))
    parts, total = [], 0

    def flush(records):
        nonlocal total
        raw = canonical_bytes({"format": "marketdata-checkpoint-shard-v2", "records": records})
        need(len(raw) <= size, "checkpoint record exceeds shard budget")
        total += len(raw)
        need(total <= budget, "checkpoint exceeds total budget")
        compressed = gzip.compress(raw, mtime=0)
        digest = sha256_bytes(compressed)
        name = f"checkpoints/{digest}.json.gz"
        put_immutable(path.parent / name, compressed)
        parts.append({"path": name, "sha256": digest, "bytes": len(compressed), "decoded_bytes": len(raw)})

    for bucket in sorted(buckets):
        records, used = [], 80
        for kind, row in buckets[bucket]:
            record = {"kind": kind, "value": row}
            length = len(canonical_bytes(record)) + 1
            if records and used + length > size:
                flush(records)
                records, used = [], 80
            records.append(record)
            used += length
        if records:
            flush(records)
    index = {"format": "marketdata-checkpoint-v2", "parts": parts,
             "meta": value["meta"], "pit_complete": False}
    raw = canonical_bytes(index)
    need(len(raw) <= FORMAT_BOUND, "checkpoint index exceeds bound")
    put_immutable(path, raw)
    return 1 + len(parts)


def read_values(path, index, config):
    if index["format"] == "marketdata-checkpoint-v1":
        yield index
        return
    budget = config["checkpoint_total_bytes"]
    need(sum(p["decoded_bytes"] for p in index["parts"]) <= budget, "checkpoint exceeds total budget")
    for part in index["parts"]:
        target = path.parent / part["path"]
        need(not target.parent.is_symlink() and not target.is_symlink() and target.is_file()
             and target.stat().st_size == part["bytes"], "checkpoint shard missing or wrong size")
        raw = target.read_bytes()
        need(sha256_bytes(raw) == part["sha256"], "checkpoint shard hash differs")
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as source:
            decoded = source.read(part["decoded_bytes"] + 1)
        need(len(decoded) == part["decoded_bytes"], "checkpoint decoded size differs")
        shard = json.loads(decoded)
        need(shard.get("format") == "marketdata-checkpoint-shard-v2", "unsupported checkpoint shard")
        value = {"coverage": [], "pending": []}
        for record in shard["records"]:
            need(record["kind"] in value, "invalid checkpoint record kind")
            value[record["kind"]].append(record["value"])
        yield value


def restore_from_archive(collector, archive, pin, destination, cache):
    from vendor.immutable_cache_release import verify_directory
    catalog = archive.catalog(pin)

    def restore_or_verify(root, selected):
        if not root.exists():
            archive.restore(pin, root, cache, paths=selected)
            return
        control = root / ".marketdata-receipt"
        verify_directory(control / "snapshot.json", root)
        receipt = json.loads((control / "receipt.json").read_bytes())
        need(receipt.get("catalog_pin") == pin and receipt.get("selection") == sorted(selected),
             "restored checkpoint belongs to another pin or selection")
        files = [f for f in catalog["snapshot"]["files"] if f["path"] in selected]
        expected = dict(catalog["snapshot"], files=files, total_bytes=sum(f["bytes"] for f in files))
        need(json.loads((control / "snapshot.json").read_bytes()) == expected,
             "restored checkpoint manifest differs from pinned catalog")

    manifest = "root/_producer/checkpoint.json"
    index_root = destination.with_name(destination.name + "-index")
    restore_or_verify(index_root, [manifest])
    index, _ = read_index(index_root / manifest)
    if index["format"] == "marketdata-checkpoint-v1":
        return collector.restore_checkpoint(index_root / manifest)
    selected = [manifest, *("root/_producer/" + p["path"] for p in index["parts"])]
    restore_or_verify(destination, selected)
    return collector.restore_checkpoint(destination / manifest)
