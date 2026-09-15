#!/usr/bin/env python3
"""Manual zstd/age object archives with pinned, selective local restoration.

The local index is mutable producer state, never a research input or a release
asset. Only closed catalogs, keyring.age and ciphertext objects are transportable.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import tempfile

import pyrage
from pyrage import passphrase, x25519
import yaml
import zstandard as zstd

from cache_common import blob_path, checked_blob, publish_directory, put_immutable, real_directory, writer_lock
from vendor.immutable_cache_release import (
    Refusal, canonical_bytes, is_hex64, need, portable_parts, portable_snapshot,
    sha256_bytes, signature, source_path, validate_snapshot, verify_directory,
)

FORMAT = "marketdata-zstd-age-v1"
MAX_PACK_BYTES = 64 * 1024 * 1024
MAX_CATALOG_BYTES = 64 * 1024 * 1024


def read_key_file(path: Path) -> str:
    path = path.expanduser()
    need(not path.is_symlink(), "key symlink refused")
    info = path.stat()
    need(stat.S_ISREG(info.st_mode), "key must be a regular file")
    need(info.st_mode & 0o077 == 0, "key file must be owner-only; permissions were not changed")
    need(16 <= info.st_size <= 4096, "key must contain 16..4096 bytes")
    # Preserve the exact bytes, including a possible newline, for reproducibility.
    return base64.b64encode(path.read_bytes()).decode("ascii")


class Archive:
    def __init__(self, root: Path, key_file: Path, *, create: bool = False,
                 pack_bytes: int = 8 * 1024 * 1024, compression_level: int = 9,
                 object_loader=None, object_exists=None):
        self.root = real_directory(root)
        need(0 < pack_bytes <= MAX_PACK_BYTES, "pack size out of bounds")
        need(1 <= compression_level <= 19, "compression level out of bounds")
        self.pack_bytes = pack_bytes
        self.compression_level = compression_level
        self.object_loader, self.object_exists = object_loader, object_exists
        self.compressor = zstd.ZstdCompressor(level=compression_level, write_checksum=True)
        keyring = self.root / "keyring.age"
        password = read_key_file(key_file)
        if not keyring.exists():
            with writer_lock(self.root):
                need(create, "archive keyring missing")
                need(not (self.root / "index.sqlite").exists(), "refusing a new keyring over an old index")
                if not keyring.exists():
                    identity = x25519.Identity.generate()
                    put_immutable(keyring, passphrase.encrypt(str(identity).encode("ascii"), password))
        need(not keyring.is_symlink() and keyring.stat().st_size < 16384, "invalid keyring")
        encrypted = keyring.read_bytes()
        try:
            self.identity = x25519.Identity.from_str(passphrase.decrypt(encrypted, password).decode("ascii"))
        except Exception:
            raise Refusal("cannot unlock archive: wrong key or damaged keyring") from None
        self.keyring_sha256 = sha256_bytes(encrypted)
        self.recipient = self.identity.to_public()

    def seal(self, value: bytes) -> bytes:
        return pyrage.encrypt(self.compressor.compress(value), [self.recipient])

    def require_object(self, pin: str) -> None:
        path = self.root / "objects" / pin[:2] / pin
        if path.exists() or path.is_symlink():
            blob_path(self.root / "objects", pin)
        else:
            need(self.object_exists is not None and self.object_exists(pin), "object not available locally or in pinned transport")

    def read_object(self, pin: str) -> bytes:
        path = self.root / "objects" / pin[:2] / pin
        if path.exists() or path.is_symlink():
            return checked_blob(self.root / "objects", pin)
        need(self.object_loader is not None, "missing object and no explicit transport")
        data = self.object_loader(pin)
        need(len(data) <= MAX_PACK_BYTES + 2 * 1024 * 1024 and sha256_bytes(data) == pin, "transport object differs")
        return data

    def open(self, value: bytes, limit: int) -> bytes:
        try:
            packed = pyrage.decrypt(value, [self.identity])
            size = zstd.frame_content_size(packed)
            need(0 <= size <= limit, "decompressed object exceeds bound")
            return zstd.ZstdDecompressor().decompress(packed, max_output_size=limit, allow_extra_data=False)
        except Refusal:
            raise
        except Exception:
            raise Refusal("encrypted object authentication/decompression failed") from None

    def catalog(self, pin: str) -> dict:
        need(is_hex64(pin), "explicit catalog SHA-256 required")
        path = self.root / "catalogs" / pin
        need(not path.parent.is_symlink() and not path.is_symlink(), "symlink catalog refused")
        need(path.stat().st_size <= MAX_CATALOG_BYTES + 1024 * 1024, "catalog too large")
        ciphertext = path.read_bytes()
        need(sha256_bytes(ciphertext) == pin, "catalog pin differs")
        raw = self.open(ciphertext, MAX_CATALOG_BYTES)
        value = json.loads(raw)
        self.validate_catalog(value, raw)
        return value

    def validate_catalog(self, value: dict, raw: bytes) -> None:
        need(isinstance(value, dict), "catalog must be an object")
        need(raw == canonical_bytes(value) and value.get("format") == FORMAT, "invalid catalog format")
        need(value.get("keyring_sha256") == self.keyring_sha256, "catalog keyring differs")
        need(all(isinstance(value.get(k), dict) for k in ("snapshot", "files", "chunks")), "invalid catalog structure")
        # Reuse the same snapshot validator as the immutable release client.
        with tempfile.TemporaryDirectory(prefix=".validate-", dir=self.root) as work:
            manifest = Path(work) / "snapshot.json"
            put_immutable(manifest, canonical_bytes(value["snapshot"]))
            _, snapshot_id = validate_snapshot(manifest)
        need(snapshot_id == value.get("snapshot_id"), "snapshot pin differs")
        files = value["snapshot"]["files"]
        need(set(value["files"]) == {f["path"] for f in files}, "catalog file mapping differs")
        used = set()
        object_sizes = {}
        for item in files:
            name = item["path"]
            need("/".join(portable_parts(name)) == name, "noncanonical path")
            total = 0
            need(isinstance(value["files"][name], list), "chunk list required")
            for digest in value["files"][name]:
                need(is_hex64(digest), "invalid chunk pin")
                used.add(digest)
                chunk = value["chunks"].get(digest)
                need(isinstance(chunk, dict) and set(chunk) == {"object", "offset", "bytes", "pack_bytes"}, "invalid chunk record")
                need(is_hex64(chunk["object"]), "invalid encrypted object pin")
                offset, size, packed = chunk["offset"], chunk["bytes"], chunk["pack_bytes"]
                need(all(type(n) is int for n in (offset, size, packed)), "invalid chunk dimensions")
                need(0 <= offset < packed <= MAX_PACK_BYTES and 0 < size <= packed - offset, "invalid chunk range")
                need(object_sizes.setdefault(chunk["object"], packed) == packed, "inconsistent object length")
                total += size
            need(total == item["bytes"], "chunk lengths differ from file")
        need(used == set(value["chunks"]), "unused or missing chunk records")

    def prepare(self, inventory: Path, *, parent: str | None = None) -> dict:
        with writer_lock(self.root):
            incoming, roots = portable_snapshot(inventory)
            previous = self.catalog(parent) if parent else None
            file_rows = {f["path"]: f for f in previous["snapshot"]["files"]} if previous else {}
            mappings = dict(previous["files"]) if previous else {}
            chunks = dict(previous["chunks"]) if previous else {}
            index_path = self.root / "index.sqlite"
            need(not index_path.is_symlink(), "symlink producer index refused")
            db = sqlite3.connect(index_path)
            try:
                db.execute("PRAGMA synchronous=FULL")
                db.execute("CREATE TABLE IF NOT EXISTS chunks (sha TEXT PRIMARY KEY, ref TEXT NOT NULL)")
                db.execute("CREATE TABLE IF NOT EXISTS catalogs (sha TEXT PRIMARY KEY, pin TEXT NOT NULL)")
                pending: dict[str, tuple[int, int]] = {}
                pack = bytearray()
                new_objects: list[str] = []

                def flush() -> None:
                    if not pack:
                        return
                    cipher = self.seal(bytes(pack))
                    pin = sha256_bytes(cipher)
                    put_immutable(self.root / "objects" / pin[:2] / pin, cipher)
                    for digest, (offset, length) in pending.items():
                        ref = {"object": pin, "offset": offset, "bytes": length, "pack_bytes": len(pack)}
                        chunks[digest] = ref
                        db.execute("INSERT INTO chunks VALUES (?, ?) ON CONFLICT(sha) DO NOTHING",
                                   (digest, canonical_bytes(ref).decode("utf-8")))
                    db.commit()
                    new_objects.append(pin)
                    pack.clear()
                    pending.clear()

                for item in incoming["files"]:
                    parts = portable_parts(item["path"])
                    source = source_path(roots[parts[0]], Path(*parts[1:]))
                    need(source.suffix.lower() not in {".db", ".sqlite", ".sqlite3", ".duckdb"}
                         and not source.name.endswith(("-wal", ".wal", "-shm")),
                         "database runtime refused: import a closed export instead")
                    before = signature(source)
                    file_hash = hashlib.sha256()
                    mapping: list[str] = []
                    with source.open("rb") as handle:
                        while block := handle.read(self.pack_bytes):
                            file_hash.update(block)
                            digest = sha256_bytes(block)
                            mapping.append(digest)
                            if digest in chunks or digest in pending:
                                continue
                            known = db.execute("SELECT ref FROM chunks WHERE sha=?", (digest,)).fetchone()
                            if known:
                                chunks[digest] = json.loads(known[0])
                                continue
                            if len(pack) + len(block) > self.pack_bytes:
                                flush()
                            pending[digest] = (len(pack), len(block))
                            pack.extend(block)
                    need(before == signature(source), "source changed during packing")
                    need(file_hash.hexdigest() == item["sha256"], "source differs from closed inventory")
                    mappings[item["path"]] = mapping
                    file_rows[item["path"]] = item
                flush()
                used = {sha for mapping in mappings.values() for sha in mapping}
                chunks = {sha: chunks[sha] for sha in sorted(used)}
                for pin in {ref["object"] for ref in chunks.values()}:
                    # Parent bytes stay immutable. Full checksums are verified on read;
                    # an append must not re-read the entire historical archive.
                    self.require_object(pin)
                files = sorted(file_rows.values(), key=lambda item: item["path"])
                lineage = {"parent_catalog": parent, "incoming_inventory": incoming["source_inventory_sha256"]}
                snapshot = {"schema_version": 1, "files": files,
                            "total_bytes": sum(f["bytes"] for f in files),
                            "source_inventory_sha256": sha256_bytes(canonical_bytes(lineage))}
                value = {"format": FORMAT, "keyring_sha256": self.keyring_sha256, "lineage": lineage,
                         "snapshot": snapshot, "snapshot_id": sha256_bytes(canonical_bytes(snapshot)),
                         "files": mappings, "chunks": chunks}
                raw = canonical_bytes(value)
                need(len(raw) <= MAX_CATALOG_BYTES, "catalog exceeds format bound; partition the inventory")
                self.validate_catalog(value, raw)
                digest = sha256_bytes(raw)
                known = db.execute("SELECT pin FROM catalogs WHERE sha=?", (digest,)).fetchone()
                if known:
                    pin = known[0]
                    self.catalog(pin)
                else:
                    cipher = self.seal(raw)
                    pin = sha256_bytes(cipher)
                    put_immutable(self.root / "catalogs" / pin, cipher)
                    db.execute("INSERT INTO catalogs VALUES (?, ?)", (digest, pin))
                    db.commit()
                return {"catalog_pin": pin, "snapshot_id": value["snapshot_id"],
                        "new_objects": new_objects, "files": len(files), "total_bytes": snapshot["total_bytes"]}
            finally:
                db.close()

    def restore(self, pin: str, destination: Path, cache: Path, *, paths: list[str] | None = None) -> dict:
        destination, cache = destination.absolute(), cache.absolute()
        need(not cache.is_relative_to(destination) and not destination.is_relative_to(cache),
             "cache and restore destination must not overlap")
        need(not destination.is_relative_to(self.root) and not self.root.is_relative_to(destination),
             "archive and restore destination must not overlap")
        need(not cache.is_relative_to(self.root) and not self.root.is_relative_to(cache),
             "archive and client cache must not overlap")
        need(not destination.exists() and not destination.is_symlink(), "restore destination already exists")
        real_directory(destination.parent)
        cache = real_directory(cache)
        index = cache / "index.sqlite"
        need(not index.is_symlink(), "symlink client index refused")
        value = self.catalog(pin)
        selected = set(paths) if paths else set(value["files"])
        need(selected and selected <= set(value["files"]), "selection contains unknown paths")
        files = [f for f in value["snapshot"]["files"] if f["path"] in selected]
        projection = dict(value["snapshot"], files=files, total_bytes=sum(f["bytes"] for f in files))
        copied: set[str] = set()
        transferred = 0
        staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.restoring-", dir=destination.parent))
        db = sqlite3.connect(index, timeout=30)
        try:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("CREATE TABLE IF NOT EXISTS chunks (sha TEXT PRIMARY KEY, ref TEXT NOT NULL)")
            # Keep a single decompressed pack in memory, never the entire archive.
            current_pin, current_pack = None, b""
            for item in files:
                target = staging.joinpath(*portable_parts(item["path"]))
                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open("xb") as handle:
                    for digest in value["files"][item["path"]]:
                        ref = value["chunks"][digest]
                        known = db.execute("SELECT ref FROM chunks WHERE sha=?", (digest,)).fetchone()
                        if known:
                            local = json.loads(known[0])
                            need(isinstance(local, dict) and is_hex64(local.get("object")), "invalid cached object reference")
                            local_path = cache / local["object"][:2] / local["object"]
                            if local_path.exists() or local_path.is_symlink():
                                need(all(type(local[k]) is int for k in ("offset", "bytes", "pack_bytes")),
                                     "invalid cached chunk dimensions")
                                need(0 <= local["offset"] < local["pack_bytes"] <= MAX_PACK_BYTES
                                     and 0 < local["bytes"] <= local["pack_bytes"] - local["offset"],
                                     "invalid cached chunk range")
                                ref = local
                        object_pin = ref["object"]
                        if object_pin != current_pin:
                            cached = cache / object_pin[:2] / object_pin
                            if not cached.exists() and not cached.is_symlink():
                                cipher = self.read_object(object_pin)
                                put_immutable(cached, cipher)
                                copied.add(object_pin)
                                transferred += len(cipher)
                            current_pack = self.open(checked_blob(cache, object_pin), MAX_PACK_BYTES)
                            current_pin = object_pin
                        need(len(current_pack) == ref["pack_bytes"], "pack length differs")
                        block = current_pack[ref["offset"]:ref["offset"] + ref["bytes"]]
                        need(sha256_bytes(block) == digest, "chunk checksum differs")
                        handle.write(block)
                        db.execute("INSERT OR REPLACE INTO chunks VALUES (?,?)",
                                   (digest, canonical_bytes(ref).decode()))
                    handle.flush()
                    os.fsync(handle.fileno())
                target.chmod(0o444)
                db.commit()
            control = staging / ".marketdata-receipt"
            need(not control.exists(), "reserved receipt path collision")
            control.mkdir()
            manifest = control / "snapshot.json"
            put_immutable(manifest, canonical_bytes(projection))
            verified = verify_directory(manifest, staging)
            receipt = {**verified, "catalog_pin": pin, "parent_snapshot_id": value["snapshot_id"],
                       "selection": sorted(selected), "copied_objects": sorted(copied),
                       "transferred_bytes": transferred, "read_only_files": True}
            put_immutable(control / "receipt.json", canonical_bytes(receipt))
            publish_directory(staging, destination)
            return receipt
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        finally:
            db.close()

    def compact(self, pin: str, *, execute: bool = False) -> dict:
        """Repack a pinned view; preserve every old catalog and pack indefinitely."""
        with writer_lock(self.root):
            original = self.catalog(pin)
            references = original["chunks"]
            objects = {ref["object"]: ref["pack_bytes"] for ref in references.values()}
            grouped: dict[str, list] = {}
            for digest, ref in sorted(references.items()):
                need(ref["bytes"] <= self.pack_bytes, "target pack smaller than existing chunk; no implicit rechunk")
                grouped.setdefault(ref["object"], []).append((digest, ref))
            target_objects, filled = 0, 0
            for object_pin in sorted(grouped):
                for _, ref in grouped[object_pin]:
                    if not filled or filled + ref["bytes"] > self.pack_bytes:
                        target_objects += 1
                        filled = 0
                    filled += ref["bytes"]
            live_bytes = sum(ref["bytes"] for ref in references.values())
            useful = target_objects < len(objects) or live_bytes < sum(objects.values())
            result = {"source_catalog": pin, "source_objects": len(objects),
                      "source_pack_bytes": sum(objects.values()), "live_chunk_bytes": live_bytes,
                      "target_pack_bytes": self.pack_bytes, "target_objects": target_objects,
                      "useful": useful, "executed": execute and useful,
                      "old_objects_deleted": 0}
            if not execute:
                return result
            if not useful:
                return {**result, "catalog_pin": pin, "snapshot_id": original["snapshot_id"], "new_objects": []}
            job = sha256_bytes(canonical_bytes({"format": FORMAT, "source_catalog": pin,
                                               "pack_bytes": self.pack_bytes,
                                               "compression_level": self.compression_level}))
            completion = self.root / "compactions" / job
            need(not completion.parent.is_symlink() and not completion.is_symlink(), "symlink compaction receipt refused")
            if completion.exists():
                need(completion.is_file() and completion.stat().st_size < 16384, "invalid compaction receipt")
                saved = json.loads(self.open(completion.read_bytes(), 8192))
                previous = self.catalog(saved["catalog_pin"])
                need(previous.get("compaction_of") == pin and previous["snapshot_id"] == original["snapshot_id"],
                     "compaction receipt differs from source")
                for ref in previous["chunks"].values():
                    self.require_object(ref["object"])
                return {**result, "executed": False, "reused_compaction": True,
                        "catalog_pin": saved["catalog_pin"], "snapshot_id": original["snapshot_id"], "new_objects": []}
            mappings: dict = {}
            pending: dict[str, tuple[int, int]] = {}
            pack = bytearray()
            created = []

            def flush() -> None:
                if not pack:
                    return
                cipher = self.seal(bytes(pack))
                digest = sha256_bytes(cipher)
                put_immutable(self.root / "objects" / digest[:2] / digest, cipher)
                for chunk, (offset, size) in pending.items():
                    mappings[chunk] = {"object": digest, "offset": offset, "bytes": size, "pack_bytes": len(pack)}
                created.append(digest)
                pending.clear()
                pack.clear()

            for object_pin in sorted(objects):
                source = self.open(self.read_object(object_pin), MAX_PACK_BYTES)
                need(len(source) == objects[object_pin], "source pack length differs")
                for digest, ref in grouped[object_pin]:
                    block = source[ref["offset"]:ref["offset"] + ref["bytes"]]
                    need(sha256_bytes(block) == digest, "compaction input checksum differs")
                    need(len(block) <= self.pack_bytes, "target pack smaller than existing chunk; no implicit rechunk")
                    if len(pack) + len(block) > self.pack_bytes:
                        flush()
                    pending[digest] = (len(pack), len(block))
                    pack.extend(block)
            flush()
            compacted = dict(original, chunks=mappings, compaction_of=pin)
            raw = canonical_bytes(compacted)
            need(len(raw) <= MAX_CATALOG_BYTES, "compacted catalog exceeds bound")
            self.validate_catalog(compacted, raw)
            cipher = self.seal(raw)
            new_pin = sha256_bytes(cipher)
            put_immutable(self.root / "catalogs" / new_pin, cipher)
            self.catalog(new_pin)
            put_immutable(completion, self.seal(canonical_bytes({"catalog_pin": new_pin})))
            return {**result, "catalog_pin": new_pin, "snapshot_id": original["snapshot_id"],
                    "new_objects": created}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--key-file", type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare", help="Import a closed inventory, never a running DB")
    prepare.add_argument("--inventory", type=Path, required=True)
    prepare.add_argument("--parent", help="Explicit previous catalog pin; no latest lookup")
    restore = commands.add_parser("restore")
    restore.add_argument("--pin", required=True)
    restore.add_argument("--destination", type=Path, required=True)
    restore.add_argument("--cache", type=Path, required=True)
    restore.add_argument("--path", action="append", dest="paths")
    compact = commands.add_parser("compact", help="Repack a pinned view; dry run unless --execute")
    compact.add_argument("--pin", required=True)
    compact.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    config = yaml.safe_load(args.config.read_text())["archive"]
    try:
        archive = Archive(args.archive, args.key_file or Path(config["key_file"]),
                          create=args.command == "prepare", pack_bytes=config["pack_bytes"],
                          compression_level=config["compression_level"])
        if args.command == "prepare":
            result = archive.prepare(args.inventory, parent=args.parent)
        elif args.command == "restore":
            result = archive.restore(args.pin, args.destination, args.cache, paths=args.paths)
        else:
            result = archive.compact(args.pin, execute=args.execute)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (Refusal, OSError, ValueError, KeyError) as exc:
        parser.exit(1, f"archive refused: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
