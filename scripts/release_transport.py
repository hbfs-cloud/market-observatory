#!/usr/bin/env python3
"""Pinned encrypted GitHub Releases transport; never schedules or prunes data."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import time
from urllib.parse import quote
import uuid

import yaml

from cache_common import put_immutable, real_directory, writer_lock
from object_archive import Archive, MAX_CATALOG_BYTES
from vendor.immutable_cache_release import Refusal, canonical_bytes, is_hex64, need, sha256_bytes

READY_FORMAT = "marketdata-ready-v1"
INDEX_FORMAT = "marketdata-transport-v1"


def timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    need(parsed.tzinfo is not None, "explicit timestamp offset required")
    return parsed.astimezone(timezone.utc)


def asset_reference(asset: dict, release: dict, digest: str, size: int) -> dict:
    need(asset.get("state") == "uploaded", "asset is not uploaded")
    need(asset.get("size") == size and asset.get("digest") == f"sha256:{digest}", "remote asset fingerprint differs")
    need(type(asset.get("id")) is int and asset["id"] > 0, "invalid asset ID")
    return {"id": asset["id"], "release": release["id"], "sha256": digest, "bytes": size}


class GitHub:
    """gh owns authentication; never extract a token or accept arbitrary hosts."""
    def __init__(self, config: dict, *, runner=subprocess.run, sleep=time.sleep):
        self.config, self.runner, self.sleep = config, runner, sleep
        self.repo = config["repository"]
        need(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", self.repo), "invalid repository")
        self.prefix = f"repos/{self.repo}"
        self.transferred = 0

    def api(self, suffix: str, *, method="GET", value=None, optional=False, binary=False):
        path = self.prefix + (f"/{suffix}" if suffix else "")
        args = ["gh", "api", "--hostname", "github.com", "--include", "--method", method,
                "-H", "Accept: application/octet-stream" if binary else "Accept: application/vnd.github+json",
                "-H", f"X-GitHub-Api-Version: {self.config['api_version']}", path]
        raw = None
        if value is not None:
            args += ["--input", "-"]
            raw = canonical_bytes(value)
        env = dict(os.environ, GH_PROMPT_DISABLED="1", GH_PAGER="cat")
        env.pop("GH_DEBUG", None)
        for attempt in range(self.config["max_attempts"]):
            result = self.runner(args, input=raw, capture_output=True, timeout=self.config["timeout_seconds"], env=env)
            data = result.stdout
            status, headers = 0, {}
            # gh may print both redirect and final response header blocks.
            while data.startswith(b"HTTP/"):
                separator = b"\r\n\r\n" if b"\r\n\r\n" in data else b"\n\n"
                head, found, data = data.partition(separator)
                need(found, "invalid gh HTTP framing")
                lines = head.decode("ascii", errors="replace").splitlines()
                status = int(lines[0].split()[1])
                headers = {k.lower(): v.strip() for line in lines[1:] if ":" in line
                           for k, v in [line.split(":", 1)]}
            if status == 404 and optional:
                return None
            if result.returncode == 0 and 200 <= status < 300:
                need(len(data) <= self.config["max_asset_bytes"], "API response exceeds bound")
                if binary:
                    self.transferred += len(data)
                    return data
                return json.loads(data) if data.strip() else None
            rate_limited = status == 429 or (status == 403 and
                                           ("retry-after" in headers or headers.get("x-ratelimit-remaining") == "0"))
            if not (rate_limited or status in (408, 500, 502, 503, 504)):
                raise Refusal(f"GitHub {method} refused (HTTP {status}); response body/credentials suppressed")
            if attempt + 1 == self.config["max_attempts"]:
                raise Refusal(f"GitHub retry budget exhausted (HTTP {status})")
            from incremental_collector import retry_delay
            normalized = {"Retry-After": headers.get("retry-after", "")}
            delay = retry_delay(normalized, attempt + 1, self.config, time.time())
            if headers.get("x-ratelimit-remaining") == "0":
                delay = max(delay, float(headers.get("x-ratelimit-reset", "0")) - time.time())
            self.sleep(max(0, delay))
        raise Refusal("GitHub request failed")

    def preflight(self):
        repo = self.api("")
        need(repo.get("private") is True, "archive repository must be private")
        need(not repo.get("archived"), "archive repository is archived")
        if self.config["require_immutable_releases"]:
            policy = self.api("immutable-releases")
            need(policy.get("enabled") is True, "immutable releases must be enabled by the owner before publishing")
        return repo

    def pages(self, suffix):
        separator = "&" if "?" in suffix else "?"
        for page in range(1, self.config["max_pages"] + 1):
            values = self.api(f"{suffix}{separator}per_page=100&page={page}")
            need(isinstance(values, list), "expected paginated list")
            yield from values
            if len(values) < 100:
                return
        raise Refusal("pagination budget exceeded; list is not complete")

    def releases(self):
        return self.pages("releases")

    def find_release(self, tag):
        release = self.api(f"releases/tags/{quote(tag, safe='')}", optional=True)
        if release is not None:
            return release
        return next((r for r in self.releases() if r["tag_name"] == tag), None)

    def draft(self, tag):
        found = self.find_release(tag)
        if found:
            return found
        return self.api("releases", method="POST", value={"tag_name": tag, "name": tag,
                        "draft": True, "make_latest": "false", "generate_release_notes": False})

    def assets(self, release):
        return list(self.pages(f"releases/{release['id']}/assets"))

    def ensure_asset(self, release, path, name):
        need(path.name == name, "asset name must match the explicit local filename")
        data = path.read_bytes()
        digest = sha256_bytes(data)
        need(len(data) <= self.config["max_asset_bytes"], "asset exceeds configured bound")
        existing = next((a for a in self.assets(release) if a["name"] == name), None)
        if existing is None:
            need(release["draft"], "cannot append an asset to a closed release")
            # gh uploads without --clobber. A lost response is resolved by listing
            # the exact immutable name, never by deleting/replacing an asset.
            env = dict(os.environ, GH_PROMPT_DISABLED="1", GH_PAGER="cat", GH_HOST="github.com")
            env.pop("GH_DEBUG", None)
            uploaded = self.runner(["gh", "release", "upload", release["tag_name"], "--repo", self.repo,
                                    str(path)], capture_output=True,
                                   timeout=self.config["timeout_seconds"], env=env)
            existing = next((a for a in self.assets(release) if a["name"] == name), None)
            need(existing is not None, f"asset upload incomplete (exit {uploaded.returncode}); retry same publication")
        return asset_reference(existing, release, digest, len(data))

    def close(self, release, body):
        if release["draft"]:
            release = self.api(f"releases/{release['id']}", method="PATCH",
                               value={"draft": False, "make_latest": "false", "body": body})
        need(not release["draft"] and release.get("body", "") == body, "closed release differs")
        if self.config["require_immutable_releases"]:
            need(release.get("immutable") is True, "release was not made immutable")
        return release

    def download(self, ref):
        validate_ref(ref, self.config["max_asset_bytes"])
        data = self.api(f"releases/assets/{ref['id']}", binary=True)
        need(len(data) == ref["bytes"] and sha256_bytes(data) == ref["sha256"], "download fingerprint differs")
        return data

    def lock_status(self):
        ref = self.api("git/ref/tags/marketdata-writer-lock", optional=True)
        return {"locked": ref is not None, "lock_sha": ref["object"]["sha"] if ref else None}

    def unlock(self, expected_sha, *, owner_stopped=False):
        need(owner_stopped and re.fullmatch(r"[0-9a-f]{40}", expected_sha),
             "manual lock recovery requires the exact SHA and confirmation that the owner is stopped")
        current = self.lock_status()
        need(current["locked"] and current["lock_sha"] == expected_sha, "lock changed; refusing recovery")
        self.api("git/refs/tags/marketdata-writer-lock", method="DELETE")
        return {"unlocked_sha": expected_sha, "automatic_takeover": False}

    @contextmanager
    def lock(self):
        repo = self.preflight()
        head = self.api(f"git/ref/heads/{quote(repo['default_branch'], safe='')}")["object"]["sha"]
        name = "marketdata-writer-lock"
        obj = self.api("git/tags", method="POST", value={"tag": name, "message": str(uuid.uuid4()),
                                                      "object": head, "type": "commit"})
        self.api("git/refs", method="POST", value={"ref": f"refs/tags/{name}", "sha": obj["sha"]})
        try:
            yield
        finally:
            current = self.api(f"git/ref/tags/{name}")
            need(current["object"]["sha"] == obj["sha"], "writer lock ownership changed; do not delete it")
            self.api(f"git/refs/tags/{name}", method="DELETE")


def validate_ref(ref, limit):
    need(isinstance(ref, dict) and set(ref) == {"id", "release", "sha256", "bytes"}, "invalid asset reference")
    need(is_hex64(ref["sha256"]) and type(ref["bytes"]) is int and 0 < ref["bytes"] <= limit, "invalid asset bounds")
    need(all(type(ref[k]) is int and ref[k] > 0 for k in ("id", "release")), "invalid asset IDs")


class Transport:
    def __init__(self, backend, config):
        self.backend, self.config = backend, config

    def published(self, tag, ready_pin=None, *, release=None):
        need(re.fullmatch(r"md-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{64}", tag), "explicit snapshot release tag required")
        release = release or self.backend.find_release(tag)
        need(release is not None and not release["draft"], "snapshot release missing or still draft")
        if self.config["require_immutable_releases"]:
            need(release.get("immutable") is True, "mutable release refused")
        ready = json.loads(release.get("body") or "null")
        need(isinstance(ready, dict) and ready.get("format") == READY_FORMAT, "release has no READY commit")
        raw = canonical_bytes(ready)
        pin = sha256_bytes(raw)
        need(ready_pin is None or pin == ready_pin, "READY pin differs")
        need(ready.get("repository") == self.backend.repo and ready.get("tag") == tag, "READY repository/tag differs")
        need(is_hex64(ready.get("catalog_pin")) and is_hex64(ready.get("snapshot_id")), "invalid READY pins")
        timestamp(ready["as_of"])
        for key in ("keyring", "catalog", "index"):
            validate_ref(ready[key], self.config["max_asset_bytes"])
            need(ready[key]["release"] == release["id"], "control asset belongs to another release")
        need(ready["catalog"]["sha256"] == ready["catalog_pin"], "catalog reference differs")
        return ready, pin

    def mount(self, tag, ready_pin, root, key_file):
        need(is_hex64(ready_pin), "restoration requires an explicit READY pin")
        ready, _ = self.published(tag, ready_pin)
        root = real_directory(root)
        def obtain(ref, path):
            real_directory(path.parent)
            if path.exists() or path.is_symlink():
                need(not path.is_symlink() and path.is_file() and path.stat().st_size == ref["bytes"], "cached control differs")
                data = path.read_bytes()
                need(sha256_bytes(data) == ref["sha256"], "cached control checksum differs")
            else:
                data = self.backend.download(ref)
                put_immutable(path, data)
            return data
        obtain(ready["keyring"], root / "keyring.age")
        archive = Archive(root, key_file)
        obtain(ready["catalog"], root / "catalogs" / ready["catalog_pin"])
        encrypted = obtain(ready["index"], root / "transport" / ready["index"]["sha256"])
        index = json.loads(archive.open(encrypted, MAX_CATALOG_BYTES))
        need(index.get("format") == INDEX_FORMAT and index.get("catalog_pin") == ready["catalog_pin"], "transport index differs")
        catalog = archive.catalog(ready["catalog_pin"])
        need(catalog["snapshot_id"] == ready["snapshot_id"], "READY snapshot differs")
        objects = {c["object"] for c in catalog["chunks"].values()}
        need(set(index["objects"]) == objects, "transport dependency closure differs")
        for digest, ref in index["objects"].items():
            validate_ref(ref, self.config["max_asset_bytes"])
            need(ref["sha256"] == digest, "transport object pin differs")
        archive.object_loader = lambda pin: self.backend.download(index["objects"][pin])
        archive.object_exists = lambda pin: pin in index["objects"]
        return archive, ready, index

    def publish(self, archive, pin, as_of, *, parent=None):
        instant = timestamp(as_of)
        need(instant <= datetime.now(timezone.utc), "future publication cutoff refused")
        as_of = instant.isoformat().replace("+00:00", "Z")
        tag = f"md-{instant.strftime('%Y%m%dT%H%M%SZ')}-{pin}"
        catalog = archive.catalog(pin)
        with writer_lock(archive.root), self.backend.lock():
            existing = self.backend.find_release(tag)
            if existing and not existing["draft"]:
                ready, ready_pin = self.published(tag)
                need(ready["catalog_pin"] == pin and ready["snapshot_id"] == catalog["snapshot_id"], "existing release differs")
                return {"tag": tag, "ready_pin": ready_pin, "catalog_pin": pin, "new_objects": 0}
            committed = [r for r in self.backend.releases() if not r["draft"]
                         and re.fullmatch(r"md-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{64}", r["tag_name"])]
            if committed:
                tip = max(committed, key=lambda r: r["id"])
                tip_ready, _ = self.published(tip["tag_name"], release=tip)
                need(parent is not None and parent["catalog_pin"] == tip_ready["catalog_pin"],
                     "stale or missing producer parent; rebase collection on the committed head")
                need(timestamp(as_of) >= timestamp(tip_ready["as_of"]), "archive cutoff must not move backwards")
                ancestor = catalog.get("compaction_of", catalog["lineage"].get("parent_catalog"))
                visited = {pin}
                while ancestor and ancestor != parent["catalog_pin"] and ancestor not in visited:
                    visited.add(ancestor)
                    previous = archive.catalog(ancestor)
                    ancestor = previous.get("compaction_of", previous["lineage"].get("parent_catalog"))
                need(ancestor == parent["catalog_pin"], "catalog does not descend from the committed parent")
            refs = {}
            if parent:
                need(parent.get("format") == INDEX_FORMAT, "invalid parent transport index")
                # The parent must have been loaded/verified with mount, not inferred
                # from a mutable latest release or a directory listing.
                refs.update(parent["objects"])
            objects = sorted({c["object"] for c in catalog["chunks"].values()})
            missing = [obj for obj in objects if obj not in refs]
            capacity = self.config["assets_per_release"]
            need(4 <= capacity <= 1000, "asset limit must be 4..1000")
            work = real_directory(archive.root / "publication" / tag)
            for number, offset in enumerate(range(0, len(missing), capacity)):
                batch = self.backend.draft(f"{tag}-objects-{number:05d}")
                for obj in missing[offset:offset + capacity]:
                    path = work / f"{obj}.age"
                    if not path.exists():
                        put_immutable(path, archive.read_object(obj))
                    refs[obj] = self.backend.ensure_asset(batch, path, path.name)
                self.backend.close(batch, "Encrypted immutable data objects; referenced by a pinned READY catalog.")
            release = self.backend.draft(tag)
            keyring = self.backend.ensure_asset(release, archive.root / "keyring.age", "keyring.age")
            catalog_path = work / f"{pin}.age"
            put_immutable(catalog_path, (archive.root / "catalogs" / pin).read_bytes())
            catalog_ref = self.backend.ensure_asset(release, catalog_path, catalog_path.name)
            index = {"format": INDEX_FORMAT, "catalog_pin": pin, "objects": {o: refs[o] for o in objects}}
            for obj, ref in index["objects"].items():
                validate_ref(ref, self.config["max_asset_bytes"])
                need(ref["sha256"] == obj, "parent object reference differs")
            index_path = work / "transport.age"
            raw = canonical_bytes(index)
            need(len(raw) <= MAX_CATALOG_BYTES, "transport index exceeds bound; partition dataset")
            if index_path.exists():
                need(not index_path.is_symlink() and archive.open(index_path.read_bytes(), MAX_CATALOG_BYTES) == raw,
                     "resumed publication index differs")
            else:
                uploaded = next((a for a in self.backend.assets(release) if a["name"] == "transport.age"), None)
                if uploaded:
                    digest = uploaded.get("digest", "").removeprefix("sha256:")
                    ref = asset_reference(uploaded, release, digest, uploaded["size"])
                    cipher = self.backend.download(ref)
                    need(archive.open(cipher, MAX_CATALOG_BYTES) == raw, "remote resumed index differs")
                else:
                    cipher = archive.seal(raw)
                put_immutable(index_path, cipher)
            index_ref = self.backend.ensure_asset(release, index_path, "transport.age")
            ready = {"format": READY_FORMAT, "repository": self.backend.repo, "tag": tag, "as_of": as_of,
                     "catalog_pin": pin, "snapshot_id": catalog["snapshot_id"],
                     "keyring": keyring, "catalog": catalog_ref, "index": index_ref}
            ready_raw = canonical_bytes(ready)
            ready_pin = sha256_bytes(ready_raw)
            ready_path = work / f"READY-{ready_pin}.json"
            put_immutable(ready_path, ready_raw)
            self.backend.ensure_asset(release, ready_path, ready_path.name)
            self.backend.close(release, ready_raw.decode())
            self.published(tag, ready_pin)
            return {"tag": tag, "ready_pin": ready_pin, "catalog_pin": pin, "new_objects": len(missing)}

    def resolve(self, at, *, known_before=None):
        cutoff = timestamp(at)
        knowledge = timestamp(known_before) if known_before else datetime.now(timezone.utc)
        candidates = []
        for release in self.backend.releases():
            tag = release["tag_name"]
            if release["draft"] or not re.fullmatch(r"md-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{64}", tag):
                continue
            ready, pin = self.published(tag, release=release)
            published = timestamp(release["published_at"])
            if timestamp(ready["as_of"]) <= cutoff and published <= knowledge:
                candidates.append((timestamp(ready["as_of"]), published, tag, pin, ready))
        need(candidates, "no published snapshot at the requested cutoffs")
        _, _, tag, pin, ready = max(candidates)
        return {"tag": tag, "ready_pin": pin, "catalog_pin": ready["catalog_pin"],
                "snapshot_id": ready["snapshot_id"], "as_of": ready["as_of"],
                "note": "archive vintage selection, not a PIT data qualification"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--key-file", type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("--archive", type=Path, required=True)
    publish.add_argument("--pin", required=True)
    publish.add_argument("--as-of", required=True)
    publish.add_argument("--parent-tag")
    publish.add_argument("--parent-ready")
    publish.add_argument("--execute", action="store_true")
    resolve = commands.add_parser("resolve")
    resolve.add_argument("--at", required=True)
    resolve.add_argument("--known-before")
    restore = commands.add_parser("restore")
    restore.add_argument("--tag", required=True)
    restore.add_argument("--ready-pin", required=True)
    restore.add_argument("--control", type=Path, required=True)
    restore.add_argument("--destination", type=Path, required=True)
    restore.add_argument("--cache", type=Path, required=True)
    restore.add_argument("--path", action="append", dest="paths")
    commands.add_parser("lock-status")
    unlock = commands.add_parser("unlock", help="Manual crash recovery, never an automatic lease takeover")
    unlock.add_argument("--expected-sha", required=True)
    unlock.add_argument("--confirm-owner-stopped", action="store_true")
    unlock.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    try:
        config = yaml.safe_load(args.config.read_text())
        need(config["schema_version"] == 1 and config["automatic_scheduling"] is False, "manual policy required")
        backend = GitHub(config)
        transport = Transport(backend, config)
        key = args.key_file or Path(config["key_file"])
        if args.command == "publish":
            need(args.execute, "publication requires explicit --execute; no remote writes performed")
            archive = Archive(args.archive, key)
            parent = None
            if args.parent_tag or args.parent_ready:
                need(args.parent_tag and args.parent_ready, "both parent tag and READY pin required")
                archive, _, parent = transport.mount(args.parent_tag, args.parent_ready, args.archive, key)
            result = transport.publish(archive, args.pin, args.as_of, parent=parent)
        elif args.command == "resolve":
            result = transport.resolve(args.at, known_before=args.known_before)
        elif args.command == "lock-status":
            result = backend.lock_status()
        elif args.command == "unlock":
            need(args.execute, "lock recovery requires --execute")
            result = backend.unlock(args.expected_sha, owner_stopped=args.confirm_owner_stopped)
        else:
            archive, ready, _ = transport.mount(args.tag, args.ready_pin, args.control, key)
            result = archive.restore(ready["catalog_pin"], args.destination, args.cache, paths=args.paths)
            result["http_download_bytes_including_control"] = backend.transferred
        print(json.dumps(result, sort_keys=True))
        return 0
    except (Refusal, OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"transport refused: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
