#!/usr/bin/env python3
"""Package, publish, restore and verify hash-pinned immutable cache snapshots.

``prepare`` converts a sealed TRANSFER_INPUTS.json into a portable snapshot whose
logical paths begin with ``root/`` or ``ssd/``.  It streams the listed regular
files into a reproducible tar.gz and splits it into GitHub-safe assets.  ``publish``
uses only the authenticated ``gh`` CLI; it never stores a token, replaces an asset,
or reuses a published tag.  ``restore`` verifies every downloaded asset before it
extracts to a newly-created destination, then publishes that directory atomically.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import tarfile
import tempfile
import uuid
from typing import Any, BinaryIO, Iterable


GITHUB_API_VERSION = "2026-03-10"
MAX_ASSET_BYTES = 2 * 1024**3
DEFAULT_PART_BYTES = 1024**3
SCHEMA_VERSION = 1


class Refusal(RuntimeError):
    """The requested operation cannot preserve the immutable-cache contract."""


def need(condition: bool, message: str) -> None:
    if not condition:
        raise Refusal(message)


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n"


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_canonical(path: Path, value: Any) -> None:
    with path.open("xb") as handle:
        handle.write(canonical_bytes(value))


def signature(path: Path) -> tuple[int, int, int, int, int, int]:
    value = path.stat()
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns,
            value.st_ctime_ns, value.st_nlink)


def is_hex64(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def valid_root_label(value: object) -> bool:
    return isinstance(value, str) and bool(value) and value[0].isalpha() and all(char.isalnum() or char in "_-" for char in value)


def portable_parts(value: str) -> tuple[str, ...]:
    candidate = PurePosixPath(value)
    parts = candidate.parts
    need(bool(parts) and valid_root_label(parts[0]), "portable path must start with a valid root label")
    need(not candidate.is_absolute() and len(parts) > 1, "portable path must name a file below its root")
    need(all(part not in {"", ".", ".."} for part in parts), "portable path traversal refused")
    need("\\" not in value, "portable path backslash refused")
    return parts


def source_path(root: Path, relative: Path) -> Path:
    """Return a lexical source path only after refusing every symlink component."""
    need(root.is_absolute(), "source root must be absolute")
    need(not root.is_symlink() and root.is_dir(), f"source root is not a real directory: {root}")
    candidate = root
    for component in relative.parts:
        need(component not in {"", ".", ".."}, "source path traversal refused")
        candidate /= component
        need(candidate.exists() or candidate.is_symlink(), f"listed source missing: {candidate}")
        need(not candidate.is_symlink(), f"symlink refused: {candidate}")
    mode = candidate.lstat().st_mode
    need(stat.S_ISREG(mode), f"listed source is not a regular file: {candidate}")
    return candidate


def fingerprint_source(path: Path, root: Path, relative: Path) -> tuple[int, str, tuple[int, int, int, int, int, int]]:
    checked = source_path(root, relative)
    before = signature(checked)
    digest = sha256_file(checked)
    after = signature(checked)
    need(before == after, f"source changed while hashing: {checked}")
    return before[2], digest, before


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Refusal(f"invalid JSON: {path}: {exc}") from exc


def portable_snapshot(inventory_path: Path) -> tuple[dict[str, Any], dict[str, Path]]:
    raw = inventory_path.read_bytes()
    inventory = read_json(inventory_path)
    need(isinstance(inventory, dict) and inventory.get("schema_version") == 1, "TRANSFER_INPUTS schema_version 1 required")
    roots_value = inventory.get("source_roots")
    need(isinstance(roots_value, dict) and roots_value, "non-empty source_roots required")
    roots = {name: Path(value) for name, value in roots_value.items() if isinstance(value, str)}
    need(set(roots) == set(roots_value) and all(valid_root_label(name) for name in roots), "source root labels and paths invalid")
    for name, root in roots.items():
        need(root.is_absolute() and root.is_dir() and not root.is_symlink(), f"source root is not a real absolute directory: {name}")
    root_items = list(roots.items())
    for index, (_, left) in enumerate(root_items):
        for _, right in root_items[index + 1:]:
            need(not left.is_relative_to(right) and not right.is_relative_to(left), "source roots must not be nested")
    files = inventory.get("files")
    need(isinstance(files, list) and files, "non-empty files required")
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    total = 0
    for item in files:
        need(isinstance(item, dict), "file item must be an object")
        path_value, bytes_value, digest = item.get("path"), item.get("bytes"), item.get("sha256")
        need(isinstance(path_value, str) and Path(path_value).is_absolute(), "file path must be absolute")
        need(isinstance(bytes_value, int) and bytes_value >= 0 and is_hex64(digest), "invalid file fingerprint")
        logical = Path(path_value)
        matches = [name for name, root in roots.items() if logical.is_relative_to(root)]
        need(len(matches) == 1, f"file must be under exactly one source root: {logical}")
        name = matches[0]
        relative = logical.relative_to(roots[name])
        need(not relative.is_absolute() and all(part not in {"", ".", ".."} for part in relative.parts), "non-canonical source path")
        portable = f"{name}/{relative.as_posix()}"
        portable_parts(portable)
        need(portable not in seen, f"duplicate portable path: {portable}")
        seen.add(portable)
        actual_bytes, actual_digest, _ = fingerprint_source(logical, roots[name], relative)
        need((actual_bytes, actual_digest) == (bytes_value, digest), f"source differs from inventory: {logical}")
        rows.append({"path": portable, "bytes": bytes_value, "sha256": digest})
        total += bytes_value
    declared_total = inventory.get("total_bytes")
    need(declared_total == total, "inventory total_bytes differs from file rows")
    rows.sort(key=lambda item: item["path"])
    return ({"schema_version": SCHEMA_VERSION, "source_inventory_sha256": sha256_bytes(raw),
             "files": rows, "total_bytes": total}, roots)


def parse_root_argument(value: str) -> tuple[str, Path]:
    name, separator, raw_path = value.partition("=")
    need(separator == "=" and valid_root_label(name) and raw_path, "--root must be LABEL=/absolute/path")
    path = Path(raw_path)
    need(path.is_absolute() and path.is_dir() and not path.is_symlink(), f"inventory root is not a real directory: {path}")
    return name, path


def inventory_from_roots(root_arguments: list[str], out: Path) -> dict[str, Any]:
    """Freeze only the explicitly named cache roots; never infer a repository tree."""
    need(not out.exists() and not out.is_symlink(), "inventory output already exists")
    roots: dict[str, Path] = {}
    for argument in root_arguments:
        name, path = parse_root_argument(argument)
        need(name not in roots, f"duplicate inventory root label: {name}")
        roots[name] = path
    need(roots, "inventory requires at least one explicit --root")
    root_items = list(roots.items())
    for index, (_, left) in enumerate(root_items):
        for _, right in root_items[index + 1:]:
            need(not left.is_relative_to(right) and not right.is_relative_to(left), "inventory roots must not be nested")
    files: list[dict[str, Any]] = []
    for name in sorted(roots):
        root = roots[name]
        for current, directories, names in os.walk(root, followlinks=False):
            current_path = Path(current)
            for directory in directories:
                candidate = current_path / directory
                need(not candidate.is_symlink(), f"symlink directory refused: {candidate}")
            for filename in names:
                candidate = current_path / filename
                need(not candidate.is_symlink(), f"symlink file refused: {candidate}")
                if not stat.S_ISREG(candidate.lstat().st_mode):
                    raise Refusal(f"non-regular inventory member refused: {candidate}")
                relative = candidate.relative_to(root)
                size, digest, _ = fingerprint_source(candidate, root, relative)
                files.append({"path": str(candidate), "bytes": size, "sha256": digest})
    files.sort(key=lambda item: item["path"])
    document = {"schema_version": SCHEMA_VERSION, "files": files,
                "total_bytes": sum(item["bytes"] for item in files),
                "source_roots": {name: str(roots[name]) for name in sorted(roots)}}
    write_canonical(out, document)
    return document


def validate_snapshot(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    value = read_json(path)
    need(isinstance(value, dict) and value.get("schema_version") == SCHEMA_VERSION, "snapshot schema invalid")
    need(raw == canonical_bytes(value), "snapshot JSON is not canonical")
    need(is_hex64(value.get("source_inventory_sha256")), "snapshot source inventory pin invalid")
    files = value.get("files")
    need(isinstance(files, list) and files, "snapshot files invalid")
    previous = ""
    total = 0
    for item in files:
        need(isinstance(item, dict) and set(item) == {"path", "bytes", "sha256"}, "snapshot file row invalid")
        name, size, digest = item["path"], item["bytes"], item["sha256"]
        need(isinstance(name, str) and isinstance(size, int) and size >= 0 and is_hex64(digest), "snapshot fingerprint invalid")
        portable_parts(name)
        need(name > previous, "snapshot paths must be sorted and unique")
        previous, total = name, total + size
    need(value.get("total_bytes") == total, "snapshot total_bytes invalid")
    return value, sha256_bytes(raw)


def write_tar_gz(destination: Path, snapshot: dict[str, Any], roots: dict[str, Path]) -> None:
    with destination.open("xb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0, compresslevel=9) as compressed:
            with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.GNU_FORMAT) as archive:
                for item in snapshot["files"]:
                    parts = portable_parts(item["path"])
                    source = source_path(roots[parts[0]], Path(*parts[1:]))
                    size, digest, _ = fingerprint_source(source, roots[parts[0]], Path(*parts[1:]))
                    need((size, digest) == (item["bytes"], item["sha256"]), f"source changed before archive entry: {source}")
                    info = tarfile.TarInfo(item["path"])
                    info.size, info.mode, info.mtime = size, 0o644, 0
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    with source.open("rb") as stream:
                        archive.addfile(info, stream)
    for item in snapshot["files"]:
        parts = portable_parts(item["path"])
        source = source_path(roots[parts[0]], Path(*parts[1:]))
        size, digest, _ = fingerprint_source(source, roots[parts[0]], Path(*parts[1:]))
        need((size, digest) == (item["bytes"], item["sha256"]), f"source changed while archiving: {source}")


def split_archive(archive: Path, destination: Path, snapshot_id: str, part_bytes: int) -> list[dict[str, Any]]:
    need(0 < part_bytes < MAX_ASSET_BYTES, "--part-bytes must be positive and below 2 GiB")
    temporary: list[Path] = []
    with archive.open("rb") as source:
        index = 1
        while True:
            block = source.read(part_bytes)
            if not block:
                break
            candidate = destination / f".part-{index:06d}"
            with candidate.open("xb") as handle:
                handle.write(block)
            temporary.append(candidate)
            index += 1
    need(temporary, "empty archive refused")
    width = len(str(len(temporary)))
    parts: list[dict[str, Any]] = []
    for index, candidate in enumerate(temporary, 1):
        name = f"archive-{snapshot_id}.tar.gz.part-{index:0{width}d}-of-{len(temporary):0{width}d}"
        target = destination / name
        candidate.rename(target)
        size = target.stat().st_size
        need(size < MAX_ASSET_BYTES, "archive part reaches GitHub 2 GiB limit")
        parts.append({"name": name, "bytes": size, "sha256": sha256_file(target)})
    return parts


def package_index(snapshot_id: str, snapshot_file: Path, archive_bytes: int, archive_sha256: str,
                  parts: list[dict[str, Any]]) -> dict[str, Any]:
    return {"schema_version": SCHEMA_VERSION, "snapshot_id": snapshot_id,
            "snapshot": {"name": snapshot_file.name, "bytes": snapshot_file.stat().st_size,
                         "sha256": sha256_file(snapshot_file)},
            "archive": {"name": f"archive-{snapshot_id}.tar.gz", "bytes": archive_bytes,
                        "sha256": archive_sha256, "parts": parts}}


def prepare(inventory: Path, out: Path, *, part_bytes: int = DEFAULT_PART_BYTES) -> dict[str, Any]:
    need(not out.exists() and not out.is_symlink(), "prepare output already exists")
    need(out.parent.is_dir(), "prepare output parent must exist")
    snapshot, roots = portable_snapshot(inventory)
    snapshot_payload = canonical_bytes(snapshot)
    snapshot_id = sha256_bytes(snapshot_payload)
    staging = out.parent / f".{out.name}.preparing-{uuid.uuid4().hex}"
    staging.mkdir()
    try:
        snapshot_file = staging / f"snapshot-{snapshot_id}.json"
        write_canonical(snapshot_file, snapshot)
        archive = staging / ".archive.tar.gz"
        write_tar_gz(archive, snapshot, roots)
        archive_bytes, archive_sha = archive.stat().st_size, sha256_file(archive)
        parts = split_archive(archive, staging, snapshot_id, part_bytes)
        archive.unlink()
        index = package_index(snapshot_id, snapshot_file, archive_bytes, archive_sha, parts)
        index_file = staging / f"release-{snapshot_id}.json"
        write_canonical(index_file, index)
        os.replace(staging, out)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {"snapshot_id": snapshot_id, "snapshot": snapshot_file.name, "release": index_file.name,
            "parts": len(parts), "total_bytes": snapshot["total_bytes"]}


def load_release_index(path: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    value = read_json(path)
    need(raw == canonical_bytes(value), "release index JSON is not canonical")
    need(isinstance(value, dict) and value.get("schema_version") == SCHEMA_VERSION and is_hex64(value.get("snapshot_id")), "release index invalid")
    snapshot, archive = value.get("snapshot"), value.get("archive")
    need(isinstance(snapshot, dict) and isinstance(archive, dict), "release index assets missing")
    for entry in (snapshot, archive):
        need(isinstance(entry.get("name"), str) and isinstance(entry.get("bytes"), int) and entry["bytes"] >= 0 and is_hex64(entry.get("sha256")), "release index fingerprint invalid")
    parts = archive.get("parts")
    need(isinstance(parts, list) and parts, "release archive parts missing")
    for part in parts:
        need(isinstance(part, dict) and set(part) == {"name", "bytes", "sha256"}, "release part invalid")
        need(isinstance(part["name"], str) and isinstance(part["bytes"], int) and 0 < part["bytes"] < MAX_ASSET_BYTES and is_hex64(part["sha256"]), "release part fingerprint invalid")
    need(len({part["name"] for part in parts}) == len(parts), "duplicate release part")
    return value


def verify_asset(path: Path, expected: dict[str, Any], root: Path) -> None:
    relative = path.relative_to(root)
    checked = source_path(root, relative)
    size, digest, _ = fingerprint_source(checked, root, relative)
    need((size, digest) == (expected["bytes"], expected["sha256"]), f"asset differs: {path.name}")


def package_files(package: Path) -> tuple[dict[str, Any], Path, Path]:
    need(package.is_dir() and not package.is_symlink(), "package must be a real directory")
    indices = list(package.glob("release-*.json"))
    need(len(indices) == 1 and not indices[0].is_symlink(), "package must contain exactly one release index")
    index_path = indices[0]
    index = load_release_index(index_path)
    snapshot_path = package / index["snapshot"]["name"]
    verify_asset(snapshot_path, index["snapshot"], package)
    snapshot, snapshot_id = validate_snapshot(snapshot_path)
    need(snapshot_id == index["snapshot_id"], "snapshot ID differs from release index")
    parts = []
    for item in index["archive"]["parts"]:
        path = package / item["name"]
        verify_asset(path, item, package)
        parts.append(path)
    return index, snapshot_path, index_path


class JoinedReader(io.RawIOBase):
    def __init__(self, paths: Iterable[Path]):
        self.paths = iter(paths)
        self.current: BinaryIO | None = None

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        chunks: list[bytes] = []
        remaining = size
        while remaining != 0:
            if self.current is None:
                try:
                    self.current = next(self.paths).open("rb")
                except StopIteration:
                    break
            value = self.current.read(remaining)
            if value:
                chunks.append(value)
                if remaining > 0:
                    remaining -= len(value)
            else:
                self.current.close()
                self.current = None
        return b"".join(chunks)

    def close(self) -> None:
        if self.current is not None:
            self.current.close()
        super().close()


def archive_fingerprint(parts: list[Path]) -> tuple[int, str]:
    digest, total = hashlib.sha256(), 0
    for part in parts:
        with part.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
                total += len(block)
    return total, digest.hexdigest()


def extract_archive(parts: list[Path], snapshot: dict[str, Any], destination: Path) -> None:
    expected = iter(snapshot["files"])
    with JoinedReader(parts) as joined:
        with tarfile.open(fileobj=joined, mode="r|gz") as archive:
            for member in archive:
                try:
                    item = next(expected)
                except StopIteration as exc:
                    raise Refusal("archive contains an unexpected member") from exc
                need(member.isreg() and member.name == item["path"], "archive member is not the expected regular path")
                parts_name = portable_parts(member.name)
                need(member.size == item["bytes"], f"archive member size differs: {member.name}")
                target = destination.joinpath(*parts_name)
                target.parent.mkdir(parents=True, exist_ok=True)
                need(not target.exists() and not target.is_symlink(), f"archive target already exists: {member.name}")
                source = archive.extractfile(member)
                need(source is not None, f"cannot read archive member: {member.name}")
                digest, written = hashlib.sha256(), 0
                with target.open("xb") as handle:
                    for block in iter(lambda: source.read(1024 * 1024), b""):
                        handle.write(block)
                        digest.update(block)
                        written += len(block)
                need((written, digest.hexdigest()) == (item["bytes"], item["sha256"]), f"archive member differs: {member.name}")
    try:
        next(expected)
    except StopIteration:
        return
    raise Refusal("archive ended before all manifest files")


def verify_directory(snapshot_path: Path, directory: Path) -> dict[str, Any]:
    snapshot, snapshot_id = validate_snapshot(snapshot_path)
    need(directory.is_dir() and not directory.is_symlink(), "verification directory must be a real directory")
    for item in snapshot["files"]:
        target = directory.joinpath(*portable_parts(item["path"]))
        relative = target.relative_to(directory)
        size, digest, _ = fingerprint_source(target, directory, relative)
        need((size, digest) == (item["bytes"], item["sha256"]), f"restored file differs: {item['path']}")
    return {"snapshot_id": snapshot_id, "files": len(snapshot["files"]), "total_bytes": snapshot["total_bytes"]}


def restore_package(package: Path, destination: Path) -> dict[str, Any]:
    need(not destination.exists() and not destination.is_symlink(), "restore destination already exists")
    index, snapshot_path, _ = package_files(package)
    parts = [package / item["name"] for item in index["archive"]["parts"]]
    archive_size, archive_hash = archive_fingerprint(parts)
    need((archive_size, archive_hash) == (index["archive"]["bytes"], index["archive"]["sha256"]), "combined archive differs")
    snapshot, snapshot_id = validate_snapshot(snapshot_path)
    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.restoring-", dir=destination.parent))
    try:
        extract_archive(parts, snapshot, staging)
        verify_directory(snapshot_path, staging)
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {"snapshot_id": snapshot_id, "destination": str(destination), "files": len(snapshot["files"])}


def gh(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(command, text=True, capture_output=True, check=check)
    except FileNotFoundError as exc:
        raise Refusal("GitHub CLI gh is required") from exc
    except subprocess.CalledProcessError as exc:
        raise Refusal(exc.stderr.strip() or exc.stdout.strip() or "gh command failed") from exc


def gh_api(arguments: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return gh(["gh", "api", "-H", "Accept: application/vnd.github+json", "-H",
               f"X-GitHub-Api-Version: {GITHUB_API_VERSION}", *arguments], check=check)


def gh_json(arguments: list[str]) -> dict[str, Any]:
    result = gh_api(arguments)
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise Refusal("gh API did not return JSON") from exc
    need(isinstance(value, dict), "gh API response must be an object")
    return value


def find_release_by_tag(repo: str, tag: str) -> dict[str, Any] | None:
    """Find one draft even though releases/tags/{tag} intentionally hides drafts."""
    matches: list[dict[str, Any]] = []
    page = 1
    while True:
        result = gh_api([f"repos/{repo}/releases?per_page=100&page={page}"])
        try:
            releases = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise Refusal("release list API response invalid") from exc
        need(isinstance(releases, list) and all(isinstance(item, dict) for item in releases), "release list API response invalid")
        matches.extend(item for item in releases if item.get("tag_name") == tag)
        if len(releases) < 100:
            break
        page += 1
    need(len(matches) <= 1, "more than one release has the immutable cache tag")
    return matches[0] if matches else None


def is_not_found(result: subprocess.CompletedProcess[str]) -> bool:
    return result.returncode != 0 and "404" in (result.stderr + result.stdout)


def release_tag(snapshot_id: str) -> str:
    return f"cache-sha256-{snapshot_id}"


def release_body(snapshot_id: str) -> str:
    return f"Immutable cache snapshot {snapshot_id}\\n\\nsnapshot_sha256: {snapshot_id}"


def asset_names(index: dict[str, Any], index_path: Path) -> list[str]:
    return [index["snapshot"]["name"], *(part["name"] for part in index["archive"]["parts"]), index_path.name]


def asset_expectations(index: dict[str, Any], index_path: Path) -> dict[str, dict[str, Any]]:
    expected = {index["snapshot"]["name"]: index["snapshot"],
                **{part["name"]: part for part in index["archive"]["parts"]}}
    expected[index_path.name] = {"bytes": index_path.stat().st_size, "sha256": sha256_file(index_path)}
    return expected


def verify_server_assets(release: dict[str, Any], expected: dict[str, dict[str, Any]], *, allow_missing: bool) -> None:
    assets = release.get("assets")
    need(isinstance(assets, list), "release assets invalid")
    actual = [item for item in assets if isinstance(item, dict)]
    names = [item.get("name") for item in actual]
    need(len(actual) == len(assets) and all(isinstance(name, str) for name in names) and len(set(names)) == len(names), "release asset names invalid")
    actual_names = set(names)
    need(actual_names.issubset(expected) if allow_missing else actual_names == set(expected), "release assets do not exactly match prepared package")
    for asset in actual:
        pin = expected[asset["name"]]
        need(asset.get("size") == pin["bytes"] and asset.get("digest") == f"sha256:{pin['sha256']}",
             f"server asset size or SHA256 differs: {asset['name']}")


def ensure_publish_prerequisites(repo: str) -> None:
    metadata = gh_json([f"repos/{repo}"])
    need(metadata.get("private") is True, "publish requires a private repository")
    settings = gh_api([f"repos/{repo}/immutable-releases"], check=False)
    need(settings.returncode == 0, "immutable releases must be enabled before publish")
    try:
        immutable = json.loads(settings.stdout)
    except json.JSONDecodeError as exc:
        raise Refusal("immutable releases API response invalid") from exc
    need(isinstance(immutable, dict) and immutable.get("enabled") is True, "immutable releases must be enabled before publish")


def download_release_asset(repo: str, tag: str, name: str, destination: Path) -> Path:
    gh(["gh", "release", "download", tag, "--repo", repo, "--pattern", name, "--dir", str(destination)])
    path = destination / name
    need(path.is_file() and not path.is_symlink(), f"release asset was not downloaded: {name}")
    return path


def draft_is_compatible(repo: str, tag: str, release: dict[str, Any], index: dict[str, Any], package: Path,
                        index_path: Path) -> None:
    need(release.get("draft") is True and release.get("tag_name") == tag and release.get("name") == tag and
         release.get("body") == release_body(index["snapshot_id"]), "existing draft is not strictly compatible")
    verify_server_assets(release, asset_expectations(index, index_path), allow_missing=True)


def publish(package: Path, repo: str) -> dict[str, Any]:
    index, _, index_path = package_files(package)
    ensure_publish_prerequisites(repo)
    tag = release_tag(index["snapshot_id"])
    found = gh_api([f"repos/{repo}/releases/tags/{tag}"], check=False)
    if found.returncode == 0:
        release = json.loads(found.stdout)
        need(isinstance(release, dict), "release response invalid")
        need(release.get("draft") is True, "published release tag will never be reused")
        release_id = release.get("id")
        need(isinstance(release_id, int), "draft release ID invalid")
        release = gh_json([f"repos/{repo}/releases/{release_id}"])
        draft_is_compatible(repo, tag, release, index, package, index_path)
    else:
        need(is_not_found(found), "cannot determine whether release tag exists")
        release = find_release_by_tag(repo, tag)
        if release is not None:
            need(release.get("draft") is True, "published release tag will never be reused")
            release_id = release.get("id")
            need(isinstance(release_id, int), "draft release ID invalid")
            release = gh_json([f"repos/{repo}/releases/{release_id}"])
            draft_is_compatible(repo, tag, release, index, package, index_path)
        else:
            existing_tag = gh_api([f"repos/{repo}/git/ref/tags/{tag}"], check=False)
            need(is_not_found(existing_tag), "tag already exists without a draft release")
            release = gh_json(["--method", "POST", f"repos/{repo}/releases", "-f", f"tag_name={tag}",
                               "-f", f"name={tag}", "-f", f"body={release_body(index['snapshot_id'])}", "-F", "draft=true"])
            release_id = release.get("id")
            need(isinstance(release_id, int), "new draft release ID invalid")
            release = gh_json([f"repos/{repo}/releases/{release_id}"])
            draft_is_compatible(repo, tag, release, index, package, index_path)
    existing_names = {asset["name"] for asset in release.get("assets", [])}
    for name in asset_names(index, index_path):
        if name not in existing_names:
            gh(["gh", "release", "upload", tag, str(package / name), "--repo", repo])
    fresh = gh_json([f"repos/{repo}/releases/{release_id}"])
    expected_names = set(asset_names(index, index_path))
    need(fresh.get("draft") is True and fresh.get("tag_name") == tag and fresh.get("name") == tag and
         fresh.get("body") == release_body(index["snapshot_id"]), "draft changed during upload")
    verify_server_assets(fresh, asset_expectations(index, index_path), allow_missing=False)
    published = gh_json(["--method", "PATCH", f"repos/{repo}/releases/{release_id}", "-F", "draft=false"])
    need(published.get("draft") is False and published.get("immutable") is True, "release was not published immutable")
    return {"repo": repo, "tag": tag, "snapshot_id": index["snapshot_id"], "assets": len(expected_names)}


def restore(repo: str, snapshot_id: str, destination: Path, *, manifest_out: Path | None = None) -> dict[str, Any]:
    need(is_hex64(snapshot_id), "--snapshot must be a full lowercase SHA256")
    need(not destination.exists() and not destination.is_symlink(), "restore destination already exists")
    if manifest_out is not None:
        need(not manifest_out.exists() and not manifest_out.is_symlink() and manifest_out.parent.is_dir(), "manifest output must be a new file below an existing directory")
    tag = release_tag(snapshot_id)
    release = gh_json([f"repos/{repo}/releases/tags/{tag}"])
    need(release.get("draft") is False and release.get("immutable") is True and release.get("tag_name") == tag,
         "restore requires the immutable published release")
    with tempfile.TemporaryDirectory(prefix="immutable-cache-download-") as temporary:
        package = Path(temporary)
        index_name = f"release-{snapshot_id}.json"
        index_path = download_release_asset(repo, tag, index_name, package)
        index = load_release_index(index_path)
        need(index["snapshot_id"] == snapshot_id, "release index snapshot differs from requested snapshot")
        expected = set(asset_names(index, index_path))
        assets = release.get("assets")
        need(isinstance(assets, list) and {asset.get("name") for asset in assets if isinstance(asset, dict)} == expected,
             "release assets do not exactly match release index")
        for entry in [index["snapshot"], *index["archive"]["parts"]]:
            downloaded = download_release_asset(repo, tag, entry["name"], package)
            verify_asset(downloaded, entry, package)
        result = restore_package(package, destination)
        if manifest_out is not None:
            with manifest_out.open("xb") as handle:
                handle.write((package / index["snapshot"]["name"]).read_bytes())
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--inventory", type=Path, required=True)
    prepare_parser.add_argument("--out", type=Path, required=True)
    prepare_parser.add_argument("--part-bytes", type=int, default=DEFAULT_PART_BYTES)
    inventory_parser = commands.add_parser("inventory")
    inventory_parser.add_argument("--root", action="append", required=True, metavar="LABEL=/ABSOLUTE/PATH")
    inventory_parser.add_argument("--out", type=Path, required=True)
    publish_parser = commands.add_parser("publish")
    publish_parser.add_argument("--package", type=Path, required=True)
    publish_parser.add_argument("--repo", required=True, metavar="OWNER/REPO")
    restore_parser = commands.add_parser("restore")
    restore_parser.add_argument("--repo", required=True, metavar="OWNER/REPO")
    restore_parser.add_argument("--snapshot", required=True)
    restore_parser.add_argument("--destination", type=Path, required=True)
    restore_parser.add_argument("--manifest-out", type=Path)
    local_parser = commands.add_parser("restore-local")
    local_parser.add_argument("--package", type=Path, required=True)
    local_parser.add_argument("--destination", type=Path, required=True)
    verify_parser = commands.add_parser("verify")
    verify_parser.add_argument("--manifest", type=Path, required=True)
    verify_parser.add_argument("--destination", "--directory", dest="directory", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "inventory":
        document = inventory_from_roots(args.root, args.out.absolute())
        result = {"inventory": str(args.out.absolute()), "files": len(document["files"]), "total_bytes": document["total_bytes"]}
    elif args.command == "prepare":
        result = prepare(args.inventory.absolute(), args.out.absolute(), part_bytes=args.part_bytes)
    elif args.command == "publish":
        result = publish(args.package.absolute(), args.repo)
    elif args.command == "restore":
        manifest_out = args.manifest_out.absolute() if args.manifest_out is not None else None
        result = restore(args.repo, args.snapshot, args.destination.absolute(), manifest_out=manifest_out)
    elif args.command == "restore-local":
        result = restore_package(args.package.absolute(), args.destination.absolute())
    else:
        result = verify_directory(args.manifest.absolute(), args.directory.absolute())
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
