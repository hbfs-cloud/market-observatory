"""Local atomic writes and exclusive writer guards for manual cache work."""
from __future__ import annotations

from contextlib import contextmanager
import ctypes
import fcntl
import os
from pathlib import Path
import sys
import tempfile

from vendor.immutable_cache_release import Refusal, need, sha256_bytes


def real_directory(path: Path) -> Path:
    path = path.absolute()
    for parent in [*reversed(path.parents), path]:
        need(not parent.is_symlink(), f"symlink directory refused: {parent}")
    path.mkdir(parents=True, exist_ok=True)
    need(path.is_dir(), "directory required")
    return path


def sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def put_immutable(path: Path, data: bytes) -> None:
    real_directory(path.parent)
    if path.exists() or path.is_symlink():
        need(not path.is_symlink() and path.is_file(), "invalid immutable target")
        need(path.read_bytes() == data, "immutable target differs")
        return
    fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    temp = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp, path)
        except FileExistsError:
            need(not path.is_symlink() and path.read_bytes() == data, "immutable collision")
        sync_directory(path.parent)
    finally:
        temp.unlink(missing_ok=True)


@contextmanager
def writer_lock(root: Path):
    root = real_directory(root)
    target = root / ".writer.lock"
    need(not target.is_symlink(), "symlink lock refused")
    with target.open("a+b") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Refusal("another cache writer is active") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def blob_path(root: Path, name: str, limit: int = 66 * 1024 * 1024) -> Path:
    need(len(name) == 64 and all(c in "0123456789abcdef" for c in name), "invalid object pin")
    path = root / name[:2] / name
    need(not root.is_symlink() and not path.parent.is_symlink() and not path.is_symlink(), "symlink object refused")
    need(path.is_file() and path.stat().st_size <= limit, "object missing or exceeds bound")
    return path


def checked_blob(root: Path, name: str) -> bytes:
    path = blob_path(root, name)
    data = path.read_bytes()
    need(sha256_bytes(data) == name, "object checksum differs")
    return data


def publish_directory(staging: Path, destination: Path) -> None:
    """Atomic rename without replacing even an empty concurrent destination."""
    # Flush nested directory entries before exposing the complete tree.
    for directory, _, _ in os.walk(staging, topdown=False):
        sync_directory(Path(directory))
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        rename = libc.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        result = rename(os.fsencode(staging), os.fsencode(destination), 0x4)
    elif sys.platform.startswith("linux"):
        rename = libc.renameat2
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        result = rename(-100, os.fsencode(staging), -100, os.fsencode(destination), 1)
    else:
        raise Refusal("atomic no-replace directory publication requires Linux or macOS")
    if result:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(destination))
    sync_directory(destination.parent)
