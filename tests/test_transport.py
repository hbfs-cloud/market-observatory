from contextlib import contextmanager
import copy
import json
from pathlib import Path
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch

import yaml

import test_pipeline as fixtures
from test_pipeline import TemporaryCase, ROOT
from release_transport import GitHub, Transport, asset_reference
from vendor.immutable_cache_release import Refusal, sha256_bytes


class MemoryReleases:
    """Strict fake API: immutable publication, unique assets, explicit crashes."""
    repo = "test/private"
    def __init__(self):
        self.rows, self.blobs = {}, {}
        self.mutex = threading.Lock()
        self.uploaded = self.downloaded = 0
        self.fail_after = None

    @contextmanager
    def lock(self):
        if not self.mutex.acquire(blocking=False):
            raise Refusal("writer already active")
        try:
            yield
        finally:
            self.mutex.release()

    def releases(self):
        return list(self.rows.values())

    def find_release(self, tag):
        return self.rows.get(tag)

    def assets(self, release):
        return list(release["assets"])

    def draft(self, tag):
        if tag not in self.rows:
            self.rows[tag] = {"id": len(self.rows) + 1, "tag_name": tag, "draft": True,
                              "immutable": False, "assets": [], "body": ""}
        return self.rows[tag]

    def ensure_asset(self, release, path, name):
        data = path.read_bytes()
        digest = sha256_bytes(data)
        existing = next((a for a in release["assets"] if a["name"] == name), None)
        if existing is None:
            if not release["draft"]:
                raise Refusal("closed")
            if self.fail_after is not None and self.uploaded >= self.fail_after:
                raise OSError("injected lost connection")
            ident = len(self.blobs) + 1
            self.blobs[ident] = data
            existing = {"id": ident, "name": name, "size": len(data), "digest": "sha256:" + digest, "state": "uploaded"}
            release["assets"].append(existing)
            self.uploaded += 1
        return asset_reference(existing, release, digest, len(data))

    def close(self, release, body):
        if not release["draft"] and release["body"] != body:
            raise Refusal("immutable body differs")
        release.update(draft=False, immutable=True, body=body, published_at="2026-09-15T12:00:00Z")
        return release

    def download(self, ref):
        data = self.blobs[ref["id"]]
        if len(data) != ref["bytes"] or sha256_bytes(data) != ref["sha256"]:
            raise Refusal("fingerprint differs")
        self.downloaded += len(data)
        return data


class TransportTests(TemporaryCase):
    setUpClass = classmethod(fixtures.ArchiveTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.ArchiveTests.tearDownClass.__func__)
    inventory = fixtures.ArchiveTests.inventory

    def setUp(self):
        super().setUp()
        self.number = 0
        self.archive = copy.copy(self.template)
        self.archive.root = self.root / "producer"
        self.archive.root.mkdir()
        from cache_common import put_immutable
        put_immutable(self.archive.root / "keyring.age", (self.template.root / "keyring.age").read_bytes())
        self.config = yaml.safe_load((ROOT / "config/transport.yaml").read_text())
        self.config["assets_per_release"] = 4
        self.api = MemoryReleases()
        self.transport = Transport(self.api, self.config)

    def test_upload_mount_selective_append_and_fresh_producer_without_full_download(self):
        first = self.archive.prepare(self.inventory({"a": b"a" * 70, "b": b"b" * 70}))
        one = self.transport.publish(self.archive, first["catalog_pin"], "2026-09-14T00:00:00Z")
        uploaded = self.api.uploaded
        again = self.transport.publish(self.archive, first["catalog_pin"], "2026-09-14T00:00:00Z")
        self.assertEqual(self.api.uploaded, uploaded)
        self.assertEqual(one["ready_pin"], again["ready_pin"])
        client, ready, index = self.transport.mount(one["tag"], one["ready_pin"], self.root / "client", self.key)
        self.assertFalse((client.root / "objects").exists())
        receipt = client.restore(ready["catalog_pin"], self.root / "one", self.root / "cache", paths=["root/a"])
        self.assertEqual((self.root / "one/root/a").read_bytes(), b"a" * 70)
        self.assertFalse((self.root / "one/root/b").exists())
        before = self.api.downloaded
        second = client.prepare(self.inventory({"c": b"c"}), parent=ready["catalog_pin"])
        self.assertEqual(self.api.downloaded, before)
        two = self.transport.publish(client, second["catalog_pin"], "2026-09-15T00:00:00Z", parent=index)
        self.assertEqual(two["new_objects"], 1)
        newer, new_ready, _ = self.transport.mount(two["tag"], two["ready_pin"], client.root, self.key)
        hot = newer.restore(new_ready["catalog_pin"], self.root / "two", self.root / "cache", paths=["root/a", "root/c"])
        self.assertEqual(len(hot["copied_objects"]), 1)
        self.assertGreater(receipt["transferred_bytes"], 0)

    def test_interrupted_upload_is_invisible_until_ready_and_resumes(self):
        first = self.archive.prepare(self.inventory({str(i): bytes([i]) * 64 for i in range(9)}))
        self.api.fail_after = 5
        with self.assertRaises(OSError):
            self.transport.publish(self.archive, first["catalog_pin"], "2026-09-14T00:00:00Z")
        with self.assertRaises(Refusal):
            self.transport.resolve("2026-09-15T00:00:00Z")
        self.api.fail_after = None
        completed = self.transport.publish(self.archive, first["catalog_pin"], "2026-09-14T00:00:00Z")
        self.assertEqual(self.transport.resolve("2026-09-15T00:00:00Z")["ready_pin"], completed["ready_pin"])
        self.assertTrue(all(len(r["assets"]) <= 4 for r in self.api.rows.values()))

    def test_two_clients_and_compaction_keep_old_release_usable(self):
        first = self.archive.prepare(self.inventory({"a": b"a"}))
        second = self.archive.prepare(self.inventory({"b": b"b"}), parent=first["catalog_pin"])
        published = self.transport.publish(self.archive, second["catalog_pin"], "2026-09-14T00:00:00Z")
        old, ready, index = self.transport.mount(published["tag"], published["ready_pin"], self.root / "old", self.key)
        old.restore(ready["catalog_pin"], self.root / "before", self.root / "cache")
        compacted = self.archive.compact(second["catalog_pin"], execute=True)
        new = self.transport.publish(self.archive, compacted["catalog_pin"], "2026-09-14T01:00:00Z", parent=index)
        hot, new_ready, _ = self.transport.mount(new["tag"], new["ready_pin"], self.root / "old", self.key)
        self.assertEqual(hot.restore(new_ready["catalog_pin"], self.root / "after", self.root / "cache")["transferred_bytes"], 0)
        cold, _, _ = self.transport.mount(published["tag"], published["ready_pin"], self.root / "cold", self.key)
        cold.restore(ready["catalog_pin"], self.root / "cold-view", self.root / "cold-cache")
        self.assertEqual((self.root / "cold-view/root/b").read_bytes(), b"b")

    def test_wrong_ready_pin_modified_remote_bytes_and_knowledge_cutoff_refused(self):
        first = self.archive.prepare(self.inventory({"a": b"a"}))
        published = self.transport.publish(self.archive, first["catalog_pin"], "2026-09-14T00:00:00Z")
        with self.assertRaises(Refusal):
            self.transport.mount(published["tag"], "0" * 64, self.root / "bad", self.key)
        with self.assertRaises(Refusal):
            self.transport.resolve("2026-09-14T00:00:00Z", known_before="2026-09-14T12:00:00Z")
        ready, _ = self.transport.published(published["tag"])
        self.api.blobs[ready["catalog"]["id"]] = b"corrupted"
        with self.assertRaises(Refusal):
            self.transport.mount(published["tag"], published["ready_pin"], self.root / "corrupt", self.key)

    def test_distributed_writer_exclusion(self):
        first = self.archive.prepare(self.inventory({"a": b"a"}))
        with self.api.lock(), self.assertRaises(Refusal):
            self.transport.publish(self.archive, first["catalog_pin"], "2026-09-14T00:00:00Z")
        self.assertEqual(self.api.uploaded, 0)

    def test_stale_producer_cannot_hide_a_previously_committed_delta(self):
        first = self.archive.prepare(self.inventory({"a": b"a"}))
        one = self.transport.publish(self.archive, first["catalog_pin"], "2026-09-13T00:00:00Z")
        _, _, parent = self.transport.mount(one["tag"], one["ready_pin"], self.archive.root, self.key)
        second = self.archive.prepare(self.inventory({"b": b"b"}), parent=first["catalog_pin"])
        self.transport.publish(self.archive, second["catalog_pin"], "2026-09-14T00:00:00Z", parent=parent)
        stale = self.archive.prepare(self.inventory({"c": b"c"}), parent=first["catalog_pin"])
        with self.assertRaisesRegex(Refusal, "stale"):
            self.transport.publish(self.archive, stale["catalog_pin"], "2026-09-15T00:00:00Z", parent=parent)


class GitHubAPITests(unittest.TestCase):
    def test_http_rate_limit_headers_and_no_secret_logs(self):
        config = yaml.safe_load((ROOT / "config/transport.yaml").read_text())
        responses = [subprocess.CompletedProcess([], 1, b"HTTP/2.0 429 Too Many Requests\r\nRetry-After: 901\r\n\r\n{}", b"secret"),
                     subprocess.CompletedProcess([], 0, b"HTTP/2.0 200 OK\r\n\r\n{\"private\":true}", b"")]
        waits = []
        api = GitHub(config, runner=lambda *a, **k: responses.pop(0), sleep=waits.append)
        self.assertTrue(api.api("")["private"])
        self.assertEqual(waits, [901])
        api.runner = lambda *a, **k: subprocess.CompletedProcess([], 1, b"HTTP/2.0 403 Forbidden\r\n\r\n{}", b"secret")
        with self.assertRaises(Refusal) as error:
            api.api("")
        self.assertNotIn("secret", str(error.exception))

    def test_paginated_endpoint_does_not_truncate(self):
        config = yaml.safe_load((ROOT / "config/transport.yaml").read_text())
        api = GitHub(config)
        with patch.object(api, "api", side_effect=[list(range(100)), [100, 101]]):
            self.assertEqual(len(list(api.pages("releases"))), 102)


if __name__ == "__main__":
    unittest.main()
