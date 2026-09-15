from __future__ import annotations

from contextlib import closing
import copy
from email.utils import formatdate
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import urllib.error

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from cache_common import publish_directory, put_immutable, writer_lock
from incremental_collector import Collector, load_universe, retry_delay, utc_seconds, validate_payload
from object_archive import Archive, read_key_file
from vendor.immutable_cache_release import Refusal, canonical_bytes, inventory_from_roots, sha256_bytes


def payload(request, *, empty=False):
    value = {"meta": {"symbol": request["symbol"], "dataGranularity": request["interval"]},
             "timestamp": [] if empty else [request["start"]],
             "indicators": {"quote": [{"open": [10], "high": [12], "low": [9],
                                       "close": [11], "volume": [100]}]}}
    return {"chart": {"error": None, "result": [value]}}


class TemporaryCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()


class LocalTests(TemporaryCase):
    def test_atomic_immutable_file(self):
        target = self.root / "object"
        put_immutable(target, b"one")
        put_immutable(target, b"one")
        with self.assertRaises(Refusal):
            put_immutable(target, b"two")
        self.assertEqual(target.read_bytes(), b"one")
        self.assertEqual(list(self.root.glob(".pending-*")), [])

    def test_directory_publication_never_replaces_empty_destination(self):
        source, destination = self.root / "staging", self.root / "published"
        source.mkdir()
        (source / "data").write_bytes(b"complete")
        destination.mkdir()
        with self.assertRaises(FileExistsError):
            publish_directory(source, destination)
        self.assertTrue(source.is_dir())
        self.assertEqual(list(destination.iterdir()), [])
        destination.rmdir()
        publish_directory(source, destination)
        self.assertEqual((destination / "data").read_bytes(), b"complete")

    def test_writer_exclusion(self):
        with writer_lock(self.root):
            with self.assertRaises(Refusal):
                with writer_lock(self.root):
                    self.fail("two writers")

    def test_symlinks_refused(self):
        link = self.root / "link"
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(Refusal):
            put_immutable(link / "data", b"unsafe")

    def test_interruption_before_object_link_exposes_nothing(self):
        target = self.root / "object"
        with patch("cache_common.os.link", side_effect=OSError("injected")):
            with self.assertRaises(OSError):
                put_immutable(target, b"complete")
        self.assertFalse(target.exists())
        self.assertEqual(list(self.root.iterdir()), [])

    def test_manual_workflow_and_disabled_publisher(self):
        config = yaml.safe_load((ROOT / "config/manual.yaml").read_text())
        self.assertIs(config["automatic_scheduling"], False)
        workflow = yaml.safe_load((ROOT / ".github/workflows/nightly.yml").read_text())
        self.assertEqual(set(workflow["on"]), {"workflow_dispatch"})
        self.assertNotIn("secrets.", (ROOT / ".github/workflows/nightly.yml").read_text())
        self.assertNotIn("yahoo", (ROOT / "README.md").read_text().lower())
        result = subprocess.run(["bash", str(ROOT / "scripts/package_and_publish.sh")], capture_output=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn(b"disabled", result.stderr)


class CollectorTests(TemporaryCase):
    def setUp(self):
        super().setUp()
        self.config = yaml.safe_load((ROOT / "config/manual.yaml").read_text())["collector"]
        self.config["concurrency"] = 1
        self.config["request_windows"] = {"1d": 1, "1m": 1}
        self.now = [200000.0]
        self.calls = []

        def fetch(request, config):
            self.calls.append(request)
            return payload(request)

        self.fetch = fetch
        self.collector = Collector(self.root / "state", self.config, fetcher=fetch, clock=lambda: self.now[0])
        self.rows = [{"provider_symbol": "AAA"}]

    def test_resume_does_not_fetch_observed_windows(self):
        self.assertEqual(self.collector.plan(self.rows, 3600, 7200, "1m"), 1)
        self.assertTrue(self.collector.run()["all_windows_observed"])
        self.assertEqual(self.collector.plan(self.rows, 3600, 7200, "1m"), 0)
        self.collector.run()
        self.assertEqual(len(self.calls), 1)
        self.assertFalse(self.collector.run()["pit_complete"])

    def test_crashed_running_job_resumes(self):
        self.collector.plan(self.rows, 3600, 7200, "1m")
        with closing(self.collector.connect()) as db, db:
            db.execute("UPDATE jobs SET state='running'")
        self.assertTrue(self.collector.run()["all_windows_observed"])

    def test_200_application_error_is_quarantined(self):
        self.collector.fetcher = lambda *args: {"chart": {"error": {"code": "Not Found"}, "result": None}}
        self.collector.plan(self.rows, 3600, 7200, "1m")
        result = self.collector.run()
        self.assertEqual(result["states"], {"quarantined": 1})
        self.assertFalse(result["all_windows_observed"])
        with self.assertRaises(Refusal):
            self.collector.export(self.root / "empty")
        self.assertFalse((self.root / "empty").exists())

    def test_empty_window_is_not_complete(self):
        self.collector.fetcher = lambda r, c: payload(r, empty=True)
        self.collector.plan(self.rows, 3600, 7200, "1m")
        self.assertEqual(self.collector.run()["states"], {"unavailable": 1})

    def test_rate_limit_persists_and_does_not_shorten_retry_after(self):
        self.config["retry_max_seconds"] = 1

        def limited(*args):
            self.calls.append("limited")
            raise urllib.error.HTTPError("fake", 429, "limited", {"Retry-After": "7200"}, None)

        self.collector.fetcher = limited
        self.collector.plan(self.rows, 3600, 10800, "1m")
        self.collector.run()
        self.assertEqual(self.calls, ["limited"])
        resumed = Collector(self.root / "state", self.config, fetcher=self.fetch, clock=lambda: self.now[0])
        resumed.run()
        self.assertEqual(len(self.calls), 1)
        self.now[0] += 7201
        self.assertTrue(resumed.run()["all_windows_observed"])
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(retry_delay({"Retry-After": formatdate(self.now[0] + 9000, usegmt=True)},
                                     1, self.config, self.now[0]), 9000)

    def test_authorization_error_stops_source(self):
        def denied(*args):
            raise urllib.error.HTTPError("fake", 403, "denied", {}, None)
        self.collector.fetcher = denied
        self.collector.plan(self.rows, 3600, 7200, "1m")
        self.assertEqual(self.collector.run()["states"], {"blocked": 1})
        self.collector.fetcher = self.fetch
        self.collector.run()
        self.assertEqual(self.calls, [])

    def test_network_failure_reaches_attempt_budget(self):
        def failure(*args):
            raise TimeoutError()
        self.collector.fetcher = failure
        self.config["max_attempts"] = 2
        self.collector.plan(self.rows, 3600, 7200, "1m")
        self.assertEqual(self.collector.run()["states"], {"retry": 1})
        self.now[0] += 10000
        self.assertEqual(self.collector.run()["states"], {"failed": 1})

    def test_validation_rejects_bad_prices_duplicates_and_wrong_interval(self):
        request = {"symbol": "AAA", "interval": "1m", "start": 3600, "end": 7200}
        for field, replacement in (("low", [20]), ("close", [float("nan")]), ("volume", [-1])):
            bad = payload(request)
            bad["chart"]["result"][0]["indicators"]["quote"][0][field] = replacement
            with self.subTest(field=field), self.assertRaises(Refusal):
                validate_payload(bad, request)
        bad = payload(request)
        bad["chart"]["result"][0]["timestamp"] = [3600, 3600]
        with self.assertRaises(Refusal):
            validate_payload(bad, request)
        bad = payload(request)
        bad["chart"]["result"][0]["meta"]["dataGranularity"] = "1d"
        with self.assertRaises(Refusal):
            validate_payload(bad, request)

    def test_time_and_mapping_guards(self):
        with self.assertRaises(Refusal):
            utc_seconds("2026-09-15T00:00:00")
        with self.assertRaises(Refusal):
            self.collector.plan(self.rows, 3601, 7200, "1m")
        with self.assertRaises(Refusal):
            self.collector.plan(self.rows, 198000, 201600, "1m")
        universe = self.root / "universe.csv"
        universe.write_text("symbol,provider_symbol,asset_class,region,provider\nAAA,lon/AAA,stock,EU,yahoo\n")
        with self.assertRaises(Refusal):
            load_universe(universe)

    def test_export_is_atomic_and_checks_existing_objects(self):
        self.collector.plan(self.rows, 3600, 7200, "1m")
        self.collector.run()
        dest = self.root / "export"
        with patch("incremental_collector.publish_directory", side_effect=OSError("injected")):
            with self.assertRaises(OSError):
                self.collector.export(dest)
        self.assertFalse(dest.exists())
        result = self.collector.export(dest)
        doc = json.loads(Path(result["inventory"]).read_text())
        self.assertTrue(all(Path(item["path"]).is_file() for item in doc["files"]))
        stored = next((self.collector.root / "objects").glob("*/*"))
        stored.write_bytes(b"corrupt")
        with self.assertRaises(Refusal):
            self.collector.export(self.root / "bad-export")
        self.assertFalse((self.root / "bad-export").exists())

    def test_parallelism_is_bounded(self):
        self.config["concurrency"] = 3
        lock = threading.Lock()
        counts = [0, 0]
        def fetch(request, config):
            with lock:
                counts[0] += 1
                counts[1] = max(counts)
            time.sleep(0.02)
            with lock:
                counts[0] -= 1
            return payload(request)
        self.collector.fetcher = fetch
        self.collector.plan([{"provider_symbol": str(i)} for i in range(9)], 3600, 7200, "1m")
        self.assertTrue(self.collector.run()["all_windows_observed"])
        self.assertEqual(counts[1], 3)

    def test_closed_receipt_import_prevents_refetch_and_is_idempotent(self):
        self.collector.plan(self.rows, 3600, 7200, "1m")
        self.collector.run()
        exported = self.collector.export(self.root / "export")
        fresh = Collector(self.root / "fresh", self.config, fetcher=self.fetch, clock=lambda: self.now[0])
        imported = fresh.import_observations(Path(exported["inventory"]))
        self.assertEqual(imported["imported_windows"], 1)
        self.assertEqual(fresh.import_observations(Path(exported["inventory"]))["imported_windows"], 0)
        self.assertEqual(fresh.plan(self.rows, 3600, 7200, "1m"), 0)
        fresh.run()
        self.assertEqual(len(self.calls), 1)
        self.assertFalse(imported["pit_complete"])


class ArchiveTests(TemporaryCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = tempfile.TemporaryDirectory()
        cls.fixture_root = Path(cls.fixture.name).resolve()
        cls.key = cls.fixture_root / "test-only-key"
        cls.key.write_bytes(os.urandom(32))
        cls.key.chmod(0o600)
        cls.template = Archive(cls.fixture_root / "template", cls.key, create=True, pack_bytes=64)

    @classmethod
    def tearDownClass(cls):
        cls.fixture.cleanup()

    def setUp(self):
        super().setUp()
        self.archive = copy.copy(self.template)
        self.archive.root = self.root / "archive"
        self.archive.root.mkdir()
        put_immutable(self.archive.root / "keyring.age", (self.template.root / "keyring.age").read_bytes())
        self.number = 0

    def inventory(self, files):
        self.number += 1
        root = self.root / f"input{self.number}"
        root.mkdir()
        for name, data in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        result = self.root / f"inventory{self.number}.json"
        inventory_from_roots([f"root={root}"], result)
        return result

    def restore(self, pin, name, **kwargs):
        return self.archive.restore(pin, self.root / name, self.root / "client-cache", **kwargs)

    def test_round_trip_selective_restore_and_noop_prepare(self):
        source = self.inventory({"a": b"A" * 70, "b": b"B" * 70, "empty": b""})
        first = self.archive.prepare(source)
        self.assertEqual(self.archive.prepare(source)["catalog_pin"], first["catalog_pin"])
        self.assertEqual(self.archive.prepare(source)["new_objects"], [])
        self.restore(first["catalog_pin"], "one", paths=["root/a", "root/empty"])
        self.assertEqual((self.root / "one/root/a").read_bytes(), b"A" * 70)
        self.assertFalse((self.root / "one/root/b").exists())
        self.assertEqual((self.root / "one/root/a").stat().st_mode & 0o222, 0)
        hot = self.restore(first["catalog_pin"], "two", paths=["root/a"])
        self.assertEqual(hot["transferred_bytes"], 0)
        with self.assertRaises(Refusal):
            self.restore(first["catalog_pin"], "two")

    def test_append_reuses_old_chunks_and_preserves_previous_snapshot(self):
        one = self.archive.prepare(self.inventory({"a": b"a" * 50}))
        old = self.archive.catalog(one["catalog_pin"])
        self.restore(one["catalog_pin"], "one")
        two = self.archive.prepare(self.inventory({"b": b"b" * 10}), parent=one["catalog_pin"])
        current = self.archive.catalog(two["catalog_pin"])
        self.assertEqual(len(two["new_objects"]), 1)
        for digest in old["chunks"]:
            self.assertEqual(old["chunks"][digest], current["chunks"][digest])
        receipt = self.restore(two["catalog_pin"], "two")
        self.assertEqual(len(receipt["copied_objects"]), 1)
        self.assertEqual((self.root / "one/root/a").read_bytes(), b"a" * 50)
        self.assertFalse((self.root / "one/root/b").exists())

    def test_compaction_preserves_snapshot_and_hot_client_needs_no_new_objects(self):
        one = self.archive.prepare(self.inventory({"a": b"a" * 20}))
        two = self.archive.prepare(self.inventory({"b": b"b" * 20}), parent=one["catalog_pin"])
        pin = two["catalog_pin"]
        self.restore(pin, "before")
        old_objects = set((self.archive.root / "objects").glob("*/*"))
        plan = self.archive.compact(pin)
        self.assertFalse(plan["executed"])
        self.assertEqual(plan["target_objects"], 1)
        compacted = self.archive.compact(pin, execute=True)
        self.assertNotEqual(compacted["catalog_pin"], pin)
        self.assertEqual(compacted["snapshot_id"], two["snapshot_id"])
        self.assertEqual(compacted["old_objects_deleted"], 0)
        self.assertTrue(all(p.exists() for p in old_objects))
        self.restore(pin, "old-after")
        hot = self.restore(compacted["catalog_pin"], "hot-after")
        self.assertEqual(hot["transferred_bytes"], 0)
        cold = self.archive.restore(compacted["catalog_pin"], self.root / "cold-after", self.root / "cold-cache")
        self.assertEqual(len(cold["copied_objects"]), 1)
        for view in ("old-after", "hot-after", "cold-after"):
            self.assertEqual((self.root / view / "root/a").read_bytes(), b"a" * 20)
            self.assertEqual((self.root / view / "root/b").read_bytes(), b"b" * 20)
        noop = self.archive.compact(compacted["catalog_pin"], execute=True)
        self.assertFalse(noop["executed"])
        self.assertEqual(noop["catalog_pin"], compacted["catalog_pin"])
        repeated = self.archive.compact(pin, execute=True)
        self.assertEqual(repeated["catalog_pin"], compacted["catalog_pin"])
        self.assertEqual(repeated["new_objects"], [])
        self.assertTrue(repeated["reused_compaction"])
        three = self.archive.prepare(self.inventory({"c": b"c"}), parent=compacted["catalog_pin"])
        self.restore(three["catalog_pin"], "appended-after-compact")

    def test_interrupted_compaction_does_not_publish_or_break_old_pin(self):
        first = self.archive.prepare(self.inventory({"a": b"a"}))
        second = self.archive.prepare(self.inventory({"b": b"b"}), parent=first["catalog_pin"])
        catalogs = set((self.archive.root / "catalogs").iterdir())
        calls = [0]
        original = self.archive.seal
        def interrupted(data):
            calls[0] += 1
            if calls[0] == 2:
                raise OSError("interrupted before catalog")
            return original(data)
        with patch.object(self.archive, "seal", side_effect=interrupted):
            with self.assertRaises(OSError):
                self.archive.compact(second["catalog_pin"], execute=True)
        self.assertEqual(set((self.archive.root / "catalogs").iterdir()), catalogs)
        self.restore(second["catalog_pin"], "old-still-valid")
        self.archive.compact(second["catalog_pin"], execute=True)

    def test_old_client_can_read_during_compaction(self):
        first = self.archive.prepare(self.inventory({"a": b"a"}))
        second = self.archive.prepare(self.inventory({"b": b"b"}), parent=first["catalog_pin"])
        seal = self.archive.seal
        def while_compacting(data):
            if not (self.root / "concurrent-client").exists():
                self.restore(second["catalog_pin"], "concurrent-client")
            return seal(data)
        with patch.object(self.archive, "seal", side_effect=while_compacting):
            self.archive.compact(second["catalog_pin"], execute=True)
        self.assertEqual((self.root / "concurrent-client/root/a").read_bytes(), b"a")

    def test_cache_corruption_is_not_silently_redownloaded(self):
        first = self.archive.prepare(self.inventory({"a": b"a"}))
        self.restore(first["catalog_pin"], "good")
        object_path = next((self.root / "client-cache").glob("*/*"))
        object_path.write_bytes(b"bad")
        with self.assertRaises(Refusal):
            self.restore(first["catalog_pin"], "bad")
        self.assertFalse((self.root / "bad").exists())
        self.assertEqual(object_path.read_bytes(), b"bad")

    def test_corrupt_archive_and_publication_failure_leave_no_partial_destination(self):
        first = self.archive.prepare(self.inventory({"a": b"a"}))
        pin = first["catalog_pin"]
        with patch("object_archive.publish_directory", side_effect=OSError("injected")):
            with self.assertRaises(OSError):
                self.restore(pin, "interrupted")
        self.assertFalse((self.root / "interrupted").exists())
        obj = next((self.archive.root / "objects").glob("*/*"))
        obj.write_bytes(b"bad")
        with self.assertRaises(Refusal):
            self.archive.restore(pin, self.root / "bad", self.root / "fresh-cache")
        self.assertFalse((self.root / "bad").exists())

    def test_wrong_key_key_permissions_and_authentication_failure(self):
        wrong = self.root / "wrong-key"
        wrong.write_bytes(os.urandom(32))
        wrong.chmod(0o600)
        with self.assertRaises(Refusal):
            Archive(self.archive.root, wrong)
        wrong.chmod(0o644)
        with self.assertRaises(Refusal):
            read_key_file(wrong)
        self.assertEqual(wrong.stat().st_mode & 0o777, 0o644)
        encrypted = bytearray(self.archive.seal(b"data"))
        encrypted[-1] ^= 1
        with self.assertRaises(Refusal):
            self.archive.open(bytes(encrypted), 100)

    def test_new_reader_can_initialize_while_writer_is_active(self):
        first = self.archive.prepare(self.inventory({"a": b"a"}))
        with writer_lock(self.archive.root):
            reader = Archive(self.archive.root, self.key)
            reader.restore(first["catalog_pin"], self.root / "read-during-write", self.root / "reader-cache")

    def test_malformed_catalog_and_decompression_limit_are_rejected(self):
        prepared = self.archive.prepare(self.inventory({"a": b"a"}))
        original = self.archive.catalog(prepared["catalog_pin"])
        for corruption in ("file_sha", "range", "traversal", "snapshot_pin", "extra_chunk"):
            value = copy.deepcopy(original)
            if corruption == "file_sha":
                value["snapshot"]["files"][0]["sha256"] = "0" * 64
            elif corruption == "range":
                next(iter(value["chunks"].values()))["offset"] = -1
            elif corruption == "traversal":
                value["snapshot"]["files"][0]["path"] = "root/../escape"
            elif corruption == "snapshot_pin":
                value["snapshot_id"] = "0" * 64
            else:
                value["chunks"]["0" * 64] = next(iter(value["chunks"].values()))
            cipher = self.archive.seal(canonical_bytes(value))
            pin = sha256_bytes(cipher)
            put_immutable(self.archive.root / "catalogs" / pin, cipher)
            with self.subTest(corruption=corruption), self.assertRaises(Refusal):
                self.archive.catalog(pin)
        with self.assertRaises(Refusal):
            self.archive.open(self.archive.seal(b"X" * 1000), 10)

    def test_interrupted_append_preserves_parent_and_can_resume(self):
        first = self.archive.prepare(self.inventory({"a": b"a"}))
        delta = self.inventory({"b": b"b"})
        before = set((self.archive.root / "catalogs").iterdir())
        original = self.archive.seal
        calls = [0]
        def interrupt(data):
            calls[0] += 1
            if calls[0] == 2:
                raise OSError("before new catalog")
            return original(data)
        with patch.object(self.archive, "seal", side_effect=interrupt):
            with self.assertRaises(OSError):
                self.archive.prepare(delta, parent=first["catalog_pin"])
        self.assertEqual(set((self.archive.root / "catalogs").iterdir()), before)
        self.restore(first["catalog_pin"], "still-valid")
        resumed = self.archive.prepare(delta, parent=first["catalog_pin"])
        self.assertEqual(resumed["new_objects"], [])
        self.restore(resumed["catalog_pin"], "resumed")

    def test_producer_process_death_before_catalog_can_resume(self):
        first = self.archive.prepare(self.inventory({"a": b"a"}))
        delta = self.inventory({"b": b"b"})
        program = """
import os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from object_archive import Archive
archive = Archive(Path(sys.argv[2]), Path(sys.argv[3]), pack_bytes=64)
seal = archive.seal
calls = 0
def die_before_catalog(data):
    global calls
    calls += 1
    if calls == 2:
        os._exit(77)
    return seal(data)
archive.seal = die_before_catalog
archive.prepare(Path(sys.argv[4]), parent=sys.argv[5])
"""
        before = set((self.archive.root / "catalogs").iterdir())
        process = subprocess.run([sys.executable, "-B", "-c", program, str(ROOT / "scripts"),
                                  str(self.archive.root), str(self.key), str(delta), first["catalog_pin"]],
                                 capture_output=True, timeout=30)
        self.assertEqual(process.returncode, 77, process.stderr.decode())
        self.assertEqual(set((self.archive.root / "catalogs").iterdir()), before)
        self.restore(first["catalog_pin"], "old-after-death")
        resumed = self.archive.prepare(delta, parent=first["catalog_pin"])
        self.assertEqual(resumed["new_objects"], [])
        self.restore(resumed["catalog_pin"], "resumed-after-death")

    def test_append_does_not_rehash_historical_packs(self):
        first = self.archive.prepare(self.inventory({"a": b"a"}))
        with patch("object_archive.checked_blob", side_effect=AssertionError("historical payload read")):
            self.archive.prepare(self.inventory({"b": b"b"}), parent=first["catalog_pin"])

    def test_payload_change_creates_new_version_and_old_pin_is_unchanged(self):
        first = self.archive.prepare(self.inventory({"a": b"old", "b": b"preserved"}))
        second = self.archive.prepare(self.inventory({"a": b"corrected"}), parent=first["catalog_pin"])
        self.restore(first["catalog_pin"], "old")
        self.restore(second["catalog_pin"], "new")
        self.assertEqual((self.root / "old/root/a").read_bytes(), b"old")
        self.assertEqual((self.root / "new/root/a").read_bytes(), b"corrected")
        compacted = self.archive.compact(second["catalog_pin"], execute=True)
        self.assertLess(compacted["live_chunk_bytes"], compacted["source_pack_bytes"])
        self.assertEqual(compacted["snapshot_id"], second["snapshot_id"])

    def test_symlink_object_refused_without_exposing_restore(self):
        prepared = self.archive.prepare(self.inventory({"a": b"a"}))
        obj = next((self.archive.root / "objects").glob("*/*"))
        backup = self.root / "copy"
        backup.write_bytes(obj.read_bytes())
        obj.unlink()
        obj.symlink_to(backup)
        with self.assertRaises(Refusal):
            self.restore(prepared["catalog_pin"], "bad")
        self.assertFalse((self.root / "bad").exists())

    def test_live_database_and_changed_closed_input_refused(self):
        with self.assertRaises(Refusal):
            self.archive.prepare(self.inventory({"bars.db": b"database"}))
        inventory = self.inventory({"data.parquet": b"closed"})
        document = json.loads(inventory.read_text())
        Path(document["files"][0]["path"]).write_bytes(b"mutated")
        with self.assertRaises(Refusal):
            self.archive.prepare(inventory)

    def test_collector_to_encrypted_archive_round_trip(self):
        config = yaml.safe_load((ROOT / "config/manual.yaml").read_text())["collector"]
        collector = Collector(self.root / "collector", config, fetcher=lambda r, c: payload(r), clock=lambda: 200000)
        collector.plan([{"provider_symbol": "AAA"}], 3600, 7200, "1m")
        collector.run()
        exported = collector.export(self.root / "export")
        prepared = self.archive.prepare(Path(exported["inventory"]))
        receipt = self.restore(prepared["catalog_pin"], "restored")
        self.assertEqual(receipt["parent_snapshot_id"], prepared["snapshot_id"])
        original = next((self.root / "export/root").rglob("*.gz"))
        restored = next((self.root / "restored/root").rglob("*.gz"))
        self.assertEqual(original.read_bytes(), restored.read_bytes())


if __name__ == "__main__":
    unittest.main()
