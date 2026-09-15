import gzip
import json
from unittest.mock import patch

import yaml

import test_pipeline as fixtures
from test_pipeline import TemporaryCase, ROOT, payload
from global_archive_pipeline import run
from incremental_collector import Collector
from producer_checkpoint import restore_from_archive
from release_transport import Transport
from test_transport import MemoryReleases
from vendor.immutable_cache_release import canonical_bytes, sha256_bytes, Refusal


class GlobalArchiveTests(TemporaryCase):
    setUpClass = classmethod(fixtures.ArchiveTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.ArchiveTests.tearDownClass.__func__)

    def setUp(self):
        super().setUp()
        self.config = yaml.safe_load((ROOT / "config/global-backfill.yaml").read_text())
        self.config.update(universe="universe.csv", universe_inventory="inventory.json", minimum_free_bytes=0,
                           daily_start="1970-01-01T00:00:00Z", intraday_start="1970-01-02T00:00:00Z")
        self.config["collector"]["recent_revisions"] = {}
        self.config["collector"]["checkpoint_shard_bytes"] = 2048
        raw = b"symbol,provider_symbol,asset_class,region,provider\nSPY,SPY,etf,US,yahoo\n"
        (self.root / "universe.csv").write_bytes(raw)
        (self.root / "inventory.json").write_bytes(canonical_bytes({"rows": 1, "universe_sha256": sha256_bytes(raw)}))
        policy = yaml.safe_load((ROOT / "config/transport.yaml").read_text())
        self.transport = Transport(MemoryReleases(), policy)
        self.calls = []
        self.now = 200000

    def fetch(self, request, _):
        self.calls.append(request)
        value = payload(request)
        value["_acquisition"] = {"source_end": self.now}
        return value

    def run_at(self, work, run_id, until, **options):
        return run(self.config, self.root, self.root / work, run_id, until, self.key,
                   transport=self.transport, fetcher=self.fetch, clock=lambda: self.now, **options)

    def test_same_producer_publishes_only_new_bars_and_fresh_producer_resumes(self):
        first = self.run_at("one", "first", 172800, publish=True)
        self.assertEqual(len(self.calls), 4)
        same = self.run_at("one", "first", 172800, publish=True)
        self.assertEqual(same["ready_pin"], first["ready_pin"])
        self.assertEqual(len(self.calls), 4)
        parent = {"tag": first["tag"], "ready_pin": first["ready_pin"]}
        self.now += 3600
        second = self.run_at("one", "second", 176400, parent=parent, publish=True)
        self.assertEqual(len(self.calls), 7)
        third = self.run_at("fresh", "third", 180000,
                            parent={"tag": second["tag"], "ready_pin": second["ready_pin"]}, publish=True)
        self.assertEqual(len(self.calls), 10)
        archive, ready, _ = self.transport.mount(third["tag"], third["ready_pin"], self.root / "reader", self.key)
        archive.restore(ready["catalog_pin"], self.root / "restored", self.root / "cache")
        records = [json.loads(line) for path in (self.root / "restored/root").glob("*/batch-*.jsonl.gz")
                   for line in gzip.decompress(path.read_bytes()).splitlines()]
        self.assertEqual(len(records), 10)
        self.assertEqual(len({sha256_bytes(canonical_bytes(r["request"])) for r in records}), 10)

    def test_closed_outbox_resumes_without_fetch_and_refuses_skipped_or_stale_parent(self):
        with patch("global_archive_pipeline.Archive.prepare", side_effect=OSError("interrupted")), self.assertRaises(OSError):
            self.run_at("one", "first", 172800)
        self.assertEqual(len(self.calls), 4)
        with self.assertRaises(Refusal):
            self.run_at("one", "skipped", 176400)
        first = self.run_at("one", "first", 172800, publish=True)
        self.assertEqual(len(self.calls), 4)
        with self.assertRaises(Refusal):
            self.run_at("one", "stale", 176400)
        self.assertTrue(first["published"])

    def test_existing_checkpoint_restore_must_match_requested_pin(self):
        first = self.run_at("one", "first", 172800, publish=True)
        archive, _, _ = self.transport.mount(first["tag"], first["ready_pin"], self.root / "reader", self.key)
        destination, cache = self.root / "checkpoint", self.root / "cache"
        collector = Collector(self.root / "fresh", self.config["collector"])
        restore_from_archive(collector, archive, first["catalog_pin"], destination, cache)
        restore_from_archive(collector, archive, first["catalog_pin"], destination, cache)
        second = self.run_at("one", "second", 176400,
                             parent={"tag": first["tag"], "ready_pin": first["ready_pin"]}, publish=True)
        archive, _, _ = self.transport.mount(second["tag"], second["ready_pin"], self.root / "reader", self.key)
        with self.assertRaisesRegex(Refusal, "another pin"):
            restore_from_archive(collector, archive, second["catalog_pin"], destination, cache)
