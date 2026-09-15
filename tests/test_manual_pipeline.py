import copy
import json
import gzip
from unittest.mock import patch

import yaml

import test_pipeline as fixtures
from test_pipeline import TemporaryCase, ROOT, payload
from test_transport import MemoryReleases
from manual_pipeline import run_once
from release_transport import Transport
from vendor.immutable_cache_release import Refusal


class ManualPipelineTests(TemporaryCase):
    setUpClass = classmethod(fixtures.ArchiveTests.setUpClass.__func__)
    tearDownClass = classmethod(fixtures.ArchiveTests.tearDownClass.__func__)

    def setUp(self):
        super().setUp()
        self.config = yaml.safe_load((ROOT / "config/manual.yaml").read_text())
        self.config["collector"]["request_windows"] = {"1d": 1, "1m": 1}
        self.transport_config = yaml.safe_load((ROOT / "config/transport.yaml").read_text())
        self.api = MemoryReleases()
        self.transport = Transport(self.api, self.transport_config)
        self.universe = self.root / "universe.csv"
        self.universe.write_text("symbol,provider_symbol,asset_class,region,provider\nSPY,SPY,etf,US,yahoo\n")
        self.calls = []

    def fetch(self, request, config):
        self.calls.append(request)
        return payload(request)

    def test_new_runner_restores_only_checkpoint_and_fetches_only_delta(self):
        common = {"transport": self.transport, "publish": True, "fetcher": self.fetch, "clock": lambda: 999999}
        first = run_once(self.config, self.root / "one", "first", self.universe, 0, 7200,
                         ["1m"], self.key, **common)
        self.assertEqual(len(self.calls), 2)
        again = run_once(self.config, self.root / "one", "first", self.universe, 0, 7200,
                         ["1m"], self.key, **common)
        self.assertEqual(again["ready_pin"], first["ready_pin"])
        self.assertEqual(len(self.calls), 2)
        parent = {"tag": first["tag"], "ready_pin": first["ready_pin"]}
        second = run_once(self.config, self.root / "two", "second", self.universe, 0, 10800,
                          ["1m"], self.key, parent=parent, **common)
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(self.calls[-1]["start"], 7200)
        client, ready, _ = self.transport.mount(second["tag"], second["ready_pin"], self.root / "client", self.key)
        client.restore(ready["catalog_pin"], self.root / "restored", self.root / "cache")
        batches = list((self.root / "restored/root/1m").glob("batch-*.jsonl.gz"))
        self.assertEqual(len(batches), 2)
        self.assertEqual(sum(len(gzip.decompress(p.read_bytes()).splitlines()) for p in batches), 3)
        with self.assertRaises(Refusal):
            run_once(self.config, self.root / "two", "second", self.universe, 0, 14400,
                     ["1m"], self.key, parent=parent, **common)

    def test_resume_after_closed_export_does_not_fetch_again(self):
        args = (self.config, self.root / "one", "crash", self.universe, 0, 7200, ["1m"], self.key)
        options = {"fetcher": self.fetch, "clock": lambda: 999999}
        with patch("manual_pipeline.Archive.prepare", side_effect=OSError("interrupted")), self.assertRaises(OSError):
            run_once(*args, **options)
        self.assertEqual(len(self.calls), 2)
        completed = run_once(*args, **options)
        self.assertEqual(len(self.calls), 2)
        self.assertFalse(completed["published"])

    def test_reference_backfill_parent_does_not_claim_bar_coverage(self):
        from cache_common import put_immutable
        archive = copy.copy(self.template)
        archive.root = self.root / "legacy-archive"
        put_immutable(archive.root / "keyring.age", (self.template.root / "keyring.age").read_bytes())
        self.number = 0
        inventory = fixtures.ArchiveTests.inventory(self, {"reference.json": b"{}"})
        prepared = archive.prepare(inventory)
        first = self.transport.publish(archive, prepared["catalog_pin"], "1970-01-01T00:00:00Z")
        second = run_once(self.config, self.root / "collector", "initial", self.universe, 0, 7200,
                          ["1m"], self.key, parent={"tag": first["tag"], "ready_pin": first["ready_pin"]},
                          transport=self.transport, publish=True, fetcher=self.fetch, clock=lambda: 999999)
        self.assertFalse(second["parent_checkpoint_present"])
        self.assertEqual(len(self.calls), 2)
        self.assertTrue(second["published"])
