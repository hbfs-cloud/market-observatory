from contextlib import closing
import gzip
import json

import yaml

from test_pipeline import TemporaryCase, ROOT, payload
from incremental_collector import Collector
from producer_checkpoint import read_index, write_checkpoint
from vendor.immutable_cache_release import Refusal, canonical_bytes


class ShardedCheckpointTests(TemporaryCase):
    def setUp(self):
        super().setUp()
        self.config = yaml.safe_load((ROOT / "config/manual.yaml").read_text())["collector"]
        self.config["checkpoint_shard_bytes"] = 2048
        self.config["request_windows"] = {"1m": 1}
        self.rows = [{"provider_symbol": f"S{i}"} for i in range(20)]
        self.collector = Collector(self.root / "producer", self.config, fetcher=lambda r, c: payload(r), clock=lambda: 999999)

    def export(self, name):
        self.collector.export(self.root / name, include_checkpoint=True, only_unexported=True)
        return self.root / name / "root/_producer/checkpoint.json"

    def test_shards_restore_all_pending_and_preserve_unchanged_objects(self):
        self.collector.plan(self.rows, 0, 7200, "1m")
        first = self.export("one")
        index, _ = read_index(first)
        self.assertGreater(len(index["parts"]), 1)
        fresh = Collector(self.root / "fresh", self.config, clock=lambda: 999999)
        self.assertEqual(fresh.restore_checkpoint(first)["states"], {"pending": 40})
        self.assertEqual(fresh.plan(self.rows, 0, 7200, "1m"), 0)
        second = self.export("two")
        self.assertEqual(first.read_bytes(), second.read_bytes())
        self.collector.plan([self.rows[0]], 7200, 10800, "1m")
        third, _ = read_index(self.export("three"))
        self.assertGreater(len({p["sha256"] for p in index["parts"]} & {p["sha256"] for p in third["parts"]}), 0)

    def test_missing_or_corrupt_shard_rolls_back_restoration(self):
        self.collector.plan(self.rows, 0, 7200, "1m")
        path = self.export("one")
        index, _ = read_index(path)
        shard = path.parent / index["parts"][-1]["path"]
        shard.write_bytes(b"bad")
        fresh = Collector(self.root / "fresh", self.config, clock=lambda: 999999)
        with self.assertRaises(Refusal):
            fresh.restore_checkpoint(path)
        with closing(fresh.connect()) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM jobs").fetchone()[0], 0)

    def test_total_bound_and_unsafe_path_are_refused(self):
        self.collector.plan(self.rows, 0, 3600, "1m")
        path = self.export("one")
        index, _ = read_index(path)
        fresh = Collector(self.root / "fresh", dict(self.config, checkpoint_total_bytes=1), clock=lambda: 999999)
        with self.assertRaises(Refusal):
            fresh.restore_checkpoint(path)
        index["parts"][0]["path"] = "../escape.json.gz"
        bad = self.root / "bad.json"
        bad.write_bytes(canonical_bytes(index))
        with self.assertRaises(Refusal):
            read_index(bad)

    def test_delta_exports_do_not_rebatch_history_and_acknowledge_recovers(self):
        self.collector.plan(self.rows[:1], 0, 3600, "1m")
        self.collector.run()
        self.export("one")
        with closing(self.collector.connect()) as db, db:
            db.execute("DELETE FROM exports")
        self.collector.acknowledge_export(self.root / "one/inventory.json")
        self.export("two")
        self.assertFalse((self.root / "two/root/1m").exists())
        self.collector.plan(self.rows[:1], 0, 7200, "1m")
        self.collector.run()
        self.export("three")
        batches = list((self.root / "three/root/1m").glob("*.jsonl.gz"))
        records = [json.loads(line) for p in batches for line in gzip.decompress(p.read_bytes()).splitlines()]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["request"]["start"], 3600)

    def test_recent_corrections_are_bounded_versioned_and_retry_is_noop(self):
        self.config["recent_revisions"] = {"1m": {"lookback_seconds": 7200, "cadence_seconds": 3600}}
        row = self.rows[:1]
        self.assertEqual(self.collector.plan_recent_revisions(row, 0, 10800, "1m"), 0)
        self.collector.plan(row, 0, 10800, "1m")
        self.collector.run()
        self.assertEqual(self.collector.plan_recent_revisions(row, 0, 10800, "1m"), 0)
        self.assertEqual(self.collector.plan_recent_revisions(row, 0, 14400, "1m"), 1)
        self.assertEqual(self.collector.plan(row, 0, 14400, "1m"), 1)
        with closing(self.collector.connect()) as db:
            values = [json.loads(r[0]) for r in db.execute("SELECT request FROM jobs WHERE state='pending'")]
        self.assertEqual(sorted((v["start"], v["end"]) for v in values), [(7200, 10800), (10800, 14400)])
        self.assertEqual(self.collector.plan_recent_revisions(row, 0, 14400, "1m"), 0)
        self.collector.run()
        path = self.export("one")
        fresh = Collector(self.root / "fresh", self.config, clock=lambda: 999999)
        fresh.restore_checkpoint(path)
        self.assertEqual(fresh.plan_recent_revisions(row, 0, 14400, "1m"), 0)
