from contextlib import closing
import json

import yaml

from test_pipeline import TemporaryCase, ROOT, payload
from incremental_collector import Collector
from vendor.immutable_cache_release import Refusal, canonical_bytes


class CheckpointTests(TemporaryCase):
    def setUp(self):
        super().setUp()
        self.config = yaml.safe_load((ROOT / "config/manual.yaml").read_text())["collector"]
        self.config["request_windows"] = {"1d": 1, "1m": 1}
        self.rows = [{"provider_symbol": "SPY"}]
        self.collector = Collector(self.root / "producer", self.config, fetcher=lambda r, c: payload(r), clock=lambda: 999999)

    def save(self):
        with closing(self.collector.connect()) as db:
            value = self.collector.checkpoint(db)
        path = self.root / "checkpoint.json"
        path.write_bytes(canonical_bytes(value))
        return path, value

    def test_fresh_producer_skips_historical_windows_without_payload_download(self):
        self.collector.plan(self.rows, 0, 10800, "1m")
        self.collector.run()
        path, value = self.save()
        self.assertEqual(value["coverage"][0]["ranges"], [[0, 10800]])
        fresh = Collector(self.root / "fresh", self.config, clock=lambda: 999999)
        fresh.restore_checkpoint(path)
        fresh.restore_checkpoint(path)
        self.assertEqual(fresh.plan(self.rows, 0, 14400, "1m"), 1)
        self.assertFalse((fresh.root / "objects").exists())
        exported = fresh.export(self.root / "closed", include_checkpoint=True)
        self.assertTrue((self.root / "closed/root/_producer/checkpoint.json").is_file())
        self.assertEqual(exported["files"], 0)

    def test_holes_errors_and_cooldown_survive_checkpoint(self):
        self.collector.plan(self.rows, 0, 10800, "1m")
        self.collector.run()
        with closing(self.collector.connect()) as db, db:
            db.execute("UPDATE jobs SET state='retry',retry_at=123,attempts=2,reason='http_429' WHERE json_extract(request,'$.start')=3600")
            db.execute("INSERT OR REPLACE INTO meta VALUES ('cooldown','555')")
        path, value = self.save()
        self.assertEqual(value["coverage"][0]["ranges"], [[0, 3600], [7200, 10800]])
        fresh = Collector(self.root / "fresh", self.config, clock=lambda: 999999)
        fresh.restore_checkpoint(path)
        self.assertEqual(fresh.plan(self.rows, 0, 10800, "1m"), 0)
        with closing(fresh.connect()) as db:
            job = dict(db.execute("SELECT * FROM jobs").fetchone())
            self.assertEqual((job["state"], job["attempts"], job["retry_at"]), ("retry", 2, 123))
            self.assertEqual(db.execute("SELECT value FROM meta WHERE key='cooldown'").fetchone()[0], "555")

    def test_checkpoint_cannot_replace_live_ledger(self):
        path, _ = self.save()
        self.collector.plan(self.rows, 0, 3600, "1m")
        with self.assertRaises(Refusal):
            self.collector.restore_checkpoint(path)

    def test_backfill_coalesces_gaps_without_refetch_when_request_size_changes(self):
        self.collector.plan(self.rows, 3600, 7200, "1m")
        self.config["request_windows"]["1m"] = 24
        self.assertEqual(self.collector.plan(self.rows, 0, 86400, "1m"), 2)
        with closing(self.collector.connect()) as db:
            bounds = sorted((json.loads(r[0])["start"], json.loads(r[0])["end"])
                            for r in db.execute("SELECT request FROM jobs"))
        self.assertEqual(bounds, [(0, 3600), (3600, 7200), (7200, 86400)])
        self.assertEqual(self.collector.plan(self.rows, 0, 86400, "1m"), 0)

    def test_planning_budget_is_resumable_without_skipping_unplanned_ranges(self):
        self.assertEqual(self.collector.plan(self.rows, 0, 10800, "1m", max_new_jobs=2), 2)
        self.assertEqual(self.collector.plan(self.rows, 0, 10800, "1m", max_new_jobs=2), 1)
        self.assertEqual(self.collector.plan(self.rows, 0, 10800, "1m", max_new_jobs=2), 0)
