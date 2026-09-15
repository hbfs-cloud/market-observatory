import copy
import gzip
from argparse import Namespace

import yaml

from test_pipeline import TemporaryCase, ROOT, payload
from normalize_pit_inputs import chart_actions, index_snapshot, normalize, revisions
from pit_records import select
from vendor.immutable_cache_release import Refusal, canonical_bytes, sha256_bytes, portable_snapshot


class NormalizationTests(TemporaryCase):
    def receipt(self, amount=1, observed=1720000000):
        request = {"provider": "yahoo", "symbol": "SPY", "interval": "1d", "start": 0, "end": 3600}
        data = payload(request)
        data["chart"]["result"][0]["events"] = {"dividends": {"1000": {"date": 1000, "amount": amount}}}
        return {"schema_version": 1, "request": request, "observed_at": observed,
                "coverage": "observations_only", "payload": data}

    def test_action_effective_date_is_not_historical_knowledge(self):
        rows = chart_actions(self.receipt(), "a" * 64)
        self.assertIsNone(rows[0]["payload"]["announcement_at"])
        self.assertEqual(select(rows, "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z"), [])
        self.assertEqual(len(select(rows, "2020-01-01T00:00:00Z", "2026-01-01T00:00:00Z")), 1)

    def test_revisions_and_duplicate_observations(self):
        one = chart_actions(self.receipt(), "a" * 64)
        duplicate = chart_actions(self.receipt(observed=1720000001), "b" * 64)
        correction = chart_actions(self.receipt(amount=2, observed=1720000002), "c" * 64)
        rows = revisions(one + duplicate + correction)
        self.assertEqual([r["revision"] for r in rows], [0, 1])
        with self.assertRaises(Refusal):
            revisions(one + chart_actions(self.receipt(amount=3), "d" * 64))

    def test_invalid_splits_and_unknown_events_fail_closed(self):
        receipt = self.receipt()
        receipt["payload"]["chart"]["result"][0]["events"] = {
            "splits": {"1000": {"date": 1000, "numerator": 2, "denominator": 0}}}
        with self.assertRaises(Refusal):
            chart_actions(receipt, "a" * 64)
        receipt["payload"]["chart"]["result"][0]["events"] = {"unknown": {}}
        with self.assertRaises(Refusal):
            chart_actions(receipt, "a" * 64)

    def test_corrections_continue_pinned_history_without_reemitting_it(self):
        first = chart_actions(self.receipt(), "a" * 64)
        second = revisions(chart_actions(self.receipt(amount=2, observed=1720000002), "b" * 64), first)
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0]["revision"], 1)
        self.assertEqual(revisions(chart_actions(self.receipt(amount=2, observed=1720000003), "c" * 64), first + second), [])
        with self.assertRaises(Refusal):
            revisions(chart_actions(self.receipt(amount=3, observed=1720000001), "c" * 64), first + second)

    def test_snapshot_never_fills_unsampled_membership(self):
        raw = b"ticker,yahoo,weight\nABC,ABC,\n"
        rows = index_snapshot(raw, "a" * 64, "SP500", "2020-01-01T00:00:00Z", "2026-09-01T00:00:00Z")
        self.assertEqual(select(rows, "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z"), [])
        self.assertEqual(len(select(rows, "2020-01-01T00:00:00Z", "2026-09-02T00:00:00Z")), 1)
        self.assertEqual(select(rows, "2020-01-02T00:00:00Z", "2026-09-02T00:00:00Z"), [])
        empty = index_snapshot(b"ticker,yahoo,weight\n", "b" * 64, "SP500", "2020-02-01T00:00:00Z", "2026-09-01T00:00:00Z")
        self.assertEqual(empty[0]["payload"]["members"], [])
        with self.assertRaises(Refusal):
            index_snapshot(raw + b"ABC,ABC,\n", "a" * 64, "SP500", "2020-01-01T00:00:00Z", "2026-09-01T00:00:00Z")

    def test_closed_output_preserves_source_and_refuses_bad_pin_or_overwrite(self):
        raw = gzip.compress(canonical_bytes(self.receipt()))
        source = self.root / "input.jsonl.gz"
        source.write_bytes(raw)
        args = Namespace(kind="chart-actions", input=source, input_sha256=sha256_bytes(raw),
                         destination=self.root / "closed")
        config = yaml.safe_load((ROOT / "config/normalization.yaml").read_text())
        report = normalize(args, config)
        self.assertEqual(report["rows"], 1)
        snapshot, _ = portable_snapshot(args.destination / "inventory.json")
        self.assertEqual(len(snapshot["files"]), 2)
        self.assertEqual((args.destination / "root/sources" / args.input_sha256).read_bytes(), raw)
        with self.assertRaises(Refusal):
            normalize(args, config)
        changed = copy.copy(args)
        changed.input_sha256 = "0" * 64
        changed.destination = self.root / "refused"
        with self.assertRaises(Refusal):
            normalize(changed, config)
        self.assertFalse(changed.destination.exists())
