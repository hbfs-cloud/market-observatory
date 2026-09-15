import copy
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from incremental_collector import Collector, fetch_chart, utc_seconds, validate_payload
from collect_global import run
from price_contract import CONTRACT, quote_view
from vendor.immutable_cache_release import Refusal, canonical_bytes, sha256_bytes
from test_pipeline import payload


class PriceTests(unittest.TestCase):
    def request(self):
        return dict(symbol="AAPL", interval="1d", start=0, end=86400, adjustment=CONTRACT, include_prepost=False)

    def test_split_prices_and_volumes_reconstructed_not_dividend_adjusted(self):
        request = self.request()
        source = payload(request)
        data = source["chart"]["result"][0]
        data["events"] = {"splits": {"100": {"date": 100, "numerator": 4, "denominator": 1}},
                          "dividends": {"200": {"date": 200, "amount": 2}}}
        original = copy.deepcopy(source)
        result = quote_view(source, request, 100000, 100000)
        self.assertEqual(source, original)
        self.assertEqual(result["rows"][0]["quote"]["close"], 11)
        self.assertEqual(result["rows"][0]["as_traded_reconstructed"]["close"], 44)
        self.assertEqual(result["rows"][0]["as_traded_reconstructed"]["volume"], 25)
        self.assertFalse(result["native_raw_certified"])
        self.assertFalse(result["pit_complete"])

    def test_later_split_beyond_requested_window_is_included(self):
        request = self.request()
        source = payload(request)
        source["chart"]["result"][0]["events"] = {"splits": {
            "90000": {"date": 90000, "numerator": 1, "denominator": 10}}}
        result = quote_view(source, request, 100000, 100000)
        self.assertEqual(result["rows"][0]["as_traded_reconstructed"]["close"], 1.1)
        self.assertEqual(result["rows"][0]["as_traded_reconstructed"]["volume"], 1000)
        with self.assertRaises(Refusal):
            quote_view(source, request, 100000, request["end"])

    def test_split_at_bar_timestamp_is_not_applied_twice(self):
        request = self.request()
        source = payload(request)
        source["chart"]["result"][0]["events"] = {"splits": {
            "0": {"date": 0, "numerator": 4, "denominator": 1}}}
        result = quote_view(source, request, 100000, 100000)
        self.assertEqual(result["rows"][0]["as_traded_reconstructed"]["close"], 11)

    def test_intraday_never_assumes_daily_split_basis(self):
        request = {**self.request(), "interval": "1m"}
        source = payload(request)
        result = quote_view(source, request, 100000, request["end"])
        self.assertEqual(result["output_basis"], "native_intraday_unverified")
        self.assertNotIn("as_traded_reconstructed", result["rows"][0])

    def test_wire_requests_actions_and_daily_tail_without_fake_raw_parameter(self):
        request = self.request()
        source = canonical_bytes(payload(request))
        with patch("incremental_collector.time.time", return_value=100000), \
             patch("incremental_collector.urllib.request.urlopen", return_value=io.BytesIO(source)) as opened:
            result = fetch_chart(request, {"timeout_seconds": 1, "response_max_bytes": 10000})
        query = parse_qs(urlparse(opened.call_args.args[0].full_url).query)
        self.assertEqual(query["period2"], ["100000"])
        self.assertEqual(query["events"], ["div,splits,capitalGains"])
        self.assertNotIn("auto_adjust", query)
        self.assertEqual(result["_acquisition"]["wire_sha256"], sha256_bytes(source))

    def test_old_coverage_cannot_satisfy_new_contract(self):
        with tempfile.TemporaryDirectory() as temporary:
            config = yaml.safe_load((ROOT / "config/manual.yaml").read_text())["collector"]
            old = Collector(Path(temporary).resolve(), config, clock=lambda: 200000)
            old.plan([{"provider_symbol": "AAA"}], 3600, 7200, "1m")
            new = Collector(Path(temporary).resolve(), dict(config, price_contract=CONTRACT), clock=lambda: 200000)
            self.assertEqual(new.plan([{"provider_symbol": "AAA"}], 3600, 7200, "1m"), 1)
            self.assertEqual(new.plan([{"provider_symbol": "AAA"}], 3600, 7200, "1m"), 0)

    def test_malformed_actions_are_preserved_in_quarantine(self):
        with tempfile.TemporaryDirectory() as temporary:
            config = yaml.safe_load((ROOT / "config/manual.yaml").read_text())["collector"]
            config["price_contract"] = CONTRACT
            def fetch(request, _):
                value = payload(request)
                value["_acquisition"] = {"source_end": 200000}
                value["chart"]["result"][0]["events"] = {"splits": {"1": {"date": 1}}}
                return value
            collector = Collector(Path(temporary).resolve(), config, fetcher=fetch, clock=lambda: 200000)
            collector.plan([{"provider_symbol": "AAA"}], 86400, 172800, "1d")
            self.assertEqual(collector.run()["states"], {"quarantined": 1})


class GlobalTests(unittest.TestCase):
    def test_all_symbols_planned_and_resume_only_missing(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            raw = b"symbol,provider_symbol,asset_class,region,provider\nAAA,AAA,stock,US,yahoo\nBBB,BBB,stock,EU,yahoo\n"
            (root / "universe.csv").write_bytes(raw)
            (root / "inventory.json").write_bytes(canonical_bytes({"rows": 2, "universe_sha256": sha256_bytes(raw)}))
            config = yaml.safe_load((ROOT / "config/global-backfill.yaml").read_text())
            config["collector"]["recent_revisions"] = {}
            config.update(universe="universe.csv", universe_inventory="inventory.json",
                          intraday_start="1970-01-02T00:00:00Z",
                          minimum_free_bytes=0)
            calls = []
            def fetch(request, _):
                calls.append(request)
                value = payload(request)
                value["_acquisition"] = {"source_end": 200000}
                return value
            result = run(config, root, root / "work", 172800, execute=True, fetcher=fetch, clock=lambda: 200000)
            self.assertEqual(result["planned_symbols"], 2)
            self.assertEqual(len(calls), 8)
            self.assertTrue(result["all_windows_observed"])
            self.assertEqual({row["interval"] for row in calls}, {"1d", "1m", "15m", "1h"})
            for request in calls:
                if request["interval"] != "1d":
                    self.assertGreaterEqual(request["start"], 86400)
                    self.assertLessEqual(request["end"] + config["collector"]["bar_seconds"][request["interval"]], 172800)
            run(config, root, root / "work", 172800, execute=True, fetcher=fetch, clock=lambda: 200000)
            self.assertEqual(len(calls), 8)
            run(config, root, root / "work", 176400, execute=True, fetcher=fetch, clock=lambda: 200000)
            self.assertEqual(len(calls), 14)
            self.assertTrue(all(request["start"] == 169200 for request in calls[8:]))
            config["intraday_start"] = "1970-01-01T00:00:00Z"
            with self.assertRaises(Refusal):
                run(config, root, root / "work", 86400, execute=True, fetcher=fetch, clock=lambda: 200000)

    def test_anchor_is_fixed_paris_midnight_not_a_rolling_lookback(self):
        config = yaml.safe_load((ROOT / "config/global-backfill.yaml").read_text())
        self.assertNotIn("intraday_lookback_days", config)
        self.assertEqual(utc_seconds(config["intraday_start"]), utc_seconds("2026-09-14T22:00:00Z"))
        self.assertEqual(config["intraday_intervals"], ["1m", "15m", "1h"])

    def test_hourly_session_offset_and_closed_bar_guard(self):
        config = yaml.safe_load((ROOT / "config/global-backfill.yaml").read_text())["collector"]
        request = {"symbol": "AAA", "interval": "1h", "start": 0, "end": 3600}
        value = payload(request)
        value["chart"]["result"][0]["timestamp"] = [1800]
        self.assertEqual(validate_payload(value, request), 1)
        with tempfile.TemporaryDirectory() as temporary:
            collector = Collector(Path(temporary).resolve(), config, clock=lambda: 7200)
            with self.assertRaises(Refusal):
                collector.plan([{"provider_symbol": "AAA"}], 0, 3600, "1h")
            collector.clock = lambda: 8400
            self.assertEqual(collector.plan([{"provider_symbol": "AAA"}], 0, 3600, "1h"), 1)
