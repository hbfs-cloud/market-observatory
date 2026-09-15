#!/usr/bin/env python3
"""Build the public fetcher universe from frozen StockAnalysis listings."""

from __future__ import annotations

import argparse
import csv
from collections import Counter
import io
import json
from pathlib import Path
from typing import Any
import yaml
from cache_common import put_immutable
from vendor.immutable_cache_release import canonical_bytes, need, sha256_bytes


def load_document(path: Path) -> dict[str, Any]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    data = doc.get("data")
    if isinstance(data, dict) and set(data) == {"data"} and isinstance(data["data"], dict):
        return data["data"]
    if isinstance(data, dict):
        return data
    if isinstance(data, list):
        return {
            str(row.get("symbol") or row.get("ticker")): row
            for row in data
            if isinstance(row, dict) and (row.get("symbol") or row.get("ticker"))
        }
    raise ValueError(f"unsupported StockAnalysis document shape: {path}")


def convert_to_yahoo(symbol: str, suffixes: dict) -> str | None:
    parts = symbol.split("/")
    if len(parts) != 2:
        return symbol
    suffix = suffixes.get(parts[0].lower())
    if suffix is None:
        return None
    return parts[1] + suffix


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stockanalysis-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=Path(__file__).resolve().parents[1] / "config/universe.yaml")
    parser.add_argument("--region", action="append", help="Optional region allowlist, repeatable.")
    args = parser.parse_args()
    config = yaml.safe_load(args.config.read_text())
    need(config["schema_version"] == 1, "unsupported mapping config")

    regions = set(args.region or [])
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    excluded, sources = [], []
    for path in sorted(args.stockanalysis_root.glob("*/*/tickers-frozen.json")):
        asset_class, region = path.parts[-3], path.parts[-2]
        if regions and region not in regions:
            continue
        sources.append({"path": str(path.relative_to(args.stockanalysis_root)), "sha256": sha256_bytes(path.read_bytes())})
        for raw_symbol in sorted(load_document(path)):
            provider_symbol = convert_to_yahoo(raw_symbol, config["exchange_suffixes"])
            if not provider_symbol or any(c.isspace() or c in "/\\" for c in provider_symbol):
                excluded.append({"symbol": raw_symbol, "region": region, "reason": "unresolved_venue_mapping"})
                continue
            key = ("yahoo", provider_symbol)
            if key in seen:
                excluded.append({"symbol": raw_symbol, "region": region, "reason": "duplicate_provider_alias"})
                continue
            seen.add(key)
            rows.append({
                "symbol": raw_symbol,
                "provider_symbol": provider_symbol,
                "asset_class": asset_class,
                "region": region,
                "provider": "yahoo",
            })
    need(sources, "no frozen listing inputs")
    for asset_class, symbols in config["references"].items():
        for symbol in symbols:
            if ("yahoo", symbol) not in seen:
                seen.add(("yahoo", symbol))
                rows.append({"symbol": symbol, "provider_symbol": symbol, "asset_class": asset_class,
                             "region": "GLOBAL", "provider": "yahoo"})
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=["symbol", "provider_symbol", "asset_class", "region", "provider"])
    writer.writeheader()
    writer.writerows(sorted(rows, key=lambda row: (row["region"], row["asset_class"], row["provider_symbol"])))
    raw = stream.getvalue().encode()
    report = {"schema_version": 1, "classification": "current_universe_non_pit", "pit_complete": False,
              "rows": len(rows), "excluded": excluded, "excluded_by_reason": dict(Counter(r["reason"] for r in excluded)),
              "sources": sources, "mapping_config_sha256": sha256_bytes(canonical_bytes(config)),
              "universe_sha256": sha256_bytes(raw)}
    put_immutable(args.out, raw)
    report_path = args.out.with_suffix(".inventory.json")
    put_immutable(report_path, canonical_bytes(report))
    print(json.dumps({"out": str(args.out), "rows": len(rows), "excluded_by_reason": report["excluded_by_reason"],
                      "universe_sha256": report["universe_sha256"], "inventory": str(report_path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
