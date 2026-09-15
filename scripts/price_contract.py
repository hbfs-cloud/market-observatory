"""Explicit quote basis. Reconstructed daily prices are never native raw/PIT."""
from decimal import Decimal
from fractions import Fraction
import math

from vendor.immutable_cache_release import canonical_bytes, need, sha256_bytes

CONTRACT = "quote_actions_v2"


def number(value):
    need(type(value) in (int, float) and math.isfinite(value), "invalid numeric event")
    return Fraction(Decimal(str(value)))


def quote_view(payload, request, observed_at, source_end):
    data = payload["chart"]["result"][0]
    events = data.get("events", {})
    need(isinstance(events, dict) and set(events) <= {"splits", "dividends", "capitalGains"},
         "unsupported corporate actions")
    splits = []
    for category, entries in events.items():
        need(isinstance(entries, dict), "invalid actions collection")
        for key, event in entries.items():
            need(isinstance(event, dict) and type(event.get("date")) is int, "invalid action timestamp")
            if category == "splits":
                numerator, denominator = number(event["numerator"]), number(event["denominator"])
                need(numerator > 0 and denominator > 0, "invalid split ratio")
                ratio = numerator / denominator
                need(not any(date == event["date"] for date, _ in splits), "duplicate split date")
                if event["date"] <= observed_at:
                    splits.append((event["date"], ratio))
            else:
                need(number(event["amount"]) >= 0, "negative cash distribution")
    daily = request["interval"] == "1d"
    need(type(source_end) is int and source_end <= observed_at + 1, "invalid acquisition horizon")
    need(not daily or source_end >= int(observed_at) - 60, "daily split history must extend to acquisition time")
    rows = []
    quotes = data.get("indicators", {}).get("quote", [{}])[0]
    for i, instant in enumerate(data.get("timestamp", [])):
        if not request["start"] <= instant < request["end"]:
            continue
        values = {key: quotes[key][i] for key in ("open", "high", "low", "close", "volume")}
        if all(value is None for value in values.values()):
            continue
        factor = Fraction(1)
        if daily:
            for effective, ratio in splits:
                if instant < effective:
                    factor *= ratio
        # Keep the exact rational factor; do not round prices to a guessed tick
        # size or truncate fractional reconstructed volumes after reverse splits.
        row = {"timestamp": instant, "quote": values}
        if daily:
            row["split_factor"] = {"numerator": factor.numerator, "denominator": factor.denominator}
            row["as_traded_reconstructed"] = {
                key: float(number(value) / factor if key == "volume" else number(value) * factor)
                for key, value in values.items()}
            need(all(math.isfinite(value) for value in row["as_traded_reconstructed"].values()),
                 "reconstructed values overflow")
        rows.append(row)
    return {"contract": CONTRACT, "provider_payload_sha256": sha256_bytes(canonical_bytes(payload)),
            "client_dividend_adjustment": False, "client_price_repair": False,
            "quote_basis": "split_adjusted_not_dividend_adjusted" if daily else "native_intraday_unverified",
            "output_basis": "as_traded_reconstructed" if daily else "native_intraday_unverified",
            "source_end": source_end, "observed_at": observed_at, "rows": rows,
            "corporate_actions": events, "native_raw_certified": False, "pit_complete": False,
            "volume_basis": "inverse_split_reconstruction" if daily else "provider_native_unverified",
            "limitations": ["provider_event_completeness_unverified", "historical_announcement_times_unavailable"]}
