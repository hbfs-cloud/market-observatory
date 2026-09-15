#!/usr/bin/env python3
"""Read-consistent SEC export to stdout. Standard library only; no host writes."""
import argparse
from datetime import date, datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time

TABLES = ("sec_ingest_days", "sec_live_filings", "sec_filing_events", "insider_trades",
          "insider_ownerships", "institutional13f_filings", "institutional13f_holdings",
          "institutional13f_positions")
DATE_COLUMNS = {"insider_trades": "reported_at", "insider_ownerships": "reported_at"}


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def export(database, table, start, end, max_rows, max_seconds, output, *, compression_level=1):
    if table not in TABLES or date.fromisoformat(start) >= date.fromisoformat(end):
        raise ValueError("invalid SEC export selection")
    if type(compression_level) is not int or not 0 <= compression_level <= 9:
        raise ValueError("invalid SEC transport compression level")
    selection_column = DATE_COLUMNS.get(table, "filing_date")
    started = time.monotonic()
    db = sqlite3.connect(Path(database).absolute().as_uri() + "?mode=ro", uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    db.set_progress_handler(lambda: int(time.monotonic() - started > max_seconds), 10000)
    try:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        columns = [dict(row) for row in db.execute(f'PRAGMA table_info("{table}")')]
        if not {"id", "filing_date", selection_column}.issubset({c["name"] for c in columns}):
            raise ValueError("unexpected SEC schema")
        header = {"format": "marketdata-sec-stream-v1", "table": table, "start": start, "end": end,
                  "columns": columns, "exported_at": datetime.now(timezone.utc).isoformat(),
                  "selection_date_column": selection_column,
                  "read_consistent": True, "pit_complete": False}
        with gzip.GzipFile(fileobj=output, mode="wb", mtime=0, compresslevel=compression_level) as archive:
            archive.write(encoded({"header": header}))
            digest, count = hashlib.sha256(), 0
            query = f'SELECT * FROM "{table}" WHERE "{selection_column}" >= ? AND "{selection_column}" < ? ORDER BY id LIMIT ?'
            for row in db.execute(query, (start, end, max_rows + 1)):
                count += 1
                if count > max_rows or time.monotonic() - started > max_seconds:
                    raise ValueError("export budget exceeded; narrow the date interval")
                raw = encoded({"row": dict(row)})
                archive.write(raw)
                digest.update(raw)
            archive.write(encoded({"footer": {"rows": count, "rows_sha256": digest.hexdigest()}}))
    finally:
        db.rollback()
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--table", required=True, choices=TABLES)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--max-rows", required=True, type=int)
    parser.add_argument("--max-seconds", required=True, type=int)
    parser.add_argument("--compression-level", type=int, default=1)
    args = parser.parse_args()
    export(args.database, args.table, args.start, args.end, args.max_rows, args.max_seconds,
           sys.stdout.buffer, compression_level=args.compression_level)


if __name__ == "__main__":
    main()
