import gzip
from contextlib import closing
import io
import json
import sqlite3
import subprocess
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq

from test_pipeline import TemporaryCase
from sec_readonly_stream import export
from backfill_sec import convert_stream
from backfill_parquet import backfill
from parquet_readonly_inventory import inventory
from vendor.immutable_cache_release import Refusal


class BackfillTests(TemporaryCase):
    def setUp(self):
        super().setUp()
        self.db = self.root / "sec.db"
        with closing(sqlite3.connect(self.db)) as db, db:
            db.execute("CREATE TABLE sec_filing_events(id INTEGER PRIMARY KEY, filing_date TEXT, available_at TEXT, symbol TEXT)")
            db.executemany("INSERT INTO sec_filing_events VALUES (?,?,?,?)", [
                (1, "2026-09-11", "2026-09-11T17:00:00Z", "SPY"),
                (2, "2026-09-11", "2026-09-12T17:00:00Z", "QQQ"),
                (3, "2026-09-12", "2026-09-12T18:00:00Z", "DIA")])
        self.policy = {"max_rows": 100, "max_line_bytes": 65536, "rows_per_partition": 1, "compression_level": 3}
        self.expected = {"table": "sec_filing_events", "start": "2026-09-11", "end": "2026-09-12"}

    def stream(self):
        value = io.BytesIO()
        export(self.db, "sec_filing_events", "2026-09-11", "2026-09-12", 100, 5, value)
        path = self.root / "export.gz"
        path.write_bytes(value.getvalue())
        return path

    def test_readonly_export_preserves_original_availability_and_all_fields(self):
        before = self.db.read_bytes()
        result = convert_stream(self.stream(), self.root / "closed", self.policy, self.expected)
        self.assertEqual(self.db.read_bytes(), before)
        self.assertEqual(result["rows"], 2)
        rows = pq.read_table(self.root / "closed/part-00001.parquet").to_pylist()
        record = json.loads(rows[0]["record_json"])
        self.assertEqual(record["available_at"], "2026-09-12T17:00:00Z")
        self.assertFalse(result["pit_complete"])

    def test_truncated_or_corrupt_stream_never_claims_complete(self):
        path = self.stream()
        lines = gzip.decompress(path.read_bytes()).splitlines(keepends=True)
        path.write_bytes(gzip.compress(b"".join(lines[:-1])))
        with self.assertRaises(Refusal):
            convert_stream(path, self.root / "truncated", self.policy, self.expected)
        self.assertFalse((self.root / "truncated/provenance.json").exists())

    def test_export_refuses_limit_and_non_sec_tables(self):
        with self.assertRaises(ValueError):
            export(self.db, "sec_filing_events", "2026-09-11", "2026-09-12", 1, 5, io.BytesIO())
        with self.assertRaises(ValueError):
            export(self.db, "credentials", "2026-09-11", "2026-09-12", 1, 5, io.BytesIO())

    def test_parquet_transfer_verifies_bytes_before_atomic_publication(self):
        source = self.root / "source"
        source.mkdir()
        (source / "bars.parquet").write_bytes(b"closed legacy bytes")
        remote = inventory(source, 2, 1024, 5)
        config = {"schema_version": 1, "automatic_scheduling": False,
                  "ssh": {"host": "reader@example.invalid", "identity_file": "~/.ssh/test", "connect_timeout_seconds": 1},
                  "parquet": {"root": "/dated/cache", "dataset_id": "test", "max_files": 2,
                              "max_bytes": 1024, "max_seconds": 5, "transfer_timeout_seconds": 5}}
        def runner(args, **kwargs):
            if args[0] == "ssh":
                return subprocess.CompletedProcess(args, 0, json.dumps(remote).encode(), b"")
            (Path(args[-1]) / "bars.parquet").write_bytes(b"closed legacy bytes")
            return subprocess.CompletedProcess(args, 0, b"stats", b"")
        with patch("backfill_parquet.subprocess.run", side_effect=runner):
            result = backfill(config, self.root / "published", execute=True)
        self.assertEqual(result["files"], 1)
        self.assertFalse(result["pit_complete"])
        remote["files"][0]["sha256"] = "0" * 64
        with patch("backfill_parquet.subprocess.run", side_effect=runner), self.assertRaises(Refusal):
            backfill(config, self.root / "corrupt", execute=True)
        self.assertFalse((self.root / "corrupt").exists())

    def test_parquet_inventory_refuses_live_database_and_symlink(self):
        source = self.root / "empty"
        source.mkdir()
        (source / "bars.db").write_bytes(b"not an export")
        with self.assertRaises(ValueError):
            inventory(source, 10, 1024, 5)
        (source / "bars.parquet").symlink_to(source / "bars.db")
        with self.assertRaises(ValueError):
            inventory(source, 10, 1024, 5)
