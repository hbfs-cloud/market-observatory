import unittest

import test_pipeline  # Initialize the standalone scripts import path.
from pit_records import select
from vendor.immutable_cache_release import Refusal


class PITTests(unittest.TestCase):
    def record(self, **kwargs):
        return {"dataset": "index_membership", "entity_id": "listing-1", "event_id": "SP500-entry-1",
                "revision": 0, "known_from": "2026-09-01T10:00:00Z", "observed_at": "2026-09-01T11:00:00Z",
                "valid_from": "2026-09-10T00:00:00Z", "valid_to": None, "status": "active",
                "source_document_sha256": "a" * 64, "payload": {"index_id": "SP500"}, **kwargs}

    def test_announcement_is_not_effective_membership(self):
        self.assertEqual(select([self.record()], "2026-09-09T00:00:00Z", "2026-09-09T00:00:00Z"), [])
        self.assertEqual(len(select([self.record()], "2026-09-10T00:00:00Z", "2026-09-09T00:00:00Z")), 1)

    def test_later_correction_does_not_resurrect_old_validity(self):
        corrected = self.record(revision=1, known_from="2026-09-12T00:00:00Z", observed_at="2026-09-12T01:00:00Z",
                                valid_from="2026-09-20T00:00:00Z")
        rows = [self.record(), corrected]
        self.assertEqual(len(select(rows, "2026-09-11T00:00:00Z", "2026-09-11T00:00:00Z")), 1)
        self.assertEqual(select(rows, "2026-09-11T00:00:00Z", "2026-09-13T00:00:00Z"), [])
        retracted = self.record(revision=2, known_from="2026-09-14T00:00:00Z", observed_at="2026-09-14T01:00:00Z", status="retracted")
        self.assertEqual(select([*rows, retracted], "2026-09-21T00:00:00Z", "2026-09-15T00:00:00Z"), [])

    def test_backfill_observation_is_not_historical_local_knowledge(self):
        record = self.record(observed_at="2026-09-15T00:00:00Z")
        self.assertEqual(select([record], "2026-09-11T00:00:00Z", "2026-09-11T00:00:00Z"), [])
        self.assertEqual(len(select([record], "2026-09-11T00:00:00Z", "2026-09-11T00:00:00Z", knowledge_policy="public")), 1)

    def test_missing_provenance_and_ambiguous_revisions_are_refused(self):
        with self.assertRaises(Refusal):
            select([self.record(source_document_sha256="")], "2026-09-11T00:00:00Z", "2026-09-11T00:00:00Z")
        with self.assertRaises(Refusal):
            select([self.record(), self.record(revision=1)], "2026-09-11T00:00:00Z", "2026-09-11T00:00:00Z")
