import hashlib
import json
from pathlib import Path
import re
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]


class DistributionTests(unittest.TestCase):
    def test_reference_bytes_match_the_published_inventory(self):
        inventory = json.loads((ROOT / "universes/global-20260915.inventory.json").read_text())
        source = (ROOT / "universes/global-20260915.csv").read_bytes()
        self.assertEqual(hashlib.sha256(source).hexdigest(), inventory["universe_sha256"])
        self.assertEqual(len(source.splitlines()) - 1, inventory["rows"])

    def test_only_manual_unprivileged_validation_is_enabled(self):
        workflows = list((ROOT / ".github/workflows").glob("*.yml"))
        self.assertTrue(workflows)
        for path in workflows:
            source = path.read_text()
            workflow = yaml.safe_load(source)
            self.assertEqual(set(workflow["on"]), {"workflow_dispatch"})
            self.assertEqual(workflow["permissions"], {"contents": "read"})
            self.assertNotIn("secrets.", source)
            for job in workflow["jobs"].values():
                for step in job["steps"]:
                    if "uses" in step:
                        self.assertRegex(step["uses"], r"^actions/[a-z-]+@[0-9a-f]{40}$")

    def test_public_readme_has_no_omitted_provider_attribution(self):
        self.assertIsNone(re.search("yahoo", (ROOT / "README.md").read_text(), re.I))
