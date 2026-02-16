import os
import tempfile
import unittest
from pathlib import Path

import config
from main import main


class MainTests(unittest.TestCase):
    def test_parse_keywords_prefers_cli_value(self) -> None:
        os.environ["SEARCH_KEYWORDS"] = "ai,chip"
        self.assertEqual(config.parse_keywords("cloud,network"), ["cloud", "network"])

    def test_parse_keywords_reads_env_when_cli_empty(self) -> None:
        os.environ["SEARCH_KEYWORDS"] = "ai, chip ,"
        self.assertEqual(config.parse_keywords(None), ["ai", "chip"])

    def test_default_lookback_hours(self) -> None:
        os.environ["LOOKBACK_HOURS"] = "12"
        self.assertEqual(config.default_lookback_hours(), 12)
        os.environ["LOOKBACK_HOURS"] = "invalid"
        self.assertEqual(config.default_lookback_hours(), 24)

    def test_default_collect_workers(self) -> None:
        os.environ["COLLECT_WORKERS"] = "4"
        self.assertEqual(config.default_collect_workers(), 4)
        os.environ["COLLECT_WORKERS"] = "invalid"
        self.assertEqual(config.default_collect_workers(), 3)

    def test_create_sources_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sources.xlsx"
            code = main(["--create-sources", "--sources", str(path)])
            self.assertEqual(code, 0)
            self.assertTrue(path.exists())


if __name__ == "__main__":
    unittest.main()
