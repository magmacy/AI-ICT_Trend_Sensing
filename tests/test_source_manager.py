import tempfile
import unittest
from pathlib import Path

import pandas as pd

from source_manager import ensure_sources_file, load_sources


class SourceManagerTests(unittest.TestCase):
    def test_ensure_sources_file_creates_default_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sources.xlsx"
            created = ensure_sources_file(path)

            self.assertTrue(created.exists())
            df = pd.read_excel(created)
            self.assertGreaterEqual(len(df), 1)
            self.assertIn("URL", df.columns)

    def test_load_sources_normalizes_alias_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sources.xlsx"
            pd.DataFrame(
                [
                    {"category": "기업", "group": "AI", "name": "Test", "url": "x.com/Test"},
                ]
            ).to_excel(path, index=False)

            sources = load_sources(path)
            self.assertEqual(len(sources), 1)
            self.assertEqual(sources[0].url, "https://x.com/Test")
            self.assertEqual(sources[0].name, "Test")

    def test_load_sources_raises_when_required_column_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sources.xlsx"
            pd.DataFrame([{"구분": "기업", "그룹": "AI", "이름": "Test"}]).to_excel(path, index=False)

            with self.assertRaises(ValueError):
                load_sources(path)


if __name__ == "__main__":
    unittest.main()
