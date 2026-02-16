import tempfile
import unittest
from pathlib import Path

import pandas as pd

from storage_manager import ExcelStorageManager


class StorageManagerTests(unittest.TestCase):
    def test_merge_and_save_deduplicates_and_creates_backup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "out.xlsx"
            manager = ExcelStorageManager(output_path=str(output_path), verbose=False)

            rows_v1 = [
                {"일자": "2026-02-10", "이름": "A", "주요내용": "s1", "출처": "X", "URL": "u1", "구분": "기업", "기술분류": "AI", "원문(옵션)": "o1"},
                {"일자": "2026-02-11", "이름": "B", "주요내용": "s2", "출처": "X", "URL": "u2", "구분": "기업", "기술분류": "AI", "원문(옵션)": "o2"},
            ]
            added, total = manager.merge_and_save(rows_v1)
            self.assertEqual((added, total), (2, 2))

            rows_v2 = [
                {"일자": "2026-02-12", "이름": "C", "주요내용": "s3", "출처": "X", "URL": "u2", "구분": "기업", "기술분류": "AI", "원문(옵션)": "o3"},
                {"일자": "2026-02-12", "이름": "D", "주요내용": "s4", "출처": "X", "URL": "u3", "구분": "기업", "기술분류": "AI", "원문(옵션)": "o4"},
            ]
            added, total = manager.merge_and_save(rows_v2)
            self.assertEqual((added, total), (1, 3))

            expected_backup = output_path.with_suffix(".xlsx.bak.1")
            self.assertTrue(expected_backup.exists())

            df = pd.read_excel(output_path)
            self.assertEqual(len(df), 3)
            self.assertEqual(set(df["URL"].astype(str).tolist()), {"u1", "u2", "u3"})

    def test_merge_and_save_with_no_rows_initializes_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "empty.xlsx"
            manager = ExcelStorageManager(output_path=str(output_path), verbose=False)

            added, total = manager.merge_and_save([])
            self.assertEqual((added, total), (0, 0))
            self.assertTrue(output_path.exists())

    def test_merge_and_save_with_no_rows_returns_existing_total(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "existing.xlsx"
            manager = ExcelStorageManager(output_path=str(output_path), verbose=False)
            manager.merge_and_save(
                [
                    {
                        "게시일시": "2026-02-12 09:00:00",
                        "일자": "2026-02-12",
                        "이름": "A",
                        "주요내용": "s1",
                        "출처": "X",
                        "URL": "u1",
                        "구분": "기업",
                        "기술분류": "AI",
                        "원문(옵션)": "o1",
                    }
                ]
            )

            added, total = manager.merge_and_save([])
            self.assertEqual((added, total), (0, 1))

    def test_merge_and_save_sorts_by_posted_datetime_desc(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "sorted.xlsx"
            manager = ExcelStorageManager(output_path=str(output_path), verbose=False)

            rows = [
                {
                    "게시일시": "2026-02-10 01:00:00",
                    "일자": "2026-02-10",
                    "이름": "A",
                    "주요내용": "s1",
                    "출처": "X",
                    "URL": "u1",
                    "구분": "기업",
                    "기술분류": "AI",
                    "원문(옵션)": "o1",
                },
                {
                    "게시일시": "2026-02-12 09:00:00",
                    "일자": "2026-02-12",
                    "이름": "B",
                    "주요내용": "s2",
                    "출처": "X",
                    "URL": "u2",
                    "구분": "기업",
                    "기술분류": "AI",
                    "원문(옵션)": "o2",
                },
                {
                    "게시일시": "2026-02-11 14:30:00",
                    "일자": "2026-02-11",
                    "이름": "C",
                    "주요내용": "s3",
                    "출처": "X",
                    "URL": "u3",
                    "구분": "기업",
                    "기술분류": "AI",
                    "원문(옵션)": "o3",
                },
            ]

            added, total = manager.merge_and_save(rows)
            self.assertEqual((added, total), (3, 3))
            df = pd.read_excel(output_path)
            self.assertEqual(df["URL"].astype(str).tolist(), ["u2", "u3", "u1"])

    def test_merge_and_save_normalizes_url_for_dedup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "url_norm.xlsx"
            manager = ExcelStorageManager(output_path=str(output_path), verbose=False)

            manager.merge_and_save(
                [
                    {
                        "게시일시": "2026-02-12 09:00:00",
                        "일자": "2026-02-12",
                        "이름": "A",
                        "주요내용": "s1",
                        "출처": "X",
                        "URL": "u1 ",
                        "구분": "기업",
                        "기술분류": "AI",
                        "원문(옵션)": "o1",
                    }
                ]
            )

            added, total = manager.merge_and_save(
                [
                    {
                        "게시일시": "2026-02-12 09:00:00",
                        "일자": "2026-02-12",
                        "이름": "A",
                        "주요내용": "s1",
                        "출처": "X",
                        "URL": "u1",
                        "구분": "기업",
                        "기술분류": "AI",
                        "원문(옵션)": "o1",
                    }
                ]
            )
            self.assertEqual((added, total), (0, 1))

    def test_merge_and_save_escapes_formula_like_cells(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "formula_safe.xlsx"
            manager = ExcelStorageManager(output_path=str(output_path), verbose=False)

            rows = [
                {
                    "게시일시": "2026-02-10 01:00:00",
                    "일자": "2026-02-10",
                    "이름": "=cmd",
                    "주요내용": "+sum(1,2)",
                    "출처": "-X",
                    "URL": "https://x.com/a/status/1",
                    "구분": "@category",
                    "기술분류": "=AI",
                    "원문(옵션)": "=2+2",
                }
            ]

            added, total = manager.merge_and_save(rows)
            self.assertEqual((added, total), (1, 1))

            df = pd.read_excel(output_path)
            self.assertEqual(str(df.loc[0, "이름"]), "'=cmd")
            self.assertEqual(str(df.loc[0, "주요내용"]), "'+sum(1,2)")
            self.assertEqual(str(df.loc[0, "출처"]), "'-X")
            self.assertEqual(str(df.loc[0, "구분"]), "'@category")
            self.assertEqual(str(df.loc[0, "URL"]), "https://x.com/a/status/1")


if __name__ == "__main__":
    unittest.main()
