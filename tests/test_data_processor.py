import unittest

from data_processor import DataProcessor, SummaryResult
from sns_collector import RawPost


class DummySummarizer:
    def summarize(self, text: str) -> SummaryResult:
        return SummaryResult(summary=f"요약:{text[:20]}", tech_category="AI")


class DataProcessorTests(unittest.TestCase):
    def test_process_deduplicates_by_url_and_text_hash(self) -> None:
        processor = DataProcessor(DummySummarizer(), verbose=False)
        posts = [
            RawPost("A", "기업", "AI", "X", "https://x.com/a/status/1", "2026-02-10T01:02:03Z", "hello world"),
            RawPost("A", "기업", "AI", "X", "https://x.com/a/status/1", "2026-02-10T01:02:03Z", "hello world"),
            RawPost("A", "기업", "AI", "X", "https://x.com/a/status/2", "2026-02-10T01:02:03Z", "hello world"),
            RawPost("B", "기업", "AI", "X", "https://x.com/b/status/3", "", "check https://example.com now"),
        ]

        rows = processor.process(posts)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["게시일시"], "2026-02-10 01:02:03")
        self.assertEqual(rows[0]["일자"], "2026-02-10")
        self.assertEqual(rows[1]["기술분류"], "AI")
        self.assertNotIn("https://example.com", rows[1]["원문(옵션)"])
        self.assertTrue(rows[0]["주요내용"].startswith("ㅇ A : "))
        self.assertIn("\n - ", rows[0]["주요내용"])

    def test_clean_text_and_date_helpers(self) -> None:
        processor = DataProcessor(DummySummarizer(), verbose=False)
        cleaned = processor._clean_text(" A   test  https://abc.com\nline ")
        self.assertEqual(cleaned, "A test line")

        self.assertEqual(processor._to_date("2026-01-01T12:00:00Z"), "2026-01-01")
        self.assertEqual(processor._to_date("invalid"), "")
        self.assertEqual(processor._to_date(""), "")

    def test_briefing_format(self) -> None:
        processor = DataProcessor(DummySummarizer(), verbose=False)
        result = SummaryResult(
            summary="개방형 가중치 모델과 폐쇄형 모델 간의 지능 격차 최소화 / Claude 4.6 및 GLM-5의 등장으로 성능 격차 감소",
            tech_category="AI",
        )
        formatted = processor._format_briefing("Artificial Analysis", result)
        self.assertTrue(formatted.startswith("ㅇ Artificial Analysis : "))
        self.assertIn("\n - ", formatted)

    def test_process_escapes_excel_formula_like_values(self) -> None:
        processor = DataProcessor(DummySummarizer(), verbose=False)
        posts = [
            RawPost(
                "=Malicious",
                "+기업",
                "AI",
                "-X",
                "https://x.com/a/status/1",
                "2026-02-10T01:02:03Z",
                "@dangerous text",
            )
        ]

        rows = processor.process(posts)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertTrue(row["이름"].startswith("'"))
        self.assertTrue(row["출처"].startswith("'"))
        self.assertTrue(row["구분"].startswith("'"))
        self.assertTrue(row["원문(옵션)"].startswith("'"))


if __name__ == "__main__":
    unittest.main()
