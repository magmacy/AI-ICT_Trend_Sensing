import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from collectors.base_collector import BaseCollector
from collectors.facebook_collector import PLATFORM_FACEBOOK
from collectors.instagram_collector import InstagramCollector, PLATFORM_INSTAGRAM
from collectors.x_collector import XCollector, PLATFORM_X
from sns_collector import PLATFORM_UNKNOWN, SNSCollector


class SNSCollectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.collector = SNSCollector()

    def test_detect_platform(self) -> None:
        self.assertEqual(SNSCollector._detect_platform("https://x.com/OpenAI"), PLATFORM_X)
        self.assertEqual(SNSCollector._detect_platform("https://twitter.com/OpenAI"), PLATFORM_X)
        self.assertEqual(SNSCollector._detect_platform("https://www.instagram.com/openai"), PLATFORM_INSTAGRAM)
        self.assertEqual(SNSCollector._detect_platform("https://facebook.com/openai"), PLATFORM_FACEBOOK)
        self.assertEqual(SNSCollector._detect_platform("https://example.com/openai"), PLATFORM_UNKNOWN)

    def test_extract_handle_supports_twitter_domain(self) -> None:
        handle = XCollector._extract_handle("https://twitter.com/OpenAI")
        self.assertEqual(handle, "OpenAI")

    def test_keyword_match(self) -> None:
        self.assertTrue(BaseCollector.keyword_match("OpenAI launches new model", ["model"]))
        self.assertFalse(BaseCollector.keyword_match("OpenAI launches new model", ["semiconductor"]))
        self.assertTrue(BaseCollector.keyword_match("anything", []))

    def test_instagram_og_description_parser(self) -> None:
        desc = "123 likes, 4 comments - openai on February 1, 2026: New research update"
        parsed = InstagramCollector._parse_instagram_og_description(desc)
        self.assertEqual(parsed, "New research update")

    def test_is_instagram_post_url(self) -> None:
        self.assertTrue(InstagramCollector._is_instagram_post_url("https://www.instagram.com/p/ABC123/"))
        self.assertTrue(InstagramCollector._is_instagram_post_url("https://www.instagram.com/tv/ABC123/"))
        self.assertFalse(InstagramCollector._is_instagram_post_url("https://www.instagram.com/openai/"))

    def test_url_helpers_base(self) -> None:
        base = "https://www.instagram.com"
        absolute = BaseCollector.to_absolute_url(base, "/p/ABC123/")
        self.assertEqual(absolute, "https://www.instagram.com/p/ABC123/")

    def test_is_within_lookback(self) -> None:
        now = datetime.now(timezone.utc)
        recent = now - timedelta(hours=1)
        old = now - timedelta(hours=30)

        recent_text = recent.isoformat().replace("+00:00", "Z")
        old_text = old.isoformat().replace("+00:00", "Z")

        collector = XCollector(lookback_hours=24, verbose=False)
        self.assertTrue(collector.is_within_lookback(recent_text))
        self.assertFalse(collector.is_within_lookback(old_text))
        self.assertFalse(collector.is_within_lookback(""))

    def test_is_within_lookback_include_unknown_time(self) -> None:
        collector = XCollector(lookback_hours=24, include_unknown_time=True, verbose=False)
        self.assertTrue(collector.is_within_lookback(""))

    def test_instagram_candidate_multiplier_applied(self) -> None:
        collector = InstagramCollector(instagram_candidate_multiplier=5, verbose=False)
        self.assertEqual(collector.instagram_candidate_multiplier, 5)

    def test_timeout_ms_propagates_to_platform_collectors(self) -> None:
        collector = SNSCollector(timeout_ms=4321, verbose=False)
        self.assertEqual(collector.collectors[PLATFORM_X].timeout_ms, 4321)
        self.assertEqual(collector.collectors[PLATFORM_INSTAGRAM].timeout_ms, 4321)
        self.assertEqual(collector.collectors[PLATFORM_FACEBOOK].timeout_ms, 4321)

    def test_open_page_uses_configured_timeout(self) -> None:
        captured: dict[str, int] = {}

        class FakePage:
            def goto(self, url, wait_until, timeout):
                captured["timeout"] = timeout

            def wait_for_timeout(self, ms):
                return None

        collector = XCollector(timeout_ms=4321, nav_max_retries=0, verbose=False)
        self.assertTrue(collector.open_page(FakePage(), "src", "https://x.com/OpenAI"))
        self.assertEqual(captured["timeout"], 4321)

    def test_base_collector_log_respects_verbose(self) -> None:
        sink: list[str] = []
        collector = XCollector(verbose=False)
        collector.logger = SimpleNamespace(info=lambda msg: sink.append(msg))
        collector.log("hidden")
        self.assertEqual(sink, [])

        collector.verbose = True
        collector.log("visible")
        self.assertEqual(sink, ["visible"])


if __name__ == "__main__":
    unittest.main()
