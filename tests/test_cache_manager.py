import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cache_manager import CacheManager
from sns_collector import RawPost


class CacheManagerTests(unittest.TestCase):
    def test_post_cache_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "cache.sqlite3"
            with CacheManager(db_path=str(db_path), enabled=True) as cache:
                seen = cache.load_seen_url_hashes()
                self.assertEqual(len(seen), 0)

                posts = [
                    RawPost("A", "기업", "AI", "X", "https://x.com/a/status/1", "2026-01-01T00:00:00Z", "text1"),
                    RawPost("B", "기업", "AI", "X", "https://x.com/b/status/2", "2026-01-01T00:00:00Z", "text2"),
                ]
                cache.add_posts(posts)

                seen2 = cache.load_seen_url_hashes()
                self.assertEqual(len(seen2), 2)
                self.assertIn(CacheManager.hash_url("https://x.com/a/status/1"), seen2)

    def test_translation_cache_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "cache.sqlite3"
            with CacheManager(db_path=str(db_path), enabled=True) as cache:
                self.assertIsNone(cache.get_translation("hello"))
                cache.set_translation("hello", "안녕하세요")
                self.assertEqual(cache.get_translation("hello"), "안녕하세요")

    def test_summary_cache_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "cache.sqlite3"
            with CacheManager(db_path=str(db_path), enabled=True) as cache:
                self.assertIsNone(cache.get_summary("hello world"))
                cache.set_summary(
                    source_text="hello world",
                    summary="요약",
                    tech_category="AI",
                    headline="제목",
                    detail="상세",
                )
                cached = cache.get_summary("hello world")
                self.assertIsNotNone(cached)
                assert cached is not None
                self.assertEqual(cached["summary"], "요약")
                self.assertEqual(cached["tech_category"], "AI")

    def test_load_seen_url_hashes_with_recent_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "cache.sqlite3"
            with CacheManager(db_path=str(db_path), enabled=True) as cache:
                posts = [
                    RawPost("A", "기업", "AI", "X", "https://x.com/a/status/1", "2026-01-01T00:00:00Z", "text1"),
                    RawPost("B", "기업", "AI", "X", "https://x.com/b/status/2", "2026-01-01T00:00:00Z", "text2"),
                ]
                cache.add_posts(posts)

                old_ts = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
                recent_ts = datetime.now(timezone.utc).isoformat()
                assert cache._conn is not None
                cache._conn.execute(
                    "UPDATE post_cache SET created_at = ? WHERE url_hash = ?",
                    (old_ts, CacheManager.hash_url("https://x.com/a/status/1")),
                )
                cache._conn.execute(
                    "UPDATE post_cache SET created_at = ? WHERE url_hash = ?",
                    (recent_ts, CacheManager.hash_url("https://x.com/b/status/2")),
                )
                cache._conn.commit()

                recent_hashes = cache.load_seen_url_hashes(recent_hours=24)
                self.assertEqual(len(recent_hashes), 1)
                self.assertIn(CacheManager.hash_url("https://x.com/b/status/2"), recent_hashes)


if __name__ == "__main__":
    unittest.main()
