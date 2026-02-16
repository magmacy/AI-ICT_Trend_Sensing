from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Iterable

from models import RawPost


@dataclass
class CacheStats:
    seen_url_count: int
    translation_count: int
    summary_count: int


class CacheManager:
    def __init__(self, db_path: str = "pipeline_cache.sqlite3", enabled: bool = True) -> None:
        self.enabled = enabled
        self.db_path = Path(db_path)
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None

        if self.enabled:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._init_schema()

    def __enter__(self) -> "CacheManager":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @staticmethod
    def hash_text(value: str) -> str:
        return sha256((value or "").strip().encode("utf-8")).hexdigest()

    @staticmethod
    def hash_url(url: str) -> str:
        normalized = (url or "").strip()
        return sha256(normalized.encode("utf-8")).hexdigest()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _init_schema(self) -> None:
        if self._conn is None:
            return

        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS post_cache (
                    url_hash TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    platform TEXT,
                    source_name TEXT,
                    posted_at TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS translation_cache (
                    text_hash TEXT PRIMARY KEY,
                    source_text TEXT NOT NULL,
                    translated_text TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS summary_cache (
                    text_hash TEXT PRIMARY KEY,
                    source_text TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    tech_category TEXT NOT NULL,
                    headline TEXT,
                    detail TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_post_cache_created_at ON post_cache(created_at)")
            self._conn.commit()

    def load_seen_url_hashes(
        self,
        recent_hours: int | None = None,
        max_count: int | None = None,
    ) -> set[str]:
        if not self.enabled or self._conn is None:
            return set()

        query = "SELECT url_hash FROM post_cache"
        params: list[object] = []

        if recent_hours is not None and recent_hours > 0:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=recent_hours)).isoformat()
            query += " WHERE created_at >= ?"
            params.append(cutoff)

        query += " ORDER BY created_at DESC"

        if max_count is not None and max_count > 0:
            query += " LIMIT ?"
            params.append(int(max_count))

        with self._lock:
            cur = self._conn.cursor()
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
        return {str(row["url_hash"]) for row in rows}

    def add_posts(self, posts: Iterable[RawPost]) -> int:
        if not self.enabled or self._conn is None:
            return 0

        now = datetime.now(timezone.utc).isoformat()
        payload = []
        for post in posts:
            if not post.post_url:
                continue
            payload.append(
                (
                    self.hash_url(post.post_url),
                    post.post_url,
                    post.platform,
                    post.source_name,
                    post.posted_at,
                    now,
                )
            )

        if not payload:
            return 0

        with self._lock:
            cur = self._conn.cursor()
            cur.executemany(
                """
                INSERT OR IGNORE INTO post_cache (url_hash, url, platform, source_name, posted_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            self._conn.commit()
            inserted = cur.rowcount if cur.rowcount is not None else 0

        return max(0, inserted)

    def get_translation(self, source_text: str) -> str | None:
        if not self.enabled or self._conn is None:
            return None

        text_hash = self.hash_text(source_text)
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                "SELECT translated_text FROM translation_cache WHERE text_hash = ?",
                (text_hash,),
            )
            row = cur.fetchone()

        if row is None:
            return None
        return str(row["translated_text"])

    def set_translation(self, source_text: str, translated_text: str) -> None:
        if not self.enabled or self._conn is None:
            return

        source = (source_text or "").strip()
        translated = (translated_text or "").strip()
        if not source or not translated:
            return

        now = datetime.now(timezone.utc).isoformat()
        text_hash = self.hash_text(source)

        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO translation_cache (text_hash, source_text, translated_text, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(text_hash) DO UPDATE SET
                    translated_text=excluded.translated_text,
                    updated_at=excluded.updated_at
                """,
                (text_hash, source, translated, now),
            )
            self._conn.commit()

    def get_summary(self, source_text: str) -> dict[str, str] | None:
        if not self.enabled or self._conn is None:
            return None

        text_hash = self.hash_text(source_text)
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT summary, tech_category, headline, detail
                FROM summary_cache
                WHERE text_hash = ?
                """,
                (text_hash,),
            )
            row = cur.fetchone()

        if row is None:
            return None

        return {
            "summary": str(row["summary"] or ""),
            "tech_category": str(row["tech_category"] or "기타"),
            "headline": str(row["headline"] or ""),
            "detail": str(row["detail"] or ""),
        }

    def set_summary(
        self,
        source_text: str,
        summary: str,
        tech_category: str,
        headline: str = "",
        detail: str = "",
    ) -> None:
        if not self.enabled or self._conn is None:
            return

        source = (source_text or "").strip()
        summary_value = (summary or "").strip()
        category_value = (tech_category or "기타").strip() or "기타"
        headline_value = (headline or "").strip()
        detail_value = (detail or "").strip()
        if not source or not summary_value:
            return

        now = datetime.now(timezone.utc).isoformat()
        text_hash = self.hash_text(source)

        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO summary_cache (text_hash, source_text, summary, tech_category, headline, detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(text_hash) DO UPDATE SET
                    summary=excluded.summary,
                    tech_category=excluded.tech_category,
                    headline=excluded.headline,
                    detail=excluded.detail,
                    updated_at=excluded.updated_at
                """,
                (
                    text_hash,
                    source,
                    summary_value,
                    category_value,
                    headline_value,
                    detail_value,
                    now,
                ),
            )
            self._conn.commit()

    def stats(self) -> CacheStats:
        if not self.enabled or self._conn is None:
            return CacheStats(seen_url_count=0, translation_count=0, summary_count=0)

        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT COUNT(*) AS cnt FROM post_cache")
            seen_cnt = int(cur.fetchone()["cnt"])
            cur.execute("SELECT COUNT(*) AS cnt FROM translation_cache")
            tr_cnt = int(cur.fetchone()["cnt"])
            cur.execute("SELECT COUNT(*) AS cnt FROM summary_cache")
            sum_cnt = int(cur.fetchone()["cnt"])

        return CacheStats(seen_url_count=seen_cnt, translation_count=tr_cnt, summary_count=sum_cnt)
