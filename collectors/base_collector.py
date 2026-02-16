import logging
import random
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Callable, Sequence, Any

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from source_manager import Source
from models import RawPost

class BaseCollector(ABC):
    def __init__(
        self,
        lookback_hours: int = 24,
        include_unknown_time: bool = False,
        nav_max_retries: int = 2,
        nav_retry_base_ms: int = 800,
        timeout_ms: int = 25000,
        verbose: bool = True,
    ):
        self.lookback_hours = max(0, int(lookback_hours))
        self.include_unknown_time = bool(include_unknown_time)
        self.nav_max_retries = max(0, int(nav_max_retries))
        self.nav_retry_base_ms = max(100, int(nav_retry_base_ms))
        self.timeout_ms = max(1000, int(timeout_ms))
        self.verbose = bool(verbose)
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def collect(
        self,
        page: Any,
        source: Source,
        keywords: Sequence[str],
        limit: int,
        skip_url_checker: Callable[[str], bool] | None,
    ) -> list[RawPost]:
        pass

    def log(self, message: str) -> None:
        if not self.verbose:
            return
        self.logger.info(message)

    def open_page(self, page: Any, source_name: str, target_url: str, timeout_ms: int | None = None) -> bool:
        effective_timeout_ms = self.timeout_ms if timeout_ms is None else max(1000, int(timeout_ms))
        for attempt in range(self.nav_max_retries + 1):
            try:
                page.goto(target_url, wait_until="domcontentloaded", timeout=effective_timeout_ms)
                page.wait_for_timeout(1500)
                return True
            except PlaywrightTimeoutError:
                if attempt >= self.nav_max_retries:
                    self.log(f"timeout: {source_name} ({target_url})")
                    return False
                delay_ms = self.backoff_ms(attempt, self.nav_retry_base_ms)
                self.log(f"timeout retry {attempt + 1}/{self.nav_max_retries}: {source_name}, wait={delay_ms}ms")
                page.wait_for_timeout(delay_ms)
            except Exception as exc:
                if attempt >= self.nav_max_retries:
                    self.log(f"navigation error: {source_name} ({target_url}) - {exc}")
                    return False
                delay_ms = self.backoff_ms(attempt, self.nav_retry_base_ms)
                self.log(f"navigation retry {attempt + 1}/{self.nav_max_retries}: {source_name}, wait={delay_ms}ms")
                page.wait_for_timeout(delay_ms)
        return False

    def is_within_lookback(self, posted_at: str, cutoff: datetime | None = None) -> bool:
        if self.lookback_hours <= 0:
            return True

        dt = self.parse_datetime(posted_at)
        if dt is None:
            return self.include_unknown_time

        if cutoff is None:
            cutoff = self.current_cutoff()
        if cutoff is None:
            return True
        return dt >= cutoff

    def current_cutoff(self) -> datetime | None:
        if self.lookback_hours <= 0:
            return None
        return datetime.now(timezone.utc) - timedelta(hours=self.lookback_hours)

    @staticmethod
    def is_older_than_cutoff(posted_at: str, cutoff: datetime | None) -> bool:
        if cutoff is None:
            return False
        dt = BaseCollector.parse_datetime(posted_at)
        if dt is None:
            return False
        return dt < cutoff

    @staticmethod
    def backoff_ms(attempt: int, base_ms: int) -> int:
        jitter = random.uniform(0.8, 1.2)
        return int(base_ms * (2**attempt) * jitter)

    @staticmethod
    def parse_datetime(value: str) -> datetime | None:
        if not value:
            return None
        text = value.strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @staticmethod
    def should_skip_url(url: str, checker: Callable[[str], bool] | None) -> bool:
        if checker is None:
            return False
        try:
            return checker(url)
        except Exception:
            return False

    @staticmethod
    def short_text(value: str, max_len: int = 120) -> str:
        if len(value) <= max_len:
            return value
        return f"{value[: max_len - 3]}..."

    @staticmethod
    def keyword_match(text: str, keywords: Sequence[str]) -> bool:
        clean_keywords = [keyword.strip().lower() for keyword in keywords if keyword.strip()]
        if not clean_keywords:
            return True
        lowered = text.lower()
        return any(keyword in lowered for keyword in clean_keywords)

    @staticmethod
    def normalize_text(text: str) -> str:
        compact = re.sub(r"\s+", " ", text).strip()
        return compact[:2000]

    @staticmethod
    def to_absolute_url(base: str, href: str) -> str:
        if not href:
            return ""
        if href.startswith("http://") or href.startswith("https://"):
            return href
        if href.startswith("/"):
            return f"{base.rstrip('/')}{href}"
        return f"{base.rstrip('/')}/{href}"
