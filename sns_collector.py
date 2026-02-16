from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterable, Sequence
import logging

from playwright.sync_api import sync_playwright

from collectors.base_collector import BaseCollector
from collectors.facebook_collector import FacebookCollector
from collectors.instagram_collector import InstagramCollector
from collectors.x_collector import XCollector
from models import RawPost
from source_manager import Source

PLATFORM_X = "X"
PLATFORM_INSTAGRAM = "Instagram"
PLATFORM_FACEBOOK = "Facebook"
PLATFORM_UNKNOWN = "Unknown"


class SNSCollector:
    def __init__(
        self,
        headless: bool = True,
        timeout_ms: int = 25000,
        scroll_limit: int = 8,
        scroll_wait_ms: int = 1500,
        no_growth_break_limit: int = 2,
        old_post_break_limit: int = 8,
        nav_max_retries: int = 2,
        nav_retry_base_ms: int = 800,
        block_resources: bool = True,
        x_keyword_filter: bool = False,
        lookback_hours: int = 24,
        include_unknown_time: bool = False,
        instagram_candidate_multiplier: int = 4,
        selector_version: str = "v1",
        verbose: bool = True,
    ) -> None:
        self.headless = headless
        self.verbose = bool(verbose)
        self.timeout_ms = max(1000, int(timeout_ms))
        self.block_resources = bool(block_resources)
        # Common config for all collectors
        self.collector_config = {
            "scroll_limit": max(1, int(scroll_limit)),
            "scroll_wait_ms": max(100, int(scroll_wait_ms)),
            "no_growth_break_limit": max(0, int(no_growth_break_limit)),
            "old_post_break_limit": max(0, int(old_post_break_limit)),
            "nav_max_retries": max(0, int(nav_max_retries)),
            "nav_retry_base_ms": max(100, int(nav_retry_base_ms)),
            "timeout_ms": self.timeout_ms,
            "lookback_hours": max(0, int(lookback_hours)),
            "include_unknown_time": bool(include_unknown_time),
            "verbose": self.verbose,
        }

        # Initialize specific collectors
        self.collectors: dict[str, BaseCollector] = {
            PLATFORM_X: XCollector(
                x_keyword_filter=x_keyword_filter,
                selector_version=selector_version,
                **self.collector_config,
            ),
            PLATFORM_INSTAGRAM: InstagramCollector(
                instagram_candidate_multiplier=max(1, int(instagram_candidate_multiplier)),
                selector_version=selector_version,
                **self.collector_config,
            ),
            PLATFORM_FACEBOOK: FacebookCollector(
                selector_version=selector_version,
                **self.collector_config,
            ),
        }
    def collect_by_source(
        self,
        sources: Iterable[Source],
        keywords: Sequence[str] | None = None,
        limit_per_source: int = 30,
        parallel_workers: int = 1,
        skip_url_checker: Callable[[str], bool] | None = None,
    ) -> list[tuple[Source, list[RawPost]]]:
        source_list = list(sources)
        total_sources = len(source_list)
        workers = max(1, int(parallel_workers))

        self._log(f"start (sources={total_sources}, limit={limit_per_source}, workers={workers})")

        if total_sources == 0:
            return []

        if workers <= 1 or total_sources == 1:
            results = self._collect_by_source_sequential(
                source_list, keywords or [], limit_per_source, skip_url_checker
            )
            self._log(f"done (total_collected={sum(len(posts) for _, posts in results)})")
            return results

        results = self._collect_by_source_parallel(
            source_list, keywords or [], limit_per_source, workers, skip_url_checker
        )
        self._log(f"done (total_collected={sum(len(posts) for _, posts in results)})")
        return results

    def _collect_by_source_sequential(
        self,
        sources: list[Source],
        keywords: Sequence[str],
        limit_per_source: int,
        skip_url_checker: Callable[[str], bool] | None,
    ) -> list[tuple[Source, list[RawPost]]]:
        result: list[tuple[Source, list[RawPost]]] = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(locale="en-US")
            self._configure_context(context)
            page = context.new_page()

            try:
                for index, source in enumerate(sources, start=1):
                    platform = self._detect_platform(source.url)
                    self._log(f"[{index}/{len(sources)}] {source.name} ({platform}) collecting...")
                    source_started = time.perf_counter()
                    
                    collector = self.collectors.get(platform)
                    if collector:
                        try:
                            posts = collector.collect(page, source, keywords, limit_per_source, skip_url_checker)
                        except Exception as exc:
                            self._log(f"skip {source.name}: {exc.__class__.__name__}: {exc}")
                            posts = []
                    else:
                        self._log(f"unsupported platform: {source.url}")
                        posts = []

                    result.append((source, posts))
                    self._log(
                        f"[{index}/{len(sources)}] {source.name}: {len(posts)}건 수집 "
                        f"(elapsed={time.perf_counter() - source_started:.2f}s)"
                    )
            finally:
                context.close()
                browser.close()

        return result

    def _collect_by_source_parallel(
        self,
        sources: list[Source],
        keywords: Sequence[str],
        limit_per_source: int,
        workers: int,
        skip_url_checker: Callable[[str], bool] | None,
    ) -> list[tuple[Source, list[RawPost]]]:
        result_slots: list[tuple[Source, list[RawPost]] | None] = [None] * len(sources)
        worker_count = min(workers, len(sources))
        indexed_sources = list(enumerate(sources))
        batches: list[list[tuple[int, Source]]] = [[] for _ in range(worker_count)]
        for position, item in enumerate(indexed_sources):
            batches[position % worker_count].append(item)

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(
                    self._collect_source_batch_in_isolated_browser,
                    batch,
                    keywords,
                    limit_per_source,
                    skip_url_checker,
                ): worker_idx
                for worker_idx, batch in enumerate(batches)
                if batch
            }

            for future in as_completed(future_map):
                worker_idx = future_map[future]
                try:
                    batch_results = future.result()
                except Exception as exc:
                    self._log(f"worker-{worker_idx} failed: {exc}")
                    batch_results = []

                for index, source, posts, elapsed in batch_results:
                    platform = self._detect_platform(source.url)
                    result_slots[index] = (source, posts)
                    self._log(
                        f"[{index + 1}/{len(sources)}] ({platform}) {source.name}: {len(posts)}건 수집 "
                        f"(elapsed={elapsed:.2f}s, worker={worker_idx + 1}/{worker_count})"
                    )

        return [slot for slot in result_slots if slot is not None]

    def _collect_source_batch_in_isolated_browser(
        self,
        indexed_sources: list[tuple[int, Source]],
        keywords: Sequence[str],
        limit_per_source: int,
        skip_url_checker: Callable[[str], bool] | None,
    ) -> list[tuple[int, Source, list[RawPost], float]]:
        results: list[tuple[int, Source, list[RawPost], float]] = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(locale="en-US")
            self._configure_context(context)
            page = context.new_page()

            try:
                for index, source in indexed_sources:
                    source_started = time.perf_counter()
                    platform = self._detect_platform(source.url)
                    collector = self.collectors.get(platform)
                    
                    if collector:
                        try:
                            posts = collector.collect(page, source, keywords, limit_per_source, skip_url_checker)
                        except Exception as exc:
                            self._log(f"skip {source.name}: {exc.__class__.__name__}: {exc}")
                            posts = []
                    else:
                        self._log(f"unsupported platform: {source.url}")
                        posts = []

                    results.append((index, source, posts, time.perf_counter() - source_started))
            finally:
                context.close()
                browser.close()

        return results

    def _configure_context(self, context) -> None:
        if not self.block_resources:
            return

        def route_handler(route):
            request = route.request
            resource_type = (request.resource_type or "").lower()
            url = (request.url or "").lower()
            if resource_type in {"image", "media", "font"}:
                route.abort()
                return
            if any(token in url for token in ("doubleclick", "googlesyndication", "google-analytics", "analytics")):
                route.abort()
                return
            route.continue_()

        try:
            context.route("**/*", route_handler)
        except Exception as exc:
            self._log(f"resource blocking setup failed: {exc}")

    def _log(self, message: str) -> None:
        if not self.verbose:
            return
        logging.getLogger("SNSCollector").info(message)

    @staticmethod
    def _detect_platform(url: str) -> str:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        host = parsed.netloc.lower()

        if "x.com" in host or "twitter.com" in host:
            return PLATFORM_X
        if "instagram.com" in host:
            return PLATFORM_INSTAGRAM
        if "facebook.com" in host or "fb.com" in host:
            return PLATFORM_FACEBOOK
        return PLATFORM_UNKNOWN
