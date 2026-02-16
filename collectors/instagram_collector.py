from typing import Callable, Sequence, Any
from urllib.parse import urlparse

from selector_table import resolve_selectors
from source_manager import Source
from models import RawPost
from collectors.base_collector import BaseCollector

PLATFORM_INSTAGRAM = "Instagram"

class InstagramCollector(BaseCollector):
    def __init__(
        self,
        scroll_limit: int = 8,
        scroll_wait_ms: int = 1500,
        no_growth_break_limit: int = 2,
        old_post_break_limit: int = 8,
        instagram_candidate_multiplier: int = 4,
        selector_version: str = "v1",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.scroll_limit = max(1, int(scroll_limit))
        self.scroll_wait_ms = max(100, int(scroll_wait_ms))
        self.no_growth_break_limit = max(0, int(no_growth_break_limit))
        self.old_post_break_limit = max(0, int(old_post_break_limit))
        self.instagram_candidate_multiplier = max(1, int(instagram_candidate_multiplier))
        self.selectors = resolve_selectors(PLATFORM_INSTAGRAM, selector_version)

    def collect(
        self,
        page: Any,
        source: Source,
        keywords: Sequence[str],
        limit: int,
        skip_url_checker: Callable[[str], bool] | None,
    ) -> list[RawPost]:
        if page is None:
            return []

        if not self.open_page(page, source.name, source.url):
            return []

        candidate_limit = max(limit, limit * self.instagram_candidate_multiplier)
        post_urls = self._collect_post_urls(page, source.url, candidate_limit)
        self.log(f"{source.name} candidates: {len(post_urls)} (limit={candidate_limit})")
        results: list[RawPost] = []
        cutoff = self.current_cutoff()
        old_post_streak = 0

        for post_url in post_urls:
            if len(results) >= limit:
                break
            if self.should_skip_url(post_url, skip_url_checker):
                continue
            if not self.open_page(page, source.name, post_url):
                continue

            text = self._extract_post_text(page)
            if not text:
                continue
            if not self.keyword_match(text, keywords):
                continue

            posted_at = self._extract_time_from_page(page)
            if not self.is_within_lookback(posted_at, cutoff=cutoff):
                if self.is_older_than_cutoff(posted_at, cutoff):
                    old_post_streak += 1
                else:
                    old_post_streak = 0

                if self.old_post_break_limit > 0 and old_post_streak >= self.old_post_break_limit:
                    self.log(
                        f"{source.name} early stop: old posts streak={old_post_streak}"
                    )
                    break
                continue

            old_post_streak = 0
            results.append(
                RawPost(
                    source_name=source.name,
                    source_category=source.category,
                    source_group=source.group,
                    platform=PLATFORM_INSTAGRAM,
                    post_url=post_url,
                    posted_at=posted_at,
                    text=text,
                )
            )
            self.log(f"{source.name} matched: {len(results)}/{limit}")

        return results

    def _collect_post_urls(self, page: Any, source_url: str, candidate_limit: int) -> list[str]:
        base = self._base_url(source_url)
        post_urls: list[str] = []
        seen: set[str] = set()
        candidate_selector = ", ".join(self.selectors["post_url_candidates"])
        stale_scrolls = 0

        for scroll_idx in range(1, self.scroll_limit + 1):
            anchors = page.locator(candidate_selector)
            anchor_count = anchors.count()
            self.log(f"url scan {scroll_idx}/{self.scroll_limit}, anchors={anchor_count}")
            before_count = len(post_urls)

            for idx in range(anchor_count):
                href = anchors.nth(idx).get_attribute("href") or ""
                post_url = self.to_absolute_url(base, href)
                if not self._is_instagram_post_url(post_url):
                    continue
                if post_url in seen:
                    continue

                seen.add(post_url)
                post_urls.append(post_url)
                if len(post_urls) >= candidate_limit:
                    return post_urls

            page.mouse.wheel(0, 2400)
            page.wait_for_timeout(self.scroll_wait_ms)

            if len(post_urls) == before_count:
                stale_scrolls += 1
            else:
                stale_scrolls = 0

            if self.no_growth_break_limit > 0 and stale_scrolls >= self.no_growth_break_limit:
                self.log(f"url scan early stop: no new urls for {stale_scrolls} scrolls")
                break

        return post_urls

    def _extract_post_text(self, page: Any) -> str:
        og_desc = page.locator(self.selectors["post_og_description"]).first
        if og_desc.count() > 0:
            content = (og_desc.get_attribute("content") or "").strip()
            text = self._parse_instagram_og_description(content)
            if text:
                return text

        article = page.locator(self.selectors["post_article"]).first
        if article.count() > 0:
            return self.normalize_text(article.inner_text())

        return ""

    def _extract_time_from_page(self, page: Any) -> str:
        selector = self.selectors["post_time"]
        time_locator = page.locator(selector).first
        if time_locator.count() == 0:
            return ""
        return time_locator.get_attribute("datetime") or ""

    @staticmethod
    def _is_instagram_post_url(url: str) -> bool:
        return "/p/" in url or "/reel/" in url or "/tv/" in url

    @staticmethod
    def _base_url(source_url: str) -> str:
        parsed = urlparse(source_url)
        if not parsed.scheme or not parsed.netloc:
            return "https://www.instagram.com"
        return f"{parsed.scheme}://{parsed.netloc}"

    @staticmethod
    def _parse_instagram_og_description(description: str) -> str:
        if not description:
            return ""

        if ":" not in description:
            return description

        _, tail = description.split(":", 1)
        return tail.strip()
