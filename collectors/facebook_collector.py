from typing import Callable, Sequence, Any

from selector_table import resolve_selectors
from source_manager import Source
from models import RawPost
from collectors.base_collector import BaseCollector

PLATFORM_FACEBOOK = "Facebook"

class FacebookCollector(BaseCollector):
    def __init__(
        self,
        scroll_limit: int = 8,
        scroll_wait_ms: int = 1500,
        no_growth_break_limit: int = 2,
        old_post_break_limit: int = 8,
        selector_version: str = "v1",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.scroll_limit = max(1, int(scroll_limit))
        self.scroll_wait_ms = max(100, int(scroll_wait_ms))
        self.no_growth_break_limit = max(0, int(no_growth_break_limit))
        self.old_post_break_limit = max(0, int(old_post_break_limit))
        self.selectors = resolve_selectors(PLATFORM_FACEBOOK, selector_version)

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

        results: list[RawPost] = []
        seen_urls: set[str] = set()
        stale_scrolls = 0
        cutoff = self.current_cutoff()
        old_post_streak = 0

        for scroll_idx in range(1, self.scroll_limit + 1):
            articles = page.locator(self.selectors["post_container"])
            article_count = articles.count()
            self.log(f"{source.name} scroll {scroll_idx}/{self.scroll_limit}, articles={article_count}")
            before_seen = len(seen_urls)

            for idx in range(article_count):
                if len(results) >= limit:
                    return results

                article = articles.nth(idx)
                post_url = self._extract_post_url(article)
                if not post_url or post_url in seen_urls:
                    continue
                seen_urls.add(post_url)
                if self.should_skip_url(post_url, skip_url_checker):
                    continue

                text = self.normalize_text(article.inner_text())
                if not text:
                    continue
                if not self.keyword_match(text, keywords):
                    continue

                posted_at = self._extract_time_from_container(article)
                if not self.is_within_lookback(posted_at, cutoff=cutoff):
                    if self.is_older_than_cutoff(posted_at, cutoff):
                        old_post_streak += 1
                    else:
                        old_post_streak = 0

                    if self.old_post_break_limit > 0 and old_post_streak >= self.old_post_break_limit:
                        self.log(f"{source.name} early stop: old posts streak={old_post_streak}")
                        return results
                    continue

                old_post_streak = 0
                results.append(
                    RawPost(
                        source_name=source.name,
                        source_category=source.category,
                        source_group=source.group,
                        platform=PLATFORM_FACEBOOK,
                        post_url=post_url,
                        posted_at=posted_at,
                        text=text,
                    )
                )

            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(self.scroll_wait_ms)

            if len(seen_urls) == before_seen:
                stale_scrolls += 1
            else:
                stale_scrolls = 0

            if self.no_growth_break_limit > 0 and stale_scrolls >= self.no_growth_break_limit:
                self.log(
                    f"{source.name} early stop: no new posts for {stale_scrolls} scrolls"
                )
                break

        return results

    def _extract_post_url(self, article: Any) -> str:
        for selector in self.selectors["post_url_candidates"]:
            link = article.locator(selector).first
            if link.count() == 0:
                continue

            href = link.get_attribute("href") or ""
            absolute = self.to_absolute_url("https://www.facebook.com", href)
            if absolute:
                return absolute

        return ""

    def _extract_time_from_container(self, container: Any) -> str:
        selector = self.selectors["post_time"]
        time_locator = container.locator(selector).first
        if time_locator.count() == 0:
            return ""
        return time_locator.get_attribute("datetime") or ""
