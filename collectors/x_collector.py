from typing import Any, Callable, Sequence
from urllib.parse import quote_plus, urlparse

from collectors.base_collector import BaseCollector
from models import RawPost
from selector_table import resolve_selectors
from source_manager import Source

PLATFORM_X = "X"


class XCollector(BaseCollector):
    def __init__(
        self,
        scroll_limit: int = 8,
        scroll_wait_ms: int = 1500,
        no_growth_break_limit: int = 2,
        old_post_break_limit: int = 8,
        x_keyword_filter: bool = False,
        selector_version: str = "v1",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.scroll_limit = max(1, int(scroll_limit))
        self.scroll_wait_ms = max(100, int(scroll_wait_ms))
        self.no_growth_break_limit = max(0, int(no_growth_break_limit))
        self.old_post_break_limit = max(0, int(old_post_break_limit))
        self.x_keyword_filter = bool(x_keyword_filter)
        self.selectors = resolve_selectors(PLATFORM_X, selector_version)

    def collect(
        self,
        page: Any,
        source: Source,
        keywords: Sequence[str],
        limit: int,
        skip_url_checker: Callable[[str], bool] | None,
    ) -> list[RawPost]:
        candidate_urls = [source.url]
        try:
            search_url = self._build_search_url(source.url, keywords)
            candidate_urls.insert(0, search_url)
        except ValueError:
            pass

        for target_url in candidate_urls:
            self.log(f"try url: {self.short_text(target_url)}")
            posts = self._collect_from_page(page, source, target_url, keywords, limit, skip_url_checker)
            if posts:
                return posts
        return []

    def _collect_from_page(
        self,
        page: Any,
        source: Source,
        target_url: str,
        keywords: Sequence[str],
        limit: int,
        skip_url_checker: Callable[[str], bool] | None,
    ) -> list[RawPost]:
        if not self.open_page(page, source.name, target_url):
            return []

        collected: list[RawPost] = []
        seen_urls: set[str] = set()
        stale_scrolls = 0
        cutoff = self.current_cutoff()
        old_post_streak = 0

        for scroll_idx in range(1, self.scroll_limit + 1):
            tweets = page.locator(self.selectors["post_container"])
            tweet_count = tweets.count()
            self.log(f"{source.name} scroll {scroll_idx}/{self.scroll_limit}, tweets={tweet_count}")
            before_seen = len(seen_urls)

            for idx in range(tweet_count):
                if len(collected) >= limit:
                    return collected

                tweet = tweets.nth(idx)
                post_url = self._extract_post_url(tweet)
                if not post_url or post_url in seen_urls:
                    continue
                seen_urls.add(post_url)

                if self.should_skip_url(post_url, skip_url_checker):
                    continue

                text = self._extract_text(tweet)
                if not text:
                    continue
                if self.x_keyword_filter and not self.keyword_match(text, keywords):
                    continue

                posted_at = self._extract_datetime(tweet)
                if not self.is_within_lookback(posted_at, cutoff=cutoff):
                    if self.is_older_than_cutoff(posted_at, cutoff):
                        old_post_streak += 1
                    else:
                        old_post_streak = 0
                    if self.old_post_break_limit > 0 and old_post_streak >= self.old_post_break_limit:
                        self.log(f"{source.name} early stop: old posts streak={old_post_streak}")
                        return collected
                    continue

                old_post_streak = 0
                collected.append(
                    RawPost(
                        source_name=source.name,
                        source_category=source.category,
                        source_group=source.group,
                        platform=PLATFORM_X,
                        post_url=post_url,
                        posted_at=posted_at,
                        text=text,
                    )
                )

                if len(collected) == limit or len(collected) % 5 == 0:
                    self.log(f"{source.name} collected: {len(collected)}/{limit}")

            page.mouse.wheel(0, 3000)
            page.wait_for_timeout(self.scroll_wait_ms)

            if len(seen_urls) == before_seen:
                stale_scrolls += 1
            else:
                stale_scrolls = 0

            if self.no_growth_break_limit > 0 and stale_scrolls >= self.no_growth_break_limit:
                self.log(f"{source.name} early stop: no new posts for {stale_scrolls} scrolls")
                break

        return collected

    def _build_search_url(self, source_url: str, keywords: Sequence[str]) -> str:
        handle = self._extract_handle(source_url)
        query = self._build_query(handle, keywords)
        return f"https://x.com/search?q={quote_plus(query)}&src=typed_query&f=live"

    @staticmethod
    def _extract_handle(url: str) -> str:
        parsed = urlparse(url)
        host = parsed.netloc.lower()

        if host and "x.com" not in host and "twitter.com" not in host:
            raise ValueError(f"Not an X URL: {url}")

        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            raise ValueError(f"Could not find handle in X URL: {url}")
        return parts[0].lstrip("@")

    @staticmethod
    def _build_query(handle: str, keywords: Sequence[str]) -> str:
        base = f"from:{handle}"
        keyword_list = [keyword.strip() for keyword in keywords if keyword.strip()]
        if not keyword_list:
            return base

        keyword_expr = " OR ".join(f'"{keyword}"' for keyword in keyword_list)
        return f"({base}) ({keyword_expr})"

    def _extract_post_url(self, tweet) -> str:
        selector = self.selectors["post_link"]
        locator = tweet.locator(selector).first
        if locator.count() == 0:
            return ""

        href = locator.get_attribute("href") or ""
        if not href:
            return ""
        if href.startswith("http://") or href.startswith("https://"):
            return href
        if href.startswith("/"):
            return f"https://x.com{href}"
        return ""

    def _extract_text(self, tweet) -> str:
        selector = self.selectors["post_text"]
        text_locator = tweet.locator(selector).first
        if text_locator.count() == 0:
            return ""

        text = text_locator.inner_text().strip()
        return " ".join(text.split())

    def _extract_datetime(self, tweet) -> str:
        selector = self.selectors["post_time"]
        time_locator = tweet.locator(selector).first
        if time_locator.count() == 0:
            return ""
        return time_locator.get_attribute("datetime") or ""
