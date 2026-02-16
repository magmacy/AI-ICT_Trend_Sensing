from __future__ import annotations

from typing import Any

PLATFORM_X = "X"
PLATFORM_INSTAGRAM = "Instagram"
PLATFORM_FACEBOOK = "Facebook"

SELECTOR_TABLE: dict[str, dict[str, dict[str, Any]]] = {
    PLATFORM_X: {
        "v1": {
            "post_container": 'article[data-testid="tweet"]',
            "post_link": 'a[href*="/status/"]',
            "post_text": '[data-testid="tweetText"]',
            "post_time": "time",
        }
    },
    PLATFORM_INSTAGRAM: {
        "v1": {
            "post_url_candidates": ['a[href*="/p/"]', 'a[href*="/reel/"]', 'a[href*="/tv/"]'],
            "post_article": "article",
            "post_og_description": 'meta[property="og:description"]',
            "post_time": "time",
        }
    },
    PLATFORM_FACEBOOK: {
        "v1": {
            "post_container": 'div[role="article"]',
            "post_url_candidates": [
                'a[href*="/posts/"]',
                'a[href*="/videos/"]',
                'a[href*="/photos/"]',
                'a[href*="story_fbid="]',
                'a[href*="permalink"]',
            ],
            "post_time": "time",
        }
    },
}


def resolve_selectors(platform: str, version: str = "v1") -> dict[str, Any]:
    platform_versions = SELECTOR_TABLE.get(platform)
    if not platform_versions:
        raise ValueError(f"Unsupported selector platform: {platform}")

    if version in platform_versions:
        return platform_versions[version]

    if "v1" in platform_versions:
        return platform_versions["v1"]

    latest_version = sorted(platform_versions.keys())[-1]
    return platform_versions[latest_version]
