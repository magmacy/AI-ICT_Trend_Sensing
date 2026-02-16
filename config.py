import argparse
import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Load environment variables once when module is imported
load_dotenv()

@dataclass(frozen=True)
class RuntimeConfig:
    keywords: list[str]
    lookback_hours: int
    workers: int
    limit_per_source: int
    cache_window_hours: int
    cache_max_urls: int
    instagram_candidate_multiplier: int


def parse_keywords(cli_keywords: str | None) -> list[str]:
    if cli_keywords:
        return [item.strip() for item in cli_keywords.split(",") if item.strip()]

    env_value = os.getenv("SEARCH_KEYWORDS", "")
    return [item.strip() for item in env_value.split(",") if item.strip()]


def default_lookback_hours() -> int:
    try:
        return int(os.getenv("LOOKBACK_HOURS", "24"))
    except ValueError:
        return 24


def default_collect_workers() -> int:
    try:
        return max(1, int(os.getenv("COLLECT_WORKERS", "3")))
    except ValueError:
        return 3


def default_cache_window_hours() -> int:
    try:
        return max(0, int(os.getenv("CACHE_WINDOW_HOURS", "168")))
    except ValueError:
        return 168


def default_cache_max_urls() -> int:
    try:
        return max(0, int(os.getenv("CACHE_MAX_URLS", "200000")))
    except ValueError:
        return 200000


def default_no_growth_break_limit() -> int:
    try:
        return max(0, int(os.getenv("NO_GROWTH_BREAK_LIMIT", "2")))
    except ValueError:
        return 2


def default_old_post_break_limit() -> int:
    try:
        return max(0, int(os.getenv("OLD_POST_BREAK_LIMIT", "8")))
    except ValueError:
        return 8


def default_collector_retries() -> int:
    try:
        return max(0, int(os.getenv("COLLECTOR_RETRIES", "2")))
    except ValueError:
        return 2


def default_collector_retry_base_ms() -> int:
    try:
        return max(100, int(os.getenv("COLLECTOR_RETRY_BASE_MS", "800")))
    except ValueError:
        return 800


def default_instagram_candidate_multiplier() -> int:
    try:
        return max(1, int(os.getenv("INSTAGRAM_CANDIDATE_MULTIPLIER", "4")))
    except ValueError:
        return 4


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SNS 뉴스 수집 및 요약 파이프라인")
    parser.add_argument("--sources", default="sources.xlsx", help="수집 소스 파일 경로")
    parser.add_argument("--output", default="SNS_News_Collection.xlsx", help="출력 엑셀 파일 경로")
    parser.add_argument("--sheet", default="News_Data", help="출력 시트명")
    parser.add_argument("--create-sources", action="store_true", help="기본 sources.xlsx 생성 후 종료")
    parser.add_argument("--headful", action="store_true", help="브라우저를 표시 모드로 실행")
    parser.add_argument("--limit-per-source", type=int, default=20, help="소스당 최대 수집 건수")
    parser.add_argument("--lookback-hours", type=int, default=default_lookback_hours(), help="수집 시간 범위(기본 24시간)")
    parser.add_argument("--workers", type=int, default=default_collect_workers(), help="병렬 수집 워커 수")
    parser.add_argument("--no-growth-break-limit", type=int, default=default_no_growth_break_limit(), help="스크롤 중 신규 항목이 없을 때 조기 종료 임계값(0이면 비활성)")
    parser.add_argument("--old-post-break-limit", type=int, default=default_old_post_break_limit(), help="연속 과거 게시물 발견 시 조기 종료 임계값(0이면 비활성)")
    parser.add_argument("--collector-retries", type=int, default=default_collector_retries(), help="페이지 이동 재시도 횟수")
    parser.add_argument("--collector-retry-base-ms", type=int, default=default_collector_retry_base_ms(), help="페이지 이동 재시도 기본 대기(ms)")
    parser.add_argument("--disable-resource-blocking", action="store_true", help="수집 시 이미지/미디어 차단 비활성화")
    parser.add_argument("--x-keyword-filter", action="store_true", help="X 본문에 키워드 필터 적용(기본 비활성)")
    parser.add_argument(
        "--instagram-candidate-multiplier",
        type=int,
        default=default_instagram_candidate_multiplier(),
        help="Instagram 후보 URL 확장 배수",
    )
    parser.add_argument("--selector-version", default=os.getenv("SELECTOR_VERSION", "v1"), help="셀렉터 버전")
    parser.add_argument("--include-unknown-time", action="store_true", help="게시 시간 없는 글 포함")
    parser.add_argument("--cache-db", default=os.getenv("CACHE_DB_PATH", "pipeline_cache.sqlite3"), help="SQLite 캐시 파일")
    parser.add_argument("--cache-window-hours", type=int, default=default_cache_window_hours(), help="캐시 URL 로드 시간 범위(0이면 전체)")
    parser.add_argument("--cache-max-urls", type=int, default=default_cache_max_urls(), help="캐시 URL 최대 로드 개수(0이면 무제한)")
    parser.add_argument("--no-cache", action="store_true", help="캐시 사용 안 함")
    parser.add_argument("--keywords", default=None, help="쉼표 구분 검색 키워드")
    parser.add_argument("--no-ai", action="store_true", help="Gemini 요약 비활성화")
    parser.add_argument("--gemini-model", default=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"))
    parser.add_argument("--quiet", action="store_true", help="진행 로그 최소화")
    return parser


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    return RuntimeConfig(
        keywords=parse_keywords(args.keywords),
        lookback_hours=max(0, int(args.lookback_hours)),
        workers=max(1, int(args.workers)),
        limit_per_source=max(1, int(args.limit_per_source)),
        cache_window_hours=max(0, int(args.cache_window_hours)),
        cache_max_urls=max(0, int(args.cache_max_urls)),
        instagram_candidate_multiplier=max(1, int(args.instagram_candidate_multiplier)),
    )
