import argparse
import time
import logging
from dataclasses import dataclass
from typing import Sequence

import config
import logger
from cache_manager import CacheManager
from data_processor import DataProcessor, GeminiSummarizer
from models import RawPost
from sns_collector import SNSCollector
from source_manager import Source, ensure_sources_file, load_sources
from storage_manager import ExcelStorageManager

log = logging.getLogger("Pipeline")

@dataclass
class PipelineCounters:
    total_raw: int = 0
    total_fresh: int = 0
    total_processed: int = 0
    total_added: int = 0
    total_rows: int = 0


def _log_pipeline_start(args: argparse.Namespace, conf: config.RuntimeConfig, source_count: int) -> None:
    log.info("start")
    log.info(f"sources file: {args.sources}")
    log.info(f"loaded sources: {source_count}")
    log.info(f"keywords: {conf.keywords if conf.keywords else '(none)'}")
    log.info(f"lookback hours: {conf.lookback_hours}")
    log.info(f"workers: {conf.workers}")
    log.info(f"cache: {'off' if args.no_cache else args.cache_db}")
    log.info(f"cache window hours: {conf.cache_window_hours if conf.cache_window_hours > 0 else 'all'}")
    log.info(f"cache max urls: {conf.cache_max_urls if conf.cache_max_urls > 0 else 'unlimited'}")
    log.info(f"instagram candidate multiplier: {conf.instagram_candidate_multiplier}")
    log.info("collecting posts...")


def _build_skip_url_checker(seen_url_hashes: set[str]):
    def skip_url_checker(url: str) -> bool:
        return CacheManager.hash_url(url) in seen_url_hashes

    return skip_url_checker


def _collect_posts(
    args: argparse.Namespace,
    conf: config.RuntimeConfig,
    sources: list[Source],
    skip_url_checker,
    verbose: bool,
) -> tuple[list[tuple[Source, list[RawPost]]], float]:
    collector = SNSCollector(
        headless=not args.headful,
        lookback_hours=conf.lookback_hours,
        no_growth_break_limit=max(0, int(args.no_growth_break_limit)),
        old_post_break_limit=max(0, int(args.old_post_break_limit)),
        nav_max_retries=max(0, int(args.collector_retries)),
        nav_retry_base_ms=max(100, int(args.collector_retry_base_ms)),
        block_resources=not args.disable_resource_blocking,
        x_keyword_filter=args.x_keyword_filter,
        include_unknown_time=args.include_unknown_time,
        instagram_candidate_multiplier=conf.instagram_candidate_multiplier,
        selector_version=args.selector_version,
        verbose=verbose,
    )
    collect_started = time.perf_counter()
    source_results = collector.collect_by_source(
        sources=sources,
        keywords=conf.keywords,
        limit_per_source=conf.limit_per_source,
        parallel_workers=conf.workers,
        skip_url_checker=skip_url_checker,
    )
    return source_results, time.perf_counter() - collect_started


def _build_summarizer(args: argparse.Namespace, cache: CacheManager) -> GeminiSummarizer:
    import os
    gemini_api_key = os.getenv("GEMINI_API_KEY", "")
    if not args.no_ai and not gemini_api_key:
        log.warning("GEMINI_API_KEY 미설정: 규칙 기반 요약으로 대체합니다.")
    return GeminiSummarizer(
        api_key=gemini_api_key,
        model_name=args.gemini_model,
        enabled=not args.no_ai,
        translation_cache=cache if not args.no_cache else None,
    )


def _filter_fresh_posts(raw_posts: list[RawPost], seen_url_hashes: set[str]) -> list[RawPost]:
    fresh_posts: list[RawPost] = []
    for post in raw_posts:
        if not post.post_url:
            continue
        url_hash = CacheManager.hash_url(post.post_url)
        if url_hash in seen_url_hashes:
            continue
        seen_url_hashes.add(url_hash)
        fresh_posts.append(post)
    return fresh_posts


def _process_source_results(
    source_results: list[tuple[Source, list[RawPost]]],
    seen_url_hashes: set[str],
    processor: DataProcessor,
    verbose: bool,
) -> tuple[list[dict[str, str]], list[RawPost], PipelineCounters, float]:
    counters = PipelineCounters()
    all_rows: list[dict[str, str]] = []
    posts_to_cache: list[RawPost] = []
    process_elapsed = 0.0

    for index, (source, raw_posts) in enumerate(source_results, start=1):
        source_started = time.perf_counter()
        counters.total_raw += len(raw_posts)
        fresh_posts = _filter_fresh_posts(raw_posts, seen_url_hashes)
        counters.total_fresh += len(fresh_posts)

        if verbose:
            log.info(
                f"source {index}/{len(source_results)} {source.name}: "
                f"raw={len(raw_posts)}, fresh={len(fresh_posts)}"
            )

        if not fresh_posts:
            if verbose:
                log.info(
                    f"source {index}/{len(source_results)} {source.name}: "
                    f"elapsed={time.perf_counter() - source_started:.2f}s"
                )
            continue

        process_started = time.perf_counter()
        rows = processor.process(fresh_posts)
        process_elapsed += time.perf_counter() - process_started
        counters.total_processed += len(rows)
        all_rows.extend(rows)
        posts_to_cache.extend(fresh_posts)

        if verbose and rows:
            log.info("refined summaries:")
            for row_idx, row in enumerate(rows, start=1):
                log.info(f"[{row_idx}] {row['주요내용']}")

        if verbose:
            log.info(
                f"source {index}/{len(source_results)} {source.name}: "
                f"elapsed={time.perf_counter() - source_started:.2f}s"
            )

    return all_rows, posts_to_cache, counters, process_elapsed


def _print_pipeline_summary(
    args: argparse.Namespace,
    counters: PipelineCounters,
    collect_elapsed: float,
    process_elapsed: float,
    save_elapsed: float,
    cache_write_elapsed: float,
) -> None:
    log.info(f"raw collected: {counters.total_raw}")
    log.info(f"fresh after cache: {counters.total_fresh}")
    log.info(f"processed rows: {counters.total_processed}")
    log.info(f"added: {counters.total_added}")
    log.info(f"total rows: {counters.total_rows}")
    log.info(f"output: {args.output}")
    log.info(
        "timings(sec): "
        f"collect={collect_elapsed:.2f}, process={process_elapsed:.2f}, "
        f"save={save_elapsed:.2f}, cache_write={cache_write_elapsed:.2f}, "
        f"total={collect_elapsed + process_elapsed + save_elapsed + cache_write_elapsed:.2f}"
    )


def run_pipeline(args: argparse.Namespace) -> int:
    verbose = not args.quiet
    # Initialize logging
    logger.setup_logging(verbose=verbose)
    
    ensure_sources_file(args.sources)
    sources = load_sources(args.sources)
    conf = config.build_runtime_config(args)

    if verbose:
        _log_pipeline_start(args, conf, len(sources))

    with CacheManager(db_path=args.cache_db, enabled=not args.no_cache) as cache:
        seen_url_hashes = cache.load_seen_url_hashes(
            recent_hours=conf.cache_window_hours if conf.cache_window_hours > 0 else None,
            max_count=conf.cache_max_urls if conf.cache_max_urls > 0 else None,
        )
        if verbose and not args.no_cache:
            stats = cache.stats()
            log.info(
                f"cache urls(total)={stats.seen_url_count}, "
                f"loaded={len(seen_url_hashes)}, translations={stats.translation_count}, summaries={stats.summary_count}"
            )

        skip_url_checker = _build_skip_url_checker(seen_url_hashes)
        source_results, collect_elapsed = _collect_posts(args, conf, sources, skip_url_checker, verbose)

        summarizer = _build_summarizer(args, cache)
        processor = DataProcessor(summarizer, verbose=verbose)
        storage = ExcelStorageManager(output_path=args.output, sheet_name=args.sheet, verbose=verbose)

        all_rows, posts_to_cache, counters, process_elapsed = _process_source_results(
            source_results=source_results,
            seen_url_hashes=seen_url_hashes,
            processor=processor,
            verbose=verbose,
        )

        save_started = time.perf_counter()
        added, total_rows = storage.merge_and_save(all_rows)
        save_elapsed = time.perf_counter() - save_started
        counters.total_added = added
        counters.total_rows = total_rows

        cache_write_elapsed = 0.0
        if not args.no_cache and posts_to_cache:
            cache_started = time.perf_counter()
            cache.add_posts(posts_to_cache)
            cache_write_elapsed = time.perf_counter() - cache_started

        _print_pipeline_summary(
            args=args,
            counters=counters,
            collect_elapsed=collect_elapsed,
            process_elapsed=process_elapsed,
            save_elapsed=save_elapsed,
            cache_write_elapsed=cache_write_elapsed,
        )

        if verbose and not args.no_cache:
            stats = cache.stats()
            log.info(
                f"cache urls(total)={stats.seen_url_count}, "
                f"translations={stats.translation_count}, summaries={stats.summary_count}"
            )
            log.info("done")

    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = config.build_parser()
    args = parser.parse_args(argv)

    if args.create_sources:
        path = ensure_sources_file(args.sources)
        print(f"sources file ready: {path.resolve()}")
        return 0

    try:
        return run_pipeline(args)
    except (FileNotFoundError, ValueError, PermissionError) as exc:
        # Use print here because logging might not be setup if error occurs early
        # Or setup logging early with defaults?
        # But we only know verbose flag after parsing args.
        # So printing to stderr is safer for early errors.
        # But here we are after run_pipeline starts? No, wrapper.
        print(f"[error] {exc}")
        return 1
    except Exception as exc:
        print(f"[error] unexpected failure: {exc.__class__.__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
