from __future__ import annotations

import json
import logging
import random
import re
import time
import warnings
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Sequence

from models import RawPost


URL_PATTERN = re.compile(r"https?://\S+")
WHITESPACE_PATTERN = re.compile(r"\s+")
HANGUL_PATTERN = re.compile(r"[가-힣]")
EXCEL_FORMULA_PREFIXES = ("=", "+", "-", "@")

TECH_CATEGORY_KEYWORDS = {
    "AI": ["ai", "llm", "agent", "model", "인공지능", "생성형", "gemini", "gpt"],
    "반도체": ["반도체", "chip", "gpu", "npu", "hbm", "fab", "wafer"],
    "모바일": ["mobile", "모바일", "smartphone", "스마트폰", "android", "ios", "app"],
    "클라우드": ["cloud", "클라우드", "aws", "azure", "gcp", "saas"],
    "네트워크": ["network", "네트워크", "5g", "통신", "telecom"],
}


@dataclass
class SummaryResult:
    summary: str
    tech_category: str
    headline: str = ""
    detail: str = ""


class GeminiSummarizer:
    def __init__(
        self,
        api_key: str = "",
        model_name: str = "gemini-2.0-flash",
        enabled: bool = True,
        translation_cache=None,
    ) -> None:
        self.enabled = enabled and bool(api_key)
        self.can_translate = bool(api_key)
        self.translation_cache = translation_cache
        self._genai = None
        self._model_cache: dict[str, object] = {}
        self._model_candidates = self._build_model_candidates(model_name)
        self._active_model_idx = 0
        self._warned_summary_error = False
        self._warned_translation_error = False
        self._warned_fallback_translator = False
        self._fallback_translator = None
        self.max_api_retries = 2
        self.api_retry_base_delay_sec = 1.0
        self.logger = logging.getLogger(self.__class__.__name__)

        if self.can_translate:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", FutureWarning)
                import google.generativeai as genai

            genai.configure(api_key=api_key)
            self._genai = genai

        try:
            from deep_translator import GoogleTranslator

            self._fallback_translator = GoogleTranslator(source="auto", target="ko")
        except Exception:
            self._fallback_translator = None

    def summarize(self, text: str) -> SummaryResult:
        cached = self._get_cached_summary(text)
        if cached is not None:
            return cached

        if not self.enabled or not self.can_translate:
            fallback = self._fallback_summary(text)
            result = SummaryResult(
                summary=self._ensure_korean(fallback),
                tech_category=self._fallback_category(text),
            )
            self._set_cached_summary(text, result)
            return result

        prompt = self._build_prompt(text)
        try:
            generated = self._generate_text(prompt)
            parsed = self._parse_json(generated)

            summary = str(parsed.get("summary", "")).strip() or self._fallback_summary(text)
            headline = str(parsed.get("headline", "")).strip()
            detail = str(parsed.get("detail", "")).strip()
            tech_category = str(parsed.get("tech_category", "")).strip() or self._fallback_category(text)

            summary = self._ensure_korean(summary)
            if headline:
                headline = self._ensure_korean(headline)
            if detail:
                detail = self._ensure_korean(detail)

            result = SummaryResult(
                summary=summary,
                tech_category=tech_category,
                headline=headline,
                detail=detail,
            )
            self._set_cached_summary(text, result)
            return result
        except Exception as exc:
            if not self._warned_summary_error:
                self._warned_summary_error = True
                self.logger.warning(f"Gemini 요약 실패: {self._short_error(exc)}")
            fallback = self._fallback_summary(text)
            result = SummaryResult(
                summary=self._ensure_korean(fallback),
                tech_category=self._fallback_category(text),
            )
            self._set_cached_summary(text, result)
            return result

    def normalize_korean(self, text: str) -> str:
        return self._ensure_korean(text)

    def _get_cached_summary(self, text: str) -> SummaryResult | None:
        if self.translation_cache is None:
            return None

        getter = getattr(self.translation_cache, "get_summary", None)
        if not callable(getter):
            return None

        try:
            cached = getter(text)
        except Exception:
            return None

        if not cached:
            return None

        summary = self._ensure_korean(str(cached.get("summary", "")).strip())
        tech_category = str(cached.get("tech_category", "")).strip() or self._fallback_category(text)
        headline = self._ensure_korean(str(cached.get("headline", "")).strip()) if cached.get("headline") else ""
        detail = self._ensure_korean(str(cached.get("detail", "")).strip()) if cached.get("detail") else ""
        return SummaryResult(summary=summary, tech_category=tech_category, headline=headline, detail=detail)

    def _set_cached_summary(self, text: str, result: SummaryResult) -> None:
        if self.translation_cache is None:
            return

        setter = getattr(self.translation_cache, "set_summary", None)
        if not callable(setter):
            return

        try:
            setter(
                text,
                result.summary,
                result.tech_category,
                result.headline,
                result.detail,
            )
        except Exception:
            return

    @staticmethod
    def _build_model_candidates(primary_model: str) -> list[str]:
        candidates = [
            primary_model.strip(),
            "gemini-2.0-flash",
            "gemini-2.5-flash",
            "gemini-2.5-flash-lite",
        ]
        unique: list[str] = []
        for model in candidates:
            if model and model not in unique:
                unique.append(model)
        return unique

    @staticmethod
    def _is_model_not_found_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return "404" in message or "not found" in message or "is not found" in message

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        message = str(exc).lower()
        if not message:
            return False
        if "perday" in message or "requestsperday" in message:
            return False
        retry_tokens = [
            "429",
            "rate limit",
            "quota exceeded",
            "timed out",
            "timeout",
            "temporarily unavailable",
            "unavailable",
            "internal",
            "503",
            "500",
            "connection reset",
            "deadline exceeded",
        ]
        return any(token in message for token in retry_tokens)

    def _sleep_backoff(self, attempt: int) -> None:
        jitter = random.uniform(0.8, 1.2)
        delay = self.api_retry_base_delay_sec * (2**attempt) * jitter
        time.sleep(max(0.1, delay))

    def _get_model(self, model_name: str):
        if self._genai is None:
            raise RuntimeError("Gemini client is not initialized")
        if model_name not in self._model_cache:
            self._model_cache[model_name] = self._genai.GenerativeModel(model_name)
        return self._model_cache[model_name]

    def _generate_text(self, prompt: str) -> str:
        if not self.can_translate or self._genai is None:
            return ""

        last_error: Exception | None = None
        for offset in range(len(self._model_candidates)):
            idx = (self._active_model_idx + offset) % len(self._model_candidates)
            model_name = self._model_candidates[idx]
            model = self._get_model(model_name)
            for attempt in range(self.max_api_retries + 1):
                try:
                    response = model.generate_content(prompt)
                    self._active_model_idx = idx
                    return (response.text or "").strip()
                except Exception as exc:
                    last_error = exc
                    # Keep trying candidates to survive deleted/renamed model IDs.
                    if self._is_model_not_found_error(exc):
                        break
                    if self._is_retryable_error(exc) and attempt < self.max_api_retries:
                        self._sleep_backoff(attempt)
                        continue
                    break

        if last_error is not None:
            raise last_error
        return ""

    @staticmethod
    def _build_prompt(text: str) -> str:
        return (
            "다음 SNS 게시글을 분석하세요. 반드시 모든 필드는 한국어로 작성하세요.\\n"
            "1) headline: 핵심 제목 1문장(한국어)\\n"
            "2) detail: 근거/맥락 1문장(한국어)\\n"
            "3) summary: headline과 detail을 합친 요약(한국어)\\n"
            "4) tech_category: 다음 중 1개 선택 (AI, 반도체, 모바일, 클라우드, 네트워크, 기타)\\n"
            "JSON만 반환하세요. 스키마: "
            '{"headline":"...","detail":"...","summary":"...","tech_category":"..."}\\n\\n'
            f"[원문]\\n{text}"
        )

    @staticmethod
    def _parse_json(text: str) -> dict:
        text = text.strip()
        if text.startswith("```"):
            text = text.strip("`")
            text = text.replace("json", "", 1).strip()

        first = text.find("{")
        last = text.rfind("}")
        if first == -1 or last == -1 or first >= last:
            return {}

        payload = text[first : last + 1]
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return {}

    def _ensure_korean(self, text: str) -> str:
        clean = text.strip()
        if not clean:
            return clean
        if HANGUL_PATTERN.search(clean):
            return clean

        translated = self._translate_to_korean(clean)
        return translated or clean

    def _translate_to_korean(self, text: str) -> str:
        if self.translation_cache is not None:
            cached = self.translation_cache.get_translation(text)
            if cached and HANGUL_PATTERN.search(cached):
                return cached

        if self.can_translate:
            prompt = (
                "다음 문장을 자연스러운 한국어 한 문장으로 번역하세요. "
                "설명 없이 번역문만 출력하세요.\\n"
                f"문장: {text}"
            )
            try:
                translated = self._generate_text(prompt)
                if translated and HANGUL_PATTERN.search(translated):
                    if self.translation_cache is not None:
                        self.translation_cache.set_translation(text, translated)
                    return translated
            except Exception as exc:
                if not self._warned_translation_error:
                    self._warned_translation_error = True
                    self.logger.warning(f"Gemini 번역 실패: {self._short_error(exc)}")

        translated = self._translate_with_fallback_translator(text)
        if translated and HANGUL_PATTERN.search(translated):
            if self.translation_cache is not None:
                self.translation_cache.set_translation(text, translated)
            return translated

        return text

    def _translate_with_fallback_translator(self, text: str) -> str:
        if self._fallback_translator is None:
            return text

        try:
            translated = str(self._fallback_translator.translate(text) or "").strip()
            if translated and HANGUL_PATTERN.search(translated):
                if not self._warned_fallback_translator:
                    self._warned_fallback_translator = True
                    self.logger.info("보조 번역기(deep-translator) 폴백 활성화")
                return translated
            return text
        except Exception:
            return text

    @staticmethod
    def _short_error(exc: Exception) -> str:
        message = str(exc).strip()
        if not message:
            return exc.__class__.__name__
        return message.splitlines()[0][:220]

    @staticmethod
    def _fallback_summary(text: str) -> str:
        chunks = [chunk.strip() for chunk in re.split(r"[.!?\n]", text) if chunk.strip()]
        if not chunks:
            return "내용 없음"

        return " / ".join(chunks[:3])[:500]

    @staticmethod
    def _fallback_category(text: str) -> str:
        lowered = text.lower()
        for category, keywords in TECH_CATEGORY_KEYWORDS.items():
            if any(keyword.lower() in lowered for keyword in keywords):
                return category
        return "기타"


class DataProcessor:
    def __init__(self, summarizer: GeminiSummarizer, verbose: bool = True) -> None:
        self.summarizer = summarizer
        self.verbose = verbose
        self.logger = logging.getLogger(self.__class__.__name__)

    def process(self, raw_posts: Sequence[RawPost]) -> list[dict[str, str]]:
        if self.verbose:
            self.logger.info(f"input posts: {len(raw_posts)}")
        deduped = self._deduplicate(raw_posts)
        if self.verbose:
            self.logger.info(f"after dedup: {len(deduped)}")
        results: list[dict[str, str]] = []

        for idx, post in enumerate(deduped, start=1):
            clean_text = self._clean_text(post.text)
            summary = self.summarizer.summarize(clean_text)
            formatted_summary = self._format_briefing(post.source_name, summary)

            results.append(
                {
                    "게시일시": self._to_datetime_text(post.posted_at),
                    "일자": self._to_date(post.posted_at),
                    "이름": self._escape_excel_formula(post.source_name),
                    "주요내용": self._escape_excel_formula(formatted_summary),
                    "출처": self._escape_excel_formula(post.platform),
                    "URL": post.post_url,
                    "구분": self._escape_excel_formula(post.source_category),
                    "기술분류": self._escape_excel_formula(summary.tech_category),
                    "원문(옵션)": self._escape_excel_formula(clean_text),
                }
            )
            if self.verbose and (idx == len(deduped) or idx % 10 == 0):
                self.logger.info(f"processed {idx}/{len(deduped)}")

        return results

    def _deduplicate(self, posts: Sequence[RawPost]) -> list[RawPost]:
        unique_posts: list[RawPost] = []
        seen_urls: set[str] = set()
        seen_hashes: set[str] = set()

        for post in posts:
            if not post.post_url:
                continue
            if post.post_url in seen_urls:
                continue

            cleaned = self._clean_text(post.text)
            if not cleaned:
                continue

            digest = sha256(cleaned.lower().encode("utf-8")).hexdigest()
            if digest in seen_hashes:
                continue

            seen_urls.add(post.post_url)
            seen_hashes.add(digest)
            unique_posts.append(post)

        return unique_posts

    @staticmethod
    def _clean_text(text: str) -> str:
        no_url = URL_PATTERN.sub("", text)
        normalized = WHITESPACE_PATTERN.sub(" ", no_url)
        return normalized.strip()

    @staticmethod
    def _to_date(posted_at: str) -> str:
        if not posted_at:
            return ""

        try:
            dt = datetime.fromisoformat(posted_at.replace("Z", "+00:00"))
            return dt.date().isoformat()
        except ValueError:
            return ""

    @staticmethod
    def _to_datetime_text(posted_at: str) -> str:
        if not posted_at:
            return ""

        try:
            dt = datetime.fromisoformat(posted_at.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return ""

    def _format_briefing(self, source_name: str, summary: SummaryResult) -> str:
        headline = self._normalize_brief_text(summary.headline)
        detail = self._normalize_brief_text(summary.detail)

        if not headline or not detail:
            fallback_headline, fallback_detail = self._extract_headline_and_detail(summary.summary)
            headline = headline or fallback_headline
            detail = detail or fallback_detail

        headline = self._normalize_to_korean(headline)
        detail = self._normalize_to_korean(detail)

        if not HANGUL_PATTERN.search(headline):
            headline = f"원문 요약: {headline}" if headline else "요약 정보 없음"
        if not HANGUL_PATTERN.search(detail):
            detail = f"원문 참고: {detail}" if detail else "원문 링크를 참고하세요."

        source_label = source_name.strip() or "Unknown Source"
        return f"ㅇ {source_label} : {headline}\n - {detail}"

    @staticmethod
    def _extract_headline_and_detail(summary_text: str) -> tuple[str, str]:
        text = DataProcessor._normalize_brief_text(summary_text)
        if not text:
            return "요약 정보 없음", "원문에서 핵심 내용을 추출하지 못했습니다."

        parts = [part.strip(" -") for part in re.split(r"\s*/\s*|[.!?]\s+|\n+", text) if part.strip()]
        if len(parts) >= 2:
            headline = parts[0]
            detail = parts[1]
        elif len(parts) == 1:
            headline = parts[0]
            detail = parts[0]
        else:
            headline = "요약 정보 없음"
            detail = "원문에서 핵심 내용을 추출하지 못했습니다."

        return DataProcessor._trim(headline, 90), DataProcessor._trim(detail, 220)

    @staticmethod
    def _normalize_brief_text(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "")).strip()

    def _normalize_to_korean(self, text: str) -> str:
        normalized_text = self._normalize_brief_text(text)
        if not normalized_text:
            return normalized_text
        if HANGUL_PATTERN.search(normalized_text):
            return normalized_text

        normalizer = getattr(self.summarizer, "normalize_korean", None)
        if callable(normalizer):
            try:
                normalized = normalizer(normalized_text)
                return self._normalize_brief_text(normalized)
            except (TypeError, ValueError, RuntimeError):
                return normalized_text
        return normalized_text

    @staticmethod
    def _trim(text: str, max_len: int) -> str:
        if len(text) <= max_len:
            return text
        return f"{text[: max_len - 3]}..."

    @staticmethod
    def _escape_excel_formula(text: str) -> str:
        value = str(text or "")
        if not value or value.startswith("'"):
            return value

        stripped = value.lstrip()
        if not stripped:
            return value
        if stripped[0] in EXCEL_FORMULA_PREFIXES:
            return f"'{value}"
        return value
