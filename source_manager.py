from pathlib import Path
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

SOURCE_COLUMNS = ["구분", "그룹", "이름", "URL"]

COLUMN_ALIASES = {
    "구분": "구분",
    "category": "구분",
    "Category": "구분",
    "그룹": "그룹",
    "group": "그룹",
    "Group": "그룹",
    "이름": "이름",
    "name": "이름",
    "Name": "이름",
    "URL": "URL",
    "url": "URL",
    "Url": "URL",
}

EXAMPLE_ROWS = [
    {"구분": "기업", "그룹": "AI모델", "이름": "OpenAI", "URL": "https://x.com/OpenAI"},
    {"구분": "기업", "그룹": "AI모델", "이름": "Google AI", "URL": "https://x.com/GoogleAI"},
    {"구분": "기업", "그룹": "AI모델", "이름": "Google DeepMind", "URL": "https://x.com/GoogleDeepMind"},
]


@dataclass(frozen=True)
class Source:
    category: str
    group: str
    name: str
    url: str


def ensure_sources_file(path: str | Path = "sources.xlsx") -> Path:
    source_path = Path(path)
    if source_path.exists():
        return source_path

    source_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(EXAMPLE_ROWS, columns=SOURCE_COLUMNS).to_excel(source_path, index=False)
    return source_path


def _normalize_source_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for column in df.columns:
        if column in COLUMN_ALIASES:
            rename_map[column] = COLUMN_ALIASES[column]

    normalized = df.rename(columns=rename_map)
    missing = [column for column in SOURCE_COLUMNS if column not in normalized.columns]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"sources.xlsx 필수 컬럼 누락: {joined}")

    return normalized[SOURCE_COLUMNS]


def _iter_clean_records(df: pd.DataFrame) -> Iterable[Source]:
    for row in df.to_dict("records"):
        category = str(row.get("구분", "")).strip()
        group = str(row.get("그룹", "")).strip()
        name = str(row.get("이름", "")).strip()
        url = str(row.get("URL", "")).strip()

        if not url:
            continue
        if not url.startswith("http://") and not url.startswith("https://"):
            url = f"https://{url}"

        yield Source(category=category, group=group, name=name, url=url)


def load_sources(path: str | Path = "sources.xlsx") -> list[Source]:
    source_path = Path(path)
    if not source_path.exists():
        raise FileNotFoundError(
            f"Source file not found: {source_path}. 먼저 `python create_sources.py`를 실행하세요."
        )

    df = pd.read_excel(source_path)
    if df.empty:
        raise ValueError("sources.xlsx 파일이 비어 있습니다.")

    normalized_df = _normalize_source_columns(df)
    sources = list(_iter_clean_records(normalized_df))

    if not sources:
        raise ValueError("유효한 소스(URL)가 없습니다. sources.xlsx를 확인하세요.")

    return sources
