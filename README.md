# AI-ICT Trend Sensing

SNS(X, Instagram, Facebook)에서 최근 게시글을 수집하고, 요약/분류 후 엑셀로 저장하는 파이프라인입니다.

## 1. 주요 기능
- 소스 목록(`sources.xlsx`) 기반 다중 플랫폼 수집
- 시간 범위(lookback), 병렬 워커, 재시도, 조기 중단 조건 지원
- Gemini 기반 요약/번역(옵션), 실패 시 규칙 기반 폴백
- URL/요약/번역 캐시(SQLite)로 중복 처리 및 속도 개선
- 엑셀 저장 시 정렬/스타일 적용, 수식 인젝션 방어

## 2. 요구 사항
- Python 3.11+
- Playwright Chromium 브라우저
- Windows PowerShell 기준 예시 제공

## 3. 설치
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install
```

## 4. 환경 변수 (`.env`)
필수는 아니지만 권장:

```env
# Gemini
GEMINI_API_KEY=
GEMINI_MODEL=gemini-2.0-flash

# 기본 동작
SEARCH_KEYWORDS=ai,chip,cloud
LOOKBACK_HOURS=24
COLLECT_WORKERS=3

# 캐시
CACHE_DB_PATH=pipeline_cache.sqlite3
CACHE_WINDOW_HOURS=168
CACHE_MAX_URLS=200000

# 수집 재시도/중단
NO_GROWTH_BREAK_LIMIT=2
OLD_POST_BREAK_LIMIT=8
COLLECTOR_RETRIES=2
COLLECTOR_RETRY_BASE_MS=800
INSTAGRAM_CANDIDATE_MULTIPLIER=4
```

## 5. 소스 파일 준비
기본 `sources.xlsx` 자동 생성:
```powershell
python main.py --create-sources
```

또는:
```powershell
python create_sources.py
```

`sources.xlsx` 컬럼:
- `구분`, `그룹`, `이름`, `URL`

## 6. 실행
기본 실행:
```powershell
python main.py
```

자주 쓰는 옵션 예시:
```powershell
python main.py `
  --sources sources.xlsx `
  --output SNS_News_Collection.xlsx `
  --sheet News_Data `
  --workers 4 `
  --lookback-hours 24 `
  --keywords "ai,반도체,cloud"
```

AI 요약 비활성:
```powershell
python main.py --no-ai
```

캐시 비활성:
```powershell
python main.py --no-cache
```

헤드풀 브라우저 모드:
```powershell
python main.py --headful
```

## 7. 품질 점검
전체 점검:
```powershell
python tools\quality_check.py
```

lint 포함(ruff 설치 시):
```powershell
python tools\quality_check.py --lint
```

의존성 체크 포함:
```powershell
python tools\quality_check.py --env-check
```

직접 테스트:
```powershell
python -m pytest -q
```

## 8. 다른 PC에서 사용
```powershell
git clone https://github.com/magmacy/AI-ICT_Trend_Sensing.git
cd AI-ICT_Trend_Sensing
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install
```

그 다음, 원본 PC의 `.env`를 새 PC에 복사해서 사용하세요.

## 9. 주의 사항
- `.env`, 캐시 DB, 결과 엑셀은 보통 Git에 커밋하지 않습니다.
- 플랫폼 DOM 변경 시 `selector_table.py`의 셀렉터 업데이트가 필요할 수 있습니다.
