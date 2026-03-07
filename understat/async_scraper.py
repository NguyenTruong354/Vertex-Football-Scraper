#!/usr/bin/env python3
# ============================================================
# async_scraper.py – Orchestrator chính của Data‑Ingestion Pipeline
# ============================================================
"""
File chạy chính của pipeline.  Luồng thực thi:

    ┌──────────────────────────────────────────────────────────┐
    │  1. Nhận danh sách Match IDs (CLI hoặc auto-discover)    │
    │  2. Tạo aiohttp.ClientSession + Semaphore                │
    │  3. Async fetch JSON từ Understat API (có retry)         │
    │  4. Parse JSON response (extractor.py)                   │
    │  5. Validate → Pydantic models (schemas.py)              │
    │  6. Flatten → Pandas DataFrame → CSV / list[dict]        │
    └──────────────────────────────────────────────────────────┘

Khái niệm chính:
-----------------
• **asyncio.Semaphore**: Giống cổng xoay ở tàu điện – chỉ cho phép
  tối đa N người (coroutine) đi qua cùng lúc.  Khi 1 coroutine hoàn
  thành (release), slot trống cho coroutine tiếp theo.

      sem = asyncio.Semaphore(6)
      async with sem:        # acquire – chiếm 1 slot
          await do_work()    # … làm việc …
      # release tự động khi ra khỏi `async with`

• **tenacity retry**: Decorator tự động gọi lại hàm khi gặp exception.
  Exponential backoff: chờ 2s → 4s → 8s → … (tối đa 60s).

• **aiohttp.ClientSession**: Quản lý connection pool, cookie, header
  cho toàn bộ requests.  Tái sử dụng TCP connection → nhanh hơn nhiều
  so với tạo mới mỗi lần.

Understat API Endpoints:
  • GET /getMatchData/{match_id}    → {rosters, shots, tmpl}
  • GET /getLeagueData/{league}/{season} → {teams, players, dates}

Lưu ý: API trả content-type: text/javascript, nên phải dùng
response.text() rồi json.loads() thay vì response.json().
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any

import aiohttp
import numpy as np
import pandas as pd
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

# Internal modules
import config
from extractor import (
    build_match_info_from_api,
    flatten_league_match_entry,
    flatten_rosters,
    flatten_shots,
    parse_league_api_response,
    parse_match_api_response,
)
from schemas import (
    LeagueMatchEntry,
    MatchInfo,
    PlayerMatchStats,
    ShotData,
    safe_parse_list,
)

# League registry (project root)
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
from services.league_registry import get_league, resolve_understat_name, list_leagues, get_understat_leagues  # noqa: E402

# ────────────────────────────────────────────────────────────
# LOGGING SETUP
# ────────────────────────────────────────────────────────────

def _setup_logging() -> logging.Logger:
    """Cấu hình logging với colorlog (fallback nếu không cài)."""
    root_logger = logging.getLogger()
    # Tránh duplicate handlers nếu module được import nhiều lần
    if root_logger.handlers:
        return logging.getLogger(__name__)

    root_logger.setLevel(config.LOG_LEVEL)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(config.LOG_LEVEL)

    try:
        import colorlog
        formatter = colorlog.ColoredFormatter(
            config.LOG_FORMAT,
            datefmt=config.LOG_DATE_FORMAT,
        )
    except ImportError:
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt=config.LOG_DATE_FORMAT,
        )

    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    return logging.getLogger(__name__)


logger = _setup_logging()


# ────────────────────────────────────────────────────────────
# CUSTOM EXCEPTIONS
# ────────────────────────────────────────────────────────────

class RetryableHTTPError(Exception):
    """Ngoại lệ cho HTTP status codes có thể retry (429, 5xx)."""
    def __init__(self, status: int, url: str):
        self.status = status
        self.url = url
        super().__init__(f"HTTP {status} for {url}")


class NonRetryableHTTPError(Exception):
    """Ngoại lệ cho HTTP status codes không nên retry (403, 404…)."""
    def __init__(self, status: int, url: str):
        self.status = status
        self.url = url
        super().__init__(f"HTTP {status} for {url}")


# ────────────────────────────────────────────────────────────
# ASYNC HTTP FETCHER (với retry bằng tenacity)
# ────────────────────────────────────────────────────────────

# Decorator tenacity:
#   • retry nếu gặp RetryableHTTPError hoặc aiohttp connection errors
#   • tối đa RETRY_MAX_ATTEMPTS lần
#   • exponential wait: min=2s, max=60s, multiplier=2
#
# Cách hoạt động:
#   Lần 1 fail → chờ 2s → Lần 2 fail → chờ 4s → Lần 3 → 8s → …
#
# before_sleep_log: log mỗi lần retry để theo dõi

@retry(
    retry=retry_if_exception_type((RetryableHTTPError, aiohttp.ClientError)),
    stop=stop_after_attempt(config.RETRY_MAX_ATTEMPTS),
    wait=wait_exponential(
        multiplier=config.RETRY_MULTIPLIER,
        min=config.RETRY_WAIT_MIN_SECONDS,
        max=config.RETRY_WAIT_MAX_SECONDS,
    ),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
async def _fetch_url(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    *,
    referer: str | None = None,
) -> str:
    """
    Tải nội dung text của một URL với rate-limit (Semaphore) và auto-retry.

    Semaphore flow:
    ─────────────────────────────────────────────────────────────
    1. ``async with semaphore`` — acquire 1 slot.
       Nếu đã có N coroutine đang chạy (N = MAX_CONCURRENT_REQUESTS),
       coroutine này sẽ **block** ở đây cho đến khi có slot trống.

    2. Politeness delay — sleep ngắn giữa các request.

    3. Gửi HTTP GET request:
       • 200 → trả text content.
       • 429/5xx → raise RetryableHTTPError → tenacity tự retry.
       • 403/404… → raise NonRetryableHTTPError → bỏ qua match.

    4. Khi ra khỏi ``async with semaphore``, slot tự động release
       cho coroutine khác đang chờ.
    ─────────────────────────────────────────────────────────────

    Args:
        session:   aiohttp.ClientSession đã khởi tạo (shared).
        url:       URL cần fetch.
        semaphore: asyncio.Semaphore giới hạn concurrency.
        referer:   Optional Referer header (một số API cần).
    """
    async with semaphore:
        # Politeness delay – giảm tải cho server nguồn
        await asyncio.sleep(config.POLITENESS_DELAY_SECONDS)

        timeout = aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT_SECONDS)
        logger.debug("Fetching: %s", url)

        # Thêm Referer header nếu được cung cấp
        extra_headers: dict[str, str] = {}
        if referer:
            extra_headers["Referer"] = referer

        async with session.get(url, timeout=timeout, headers=extra_headers) as response:
            status = response.status

            if status == 200:
                text = await response.text()
                logger.info("  OK [%d bytes]: %s", len(text), url)
                return text

            if status in config.RETRYABLE_HTTP_STATUSES:
                logger.warning("  HTTP %d (retryable): %s", status, url)
                raise RetryableHTTPError(status, url)

            # Non-retryable (403, 404, …)
            logger.error("  HTTP %d (non-retryable): %s", status, url)
            raise NonRetryableHTTPError(status, url)


# ────────────────────────────────────────────────────────────
# STEP 1: Discover Match IDs từ League API
# ────────────────────────────────────────────────────────────

async def discover_match_ids(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    league: str = config.DEFAULT_LEAGUE,
    season: str = config.DEFAULT_SEASON,
) -> list[int]:
    """
    Gọi ``/getLeagueData/{league}/{season}`` và trả về
    danh sách match_id đã diễn ra (``isResult == True``).

    league có thể là league_id (VD: "EPL", "LALIGA") hoặc
    tên Understat gốc (VD: "La_liga").  Registry sẽ resolve tự động.
    """
    # Resolve league_id → Understat API name
    try:
        understat_name = resolve_understat_name(league)
    except (KeyError, ValueError):
        # Fallback: dùng trực tiếp (backward-compatible)
        understat_name = league

    api_url = config.LEAGUE_API_URL.format(league_name=understat_name, season=season)
    referer = config.LEAGUE_PAGE_URL.format(league_name=understat_name, season=season)
    logger.info("▶ Đang gọi League API: %s", api_url)

    try:
        raw_text = await _fetch_url(session, api_url, semaphore, referer=referer)
    except (RetryableHTTPError, NonRetryableHTTPError, aiohttp.ClientError) as exc:
        logger.error("Không thể gọi League API: %s", exc)
        return []

    data = parse_league_api_response(raw_text)
    if data is None:
        logger.error("Không parse được League API response.")
        return []

    # "dates" chứa danh sách tất cả trận đấu trong mùa giải
    raw_dates = data.get("dates", [])
    if not raw_dates:
        logger.warning("League API trả dates rỗng.")
        return []

    # Flatten từng entry và validate qua Pydantic
    flat_entries = [flatten_league_match_entry(e) for e in raw_dates]
    validated = safe_parse_list(
        LeagueMatchEntry, flat_entries,
        context_label=f"league_{league}_{season}",
    )

    # Chỉ lấy trận đã có kết quả
    match_ids = [entry.id for entry in validated if entry.is_result]
    logger.info(
        "✓ Tìm thấy %d trận đã diễn ra (tổng %d trận) trong mùa %s/%s.",
        len(match_ids), len(raw_dates), season, league,
    )
    return match_ids


# ────────────────────────────────────────────────────────────
# STEP 2-3: Fetch + Parse + Validate một trận đấu
# ────────────────────────────────────────────────────────────

async def process_single_match(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    match_id: int,
) -> dict[str, Any]:
    """
    Pipeline hoàn chỉnh cho **một** trận đấu:

    1. Gọi ``/getMatchData/{match_id}`` → JSON
    2. Parse JSON → shots, rosters dicts
    3. Flatten nested structures
    4. Validate qua Pydantic models
    5. Trả dict chứa dữ liệu đã clean

    Returns:
        {
            "match_id":   int,
            "match_info": MatchInfo | None,
            "shots":      list[dict],       # flat, validated
            "rosters":    list[dict],       # flat, validated
            "errors":     list[str],
        }
    """
    api_url = config.MATCH_API_URL.format(match_id=match_id)
    referer = config.MATCH_PAGE_URL.format(match_id=match_id)

    result: dict[str, Any] = {
        "match_id": match_id,
        "match_info": None,
        "shots": [],
        "rosters": [],
        "errors": [],
    }

    # ── Fetch JSON từ API ──
    try:
        raw_text = await _fetch_url(session, api_url, semaphore, referer=referer)
    except (RetryableHTTPError, NonRetryableHTTPError, aiohttp.ClientError) as exc:
        msg = f"Fetch failed for match {match_id}: {exc}"
        logger.error(msg)
        result["errors"].append(msg)
        return result

    # ── Parse JSON response ──
    api_data = parse_match_api_response(raw_text)
    if api_data is None:
        msg = f"JSON parse failed for match {match_id}"
        logger.error(msg)
        result["errors"].append(msg)
        return result

    shots_data = api_data.get("shots", {})
    rosters_data = api_data.get("rosters", {})

    # ── Build Match Info từ shots data ──
    if shots_data:
        try:
            info_dict = build_match_info_from_api(match_id, shots_data)
            result["match_info"] = MatchInfo.model_validate(info_dict)
        except Exception as exc:
            logger.warning("Match %d – match_info validation error: %s", match_id, exc)
            result["errors"].append(f"match_info validation: {exc}")

    # ── Process Shots ──
    if shots_data:
        flat_shots = flatten_shots(shots_data, match_id)
        validated_shots = safe_parse_list(
            ShotData, flat_shots, context_label=f"match_{match_id}_shots",
        )
        result["shots"] = [s.model_dump(by_alias=False) for s in validated_shots]
        logger.info("  Match %d – %d shots validated.", match_id, len(result["shots"]))
    else:
        logger.warning("  Match %d – Không có shots data.", match_id)

    # ── Process Rosters (xA, pass network) ──
    if rosters_data:
        flat_rosters = flatten_rosters(rosters_data, match_id)
        validated_rosters = safe_parse_list(
            PlayerMatchStats, flat_rosters,
            context_label=f"match_{match_id}_rosters",
        )
        result["rosters"] = [r.model_dump(by_alias=False) for r in validated_rosters]
        logger.info("  Match %d – %d player stats validated.", match_id, len(result["rosters"]))
    else:
        logger.warning("  Match %d – Không có rosters data.", match_id)

    return result


# ────────────────────────────────────────────────────────────
# STEP 4: Batch processing – nhiều trận đấu song song
# ────────────────────────────────────────────────────────────

async def process_matches_batch(
    match_ids: list[int],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Điều phối xử lý hàng loạt trận đấu.

    1. Tạo ``aiohttp.ClientSession`` duy nhất (tái sử dụng TCP connections).
    2. Tạo ``asyncio.Semaphore(N)`` để giới hạn concurrency.
    3. Tạo N async tasks bằng ``asyncio.gather()``.
    4. Thu thập kết quả, gộp thành 3 DataFrame lớn.

    Semaphore đảm bảo:
    ─────────────────────────────────────────────────────────────
    Dù có 380 trận trong mùa giải, chỉ có tối đa 6 request chạy
    đồng thời.  Khi 1 request hoàn thành → release slot → request
    tiếp theo được phép bắt đầu.

        Task 1 ──[fetch]──[done]
        Task 2 ──[fetch]────────[done]
        Task 3 ──[fetch]──[done]
        Task 4 ──────[wait]──[fetch]──[done]   ← chờ slot trống
        ...
    ─────────────────────────────────────────────────────────────

    Returns:
        (df_shots, df_rosters, df_match_infos) – 3 DataFrames.
    """
    semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)

    all_shots: list[dict] = []
    all_rosters: list[dict] = []
    all_match_infos: list[dict] = []
    error_summary: list[str] = []
    completed_count = 0
    total = len(match_ids)

    async def _process_with_progress(
        session: aiohttp.ClientSession,
        mid: int,
    ) -> dict[str, Any]:
        """Wrap process_single_match with progress logging."""
        nonlocal completed_count
        result = await process_single_match(session, semaphore, mid)
        completed_count += 1
        if completed_count % 10 == 0 or completed_count == total:
            logger.info("  ⏳ Progress: %d / %d matches (%.0f%%)",
                        completed_count, total, completed_count / total * 100)
        return result

    async with aiohttp.ClientSession(headers=config.HEADERS) as session:
        # Tạo coroutine tasks – mỗi task xử lý 1 trận
        tasks = [
            _process_with_progress(session, mid)
            for mid in match_ids
        ]

        # asyncio.gather chạy tất cả tasks "đồng thời" (giới hạn bởi Semaphore)
        # return_exceptions=True → không crash nếu 1 task lỗi
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # ── Gom kết quả ──
    for res in results:
        if isinstance(res, Exception):
            msg = f"Unhandled exception: {res}"
            logger.error(msg)
            error_summary.append(msg)
            continue

        if res.get("errors"):
            error_summary.extend(res["errors"])

        if res.get("match_info"):
            all_match_infos.append(res["match_info"].model_dump(by_alias=False))

        all_shots.extend(res.get("shots", []))
        all_rosters.extend(res.get("rosters", []))

    # ── Tạo DataFrames ──
    df_shots = pd.DataFrame(all_shots) if all_shots else pd.DataFrame()
    df_rosters = pd.DataFrame(all_rosters) if all_rosters else pd.DataFrame()
    df_match_infos = pd.DataFrame(all_match_infos) if all_match_infos else pd.DataFrame()

    # ── Logging tóm tắt ──
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info("  Trận đã xử lý   : %d / %d", len(all_match_infos), len(match_ids))
    logger.info("  Tổng shots       : %d", len(df_shots))
    logger.info("  Tổng player stats: %d", len(df_rosters))
    if error_summary:
        logger.warning("  Errors           : %d", len(error_summary))
        for err in error_summary[:10]:
            logger.warning("    → %s", err)
    logger.info("=" * 60)

    return df_shots, df_rosters, df_match_infos


# ────────────────────────────────────────────────────────────
# STEP 5: Data Quality Report
# ────────────────────────────────────────────────────────────

def log_data_quality_report(
    df_shots: pd.DataFrame,
    df_rosters: pd.DataFrame,
    df_match_infos: pd.DataFrame,
) -> None:
    """In báo cáo chất lượng dữ liệu nhanh vào log."""
    logger.info("── DATA QUALITY REPORT ──")

    if not df_shots.empty:
        null_pct = df_shots.isnull().mean() * 100
        cols_with_nulls = null_pct[null_pct > 0]
        if not cols_with_nulls.empty:
            logger.info("  Shots – columns with nulls:")
            for col, pct in cols_with_nulls.items():
                logger.info("    %-20s %.1f%% null", col, pct)
        else:
            logger.info("  Shots – no null values ✓")

        # Range checks cho tọa độ và xG
        if "x" in df_shots.columns:
            oob = ((df_shots["x"] < 0) | (df_shots["x"] > 1)).sum()
            if oob > 0:
                logger.warning("  Shots – %d rows with X out of [0,1]", oob)
        if "xg" in df_shots.columns:
            neg = (df_shots["xg"] < 0).sum()
            if neg > 0:
                logger.warning("  Shots – %d rows with negative xG", neg)
            logger.info("  Shots – xG range: [%.4f, %.4f], mean=%.4f",
                        df_shots["xg"].min(), df_shots["xg"].max(), df_shots["xg"].mean())

    if not df_rosters.empty:
        if "xg" in df_rosters.columns and "xa" in df_rosters.columns:
            logger.info("  Player stats – xG range: [%.4f, %.4f]",
                        df_rosters["xg"].min(), df_rosters["xg"].max())
            logger.info("  Player stats – xA range: [%.4f, %.4f]",
                        df_rosters["xa"].min(), df_rosters["xa"].max())

    if not df_match_infos.empty:
        logger.info("  Matches – %d unique teams: %s",
                     df_match_infos["h_team"].nunique(),
                     ", ".join(sorted(set(df_match_infos["h_team"].unique()) |
                                      set(df_match_infos["a_team"].unique()))))

    logger.info("── END QUALITY REPORT ──")


# ────────────────────────────────────────────────────────────
# STEP 6: Export
# ────────────────────────────────────────────────────────────

def export_to_csv(
    df_shots: pd.DataFrame,
    df_rosters: pd.DataFrame,
    df_match_infos: pd.DataFrame | None = None,
    *,
    league_id: str = "EPL",
) -> list[Path]:
    """
    Xuất DataFrames ra file CSV.

    Output directory:  output/understat/{league_id}/
    Filenames:         dataset_{league_id}_xg.csv, …

    Args:
        league_id: League identifier (VD: "EPL", "LALIGA")

    Returns:
        Danh sách đường dẫn file đã tạo.
    """
    output_dir = config.get_output_dir(league_id)
    csv_names = config.get_csv_filenames(league_id)
    created_files: list[Path] = []

    if not df_shots.empty:
        path = output_dir / csv_names["shots"]
        df_shots.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported shots → %s (%d rows)", path, len(df_shots))
        created_files.append(path)

    if not df_rosters.empty:
        path = output_dir / csv_names["player_stats"]
        df_rosters.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported player stats → %s (%d rows)", path, len(df_rosters))
        created_files.append(path)

    if df_match_infos is not None and not df_match_infos.empty:
        path = output_dir / csv_names["match_stats"]
        df_match_infos.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported match stats → %s (%d rows)", path, len(df_match_infos))
        created_files.append(path)

    return created_files


def to_list_of_dicts(df: pd.DataFrame) -> list[dict]:
    """
    Chuyển DataFrame thành list[dict] – sẵn sàng chèn vào Database
    (PostgreSQL, MongoDB, …) hoặc serialize thành JSON cho API.
    """
    if df.empty:
        return []
    return df.to_dict(orient="records")


# ────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ────────────────────────────────────────────────────────────

async def main(
    match_ids: list[int] | None = None,
    league: str = config.DEFAULT_LEAGUE,
    season: str = config.DEFAULT_SEASON,
    export_csv: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Entry point chính của pipeline.

    Args:
        match_ids: Danh sách Understat match IDs.  Nếu None, tự discover
                   từ League API.
        league:    League ID (VD: "EPL", "LALIGA", "BUNDESLIGA").
                   Sẽ tự resolve sang tên Understat qua league_registry.
        season:    Mùa giải (mặc định "2025").
        export_csv: Có xuất CSV không (mặc định True).

    Returns:
        (df_shots, df_rosters)
    """
    t_start = time.perf_counter()

    # Resolve league info
    try:
        league_cfg = get_league(league)
        display_name = league_cfg.display_name
    except KeyError:
        display_name = league

    logger.info("Vertex Football Scraper – Pipeline bắt đầu")
    logger.info("   League: %s (%s) | Season: %s", league, display_name, season)

    # ── Auto-discover match IDs nếu chưa cung cấp ──
    if not match_ids:
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)
        async with aiohttp.ClientSession(headers=config.HEADERS) as session:
            match_ids = await discover_match_ids(session, semaphore, league, season)

        if not match_ids:
            logger.error("Không tìm thấy trận đấu nào. Kết thúc pipeline.")
            empty = pd.DataFrame()
            return empty, empty

    logger.info("Xử lý %d trận đấu: %s…", len(match_ids), match_ids[:5])

    # ── Chạy pipeline chính ──
    df_shots, df_rosters, df_match_infos = await process_matches_batch(match_ids)

    # ── Data quality report ──
    log_data_quality_report(df_shots, df_rosters, df_match_infos)

    # ── Export ──
    if export_csv:
        export_to_csv(df_shots, df_rosters, df_match_infos, league_id=league)

    elapsed = time.perf_counter() - t_start
    logger.info("Pipeline hoàn thành trong %.1f giây.", elapsed)

    return df_shots, df_rosters


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────

def cli() -> None:
    """
    Chạy pipeline từ command line.

    Cách dùng:
        # Auto-discover toàn bộ mùa giải (EPL 2025)
        python async_scraper.py

        # Chỉ scrape một số trận cụ thể
        python async_scraper.py 28778 28779 28780

        # Đổi giải / mùa
        python async_scraper.py --league LALIGA --season 2024

        # Giới hạn số trận (lấy 20 trận gần nhất)
        python async_scraper.py --limit 20

        # Liệt kê giải đấu hỗ trợ
        python async_scraper.py --list-leagues
    """
    import argparse

    # Build danh sách giải đấu hỗ trợ cho help text
    supported = [lg.league_id for lg in get_understat_leagues()]

    parser = argparse.ArgumentParser(
        description="Vertex Football Scraper – Understat xG Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Giải đấu hỗ trợ (Understat): {', '.join(supported)}

Ví dụ:
  python async_scraper.py                                    # EPL mùa hiện tại
  python async_scraper.py --league LALIGA --season 2024      # La Liga 2024
  python async_scraper.py --league BUNDESLIGA --limit 10     # Bundesliga 10 trận
  python async_scraper.py --list-leagues                     # Xem tất cả giải
        """,
    )
    parser.add_argument(
        "match_ids",
        nargs="*",
        type=int,
        help="Understat match IDs (nếu bỏ trống → auto-discover).",
    )
    parser.add_argument(
        "--league", default=config.DEFAULT_LEAGUE,
        help=f"League ID (default: {config.DEFAULT_LEAGUE}). Choices: {', '.join(supported)}",
    )
    parser.add_argument("--season", default=config.DEFAULT_SEASON, help="Mùa giải (default: 2025)")
    parser.add_argument("--limit", type=int, default=0, help="Giới hạn số trận (0 = không giới hạn)")
    parser.add_argument("--no-csv", action="store_true", help="Không xuất CSV")
    parser.add_argument("--list-leagues", action="store_true", help="Liệt kê tất cả giải đấu hỗ trợ")
    args = parser.parse_args()

    # ── --list-leagues ──
    if args.list_leagues:
        print(list_leagues())
        return

    # ── Validate league ──
    league = args.league.upper()
    try:
        league_cfg = get_league(league)
        if not league_cfg.has_understat:
            logger.error("League '%s' không có trên Understat. Dùng --list-leagues để xem.", league)
            return
        # Use season from registry if user didn't override
        season = args.season if args.season != config.DEFAULT_SEASON else league_cfg.understat_season
    except KeyError:
        logger.warning("League '%s' không có trong registry, thử trực tiếp…", league)
        season = args.season

    ids = args.match_ids or None

    async def _run() -> None:
        nonlocal ids
        if ids is None and args.limit > 0:
            # Discover trước, rồi cắt
            sem = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)
            async with aiohttp.ClientSession(headers=config.HEADERS) as session:
                ids = await discover_match_ids(session, sem, league, season)
            ids = ids[-args.limit:]  # lấy N trận gần nhất
            logger.info("Giới hạn scrape: %d trận gần nhất.", args.limit)

        await main(
            match_ids=ids,
            league=league,
            season=season,
            export_csv=not args.no_csv,
        )

    asyncio.run(_run())


if __name__ == "__main__":
    cli()
