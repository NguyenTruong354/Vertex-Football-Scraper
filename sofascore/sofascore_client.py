#!/usr/bin/env python3
# ============================================================
# sofascore_client.py – SofaScore Heatmap & Touch Map Pipeline
# ============================================================
"""
Thu thập heatmap / touch map từ SofaScore JSON API.

Dùng nodriver (undetected Chrome) để bypass Cloudflare protection.
SofaScore API đã chặn raw HTTP requests → phải dùng browser.

Dữ liệu thu thập:
  1. Heatmap Points   — tọa độ (x, y) mỗi chạm bóng / di chuyển
  2. Average Position — vị trí trung bình cầu thủ trên sân
  3. Match Events     — danh sách trận đấu của giải

Luồng thực thi:
  1. Lấy danh sách trận đấu đã kết thúc của giải (tournament events)
  2. Với mỗi trận → lấy lineups → biết player IDs
  3. Với mỗi cầu thủ → fetch heatmap points
  4. Export ra CSV

Hỗ trợ multi-league:
  python sofascore_client.py                          # EPL
  python sofascore_client.py --league LALIGA          # La Liga
  python sofascore_client.py --league BUNDESLIGA      # Bundesliga
  python sofascore_client.py --list-leagues           # Xem tất cả

Rate limiting: 2s giữa mỗi API request
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import nodriver as uc
import pandas as pd

import config_sofascore as cfg
from config_sofascore import SSLeagueConfig, get_ss_config
from schemas_sofascore import (
    HeatmapPoint,
    MatchEvent,
    PlayerAvgPosition,
    PlayerHeatmap,
    safe_parse_list,
)

# League registry (project root)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from league_registry import get_league, list_leagues  # noqa: E402


# ────────────────────────────────────────────────────────────
# LOGGING
# ────────────────────────────────────────────────────────────
def _setup_logging() -> logging.Logger:
    root = logging.getLogger()
    if root.handlers:
        return logging.getLogger(__name__)
    root.setLevel(cfg.LOG_LEVEL)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(cfg.LOG_LEVEL)
    try:
        import colorlog
        fmt = colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s | %(levelname)-8s | %(name)s | %(message)s%(reset)s",
            datefmt="%H:%M:%S",
        )
    except ImportError:
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%H:%M:%S",
        )
    handler.setFormatter(fmt)
    root.addHandler(handler)
    return logging.getLogger(__name__)


logger = _setup_logging()


# ────────────────────────────────────────────────────────────
# HTTP CLIENT
# ────────────────────────────────────────────────────────────

class SofaScoreClient:
    """
    Async client cho SofaScore API, dùng nodriver (undetected Chrome)
    để bypass Cloudflare protection.

    Lifecycle:
        async with SofaScoreClient() as client:
            data = await client.get_json("/event/12345/heatmap/67890")
    """

    def __init__(self) -> None:
        self._browser: uc.Browser | None = None
        self._tab = None
        self._last_request_time: float = 0.0

    async def __aenter__(self) -> "SofaScoreClient":
        self._browser = await uc.start()
        # Warm up: navigate to SofaScore homepage first to get cookies/CF clearance
        self._tab = await self._browser.get("https://www.sofascore.com/")
        await asyncio.sleep(3)
        logger.info("▶ SofaScore browser client khởi tạo (nodriver)")
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._browser:
            try:
                self._browser.stop()
            except Exception:
                pass
            logger.info("✓ SofaScore browser client đóng")

    async def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < cfg.SS_DELAY_BETWEEN_REQUESTS:
            wait = cfg.SS_DELAY_BETWEEN_REQUESTS - elapsed
            await asyncio.sleep(wait)

    async def get_json(
        self,
        endpoint: str,
        *,
        retries: int = cfg.SS_MAX_RETRIES,
    ) -> dict | list | None:
        """
        GET request tới SofaScore API → JSON (via nodriver browser).

        Args:
            endpoint: Path sau API base (VD: "/event/12345/...")
            retries: Số lần retry khi lỗi

        Returns:
            JSON response dict/list, hoặc None nếu lỗi
        """
        url = f"{cfg.SS_API_BASE}{endpoint}"
        await self._rate_limit()

        for attempt in range(1, retries + 1):
            try:
                logger.debug("  📡 GET %s (attempt %d)", url, attempt)
                self._tab = await self._browser.get(url)
                self._last_request_time = time.monotonic()
                await asyncio.sleep(1.5)  # Wait for page to load

                # Extract JSON from <pre> tag (API returns JSON wrapped in HTML)
                content = await self._tab.get_content()

                # Try to extract JSON from <pre> tag
                json_str = None
                if "<pre" in content:
                    import re
                    m = re.search(r"<pre[^>]*>(.*?)</pre>", content, re.DOTALL)
                    if m:
                        json_str = m.group(1).strip()
                elif content.strip().startswith("{") or content.strip().startswith("["):
                    json_str = content.strip()

                if not json_str:
                    # Try JS extraction
                    try:
                        json_str = await self._tab.evaluate("document.body.innerText")
                    except Exception:
                        pass

                if json_str:
                    # Unescape HTML entities
                    json_str = (
                        json_str
                        .replace("&amp;", "&")
                        .replace("&lt;", "<")
                        .replace("&gt;", ">")
                        .replace("&quot;", '"')
                    )
                    data = json.loads(json_str)

                    # Check for API error
                    if isinstance(data, dict) and data.get("error"):
                        status = data["error"].get("code", 0)
                        if status == 404:
                            logger.debug("  404: %s", url)
                            return None
                        elif status == 429:
                            wait = cfg.SS_DELAY_BETWEEN_REQUESTS * (2 ** attempt)
                            logger.warning("  429 Rate limited — chờ %.1fs", wait)
                            await asyncio.sleep(wait)
                            continue
                        else:
                            logger.warning("  API error %d: %s", status, url)
                            if attempt < retries:
                                await asyncio.sleep(cfg.SS_DELAY_BETWEEN_REQUESTS)
                            continue

                    return data
                else:
                    logger.warning("  No JSON found in response: %s", url)
                    if attempt < retries:
                        await asyncio.sleep(cfg.SS_DELAY_BETWEEN_REQUESTS * 2)

            except json.JSONDecodeError as exc:
                logger.warning("  JSON parse error: %s – %s", url, exc)
                if attempt < retries:
                    await asyncio.sleep(cfg.SS_DELAY_BETWEEN_REQUESTS)
            except Exception as exc:
                logger.warning("  Request error: %s – %s", url, exc)
                if attempt < retries:
                    await asyncio.sleep(cfg.SS_DELAY_BETWEEN_REQUESTS)

        logger.error("  ✗ Failed after %d attempts: %s", retries, url)
        return None


# ────────────────────────────────────────────────────────────
# STEP 1: Get Tournament Events (Finished Matches)
# ────────────────────────────────────────────────────────────

async def fetch_tournament_events(
    client: SofaScoreClient,
    league_cfg: SSLeagueConfig,
    *,
    match_limit: int = 0,
) -> list[dict]:
    """
    Lấy danh sách trận đấu đã kết thúc của giải.

    API: /unique-tournament/{id}/season/{season_id}/events/last/{page}
    """
    events: list[dict] = []
    page = 0
    max_pages = 50  # Safety limit

    logger.info("━━ Fetching tournament events: tournament=%d, season=%d ━━",
                league_cfg.tournament_id, league_cfg.season_id)

    while page < max_pages:
        endpoint = (
            f"/unique-tournament/{league_cfg.tournament_id}"
            f"/season/{league_cfg.season_id}"
            f"/events/last/{page}"
        )
        data = await client.get_json(endpoint)
        if not data:
            break

        page_events = data.get("events", [])
        if not page_events:
            break

        for ev in page_events:
            status_type = ev.get("status", {}).get("type", "")
            if status_type != "finished":
                continue

            home = ev.get("homeTeam", {})
            away = ev.get("awayTeam", {})
            home_score_data = ev.get("homeScore", {})
            away_score_data = ev.get("awayScore", {})

            # Parse timestamp
            ts = ev.get("startTimestamp")
            match_date = None
            if ts:
                try:
                    match_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                except (OSError, ValueError):
                    pass

            event_dict = {
                "event_id": ev.get("id"),
                "tournament_id": league_cfg.tournament_id,
                "season_id": league_cfg.season_id,
                "round_num": ev.get("roundInfo", {}).get("round"),
                "home_team": home.get("name"),
                "home_team_id": home.get("id"),
                "away_team": away.get("name"),
                "away_team_id": away.get("id"),
                "home_score": home_score_data.get("current"),
                "away_score": away_score_data.get("current"),
                "status": status_type,
                "start_timestamp": ts,
                "match_date": match_date,
                "slug": ev.get("slug"),
                "league_id": league_cfg.league_id,
            }
            events.append(event_dict)

        has_next = data.get("hasNextPage", False)
        if not has_next:
            break
        page += 1

    # Sort by date
    events.sort(key=lambda e: e.get("match_date") or "")

    # Apply limit
    if match_limit > 0:
        events = events[:match_limit]

    logger.info("  ✓ %d finished events found", len(events))
    return events


# ────────────────────────────────────────────────────────────
# STEP 2: Get Match Lineups
# ────────────────────────────────────────────────────────────

async def fetch_match_lineups(
    client: SofaScoreClient,
    event_id: int,
) -> dict:
    """
    Lấy lineups cho một trận đấu.

    API: /event/{event_id}/lineups
    Returns: {"home": [...players], "away": [...players]}
    """
    data = await client.get_json(f"/event/{event_id}/lineups")
    if not data:
        return {"home": [], "away": []}

    result: dict[str, list] = {"home": [], "away": []}

    for side in ("home", "away"):
        lineup_data = data.get(side, {})
        players_raw = lineup_data.get("players", [])

        for p in players_raw:
            player_info = p.get("player", {})
            stats = p.get("statistics", {})

            player_dict = {
                "player_id": player_info.get("id"),
                "player_name": player_info.get("name") or player_info.get("shortName"),
                "position": player_info.get("position"),
                "jersey_number": player_info.get("jerseyNumber"),
                "avg_x": stats.get("averageX"),
                "avg_y": stats.get("averageY"),
                "minutes_played": stats.get("minutesPlayed"),
                "rating": stats.get("rating"),
                "substitute": p.get("substitute", False),
            }
            result[side].append(player_dict)

    return result


# ────────────────────────────────────────────────────────────
# STEP 3: Get Player Heatmap
# ────────────────────────────────────────────────────────────

async def fetch_player_heatmap(
    client: SofaScoreClient,
    event_id: int,
    player_id: int,
) -> list[dict]:
    """
    Lấy heatmap points cho một cầu thủ trong một trận.

    API: /event/{event_id}/heatmap/{player_id}
    Returns: list of {x, y, value} dicts
    """
    # Endpoint mới: /event/{id}/player/{pid}/heatmap (cũ: /event/{id}/heatmap/{pid} → 404)
    data = await client.get_json(f"/event/{event_id}/player/{player_id}/heatmap")
    if not data:
        return []

    # SofaScore returns {"heatmap": [{"x": ..., "y": ...}, ...]}
    # or directly a list
    if isinstance(data, dict):
        points = data.get("heatmap", [])
    elif isinstance(data, list):
        points = data
    else:
        return []

    return [
        {"x": p.get("x", 0), "y": p.get("y", 0), "value": p.get("value", 1)}
        for p in points
        if isinstance(p, dict) and ("x" in p and "y" in p)
    ]


# ────────────────────────────────────────────────────────────
# STEP 4: Export → CSV
# ────────────────────────────────────────────────────────────

def export_sofascore_data(
    heatmaps: list[PlayerHeatmap],
    avg_positions: list[PlayerAvgPosition],
    events: list[MatchEvent],
    *,
    league_cfg: SSLeagueConfig,
) -> list[str]:
    """Export SofaScore data ra CSV files."""
    exported: list[str] = []
    output_dir = league_cfg.output_dir

    # ── Heatmap summary (flattened — 1 row per player per match) ──
    if heatmaps:
        rows = []
        for hm in heatmaps:
            row = hm.model_dump(by_alias=False, exclude={"heatmap_points"})
            # Store serialized points as JSON string for CSV
            row["heatmap_points_json"] = (
                "[" + ",".join(
                    f'{{"x":{p.x},"y":{p.y},"v":{p.value}}}'
                    for p in hm.heatmap_points
                ) + "]"
            )
            rows.append(row)

        df = pd.DataFrame(rows)
        path = output_dir / league_cfg.heatmaps_csv
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported heatmaps → %s (%d rows)", path, len(df))
        exported.append(str(path))

    # ── Average positions ──
    if avg_positions:
        df = pd.DataFrame([p.model_dump(by_alias=False) for p in avg_positions])
        path = output_dir / league_cfg.player_positions_csv
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported avg positions → %s (%d rows)", path, len(df))
        exported.append(str(path))

    # ── Match events ──
    if events:
        df = pd.DataFrame([e.model_dump(by_alias=False) for e in events])
        path = output_dir / f"dataset_{league_cfg.league_id.lower()}_ss_events.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported events → %s (%d rows)", path, len(df))
        exported.append(str(path))

    return exported


# ────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ────────────────────────────────────────────────────────────

async def main(
    league_id: str = "EPL",
    match_limit: int = 5,
    skip_heatmaps: bool = False,
) -> dict[str, list]:
    """
    SofaScore Heatmap Pipeline – entry point chính.

    Args:
        league_id:     League ID (VD: "EPL", "LALIGA")
        match_limit:   Số trận tối đa (default: 5, 0 = tất cả).
                       Mỗi trận có ~22-30 cầu thủ → nhiều API calls.
        skip_heatmaps: Chỉ lấy avg positions (bỏ heatmap points).

    Returns:
        dict với keys: "events", "heatmaps", "avg_positions"
    """
    league_cfg = get_ss_config(league_id)

    t_start = time.perf_counter()
    logger.info("=" * 60)
    logger.info("SOFASCORE HEATMAP PIPELINE – BẮT ĐẦU")
    logger.info("  League: %s (tournament=%d, season=%d)",
                league_id, league_cfg.tournament_id, league_cfg.season_id)
    logger.info("  Match limit: %s", match_limit or "ALL")
    logger.info("=" * 60)

    all_heatmaps: list[dict] = []
    all_avg_positions: list[dict] = []

    async with SofaScoreClient() as client:
        # ── STEP 1: Get finished events ──
        logger.info("━━ STEP 1: Fetch Tournament Events ━━")
        raw_events = await fetch_tournament_events(
            client, league_cfg, match_limit=match_limit
        )

        if not raw_events:
            logger.warning("Không tìm thấy trận đấu nào. Dừng.")
            return {}

        total_matches = len(raw_events)

        # ── STEP 2 & 3: For each match, get lineups + heatmaps ──
        logger.info("━━ STEP 2: Fetch Lineups & Heatmaps (%d matches) ━━", total_matches)

        for idx, event in enumerate(raw_events, 1):
            event_id = event["event_id"]
            home = event.get("home_team", "?")
            away = event.get("away_team", "?")
            date = event.get("match_date", "?")

            logger.info("── [%d/%d] %s vs %s (%s) ──", idx, total_matches, home, away, date)

            # Fetch lineups
            lineups = await fetch_match_lineups(client, event_id)
            home_players = lineups.get("home", [])
            away_players = lineups.get("away", [])

            # Process each side
            for side, players, team_name in [
                ("home", home_players, home),
                ("away", away_players, away),
            ]:
                for player in players:
                    player_id = player.get("player_id")
                    player_name = player.get("player_name", "?")

                    if not player_id:
                        continue

                    # ── Heatmap points (if not skipped) ──
                    # Lấy heatmap trước để tính avg position từ heatmap data
                    heatmap_points: list[dict] = []
                    if not skip_heatmaps and not player.get("substitute", False):
                        heatmap_points = await fetch_player_heatmap(
                            client, event_id, player_id
                        )

                        if heatmap_points:
                            # Calculate average position from heatmap
                            xs = [p["x"] for p in heatmap_points]
                            ys = [p["y"] for p in heatmap_points]
                            avg_x = sum(xs) / len(xs) if xs else None
                            avg_y = sum(ys) / len(ys) if ys else None

                            score_str = f"{event.get('home_score', '?')}-{event.get('away_score', '?')}"

                            hm_dict = {
                                "event_id": event_id,
                                "match_date": date,
                                "home_team": home,
                                "away_team": away,
                                "score": score_str,
                                "player_id": player_id,
                                "player_name": player_name,
                                "team_name": team_name,
                                "position": player.get("position"),
                                "jersey_number": player.get("jersey_number"),
                                "heatmap_points": [
                                    HeatmapPoint(**p) for p in heatmap_points
                                ],
                                "num_points": len(heatmap_points),
                                "avg_x": avg_x,
                                "avg_y": avg_y,
                                "league_id": league_id,
                                "season": league_cfg.season,
                            }
                            all_heatmaps.append(hm_dict)

                            # ── Avg position (tính từ heatmap data) ──
                            pos_dict = {
                                "event_id": event_id,
                                "match_date": date,
                                "home_team": home,
                                "away_team": away,
                                "player_id": player_id,
                                "player_name": player_name,
                                "team_name": team_name,
                                "position": player.get("position"),
                                "jersey_number": player.get("jersey_number"),
                                "avg_x": avg_x,
                                "avg_y": avg_y,
                                "minutes_played": player.get("minutes_played"),
                                "rating": player.get("rating"),
                                "league_id": league_id,
                                "season": league_cfg.season,
                            }
                            all_avg_positions.append(pos_dict)

            logger.info("  ✓ %d players processed", len(home_players) + len(away_players))

    # ── Validate ──
    valid_events = safe_parse_list(MatchEvent, raw_events, context_label="events")
    valid_heatmaps = safe_parse_list(PlayerHeatmap, all_heatmaps, context_label="heatmaps")
    valid_positions = safe_parse_list(PlayerAvgPosition, all_avg_positions, context_label="avg_positions")

    # ── Export ──
    logger.info("━━ STEP 3: Export CSV ━━")
    exported = export_sofascore_data(
        valid_heatmaps, valid_positions, valid_events,
        league_cfg=league_cfg,
    )

    # ── Summary ──
    elapsed = time.perf_counter() - t_start
    logger.info("=" * 60)
    logger.info("SOFASCORE PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info("  Matches        : %d", len(valid_events))
    logger.info("  Heatmaps       : %d", len(valid_heatmaps))
    logger.info("  Avg positions  : %d", len(valid_positions))
    logger.info("  CSV files      : %d", len(exported))
    logger.info("  Thời gian      : %.1f giây", elapsed)
    logger.info("=" * 60)

    return {
        "events": valid_events,
        "heatmaps": valid_heatmaps,
        "avg_positions": valid_positions,
    }


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────

def cli() -> None:
    """Command-line interface cho SofaScore heatmap client."""
    import argparse

    supported = list(cfg.SS_TOURNAMENT_IDS.keys())

    parser = argparse.ArgumentParser(
        description="SofaScore Heatmap Pipeline – Touch Maps & Average Positions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Giải đấu hỗ trợ (SofaScore): {', '.join(supported)}

Dữ liệu thu thập:
  • Heatmap Points (tọa độ x, y chạm bóng / di chuyển)
  • Player Average Positions (vị trí trung bình trên sân)
  • Match Events (danh sách trận đấu + kết quả)

Ví dụ:
  python sofascore_client.py                              # EPL, 5 trận
  python sofascore_client.py --league LALIGA              # La Liga, 5 trận
  python sofascore_client.py --match-limit 10             # Tăng lên 10 trận
  python sofascore_client.py --match-limit 0              # Tất cả trận
  python sofascore_client.py --skip-heatmaps              # Chỉ avg positions
  python sofascore_client.py --list-leagues               # Xem tất cả giải

Lưu ý:
  • Mỗi trận có ~22-30 cầu thủ → mỗi trận = ~25 API calls
  • match-limit 5 = ~125 API calls (~4 phút)
  • match-limit 0 (full season) = ~9,500 API calls (~5+ giờ)
        """,
    )
    parser.add_argument(
        "--league",
        default="EPL",
        help=f"League ID (default: EPL). Choices: {', '.join(supported)}",
    )
    parser.add_argument(
        "--match-limit",
        type=int,
        default=5,
        help="Giới hạn số trận (default: 5, 0 = tất cả)",
    )
    parser.add_argument(
        "--skip-heatmaps",
        action="store_true",
        help="Chỉ lấy average positions (bỏ heatmap points)",
    )
    parser.add_argument(
        "--list-leagues",
        action="store_true",
        help="Liệt kê tất cả giải đấu hỗ trợ",
    )
    args = parser.parse_args()

    if args.list_leagues:
        print(list_leagues())
        return

    league_id = args.league.upper()
    if league_id not in cfg.SS_TOURNAMENT_IDS:
        logger.error(
            "League '%s' không có trên SofaScore. Supported: %s",
            league_id, ', '.join(supported),
        )
        return

    asyncio.run(main(
        league_id=league_id,
        match_limit=args.match_limit,
        skip_heatmaps=args.skip_heatmaps,
    ))


if __name__ == "__main__":
    cli()
