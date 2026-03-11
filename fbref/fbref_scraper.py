#!/usr/bin/env python3
# ============================================================
# fbref_scraper.py – FBref Data Ingestion Pipeline
# ============================================================
"""
Thu thập dữ liệu từ FBref cho Critical + Important items:
  1. League Standings   (BXH)
  2. Player Profiles    (tên, tuổi, quốc tịch, vị trí)
  3. Player Season Stats (goals, assists, minutes, cards…)
  4. Squad Stats        (team-level aggregates)
  5. Defensive Stats    (tackles, interceptions, blocks, pressures)
  6. Possession & Carry (progressive carries, take-ons, touches by zone)
  7. GK Stats           (PSxG, saves%, distribution)
  8. Fixture Schedule   (lịch thi đấu, kick-off, venue)
  9. Match Passing Data (pass types, progressive, key passes)

Chiến lược bypass Cloudflare:
──────────────────────────────────────────────────────────────
FBref sử dụng Cloudflare JS Challenge → chặn mọi HTTP client
(aiohttp, requests, curl_cffi, cloudscraper).

Giải pháp: dùng ``nodriver`` (undetected Chrome) ở chế độ
headed (có cửa sổ trình duyệt).  Cửa sổ sẽ mở khoảng 1-2 phút
rồi tự đóng khi pipeline hoàn thành.

Hỗ trợ multi-league:
──────────────────────────────────────────────────────────────
  python fbref_scraper.py                          # EPL (mặc định)
  python fbref_scraper.py --league LALIGA          # La Liga
  python fbref_scraper.py --league BUNDESLIGA      # Bundesliga
  python fbref_scraper.py --list-leagues           # Xem tất cả giải

Luồng thực thi:
  1. Mở Chrome (headed) → navigate to FBref league page
  2. Chờ Cloudflare JS challenge pass (~10-15s lần đầu)
  3. Parse HTML → standings + squad stats + team links
  4. Lần lượt navigate tới squad pages (reuse session)
  5. Parse HTML → player profiles + player season stats
  6. Export tất cả ra CSV

Rate limiting: 5 giây giữa mỗi page load
"""

from __future__ import annotations

import asyncio
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any

import config_fbref as cfg
import pandas as pd
from bs4 import BeautifulSoup, Comment
from config_fbref import FBrefLeagueConfig, get_fbref_config
from schemas_fbref import (
    FixtureRow,
    MatchPassingStats,
    PlayerDefensiveStats,
    PlayerGKStats,
    PlayerPossessionStats,
    PlayerProfile,
    PlayerSeasonStats,
    SquadStats,
    StandingsRow,
    safe_parse_list,
)

# League registry (project root)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from services.league_registry import (  # noqa: E402
    get_fbref_leagues,
    get_league,
    list_leagues,
)


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
# BROWSER SESSION – nodriver
# ────────────────────────────────────────────────────────────


class FBrefBrowser:
    """
    Quản lý browser session cho FBref scraping.

    Lifecycle:
        async with FBrefBrowser() as fb:
            html = await fb.fetch("https://fbref.com/...")

    Tự động:
      • Mở Chrome (headed) khi __aenter__
      • Chờ Cloudflare pass khi fetch()
      • Đóng Chrome khi __aexit__
      • Rate-limit giữa các request
    """

    def __init__(self) -> None:
        self._browser = None
        self._page = None
        self._last_fetch_time: float = 0.0

    async def __aenter__(self) -> "FBrefBrowser":
        import os
        import sys

        import nodriver as uc

        is_linux = sys.platform.startswith("linux")

        if is_linux:
            logger.info("▶ Khởi động Chrome browser (Linux Headless mode)…")
            self._browser = await uc.start(
                headless=True,
                sandbox=False,
                browser_args=[
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            )
        else:
            logger.info("▶ Khởi động Chrome browser (Local Headed mode)…")
            self._browser = await uc.start(headless=False)
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._browser:
            try:
                self._browser.stop()
            except Exception:
                pass
            logger.info("✓ Đã đóng Chrome browser.")

    async def _wait_for_cf(self) -> None:
        """Chờ Cloudflare JS challenge pass (title không còn 'Just a moment')."""
        for i in range(cfg.FBREF_CF_WAIT_MAX):
            await asyncio.sleep(1)
            try:
                title = await self._page.evaluate("document.title")
                if "moment" not in title.lower():
                    logger.info("  ✓ Cloudflare passed sau %ds – %s", i + 1, title[:60])
                    return
            except Exception:
                pass
            if i > 0 and i % 10 == 0:
                logger.info("  ⏳ Chờ Cloudflare… (%ds)", i)
        logger.warning("  ⚠ Cloudflare timeout sau %ds", cfg.FBREF_CF_WAIT_MAX)

    async def _wait_for_tables(self) -> bool:
        """Chờ cho đến khi trang có ít nhất 1 <table>."""
        for _ in range(15):
            try:
                count = await self._page.evaluate(
                    'document.querySelectorAll("table").length'
                )
                if count > 0:
                    await asyncio.sleep(cfg.FBREF_TABLE_LOAD_WAIT)
                    return True
            except Exception:
                pass
            await asyncio.sleep(0.5)
        return False

    async def _rate_limit(self) -> None:
        """Đảm bảo delay tối thiểu giữa các fetch."""
        elapsed = time.monotonic() - self._last_fetch_time
        if elapsed < cfg.FBREF_DELAY_BETWEEN_PAGES:
            wait = cfg.FBREF_DELAY_BETWEEN_PAGES - elapsed
            logger.debug("  Rate limit: chờ %.1fs", wait)
            await asyncio.sleep(wait)

    async def fetch(self, url: str) -> str:
        """
        Navigate tới URL và trả về HTML sau khi page load xong.

        Xử lý:
          1. Rate limit (delay giữa requests)
          2. Navigate + chờ DOM loaded
          3. Chờ Cloudflare nếu cần
          4. Chờ tables xuất hiện
          5. Trả về full HTML

        Returns:
            HTML string, hoặc "" nếu lỗi.
        """
        await self._rate_limit()
        logger.info("📥 Fetching: %s", url)

        try:
            self._page = await self._browser.get(url)
            self._last_fetch_time = time.monotonic()

            # Kiểm tra Cloudflare
            try:
                title = await self._page.evaluate("document.title")
                if "moment" in title.lower():
                    await self._wait_for_cf()
            except Exception:
                await asyncio.sleep(3)

            # Chờ tables
            has_tables = await self._wait_for_tables()
            if not has_tables:
                logger.warning("  ⚠ Không tìm thấy tables trên %s", url)

            # Lấy full HTML
            html = await self._page.evaluate("document.documentElement.outerHTML")
            logger.info("  ✓ HTML: %d bytes, tables=%s", len(html), has_tables)
            return html

        except Exception as exc:
            logger.error("  ✗ Fetch failed: %s – %s", url, exc)
            return ""


# ────────────────────────────────────────────────────────────
# HTML PARSERS – Extract data từ FBref HTML tables
# ────────────────────────────────────────────────────────────


def _parse_table_rows(
    soup: BeautifulSoup,
    table_id: str | None = None,
    table_id_pattern: str | None = None,
) -> tuple[list[dict], BeautifulSoup | None]:
    """
    Parse một FBref HTML table thành list[dict].

    Args:
        soup: BeautifulSoup parsed HTML
        table_id: Exact table id (e.g. "stats_standard_9")
        table_id_pattern: Regex pattern for table id

    Returns:
        (rows_as_dicts, table_element)
        Mỗi dict có keys = data-stat attributes.
    """
    table = None
    if table_id:
        table = soup.find("table", id=table_id)
    elif table_id_pattern:
        table = soup.find("table", id=re.compile(table_id_pattern))

    if table is None:
        return [], None

    tbody = table.find("tbody")
    if tbody is None:
        return [], table

    rows_data: list[dict] = []
    for tr in tbody.find_all("tr"):
        # Skip separator rows (class="thead" or "spacer")
        if tr.get("class") and any(
            c in tr["class"] for c in ("thead", "spacer", "partial_table")
        ):
            continue

        row: dict[str, Any] = {}
        for cell in tr.find_all(["th", "td"]):
            stat = cell.get("data-stat", "")
            if not stat:
                continue
            row[stat] = cell.text.strip()

            # Extract link info (player_id, team_id)
            link = cell.find("a", href=True)
            if link:
                href = link["href"]
                if "/players/" in href:
                    m = re.search(r"/players/([^/]+)/", href)
                    if m:
                        row["_player_id"] = m.group(1)
                        row["_player_url"] = href
                elif "/squads/" in href:
                    m = re.search(r"/squads/([^/]+)/", href)
                    if m:
                        row["_team_id"] = m.group(1)
                        row["_team_url"] = href

        if row:
            rows_data.append(row)

    return rows_data, table


def _extract_team_links(soup: BeautifulSoup) -> list[dict[str, str]]:
    """
    Extract team links từ standings table.

    Returns:
        [{"name": "Arsenal", "team_id": "18bb7c10", "url": "/en/squads/..."}]
    """
    teams: list[dict[str, str]] = []
    # Tìm standings table
    table = soup.find("table", id=re.compile(r"results.*_overall"))
    if not table:
        return teams

    for row in table.find("tbody", recursive=False).find_all("tr"):
        link = row.find("a", href=re.compile(r"/en/squads/"))
        if link:
            href = link["href"]
            m = re.search(r"/squads/([^/]+)/", href)
            teams.append(
                {
                    "name": link.text.strip(),
                    "team_id": m.group(1) if m else "",
                    "url": cfg.FBREF_BASE + href,
                }
            )
    return teams


# ────────────────────────────────────────────────────────────
# STEP 1: Parse League Page → Standings + Squad Stats
# ────────────────────────────────────────────────────────────


def parse_league_page(html: str) -> dict[str, Any]:
    """
    Parse FBref league overview page.

    Returns:
        {
            "standings":   list[dict],
            "squad_stats": list[dict],
            "team_links":  list[{"name", "team_id", "url"}],
        }
    """
    soup = BeautifulSoup(html, "html.parser")
    result: dict[str, Any] = {
        "standings": [],
        "squad_stats": [],
        "team_links": [],
    }

    # 1. Standings table
    standings_rows, _ = _parse_table_rows(soup, table_id_pattern=r"results.*_overall")
    # Enrich with team_id from links
    for row in standings_rows:
        if "_team_id" in row:
            row["team_id"] = row.pop("_team_id")
            row["team_url"] = row.pop("_team_url", None)
        else:
            row.pop("_team_id", None)
            row.pop("_team_url", None)
    result["standings"] = standings_rows
    logger.info("  Standings: %d teams parsed", len(standings_rows))

    # 2. Squad standard stats
    squad_rows, _ = _parse_table_rows(soup, table_id="stats_squads_standard_for")
    for row in squad_rows:
        if "_team_id" in row:
            row["team_id"] = row.pop("_team_id")
        row.pop("_team_url", None)
        row.pop("_player_id", None)
        row.pop("_player_url", None)
    result["squad_stats"] = squad_rows
    logger.info("  Squad stats: %d teams parsed", len(squad_rows))

    # 3. Team links
    result["team_links"] = _extract_team_links(soup)
    logger.info("  Team links: %d teams found", len(result["team_links"]))

    return result


# ────────────────────────────────────────────────────────────
# STEP 2: Parse Squad Page → Player Profiles + Stats
# ────────────────────────────────────────────────────────────


def parse_squad_page(
    html: str,
    team_name: str,
    team_id: str,
    *,
    league_cfg: FBrefLeagueConfig | None = None,
) -> dict[str, Any]:
    """
    Parse FBref squad page.

    Args:
        html: Raw HTML string
        team_name: Tên đội
        team_id: FBref team ID
        league_cfg: Dynamic league config (nếu None, dùng legacy defaults)

    Returns:
        {
            "profiles":     list[dict],  — player profiles
            "player_stats": list[dict],  — player season stats
        }
    """
    soup = BeautifulSoup(html, "html.parser")
    result: dict[str, Any] = {
        "profiles": [],
        "player_stats": [],
        "player_stats": [],
        "gk_stats": [],
    }

    # Resolve table IDs (dynamic hoặc legacy)
    std_table_id = (
        league_cfg.squad_standard_table_id
        if league_cfg
        else cfg.SQUAD_STANDARD_TABLE_ID
    )
    shoot_table_id = (
        league_cfg.squad_shooting_table_id
        if league_cfg
        else cfg.SQUAD_SHOOTING_TABLE_ID
    )
    gk_table_id = (
        league_cfg.squad_gk_table_id if league_cfg else cfg.SQUAD_KEEPER_TABLE_ID
    )
    season = league_cfg.season if league_cfg else cfg.FBREF_SEASON

    # ── Standard stats table (player-level) ──
    std_rows, _ = _parse_table_rows(soup, table_id=std_table_id)

    # ── Shooting stats table ──
    shoot_rows, _ = _parse_table_rows(soup, table_id=shoot_table_id)
    # Build lookup by player name for merging
    shoot_lookup: dict[str, dict] = {}
    for row in shoot_rows:
        pname = row.get("player", "")
        if pname:
            shoot_lookup[pname] = row

    for row in std_rows:
        player_name = row.get("player", "")
        if not player_name or player_name.lower() == "squad total":
            continue

        # Extract IDs
        player_id = row.pop("_player_id", None)
        player_url = row.pop("_player_url", None)
        row.pop("_team_id", None)
        row.pop("_team_url", None)

        # ── Build Profile ──
        profile = {
            "player": player_name,
            "player_id": player_id,
            "player_url": player_url,
            "nationality": row.get("nationality"),
            "position": row.get("position"),
            "age": row.get("age"),
            "team_name": team_name,
            "team_id": team_id,
            "season": season,
        }
        result["profiles"].append(profile)

        # ── Build Player Season Stats ──
        stats = dict(row)  # Copy standard stats
        stats["player_id"] = player_id
        stats["team_name"] = team_name
        stats["team_id"] = team_id

        # Merge shooting data
        shoot_data = shoot_lookup.get(player_name, {})
        for key in ("shots", "shots_on_target", "shots_on_target_pct"):
            if key in shoot_data:
                stats[key] = shoot_data[key]

        # Remove internal fields
        for k in ("matches", "_player_id", "_player_url", "_team_id", "_team_url"):
            stats.pop(k, None)

        result["player_stats"].append(stats)

    # ── GK stats table ──
    gk_rows, _ = _parse_table_rows(soup, table_id=gk_table_id)
    for row in gk_rows:
        player_name = row.get("player", "")
        if not player_name or player_name.lower() == "squad total":
            continue
        g = dict(row)
        g["player_id"] = g.pop("_player_id", None)
        g["team_name"] = team_name
        g["team_id"] = team_id
        g["season"] = season
        for k in ("_player_url", "_team_id", "_team_url"):
            g.pop(k, None)
        result["gk_stats"].append(g)

    logger.info(
        "  %s: %d players, %d with shooting data, %d gk",
        team_name,
        len(result["profiles"]),
        len(shoot_lookup),
        len(result["gk_stats"]),
    )
    return result


# ────────────────────────────────────────────────────────────
# STEP 2.5: Parse League-wide Defensive and Possession
# ────────────────────────────────────────────────────────────


def parse_league_defense_page(
    html: str,
    *,
    league_cfg: FBrefLeagueConfig | None = None,
) -> list[dict]:
    """Parse league-wide defensive stats page."""
    soup = BeautifulSoup(html, "html.parser")
    # Trên trang League-wide, bảng cầu thủ thường có id="stats_defense" (không có comp_id)
    table_id = "stats_defense"

    rows, _ = _parse_table_rows(soup, table_id=table_id)
    if not rows:
        comments = soup.find_all(string=lambda t: isinstance(t, Comment))
        for comment in comments:
            if "table" in str(comment).lower():
                comment_soup = BeautifulSoup(str(comment), "html.parser")
                rows, _ = _parse_table_rows(comment_soup, table_id=table_id)
                if rows:
                    break

    season = league_cfg.season if league_cfg else cfg.FBREF_SEASON

    result = []
    for row in rows:
        player_name = row.get("player", "")
        if not player_name or player_name.lower() == "squad total":
            continue

        # Only add players who actually played
        mins = str(row.get("minutes_90s", "")).replace(",", "")
        try:
            val = float(mins) if mins else 0.0
        except ValueError:
            val = 0.0
        if val <= 0:
            continue

        d = dict(row)
        d["player_id"] = d.pop("_player_id", None)
        # League-wide table usually has a 'squad' column
        squad_name = row.get("team", "") or row.get("squad", "")
        d["team_name"] = squad_name
        d["team_id"] = d.pop("_team_id", None)
        d["season"] = season

        for k in ("_player_url", "_team_url"):
            d.pop(k, None)

        result.append(d)

    return result


def parse_league_possession_page(
    html: str,
    *,
    league_cfg: FBrefLeagueConfig | None = None,
) -> list[dict]:
    """Parse league-wide possession stats page."""
    soup = BeautifulSoup(html, "html.parser")
    # Trên trang League-wide, bảng cầu thủ thường có id="stats_possession" (không có comp_id)
    table_id = "stats_possession"

    rows, _ = _parse_table_rows(soup, table_id=table_id)
    if not rows:
        comments = soup.find_all(string=lambda t: isinstance(t, Comment))
        for comment in comments:
            if "table" in str(comment).lower():
                comment_soup = BeautifulSoup(str(comment), "html.parser")
                rows, _ = _parse_table_rows(comment_soup, table_id=table_id)
                if rows:
                    break

    season = league_cfg.season if league_cfg else cfg.FBREF_SEASON

    result = []
    for row in rows:
        player_name = row.get("player", "")
        if not player_name or player_name.lower() == "squad total":
            continue

        mins = str(row.get("minutes_90s", "")).replace(",", "")
        try:
            val = float(mins) if mins else 0.0
        except ValueError:
            val = 0.0
        if val <= 0:
            continue

        p = dict(row)
        p["player_id"] = p.pop("_player_id", None)
        squad_name = row.get("team", "") or row.get("squad", "")
        p["team_name"] = squad_name
        p["team_id"] = p.pop("_team_id", None)
        p["season"] = season

        for k in ("_player_url", "_team_url"):
            p.pop(k, None)

        result.append(p)

    return result


# ────────────────────────────────────────────────────────────
# STEP 3: Parse Fixture Schedule Page
# ────────────────────────────────────────────────────────────


def parse_fixtures_page(
    html: str,
    *,
    league_cfg: FBrefLeagueConfig | None = None,
) -> list[dict]:
    """
    Parse FBref Scores and Fixtures page.

    URL pattern: /en/comps/{comp_id}/schedule/{slug}-Scores-and-Fixtures

    Returns:
        list[dict] — raw fixture rows ready for FixtureRow validation.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Try dynamic table ID first, fallback to pattern
    table_id = league_cfg.fixture_table_id if league_cfg else None
    fixtures_raw, table_el = _parse_table_rows(
        soup,
        table_id=table_id,
        table_id_pattern=r"sched_.*_\d+_1" if not table_id else None,
    )

    if not fixtures_raw and table_el is None:
        # Fixtures table might be inside HTML comments
        comments = soup.find_all(string=lambda t: isinstance(t, Comment))
        for comment in comments:
            if "sched_" in str(comment) or "Scores &amp; Fixtures" in str(comment):
                comment_soup = BeautifulSoup(str(comment), "html.parser")
                fixtures_raw, table_el = _parse_table_rows(
                    comment_soup,
                    table_id=table_id,
                    table_id_pattern=r"sched_.*_\d+_1" if not table_id else None,
                )
                if fixtures_raw:
                    break

    # Enrich with match IDs and team IDs from links
    # Re-parse from table element to extract link data
    if table_el is not None:
        tbody = table_el.find("tbody")
        if tbody:
            enriched: list[dict] = []
            for tr in tbody.find_all("tr"):
                if tr.get("class") and any(
                    c in tr["class"] for c in ("thead", "spacer", "partial_table")
                ):
                    continue

                row: dict[str, Any] = {}
                for cell in tr.find_all(["th", "td"]):
                    stat = cell.get("data-stat", "")
                    if not stat:
                        continue
                    row[stat] = cell.text.strip()

                    link = cell.find("a", href=True)
                    if link:
                        href = link["href"]
                        if stat == "home_team":
                            m = re.search(r"/squads/([^/]+)/", href)
                            if m:
                                row["home_team_id"] = m.group(1)
                        elif stat == "away_team":
                            m = re.search(r"/squads/([^/]+)/", href)
                            if m:
                                row["away_team_id"] = m.group(1)
                        elif stat == "match_report":
                            m = re.search(r"/matches/([^/]+)/", href)
                            if m:
                                row["match_id"] = m.group(1)
                                row["match_report_url"] = cfg.FBREF_BASE + href

                if row and row.get("date") and row.get("score"):
                    enriched.append(row)

            if enriched:
                fixtures_raw = enriched

    logger.info("  Fixtures: %d matches parsed", len(fixtures_raw))
    return fixtures_raw


# ────────────────────────────────────────────────────────────
# STEP 4: Parse Match Report → Passing Stats
# ────────────────────────────────────────────────────────────


def parse_match_report(
    html: str,
    match_id: str,
    match_date: str = "",
    home_team: str = "",
    away_team: str = "",
) -> list[dict]:
    """
    Parse FBref match report page for passing stats.

    Match reports have two passing tables (one per team).
    Table IDs: stats_{team_id}_passing

    Returns:
        list[dict] — passing stats for all players in the match.
    """
    soup = BeautifulSoup(html, "html.parser")
    all_passing: list[dict] = []

    # FBref hides some tables in HTML comments — uncomment them
    comments = soup.find_all(string=lambda t: isinstance(t, Comment))
    for comment in comments:
        if "passing" in str(comment).lower():
            comment_soup = BeautifulSoup(str(comment), "html.parser")
            for tbl in comment_soup.find_all("table"):
                tbl_id = tbl.get("id", "")
                if "passing" in tbl_id and "types" not in tbl_id:
                    soup.append(tbl)

    # Find all passing tables (pattern: stats_{team_id}_passing)
    passing_tables = soup.find_all("table", id=re.compile(r"stats_[a-f0-9]+_passing$"))

    for table in passing_tables:
        table_id = table.get("id", "")
        # Extract team_id from table ID
        m = re.search(r"stats_([a-f0-9]+)_passing$", table_id)
        team_id_from_table = m.group(1) if m else ""

        # Determine which team this is
        team_name_for_table = ""
        caption = table.find("caption")
        if caption:
            team_name_for_table = (
                caption.text.strip().replace(" Passing Stats", "").strip()
            )
            # Clean up: "Arsenal Passing Stats Table" → "Arsenal"
            team_name_for_table = re.sub(
                r"\s*(Passing|Stats|Table).*", "", team_name_for_table
            ).strip()

        tbody = table.find("tbody")
        if not tbody:
            continue

        for tr in tbody.find_all("tr"):
            if tr.get("class") and any(
                c in tr["class"] for c in ("thead", "spacer", "partial_table")
            ):
                continue

            row: dict[str, Any] = {}
            for cell in tr.find_all(["th", "td"]):
                stat = cell.get("data-stat", "")
                if not stat:
                    continue
                row[stat] = cell.text.strip()

                link = cell.find("a", href=True)
                if link and "/players/" in link["href"]:
                    pm = re.search(r"/players/([^/]+)/", link["href"])
                    if pm:
                        row["player_id"] = pm.group(1)

            player_name = row.get("player", "")
            if not player_name or player_name.lower() in ("squad total", ""):
                continue

            # Add context
            row["match_id"] = match_id
            row["match_date"] = match_date
            row["home_team"] = home_team
            row["away_team"] = away_team
            row["team_name"] = team_name_for_table
            row["team_id"] = team_id_from_table

            # Clean internal fields
            for k in ("_player_id", "_player_url", "_team_id", "_team_url"):
                row.pop(k, None)

            all_passing.append(row)

    logger.info(
        "  Match %s: %d passing records from %d tables",
        match_id[:8],
        len(all_passing),
        len(passing_tables),
    )
    return all_passing


# ────────────────────────────────────────────────────────────
# STEP 5: Export → CSV
# ────────────────────────────────────────────────────────────


def export_fbref_data(
    standings: list,
    squad_stats: list,
    profiles: list,
    player_stats: list,
    *,
    defensive_stats: list | None = None,
    possession_stats: list | None = None,
    gk_stats: list | None = None,
    fixtures: list | None = None,
    match_passing: list | None = None,
    league_cfg: FBrefLeagueConfig | None = None,
) -> list[str]:
    """Export tất cả dữ liệu FBref ra CSV files."""
    exported: list[str] = []

    # Resolve output paths (dynamic hoặc legacy)
    output_dir = league_cfg.output_dir if league_cfg else cfg.OUTPUT_DIR
    standings_csv = league_cfg.standings_csv if league_cfg else cfg.STANDINGS_CSV
    squad_stats_csv = league_cfg.squad_stats_csv if league_cfg else cfg.SQUAD_STATS_CSV
    roster_csv = league_cfg.squad_roster_csv if league_cfg else cfg.SQUAD_ROSTER_CSV
    player_csv = (
        league_cfg.player_season_stats_csv
        if league_cfg
        else cfg.PLAYER_SEASON_STATS_CSV
    )

    # Standings
    if standings:
        df = pd.DataFrame([s.model_dump(by_alias=False) for s in standings])
        path = output_dir / standings_csv
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported standings → %s (%d rows)", path, len(df))
        exported.append(str(path))

    # Squad stats
    if squad_stats:
        df = pd.DataFrame([s.model_dump(by_alias=False) for s in squad_stats])
        path = output_dir / squad_stats_csv
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported squad stats → %s (%d rows)", path, len(df))
        exported.append(str(path))

    # Player profiles
    if profiles:
        df = pd.DataFrame([p.model_dump(by_alias=False) for p in profiles])
        path = output_dir / roster_csv
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported player profiles → %s (%d rows)", path, len(df))
        exported.append(str(path))

    # Player season stats
    if player_stats:
        df = pd.DataFrame([p.model_dump(by_alias=False) for p in player_stats])
        path = output_dir / player_csv
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported player stats → %s (%d rows)", path, len(df))
        exported.append(str(path))

    # Defensive stats
    if defensive_stats:
        df = pd.DataFrame([d.model_dump(by_alias=False) for d in defensive_stats])
        path = output_dir / (
            league_cfg.defensive_stats_csv if league_cfg else cfg.DEFENSIVE_STATS_CSV
        )
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported defensive stats → %s (%d rows)", path, len(df))
        exported.append(str(path))

    # Possession stats
    if possession_stats:
        df = pd.DataFrame([p.model_dump(by_alias=False) for p in possession_stats])
        path = output_dir / (
            league_cfg.possession_stats_csv if league_cfg else cfg.POSSESSION_STATS_CSV
        )
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported possession stats → %s (%d rows)", path, len(df))
        exported.append(str(path))

    # GK stats
    if gk_stats:
        df = pd.DataFrame([g.model_dump(by_alias=False) for g in gk_stats])
        path = output_dir / (
            league_cfg.gk_stats_csv if league_cfg else cfg.GK_STATS_CSV
        )
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported GK stats → %s (%d rows)", path, len(df))
        exported.append(str(path))

    # Fixtures
    if fixtures:
        df = pd.DataFrame([f.model_dump(by_alias=False) for f in fixtures])
        path = output_dir / (league_cfg.fixture_csv if league_cfg else cfg.FIXTURE_CSV)
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported fixtures → %s (%d rows)", path, len(df))
        exported.append(str(path))

    # Match passing
    if match_passing:
        df = pd.DataFrame([m.model_dump(by_alias=False) for m in match_passing])
        path = output_dir / (
            league_cfg.match_passing_csv if league_cfg else cfg.MATCH_PASSING_CSV
        )
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported match passing → %s (%d rows)", path, len(df))
        exported.append(str(path))

    return exported


# ────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ────────────────────────────────────────────────────────────


async def main(
    standings_only: bool = False,
    team_limit: int = 0,
    league_id: str = "EPL",
    no_match_passing: bool = False,
    match_limit: int = 0,
    since_date: str | None = None,
    season: str | None = None,
) -> dict[str, list]:
    """
    FBref Data Pipeline – entry point chính.

    Args:
        standings_only:   Chỉ scrape standings (không scrape squad pages).
        team_limit:       Giới hạn số đội scrape (0 = tất cả).
        league_id:        League ID từ registry (VD: "EPL", "LALIGA").
        no_match_passing: Bỏ qua match passing data (tiết kiệm thời gian).
        match_limit:      Giới hạn số match reports để scrape (0 = tất cả).
        since_date:       Chỉ scrape match reports có date >= "YYYY-MM-DD".
        season:           Năm mùa giải bắt đầu (VD: "2024").
    """
    # ── Resolve league config ──
    league_cfg = get_fbref_config(league_id, season)

    t_start = time.perf_counter()
    logger.info("=" * 60)
    logger.info("FBREF DATA PIPELINE – BẮT ĐẦU")
    logger.info("  League: %s (comp_id=%d)", league_id, league_cfg.comp_id)
    logger.info("  Season: %s", league_cfg.season)
    logger.info("  URL:    %s", league_cfg.league_url)
    logger.info("=" * 60)

    all_standings: list = []
    all_squad_stats: list = []
    all_profiles: list = []
    all_player_stats: list = []
    all_defensive: list = []
    all_possession: list = []
    all_gk: list = []
    all_fixtures: list = []
    all_match_passing: list = []

    async with FBrefBrowser() as browser:
        # ── STEP 1: League page ──
        logger.info("━━ STEP 1: Scrape League Page ━━")
        html = await browser.fetch(league_cfg.league_url)
        if not html:
            logger.error("Không thể tải league page. Dừng pipeline.")
            return {}

        league_data = parse_league_page(html)

        # Validate standings
        all_standings = safe_parse_list(
            StandingsRow,
            league_data["standings"],
            context_label="standings",
        )
        logger.info(
            "✓ Standings: %d/%d teams validated",
            len(all_standings),
            len(league_data["standings"]),
        )

        # Validate squad stats — inject season
        for row in league_data["squad_stats"]:
            row["season"] = league_cfg.season
        all_squad_stats = safe_parse_list(
            SquadStats,
            league_data["squad_stats"],
            context_label="squad_stats",
        )
        logger.info("✓ Squad stats: %d teams validated", len(all_squad_stats))

        # ── STEP 2: Fixture Schedule ──
        logger.info("━━ STEP 2: Scrape Fixture Schedule ━━")
        fixture_html = await browser.fetch(league_cfg.fixture_url)
        if fixture_html:
            fixture_data = parse_fixtures_page(fixture_html, league_cfg=league_cfg)
            all_fixtures = safe_parse_list(
                FixtureRow,
                fixture_data,
                context_label="fixtures",
            )
            logger.info("✓ Fixtures: %d matches validated", len(all_fixtures))
        else:
            logger.warning("⚠ Không tải được fixture page, bỏ qua.")

        if not standings_only:
            # ── STEP 2.5: League-wide Defensive & Possession ──
            logger.info("━━ STEP 2.5: Scrape League-wide Defensive & Possession ━━")

            # Defense
            def_html = await browser.fetch(league_cfg.league_defense_url)
            if def_html:
                def_data = parse_league_defense_page(def_html, league_cfg=league_cfg)
                all_defensive = safe_parse_list(
                    PlayerDefensiveStats,
                    def_data,
                    context_label="league_defense",
                )
                logger.info("✓ Defensive: %d players validated", len(all_defensive))
            else:
                logger.warning("⚠ Không tải được league defense page.")

            # Possession
            poss_html = await browser.fetch(league_cfg.league_possession_url)
            if poss_html:
                poss_data = parse_league_possession_page(
                    poss_html, league_cfg=league_cfg
                )
                all_possession = safe_parse_list(
                    PlayerPossessionStats,
                    poss_data,
                    context_label="league_possession",
                )
                logger.info("✓ Possession: %d players validated", len(all_possession))
            else:
                logger.warning("⚠ Không tải được league possession page.")

        if standings_only:
            logger.info("--standings-only: bỏ qua squad pages + match reports.")
        else:
            # ── STEP 3: Squad pages ──
            team_links = league_data["team_links"]
            if team_limit > 0:
                team_links = team_links[:team_limit]
                logger.info(
                    "Giới hạn: %d/%d đội", team_limit, len(league_data["team_links"])
                )

            total_teams = len(team_links)
            logger.info("━━ STEP 3: Scrape %d Squad Pages ━━", total_teams)

            for idx, team in enumerate(team_links, 1):
                logger.info("── [%d/%d] %s ──", idx, total_teams, team["name"])
                squad_html = await browser.fetch(team["url"])
                if not squad_html:
                    logger.warning("  ✗ Không tải được squad page cho %s", team["name"])
                    continue

                squad_data = parse_squad_page(
                    squad_html,
                    team_name=team["name"],
                    team_id=team["team_id"],
                    league_cfg=league_cfg,
                )

                # Validate profiles
                valid_profiles = safe_parse_list(
                    PlayerProfile,
                    squad_data["profiles"],
                    context_label=f"profiles_{team['name']}",
                )
                all_profiles.extend(valid_profiles)

                # Validate player stats — inject season
                for ps in squad_data["player_stats"]:
                    ps["season"] = league_cfg.season
                valid_stats = safe_parse_list(
                    PlayerSeasonStats,
                    squad_data["player_stats"],
                    context_label=f"stats_{team['name']}",
                )
                all_player_stats.extend(valid_stats)

                # Validate GK stats
                valid_gk = safe_parse_list(
                    PlayerGKStats,
                    squad_data["gk_stats"],
                    context_label=f"gk_{team['name']}",
                )
                all_gk.extend(valid_gk)

                logger.info(
                    "  ✓ %s: %d profiles, %d stats, %d gk",
                    team["name"],
                    len(valid_profiles),
                    len(valid_stats),
                    len(valid_gk),
                )

            # ── STEP 4: Match Reports → Passing Data ──
            if not no_match_passing and all_fixtures:
                # Lọc matches đã có match_report_url
                matches_with_reports = [
                    f for f in all_fixtures if f.match_report_url and f.match_id
                ]

                # Filter by since_date: chỉ scrape match reports từ N ngày gần nhất.
                # FBref date format là "YYYY-MM-DD" → so sánh string ISO đúng thứ tự.
                # Áp dụng TRƯỚC match_limit để limit đếm trên set đã filtered.
                if since_date:
                    before_count = len(matches_with_reports)
                    matches_with_reports = [
                        f
                        for f in matches_with_reports
                        if f.date and f.date >= since_date
                    ]
                    logger.info(
                        "--since-date %s: %d → %d match reports (skipped %d older)",
                        since_date,
                        before_count,
                        len(matches_with_reports),
                        before_count - len(matches_with_reports),
                    )

                if match_limit > 0:
                    matches_with_reports = matches_with_reports[:match_limit]

                total_matches = len(matches_with_reports)
                if total_matches > 0:
                    logger.info("━━ STEP 4: Scrape %d Match Reports ━━", total_matches)
                    for idx, fixture in enumerate(matches_with_reports, 1):
                        logger.info(
                            "── [%d/%d] %s vs %s (%s) ──",
                            idx,
                            total_matches,
                            fixture.home_team or "?",
                            fixture.away_team or "?",
                            fixture.date or "?",
                        )
                        match_html = await browser.fetch(fixture.match_report_url)
                        if not match_html:
                            logger.warning(
                                "  ✗ Không tải được match report %s", fixture.match_id
                            )
                            continue

                        passing_data = parse_match_report(
                            match_html,
                            match_id=fixture.match_id,
                            match_date=fixture.date or "",
                            home_team=fixture.home_team or "",
                            away_team=fixture.away_team or "",
                        )
                        valid_passing = safe_parse_list(
                            MatchPassingStats,
                            passing_data,
                            context_label=f"passing_{fixture.match_id[:8]}",
                        )
                        all_match_passing.extend(valid_passing)
                else:
                    logger.info("Không có match reports để scrape.")
            elif no_match_passing:
                logger.info("--no-match-passing: bỏ qua match reports.")

    # ── STEP 5: Export ──
    logger.info("━━ STEP 5: Export CSV ━━")
    exported = export_fbref_data(
        all_standings,
        all_squad_stats,
        all_profiles,
        all_player_stats,
        defensive_stats=all_defensive,
        possession_stats=all_possession,
        gk_stats=all_gk,
        fixtures=all_fixtures,
        match_passing=all_match_passing,
        league_cfg=league_cfg,
    )

    # ── Summary ──
    elapsed = time.perf_counter() - t_start
    logger.info("=" * 60)
    logger.info("FBREF PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info("  Standings rows  : %d", len(all_standings))
    logger.info("  Squad stats     : %d", len(all_squad_stats))
    logger.info("  Fixtures        : %d", len(all_fixtures))
    logger.info("  Player profiles : %d", len(all_profiles))
    logger.info("  Player stats    : %d", len(all_player_stats))
    logger.info("  Defensive stats : %d", len(all_defensive))
    logger.info("  Possession stats: %d", len(all_possession))
    logger.info("  GK stats        : %d", len(all_gk))
    logger.info("  Match passing   : %d", len(all_match_passing))
    logger.info("  CSV files       : %d", len(exported))
    logger.info("  Thời gian       : %.1f giây", elapsed)
    logger.info("=" * 60)

    return {
        "standings": all_standings,
        "squad_stats": all_squad_stats,
        "profiles": all_profiles,
        "player_stats": all_player_stats,
        "defensive_stats": all_defensive,
        "possession_stats": all_possession,
        "gk_stats": all_gk,
        "fixtures": all_fixtures,
        "match_passing": all_match_passing,
    }


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────


def cli() -> None:
    """Command-line interface cho FBref scraper."""
    import argparse

    supported = [lg.league_id for lg in get_fbref_leagues()]

    parser = argparse.ArgumentParser(
        description="FBref Data Pipeline – Multi-League Standings, Rosters, Player Stats, Defensive, Possession, GK, Fixtures, Match Passing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Giải đấu hỗ trợ (FBref): {", ".join(supported)}

Dữ liệu thu thập:
  • Standings, Squad Stats, Player Profiles, Player Season Stats
  • Defensive Stats (tackles, interceptions, blocks, pressures)
  • Possession & Carry (touches, take-ons, progressive carries)
  • GK Stats (PSxG, saves%, distribution)
  • Fixture Schedule (lịch thi đấu, kick-off, venue)
  • Match Passing Data (pass types, progressive, key passes)

Ví dụ:
  python fbref_scraper.py                                    # EPL (mặc định)
  python fbref_scraper.py --league LALIGA                    # La Liga
  python fbref_scraper.py --league BUNDESLIGA --limit 3      # Bundesliga, 3 đội
  python fbref_scraper.py --standings-only                   # Chỉ BXH + Fixtures
  python fbref_scraper.py --no-match-passing                 # Bỏ qua match reports
  python fbref_scraper.py --match-limit 10                   # Chỉ 10 match reports
  python fbref_scraper.py --since-date 2025-03-08            # Chỉ match reports từ ngày này
  python fbref_scraper.py --list-leagues                     # Xem tất cả giải
        """,
    )
    parser.add_argument(
        "--league",
        default="EPL",
        help=f"League ID (default: EPL). Choices: {', '.join(supported)}",
    )
    parser.add_argument(
        "--standings-only",
        action="store_true",
        help="Chỉ scrape standings + fixtures, bỏ qua squad pages & match reports",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Giới hạn số đội scrape (0 = tất cả)",
    )
    parser.add_argument(
        "--no-match-passing",
        action="store_true",
        help="Bỏ qua match report passing data (tiết kiệm thời gian)",
    )
    parser.add_argument(
        "--match-limit",
        type=int,
        default=0,
        help="Giới hạn số match reports để scrape passing data (0 = tất cả)",
    )
    parser.add_argument(
        "--since-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "Chỉ scrape match reports có date >= YYYY-MM-DD. "
            "Dùng cho daily maintenance (VD: --since-date 2025-03-08). "
            "Không ảnh hưởng đến standings/squad pages."
        ),
    )
    parser.add_argument(
        "--season",
        type=str,
        default=None,
        help="Năm bắt đầu mùa giải (VD: 2023).",
    )
    parser.add_argument(
        "--list-leagues",
        action="store_true",
        help="Liệt kê tất cả giải đấu hỗ trợ",
    )
    args = parser.parse_args()

    # ── --list-leagues ──
    if args.list_leagues:
        print(list_leagues())
        return

    # ── Validate league ──
    league_id = args.league.upper()
    try:
        league_cfg_check = get_league(league_id)
        if not league_cfg_check.has_fbref:
            logger.error(
                "League '%s' không có trên FBref. Dùng --list-leagues để xem.",
                league_id,
            )
            return
    except KeyError as e:
        logger.error(str(e))
        return

    asyncio.run(
        main(
            standings_only=args.standings_only,
            team_limit=args.limit,
            league_id=league_id,
            no_match_passing=args.no_match_passing,
            match_limit=args.match_limit,
            since_date=args.since_date,
            season=args.season,
        )
    )


if __name__ == "__main__":
    cli()
