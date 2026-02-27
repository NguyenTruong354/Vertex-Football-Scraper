#!/usr/bin/env python3
# ============================================================
# fbref_scraper.py – FBref Data Ingestion Pipeline
# ============================================================
"""
Thu thập dữ liệu từ FBref cho 4 Critical items:
  1. League Standings  (BXH)
  2. Player Profiles   (tên, tuổi, quốc tịch, vị trí)
  3. Player Season Stats (goals, assists, minutes, cards…)
  4. Squad Stats       (team-level aggregates)

Chiến lược bypass Cloudflare:
──────────────────────────────────────────────────────────────
FBref sử dụng Cloudflare JS Challenge → chặn mọi HTTP client
(aiohttp, requests, curl_cffi, cloudscraper).

Giải pháp: dùng ``nodriver`` (undetected Chrome) ở chế độ
headed (có cửa sổ trình duyệt).  Cửa sổ sẽ mở khoảng 1-2 phút
rồi tự đóng khi pipeline hoàn thành.

Luồng thực thi:
  1. Mở Chrome (headed) → navigate to FBref league page
  2. Chờ Cloudflare JS challenge pass (~10-15s lần đầu)
  3. Parse HTML → standings + squad stats + team links
  4. Lần lượt navigate tới 20 squad pages (reuse session)
  5. Parse HTML → player profiles + player season stats
  6. Export tất cả ra CSV

Rate limiting: 5 giây giữa mỗi page load
─────────────────────────────────────────────────────────────

Usage:
    # Scrape tất cả (standings + 20 squad pages)
    python fbref_scraper.py

    # Chỉ scrape standings (nhanh)
    python fbref_scraper.py --standings-only

    # Giới hạn số đội (test)
    python fbref_scraper.py --limit 3
"""

from __future__ import annotations

import asyncio
import logging
import re
import sys
import time
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup, Comment

import config_fbref as cfg
from schemas_fbref import (
    PlayerProfile,
    PlayerSeasonStats,
    SquadStats,
    StandingsRow,
    safe_parse_list,
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
        import nodriver as uc
        logger.info("▶ Khởi động Chrome browser (headed mode)…")
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
            html = await self._page.evaluate(
                "document.documentElement.outerHTML"
            )
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
            teams.append({
                "name": link.text.strip(),
                "team_id": m.group(1) if m else "",
                "url": cfg.FBREF_BASE + href,
            })
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
    standings_rows, _ = _parse_table_rows(
        soup, table_id_pattern=r"results.*_overall"
    )
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
    squad_rows, _ = _parse_table_rows(
        soup, table_id="stats_squads_standard_for"
    )
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
) -> dict[str, Any]:
    """
    Parse FBref squad page.

    Returns:
        {
            "profiles":     list[dict],  — player profiles
            "player_stats": list[dict],  — player season stats
        }
    """
    soup = BeautifulSoup(html, "html.parser")
    result: dict[str, Any] = {"profiles": [], "player_stats": []}

    # ── Standard stats table (player-level) ──
    std_rows, _ = _parse_table_rows(soup, table_id=cfg.SQUAD_STANDARD_TABLE_ID)

    # ── Shooting stats table ──
    shoot_rows, _ = _parse_table_rows(soup, table_id=cfg.SQUAD_SHOOTING_TABLE_ID)
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

    logger.info(
        "  %s: %d players, %d with shooting data",
        team_name,
        len(result["profiles"]),
        len(shoot_lookup),
    )
    return result


# ────────────────────────────────────────────────────────────
# STEP 3: Export → CSV
# ────────────────────────────────────────────────────────────

def export_fbref_data(
    standings: list,
    squad_stats: list,
    profiles: list,
    player_stats: list,
) -> list[str]:
    """Export tất cả dữ liệu FBref ra CSV files."""
    exported: list[str] = []

    # Standings
    if standings:
        df = pd.DataFrame([s.model_dump(by_alias=False) for s in standings])
        path = cfg.OUTPUT_DIR / cfg.STANDINGS_CSV
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported standings → %s (%d rows)", path, len(df))
        exported.append(str(path))

    # Squad stats
    if squad_stats:
        df = pd.DataFrame([s.model_dump(by_alias=False) for s in squad_stats])
        path = cfg.OUTPUT_DIR / cfg.SQUAD_STATS_CSV
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported squad stats → %s (%d rows)", path, len(df))
        exported.append(str(path))

    # Player profiles
    if profiles:
        df = pd.DataFrame([p.model_dump(by_alias=False) for p in profiles])
        path = cfg.OUTPUT_DIR / cfg.SQUAD_ROSTER_CSV
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported player profiles → %s (%d rows)", path, len(df))
        exported.append(str(path))

    # Player season stats
    if player_stats:
        df = pd.DataFrame([p.model_dump(by_alias=False) for p in player_stats])
        path = cfg.OUTPUT_DIR / cfg.PLAYER_SEASON_STATS_CSV
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported player stats → %s (%d rows)", path, len(df))
        exported.append(str(path))

    return exported


# ────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ────────────────────────────────────────────────────────────

async def main(
    standings_only: bool = False,
    team_limit: int = 0,
) -> dict[str, list]:
    """
    FBref Data Pipeline – entry point chính.

    Args:
        standings_only: Chỉ scrape standings (không scrape squad pages).
        team_limit:     Giới hạn số đội scrape (0 = tất cả 20 đội).

    Returns:
        dict với keys: "standings", "squad_stats", "profiles", "player_stats"
    """
    t_start = time.perf_counter()
    logger.info("=" * 60)
    logger.info("FBREF DATA PIPELINE – BẮT ĐẦU")
    logger.info("=" * 60)

    all_standings: list = []
    all_squad_stats: list = []
    all_profiles: list = []
    all_player_stats: list = []

    async with FBrefBrowser() as browser:
        # ── STEP 1: League page ──
        logger.info("━━ STEP 1: Scrape League Page ━━")
        html = await browser.fetch(cfg.FBREF_LEAGUE_URL)
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
        logger.info("✓ Standings: %d/%d teams validated",
                     len(all_standings), len(league_data["standings"]))

        # Validate squad stats
        all_squad_stats = safe_parse_list(
            SquadStats,
            league_data["squad_stats"],
            context_label="squad_stats",
        )
        logger.info("✓ Squad stats: %d teams validated", len(all_squad_stats))

        if standings_only:
            logger.info("--standings-only: bỏ qua squad pages.")
        else:
            # ── STEP 2: Squad pages ──
            team_links = league_data["team_links"]
            if team_limit > 0:
                team_links = team_links[:team_limit]
                logger.info("Giới hạn: %d/%d đội", team_limit, len(league_data["team_links"]))

            total_teams = len(team_links)
            logger.info("━━ STEP 2: Scrape %d Squad Pages ━━", total_teams)

            for idx, team in enumerate(team_links, 1):
                logger.info(
                    "── [%d/%d] %s ──", idx, total_teams, team["name"]
                )
                squad_html = await browser.fetch(team["url"])
                if not squad_html:
                    logger.warning("  ✗ Không tải được squad page cho %s", team["name"])
                    continue

                squad_data = parse_squad_page(
                    squad_html,
                    team_name=team["name"],
                    team_id=team["team_id"],
                )

                # Validate profiles
                valid_profiles = safe_parse_list(
                    PlayerProfile,
                    squad_data["profiles"],
                    context_label=f"profiles_{team['name']}",
                )
                all_profiles.extend(valid_profiles)

                # Validate player stats
                valid_stats = safe_parse_list(
                    PlayerSeasonStats,
                    squad_data["player_stats"],
                    context_label=f"stats_{team['name']}",
                )
                all_player_stats.extend(valid_stats)

                logger.info(
                    "  ✓ %s: %d profiles, %d player stats",
                    team["name"], len(valid_profiles), len(valid_stats),
                )

    # ── STEP 3: Export ──
    logger.info("━━ STEP 3: Export CSV ━━")
    exported = export_fbref_data(
        all_standings, all_squad_stats, all_profiles, all_player_stats,
    )

    # ── Summary ──
    elapsed = time.perf_counter() - t_start
    logger.info("=" * 60)
    logger.info("FBREF PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info("  Standings rows  : %d", len(all_standings))
    logger.info("  Squad stats     : %d", len(all_squad_stats))
    logger.info("  Player profiles : %d", len(all_profiles))
    logger.info("  Player stats    : %d", len(all_player_stats))
    logger.info("  CSV files       : %d", len(exported))
    logger.info("  Thời gian       : %.1f giây", elapsed)
    logger.info("=" * 60)

    return {
        "standings": all_standings,
        "squad_stats": all_squad_stats,
        "profiles": all_profiles,
        "player_stats": all_player_stats,
    }


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────

def cli() -> None:
    """Command-line interface cho FBref scraper."""
    import argparse

    parser = argparse.ArgumentParser(
        description="FBref EPL Data Pipeline – Standings, Rosters, Player Stats",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ:
  python fbref_scraper.py                    # Scrape tất cả (standings + 20 đội)
  python fbref_scraper.py --standings-only   # Chỉ BXH
  python fbref_scraper.py --limit 3          # Test với 3 đội đầu tiên
        """,
    )
    parser.add_argument(
        "--standings-only",
        action="store_true",
        help="Chỉ scrape standings, bỏ qua squad pages",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Giới hạn số đội scrape (0 = tất cả)",
    )
    args = parser.parse_args()

    asyncio.run(main(standings_only=args.standings_only, team_limit=args.limit))


if __name__ == "__main__":
    cli()
