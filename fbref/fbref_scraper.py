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

try:
    from fbref import config_fbref as cfg
    from fbref.config_fbref import FBrefLeagueConfig, get_fbref_config
except ImportError:
    import config_fbref as cfg
    from config_fbref import FBrefLeagueConfig, get_fbref_config

import pandas as pd
from bs4 import BeautifulSoup, Comment

try:
    from fbref import schemas_fbref as schemas
except ImportError:
    import schemas_fbref as schemas

try:
    from fbref.schemas_fbref import (
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
except ImportError:
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
    Quản lý browser session cho FBref scraping — Playwright CDP.

    Chiến lược: Khởi động Chrome thật với --remote-debugging-port,
    rồi kết nối Playwright qua CDP.  Cloudflare không thể phát hiện
    vì đây là Chrome thật, không phải automated browser.

    Lifecycle:
        async with FBrefBrowser() as fb:
            html = await fb.fetch("https://fbref.com/...")

    Tự động:
      • Mở Chrome thật khi __aenter__
      • Kết nối Playwright CDP
      • Chờ Cloudflare pass khi fetch()
      • Đóng Chrome khi __aexit__
      • Rate-limit giữa các request
    """

    _CDP_PORT = 9222

    def __init__(self) -> None:
        self._chrome_proc = None
        self._playwright = None
        self._browser = None
        self._page = None
        self._last_fetch_time: float = 0.0

    async def __aenter__(self) -> "FBrefBrowser":
        import os
        import shutil
        import socket
        import subprocess
        import sys

        is_linux = sys.platform == "linux"

        # Find Chrome executable
        if is_linux:
            chrome_path = shutil.which("google-chrome") or shutil.which("chromium-browser") or "/usr/bin/google-chrome"
        else:
            chrome_path = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
            if not Path(chrome_path).exists():
                chrome_path = r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"

        # Persistent profile dir (Cloudflare cookies survive between runs)
        profile_dir = Path(__file__).resolve().parent / ".chrome_cdp_profile"
        profile_dir.mkdir(parents=True, exist_ok=True)

        # Kill any existing Chrome with the same debugging port
        if is_linux:
            os.system(f"pkill -f 'remote-debugging-port={self._CDP_PORT}' 2>/dev/null")
        else:
            os.system("taskkill /f /im chrome.exe 2>nul")
        await asyncio.sleep(2)

        # Build Chrome args
        chrome_args = [
            chrome_path,
            f"--remote-debugging-port={self._CDP_PORT}",
            "--remote-debugging-address=127.0.0.1",
            f"--user-data-dir={profile_dir}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-popup-blocking",
            "--mute-audio",
        ]
        if is_linux:
            chrome_args += ["--headless=new", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"]
        
        chrome_args.append("about:blank")

        logger.info("▶ Launching Chrome CDP (profile: %s)…", profile_dir)
        self._chrome_proc = subprocess.Popen(
            chrome_args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Wait for CDP port to become available
        for attempt in range(30):
            await asyncio.sleep(1)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if sock.connect_ex(("127.0.0.1", self._CDP_PORT)) == 0:
                sock.close()
                logger.info("  ✓ Chrome CDP ready on port %d (took %ds)", self._CDP_PORT, attempt + 1)
                break
            sock.close()
        else:
            raise RuntimeError(f"Chrome failed to start on port {self._CDP_PORT}")

        # Connect Playwright via CDP
        from playwright.async_api import async_playwright
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.connect_over_cdp(
            f"http://127.0.0.1:{self._CDP_PORT}"
        )
        logger.info("  ✓ Playwright CDP connected")

        return self

    async def __aexit__(self, *args: Any) -> None:
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        try:
            if self._playwright:
                await self._playwright.stop()
        except Exception:
            pass
        if self._chrome_proc:
            try:
                self._chrome_proc.terminate()
                self._chrome_proc.wait(timeout=5)
            except Exception:
                try:
                    self._chrome_proc.kill()
                except Exception:
                    pass
        logger.info("✓ Chrome CDP closed.")

    async def _wait_for_cf(self, page) -> None:
        """Chờ Cloudflare JS challenge pass (title không còn 'Just a moment')."""
        for i in range(cfg.FBREF_CF_WAIT_MAX):
            await asyncio.sleep(1)
            try:
                title = await page.title()
                if "moment" not in title.lower():
                    logger.info("  ✓ Cloudflare passed sau %ds – %s", i + 1, title[:60])
                    return
            except Exception:
                pass
            if i > 0 and i % 10 == 0:
                logger.info("  ⏳ Chờ Cloudflare… (%ds)", i)
        logger.warning("  ⚠ Cloudflare timeout sau %ds", cfg.FBREF_CF_WAIT_MAX)

    async def _wait_for_tables(self, page) -> bool:
        """Chờ cho đến khi trang có ít nhất 1 <table>."""
        for _ in range(15):
            try:
                count = await page.evaluate(
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
        """Đảm bảo delay ngẫu nhiên để trông giống người thật hơn."""
        import random
        base_delay = cfg.FBREF_DELAY_BETWEEN_PAGES
        # Delay ngẫu nhiên từ 80% đến 150% của config
        jitter = random.uniform(base_delay * 0.8, base_delay * 1.5)
        
        elapsed = time.monotonic() - self._last_fetch_time
        if elapsed < jitter:
            wait = jitter - elapsed
            logger.debug("  Rate limit: chờ ngẫu nhiên %.1fs", wait)
            await asyncio.sleep(wait)

    async def _simulate_human(self, page) -> None:
        """Giả lập hành vi người dùng (scroll) để bypass behavioral checks."""
        import random
        try:
            # Scroll xuống một chút
            scroll_dist = random.randint(300, 700)
            await page.mouse.wheel(0, scroll_dist)
            await asyncio.sleep(random.uniform(0.5, 1.5))
            # Scroll lên lại
            await page.mouse.wheel(0, -random.randint(100, 300))
            await asyncio.sleep(random.uniform(0.2, 0.8))
        except Exception:
            pass

    async def fetch(self, url: str) -> str:
        """
        Navigate tới URL và trả về HTML sau khi page load xong.
        Xử lý thêm: Giả lập hành vi người dùng để tránh bị ban.
        """
        await self._rate_limit()
        logger.info("📥 Fetching: %s", url)

        try:
            context = self._browser.contexts[0]
            page = await context.new_page()
            self._page = page
            
            # Navigate
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            self._last_fetch_time = time.monotonic()

            # Giả lập người dùng trước khi check Cloudflare/Tables
            await self._simulate_human(page)

            # Kiểm tra Cloudflare
            try:
                title = await page.title()
                if "moment" in title.lower():
                    await self._wait_for_cf(page)
            except Exception:
                await asyncio.sleep(3)

            # Chờ tables
            has_tables = await self._wait_for_tables(page)
            if not has_tables:
                logger.warning("  ⚠ Không tìm thấy tables trên %s", url)

            # Lấy full HTML
            html = await page.content()
            logger.info("  ✓ HTML: %d bytes, tables=%s", len(html), has_tables)
            
            await page.close()
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
    table_obj: BeautifulSoup | None = None,
) -> tuple[list[dict], BeautifulSoup | None]:
    """
    Parse một FBref HTML table thành list[dict].

    Args:
        soup: BeautifulSoup parsed HTML
        table_id: Exact table id (e.g. "stats_standard_9")
        table_id_pattern: Regex pattern for table id
        table_obj: Trực tiếp truyền BeautifulSoup object của table

    Returns:
        (rows_as_dicts, table_element)
        Mỗi dict có keys = data-stat attributes.
    """
    table = table_obj
    if not table:
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
    Extract team links từ TẤT CẢ các standings tables (hỗ trợ UCL nhiều bảng).

    Returns:
        [{"name": "Arsenal", "team_id": "18bb7c10", "url": "/en/squads/..."}]
    """
    teams: list[dict[str, str]] = []
    seen_ids = set()

    # Tìm TẤT CẢ standings tables
    import re
    tables = soup.find_all("table", id=re.compile(r"results.*_overall"))
    
    for table in tables:
        tbody = table.find("tbody", recursive=False)
        if not tbody:
            continue
            
        for row in tbody.find_all("tr"):
            link = row.find("a", href=re.compile(r"/en/squads/"))
            if link:
                href = link["href"]
                m = re.search(r"/squads/([^/]+)/", href)
                team_id = m.group(1) if m else ""
                
                if team_id and team_id not in seen_ids:
                    teams.append(
                        {
                            "name": link.text.strip(),
                            "team_id": team_id,
                            "url": cfg.FBREF_BASE + href,
                        }
                    )
                    seen_ids.add(team_id)
    return teams


# ────────────────────────────────────────────────────────────
# STEP 1: Parse League Page → Standings + Squad Stats
# ────────────────────────────────────────────────────────────


def parse_league_page(html: str) -> dict[str, Any]:
    """
    Parse FBref league overview page.
    """
    soup = BeautifulSoup(html, "html.parser")

    # ── Unwrap tables hidden inside HTML comments (historical pages) ──
    from bs4 import Comment
    comments = soup.find_all(string=lambda t: isinstance(t, Comment))
    unwrapped = 0
    for comment in comments:
        c_str = str(comment)
        if "<table" in c_str.lower():
            # Replace the comment with its content so it becomes part of the DOM
            comment.replace_with(BeautifulSoup(c_str, "html.parser"))
            unwrapped += 1
    if unwrapped:
        logger.info("  📦 Unwrapped %d tables from HTML comments", unwrapped)

    result: dict[str, Any] = {
        "standings": [],
        "squad_stats": [],
        "team_links": [],
    }

    # 1. Standings table(s) - UCL has multiple groups
    all_standings_tables = soup.find_all("table", id=re.compile(r"^results.*_overall$"))
    
    all_standings_rows = []
    seen_standings_teams = set()
    
    for tbl in all_standings_tables:
        rows, _ = _parse_table_rows(soup, table_obj=tbl)
        for row in rows:
            tid = row.get("_team_id")
            if tid and tid not in seen_standings_teams:
                # Normalize keys for consistency
                if "_team_id" in row:
                    row["team_id"] = row.pop("_team_id")
                    row["team_url"] = row.pop("_team_url", None)
                all_standings_rows.append(row)
                seen_standings_teams.add(tid)
    
    result["standings"] = all_standings_rows
    logger.info("  Standings: %d teams parsed", len(all_standings_rows))

    # 2. Squad standard stats - Flexible ID lookup
    squad_table = soup.find("table", id=re.compile(r"^stats_squads_standard_.*"))
    if not squad_table:
        squad_table = soup.find("table", id=lambda x: x and "squads_standard" in x)
    
    if squad_table:
        squad_rows, _ = _parse_table_rows(soup, table_obj=squad_table)
        for row in squad_rows:
            if "_team_id" in row:
                row["team_id"] = row.pop("_team_id")
        result["squad_stats"] = squad_rows
        logger.info("  Squad stats: %d teams parsed", len(squad_rows))
    else:
        # Fallback for tournaments: Use standings data as basic squad stats if table is missing
        if all_standings_rows:
            logger.info("  Squad stats table missing, using standings as fallback")
            result["squad_stats"] = all_standings_rows
        else:
            logger.warning("  ⚠ Could not find squad stats table or standings")

    # 3. Team links
    result["team_links"] = _extract_team_links(soup)
    return result


def parse_squad_page(
    html: str,
    team_name: str,
    team_id: str,
    *,
    league_cfg: FBrefLeagueConfig | None = None,
) -> dict[str, Any]:
    """
    Parse FBref squad page.
    """
    soup = BeautifulSoup(html, "html.parser")

    # ── Unwrap tables ──
    from bs4 import Comment
    comments = soup.find_all(string=lambda t: isinstance(t, Comment))
    for comment in comments:
        c_str = str(comment)
        if "<table" in c_str.lower():
            comment.replace_with(BeautifulSoup(c_str, "html.parser"))

    result: dict[str, Any] = {
        "profiles": [],
        "player_stats": [],
        "gk_stats": [],
    }

    # Resolve Base table IDs
    std_base = "stats_standard"
    shoot_base = "stats_shooting"
    gk_base = "stats_keeper"
    season = league_cfg.season if league_cfg else cfg.FBREF_SEASON

    # ── Flexible lookup for tables (handles ID suffixes like _20, _9, etc.) ──
    std_table = soup.find("table", id=re.compile(f"^{std_base}.*"))
    shoot_table = soup.find("table", id=re.compile(f"^{shoot_base}.*"))
    gk_table = soup.find("table", id=re.compile(f"^{gk_base}.*"))

    # Parse rows
    std_rows, _ = _parse_table_rows(soup, table_obj=std_table)
    shoot_rows, _ = _parse_table_rows(soup, table_obj=shoot_table)
    
    # Merge shooting
    shoot_lookup: dict[str, dict] = {}
    for row in shoot_rows:
        pname = row.get("player", "")
        if pname: shoot_lookup[pname] = row

    for row in std_rows:
        player_name = row.get("player", "")
        if not player_name or player_name.lower() == "squad total":
            continue

        player_id = row.pop("_player_id", None)
        player_url = row.pop("_player_url", None)

        result["profiles"].append({
            "player": player_name,
            "player_id": player_id,
            "player_url": player_url,
            "nationality": row.get("nationality"),
            "position": row.get("position"),
            "age": row.get("age"),
            "team_name": team_name,
            "team_id": team_id,
            "season": season,
        })

        stats = dict(row)
        stats["player_id"] = player_id
        stats["team_name"] = team_name
        stats["team_id"] = team_id

        shoot_data = shoot_lookup.get(player_name, {})
        for key in ("shots", "shots_on_target", "shots_on_target_pct"):
            if key in shoot_data: stats[key] = shoot_data[key]

        for k in ("matches", "_player_id", "_player_url", "_team_id", "_team_url"):
            stats.pop(k, None)
        result["player_stats"].append(stats)

    # GK
    gk_rows, _ = _parse_table_rows(soup, table_obj=gk_table)
    for row in gk_rows:
        pname = row.get("player", "")
        if not pname or pname.lower() == "squad total": continue
        g = dict(row)
        g["player_id"] = g.pop("_player_id", None)
        g["team_name"] = team_name
        g["team_id"] = team_id
        g["season"] = season
        for k in ("_player_url", "_team_id", "_team_url"): g.pop(k, None)
        result["gk_stats"].append(g)

    logger.info("  %s: %d players", team_name, len(result["profiles"]))
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
                # ── Note: FBref removed match passing data (Opta/StatsBomb licensing) ──
                # Permanently skipping match report scraping to save time.
                matches_with_reports = []
                logger.info("  i FBref không còn cung cấp Match Passing data -> Bỏ qua cào Match Reports.")

                total_matches = len(matches_with_reports)
                if total_matches > 0:
                    logger.info("━━ STEP 4: Scrape %d Match Reports ━━", total_matches)

                    # ── Dedup: Load existing match_ids from CSV ──
                    existing_match_ids: set[str] = set()
                    passing_csv_path = league_cfg.output_dir / league_cfg.match_passing_csv
                    if passing_csv_path.exists():
                        try:
                            df_existing = pd.read_csv(passing_csv_path, usecols=["match_id"])
                            existing_match_ids = set(df_existing["match_id"].dropna().unique())
                            logger.info(
                                "  📋 Đã có %d match reports trong CSV → sẽ skip",
                                len(existing_match_ids),
                            )
                        except Exception:
                            pass

                    skipped = 0
                    for idx, fixture in enumerate(matches_with_reports, 1):
                        # Skip if already scraped
                        if fixture.match_id in existing_match_ids:
                            logger.info(
                                "── [%d/%d] SKIP %s vs %s (%s) (Đã lấy) ──",
                                idx, total_matches,
                                fixture.home_team or "?",
                                fixture.away_team or "?",
                                fixture.date or "?",
                            )
                            skipped += 1
                            continue

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

                    if skipped > 0:
                        logger.info("  ✓ Đã skip %d/%d match reports (đã cào trước đó)", skipped, total_matches)
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
