#!/usr/bin/env python3
# ============================================================
# tm_scraper.py – Transfermarkt Data Ingestion Pipeline
# ============================================================
"""
Thu thập dữ liệu từ Transfermarkt:
  1. Team Metadata   (stadium, manager, formation, logo)
  2. Market Values   (giá trị chuyển nhượng từng cầu thủ)

Chiến lược bypass Cloudflare:
──────────────────────────────────────────────────────────────
Transfermarkt sử dụng Cloudflare JS Challenge → dùng nodriver
(undetected Chrome) ở chế độ headed.

Hỗ trợ multi-league:
──────────────────────────────────────────────────────────────
  python tm_scraper.py                           # EPL (mặc định)
  python tm_scraper.py --league LALIGA           # La Liga
  python tm_scraper.py --league BUNDESLIGA       # Bundesliga
  python tm_scraper.py --list-leagues            # Xem tất cả giải

Luồng thực thi:
  1. Mở Chrome (headed) → navigate to TM league page
  2. Chờ Cloudflare JS challenge pass
  3. Parse HTML → extract team links
  4. Navigate tới từng team page → extract metadata
  5. Navigate tới kader page → extract market values
  6. Export tất cả ra CSV

Rate limiting: 6 giây giữa mỗi page load
"""

from __future__ import annotations

import asyncio
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup

import config_tm as cfg
from config_tm import TMLeagueConfig, get_tm_config
from schemas_tm import (
    PlayerMarketValue,
    TeamMetadata,
    safe_parse_list,
)

# League registry (project root)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from league_registry import get_league, get_transfermarkt_leagues, list_leagues  # noqa: E402


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
# BROWSER SESSION – nodriver (tái sử dụng pattern từ fbref)
# ────────────────────────────────────────────────────────────

class TMBrowser:
    """
    Quản lý browser session cho Transfermarkt scraping.

    Lifecycle:
        async with TMBrowser() as tm:
            html = await tm.fetch("https://www.transfermarkt.com/...")
    """

    def __init__(self) -> None:
        self._browser = None
        self._page = None
        self._last_fetch_time: float = 0.0

    async def __aenter__(self) -> "TMBrowser":
        import nodriver as uc
        import os
        is_ci = os.environ.get("CI", "").lower() == "true"
        if is_ci:
            logger.info("▶ Khởi động Chrome browser (headless, CI mode) cho Transfermarkt…")
            self._browser = await uc.start(headless=True, sandbox=False)
        else:
            logger.info("▶ Khởi động Chrome browser (headed mode) cho Transfermarkt…")
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
        """Chờ Cloudflare JS challenge pass."""
        for i in range(cfg.TM_CF_WAIT_MAX):
            await asyncio.sleep(1)
            try:
                title = await self._page.evaluate("document.title")
                if "moment" not in title.lower() and "attention" not in title.lower():
                    logger.info("  ✓ Cloudflare passed sau %ds – %s", i + 1, title[:60])
                    return
            except Exception:
                pass
            if i > 0 and i % 10 == 0:
                logger.info("  ⏳ Chờ Cloudflare… (%ds)", i)
        logger.warning("  ⚠ Cloudflare timeout sau %ds", cfg.TM_CF_WAIT_MAX)

    async def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_fetch_time
        if elapsed < cfg.TM_DELAY_BETWEEN_PAGES:
            wait = cfg.TM_DELAY_BETWEEN_PAGES - elapsed
            logger.debug("  Rate limit: chờ %.1fs", wait)
            await asyncio.sleep(wait)

    async def fetch(self, url: str) -> str:
        """Navigate tới URL và trả về HTML."""
        await self._rate_limit()
        logger.info("📥 Fetching: %s", url)

        try:
            self._page = await self._browser.get(url)
            self._last_fetch_time = time.monotonic()

            # Kiểm tra Cloudflare
            try:
                title = await self._page.evaluate("document.title")
                if "moment" in title.lower() or "attention" in title.lower():
                    await self._wait_for_cf()
            except Exception:
                await asyncio.sleep(3)

            # Chờ content load
            await asyncio.sleep(cfg.TM_TABLE_LOAD_WAIT)

            html = await self._page.evaluate(
                "document.documentElement.outerHTML"
            )
            logger.info("  ✓ HTML: %d bytes", len(html))
            return html

        except Exception as exc:
            logger.error("  ✗ Fetch failed: %s – %s", url, exc)
            return ""


# ────────────────────────────────────────────────────────────
# HELPERS
# ────────────────────────────────────────────────────────────

def _parse_market_value_str(mv_str: str) -> float | None:
    """
    Parse market value string to numeric EUR.

    "€65.00m" → 65_000_000.0
    "€800k"   → 800_000.0
    "€1.23bn" → 1_230_000_000.0
    """
    if not mv_str:
        return None
    cleaned = mv_str.strip().replace("€", "").replace("$", "").replace("£", "")
    cleaned = cleaned.strip()
    if not cleaned or cleaned == "-":
        return None

    multiplier = 1.0
    if cleaned.endswith("bn"):
        multiplier = 1_000_000_000
        cleaned = cleaned[:-2]
    elif cleaned.endswith("m"):
        multiplier = 1_000_000
        cleaned = cleaned[:-1]
    elif cleaned.endswith("k"):
        multiplier = 1_000
        cleaned = cleaned[:-1]
    elif cleaned.endswith("Th."):
        multiplier = 1_000
        cleaned = cleaned[:-3]

    try:
        return float(cleaned.strip()) * multiplier
    except (ValueError, TypeError):
        return None


# ────────────────────────────────────────────────────────────
# STEP 1: Parse League Page → Team Links
# ────────────────────────────────────────────────────────────

def parse_league_page(html: str) -> list[dict[str, str]]:
    """
    Parse TM league overview page → extract team links.

    Returns:
        [{"name": "Arsenal FC", "team_id": "11", "url": "/arsenal-fc/..."}]
    """
    soup = BeautifulSoup(html, "html.parser")
    teams: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    # TM team links in the main table
    # Pattern: /verein/{team_id}/saison_id/
    for link in soup.find_all("a", href=re.compile(r"/startseite/verein/\d+/?")):
        href = link.get("href", "")
        m = re.search(r"/verein/(\d+)", href)
        if not m:
            continue
        team_id = m.group(1)
        if team_id in seen_ids:
            continue

        name = link.get("title") or link.text.strip()
        if not name or len(name) < 2:
            continue

        seen_ids.add(team_id)
        # Build full team URL
        slug_match = re.match(r"/([^/]+)/", href)
        slug = slug_match.group(1) if slug_match else ""

        teams.append({
            "name": name,
            "team_id": team_id,
            "slug": slug,
            "url": cfg.TM_BASE + href,
        })

    logger.info("  TM League page: %d teams found", len(teams))
    return teams


# ────────────────────────────────────────────────────────────
# STEP 2: Parse Team Page → Metadata
# ────────────────────────────────────────────────────────────

def parse_team_page(
    html: str,
    team_name: str,
    team_id: str,
    team_url: str = "",
    *,
    league_cfg: TMLeagueConfig | None = None,
) -> dict:
    """
    Parse TM team overview page → extract metadata.

    Extracts: stadium, manager, squad info, logo, formation.
    """
    soup = BeautifulSoup(html, "html.parser")

    meta: dict[str, Any] = {
        "team_name": team_name,
        "team_id": team_id,
        "team_url": team_url,
        "league_id": league_cfg.league_id if league_cfg else None,
        "season": league_cfg.season if league_cfg else None,
    }

    # ── Logo URL ──
    logo_img = soup.find("img", class_=re.compile(r"TM_LARGE|tiny_wappen|data-src"))
    if not logo_img:
        # Fallback: look for team header logo
        header = soup.find("div", class_=re.compile(r"data-header__profile-image"))
        if header:
            logo_img = header.find("img")
    if logo_img:
        meta["logo_url"] = logo_img.get("src") or logo_img.get("data-src")

    # ── Extract data from "dataHeader" section ──
    # Transfermarkt has structured data in spans with specific labels

    # Stadium
    stadium_link = soup.find("a", href=re.compile(r"/stadion/"))
    if stadium_link:
        meta["stadium_name"] = stadium_link.text.strip()
        meta["stadium_url"] = cfg.TM_BASE + stadium_link.get("href", "")

    # Stadium capacity — look near stadium name
    for span in soup.find_all("span", class_=re.compile(r"info-table__content")):
        label_span = span.find_previous("span", class_=re.compile(r"info-table__content--regular|info-table__content--bold"))
        text = span.text.strip()
        if "Seats" in text or "seats" in text:
            meta["stadium_capacity"] = text

    # Alternative: look for capacity in data-header or profile section
    if "stadium_capacity" not in meta:
        for text_node in soup.find_all(string=re.compile(r"[\d,.]+ Seats", re.I)):
            meta["stadium_capacity"] = text_node.strip()
            break

    # ── Manager / Coach ──
    coach_link = soup.find("a", href=re.compile(r"/trainer/"))
    if coach_link:
        meta["manager_name"] = coach_link.text.strip()
        meta["manager_url"] = cfg.TM_BASE + coach_link.get("href", "")

    # ── Extract info-table data ──
    for row in soup.find_all("li", class_=re.compile(r"data-header__details")):
        label_el = row.find("span", class_=re.compile(r"data-header__label"))
        value_el = row.find("span", class_=re.compile(r"data-header__content"))
        if not label_el or not value_el:
            continue
        label = label_el.text.strip().lower()
        value = value_el.text.strip()

        if "squad size" in label:
            meta["squad_size"] = value
        elif "average age" in label or "avg age" in label:
            meta["avg_age"] = value
        elif "foreigner" in label:
            meta["num_foreigners"] = value
        elif "current transfer record" in label or "total market" in label:
            meta["total_market_value"] = value

    # ── Try alternative data extraction from profile blocks ──
    profile_items = soup.find_all("div", class_=re.compile(r"data-header__items|data-header__details"))
    for item in profile_items:
        text = item.get_text(separator=" ", strip=True)
        if "Squad size" in text:
            m = re.search(r"Squad size[:\s]*(\d+)", text)
            if m and "squad_size" not in meta:
                meta["squad_size"] = m.group(1)
        if "Average age" in text:
            m = re.search(r"Average age[:\s]*([\d.]+)", text)
            if m and "avg_age" not in meta:
                meta["avg_age"] = m.group(1)
        if "Foreigners" in text:
            m = re.search(r"Foreigners[:\s]*(\d+)", text)
            if m and "num_foreigners" not in meta:
                meta["num_foreigners"] = m.group(1)

    # ── Formation — extract from recent match or tactics section ──
    formation_pattern = re.compile(r"\b(\d-\d-\d(?:-\d)?(?:-\d)?)\b")
    # Look in page header or tactics section
    for el in soup.find_all(string=formation_pattern):
        match = formation_pattern.search(str(el))
        if match:
            meta["formation"] = match.group(1)
            break

    # ── Total market value from header ──
    if "total_market_value" not in meta:
        mv_el = soup.find("a", class_=re.compile(r"data-header__market-value-wrapper"))
        if mv_el:
            meta["total_market_value"] = mv_el.text.strip()

    logger.info("  %s: stadium=%s, manager=%s, formation=%s",
                team_name,
                meta.get("stadium_name", "?"),
                meta.get("manager_name", "?"),
                meta.get("formation", "?"))
    return meta


# ────────────────────────────────────────────────────────────
# STEP 3: Parse Kader Page → Market Values
# ────────────────────────────────────────────────────────────

def parse_kader_page(
    html: str,
    team_name: str,
    team_id: str,
    *,
    league_cfg: TMLeagueConfig | None = None,
) -> list[dict]:
    """
    Parse TM kader page → extract player market values.

    URL: /{slug}/kader/verein/{id}/saison_id/{year}/plus/1
    Table with class "items" contains all players.
    """
    soup = BeautifulSoup(html, "html.parser")
    players: list[dict] = []

    # Find the main squad table
    table = soup.find("table", class_=re.compile(r"items"))
    if not table:
        logger.warning("  %s: Không tìm thấy bảng cầu thủ", team_name)
        return players

    tbody = table.find("tbody")
    if not tbody:
        return players

    for row in tbody.find_all("tr", class_=re.compile(r"odd|even")):
        player: dict[str, Any] = {
            "team_name": team_name,
            "team_id": team_id,
            "league_id": league_cfg.league_id if league_cfg else None,
            "season": league_cfg.season if league_cfg else None,
        }

        cells = row.find_all("td")
        if len(cells) < 4:
            continue

        # ── Shirt number ──
        number_cell = row.find("div", class_=re.compile(r"rn_nummer"))
        if number_cell:
            player["shirt_number"] = number_cell.text.strip()

        # ── Player name & links ──
        player_link = row.find("a", class_=re.compile(r"spielprofil_tooltip"))
        if not player_link:
            # Alternative selector
            player_link = row.find("a", href=re.compile(r"/profil/spieler/\d+"))
        if player_link:
            player["player_name"] = player_link.text.strip()
            href = player_link.get("href", "")
            player["player_url"] = cfg.TM_BASE + href
            m = re.search(r"/spieler/(\d+)", href)
            if m:
                player["player_id"] = m.group(1)
        else:
            # Try any text in the hauptlink cell
            hauptlink = row.find("td", class_=re.compile(r"hauptlink"))
            if hauptlink:
                player["player_name"] = hauptlink.text.strip()

        if not player.get("player_name"):
            continue

        # ── Player image ──
        img = row.find("img", class_=re.compile(r"bilderrahmen"))
        if not img:
            img = row.find("img", {"data-src": True})
        if img:
            player["player_image_url"] = img.get("data-src") or img.get("src")

        # ── Position ──
        # Position is typically in a specific cell or in inline-table
        pos_cells = row.find_all("td", class_=re.compile(r"posrela"))
        for pc in pos_cells:
            pos_text = pc.find("tr", class_=re.compile(r""))
            if pos_text:
                # Position is usually in a sub-table
                rows_inner = pc.find_all("tr")
                for inner_row in rows_inner:
                    txt = inner_row.text.strip()
                    if txt and txt != player.get("player_name", ""):
                        player["position"] = txt
                        break

        # ── Date of birth ──
        for cell in cells:
            text = cell.text.strip()
            # Pattern: "Feb 13, 2001 (24)" or "(24)"
            dob_match = re.search(r"([A-Z][a-z]{2} \d{1,2}, \d{4})", text)
            if dob_match:
                player["date_of_birth"] = dob_match.group(1)
            age_match = re.search(r"\((\d{2})\)", text)
            if age_match:
                player["age"] = age_match.group(1)

        # ── Nationality — flag images ──
        flag_imgs = row.find_all("img", class_=re.compile(r"flaggenrahmen"))
        if flag_imgs:
            player["nationality"] = flag_imgs[0].get("title", "")
            if len(flag_imgs) > 1:
                player["second_nationality"] = flag_imgs[1].get("title", "")

        # ── Height ──
        for cell in cells:
            text = cell.text.strip()
            height_match = re.search(r"(\d{1,2}[,.]?\d{0,2})\s*m\b", text)
            if height_match:
                h_str = height_match.group(1).replace(",", ".")
                try:
                    player["height_cm"] = int(float(h_str) * 100)
                except (ValueError, TypeError):
                    pass

        # ── Foot ──
        for cell in cells:
            text = cell.text.strip().lower()
            if text in ("right", "left", "both"):
                player["foot"] = text
                break

        # ── Contract / Joined ──
        for cell in cells:
            text = cell.text.strip()
            joined_match = re.search(r"([A-Z][a-z]{2} \d{1,2}, \d{4})", text)
            if joined_match and "joined" not in player:
                # Distinguish joined vs contract — joined comes first
                if "date_of_birth" in player and text == player.get("date_of_birth"):
                    continue
                player.setdefault("joined", joined_match.group(1))

        # ── Market value — always last visible column ──
        mv_cell = row.find("td", class_=re.compile(r"rechts hauptlink"))
        if mv_cell:
            mv_text = mv_cell.text.strip()
            player["market_value"] = mv_text
            player["market_value_numeric"] = _parse_market_value_str(mv_text)

        players.append(player)

    logger.info("  %s: %d players with market values", team_name, len(players))
    return players


# ────────────────────────────────────────────────────────────
# STEP 4: Export → CSV
# ────────────────────────────────────────────────────────────

def export_tm_data(
    team_metadata: list,
    market_values: list,
    *,
    league_cfg: TMLeagueConfig | None = None,
) -> list[str]:
    """Export Transfermarkt data ra CSV files."""
    exported: list[str] = []

    if not league_cfg:
        league_cfg = get_tm_config("EPL")

    output_dir = league_cfg.output_dir

    if team_metadata:
        df = pd.DataFrame([t.model_dump(by_alias=False) for t in team_metadata])
        path = output_dir / league_cfg.team_metadata_csv
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported team metadata → %s (%d rows)", path, len(df))
        exported.append(str(path))

    if market_values:
        df = pd.DataFrame([p.model_dump(by_alias=False) for p in market_values])
        path = output_dir / league_cfg.market_values_csv
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info("Exported market values → %s (%d rows)", path, len(df))
        exported.append(str(path))

    return exported


# ────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ────────────────────────────────────────────────────────────

async def main(
    league_id: str = "EPL",
    team_limit: int = 0,
    metadata_only: bool = False,
) -> dict[str, list]:
    """
    Transfermarkt Data Pipeline – entry point chính.

    Args:
        league_id:     League ID từ registry (VD: "EPL", "LALIGA").
        team_limit:    Giới hạn số đội scrape (0 = tất cả).
        metadata_only: Chỉ scrape team metadata (bỏ market values).

    Returns:
        dict với keys: "team_metadata", "market_values"
    """
    league_cfg = get_tm_config(league_id)

    t_start = time.perf_counter()
    logger.info("=" * 60)
    logger.info("TRANSFERMARKT DATA PIPELINE – BẮT ĐẦU")
    logger.info("  League: %s (comp=%s)", league_id, league_cfg.tm_comp_id)
    logger.info("  Season: %s", league_cfg.season)
    logger.info("  URL:    %s", league_cfg.league_url)
    logger.info("=" * 60)

    all_metadata: list = []
    all_market_values: list = []

    async with TMBrowser() as browser:
        # ── STEP 1: League page → team links ──
        logger.info("━━ STEP 1: Scrape League Page ━━")
        html = await browser.fetch(league_cfg.league_url)
        if not html:
            logger.error("Không thể tải TM league page. Dừng pipeline.")
            return {}

        team_links = parse_league_page(html)
        if team_limit > 0:
            team_links = team_links[:team_limit]
            logger.info("Giới hạn: %d đội", team_limit)

        total_teams = len(team_links)

        # ── STEP 2: Team pages → metadata ──
        logger.info("━━ STEP 2: Scrape %d Team Pages ━━", total_teams)
        for idx, team in enumerate(team_links, 1):
            logger.info("── [%d/%d] %s ──", idx, total_teams, team["name"])

            # Team overview page
            team_html = await browser.fetch(team["url"])
            if not team_html:
                logger.warning("  ✗ Không tải được team page cho %s", team["name"])
                continue

            meta = parse_team_page(
                team_html,
                team_name=team["name"],
                team_id=team["team_id"],
                team_url=team["url"],
                league_cfg=league_cfg,
            )
            all_metadata.append(meta)

            # ── STEP 3: Kader page → market values ──
            if not metadata_only:
                kader_url = (
                    f"{cfg.TM_BASE}/{team['slug']}/kader/verein/"
                    f"{team['team_id']}/saison_id/{league_cfg.season}/plus/1"
                )
                kader_html = await browser.fetch(kader_url)
                if kader_html:
                    players = parse_kader_page(
                        kader_html,
                        team_name=team["name"],
                        team_id=team["team_id"],
                        league_cfg=league_cfg,
                    )
                    all_market_values.extend(players)
                else:
                    logger.warning("  ✗ Không tải được kader page cho %s", team["name"])

    # ── Validate ──
    valid_metadata = safe_parse_list(TeamMetadata, all_metadata, context_label="team_metadata")
    valid_mvs = safe_parse_list(PlayerMarketValue, all_market_values, context_label="market_values")

    # ── Export ──
    logger.info("━━ STEP 4: Export CSV ━━")
    exported = export_tm_data(valid_metadata, valid_mvs, league_cfg=league_cfg)

    # ── Summary ──
    elapsed = time.perf_counter() - t_start
    logger.info("=" * 60)
    logger.info("TRANSFERMARKT PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info("  Teams metadata  : %d", len(valid_metadata))
    logger.info("  Market values   : %d", len(valid_mvs))
    logger.info("  CSV files       : %d", len(exported))
    logger.info("  Thời gian       : %.1f giây", elapsed)
    logger.info("=" * 60)

    return {
        "team_metadata": valid_metadata,
        "market_values": valid_mvs,
    }


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────

def cli() -> None:
    """Command-line interface cho Transfermarkt scraper."""
    import argparse

    supported = [lg.league_id for lg in get_transfermarkt_leagues()]

    parser = argparse.ArgumentParser(
        description="Transfermarkt Data Pipeline – Team Metadata & Market Values",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Giải đấu hỗ trợ (Transfermarkt): {', '.join(supported)}

Dữ liệu thu thập:
  • Team Metadata (logo, stadium, manager, formation, squad info)
  • Player Market Values (giá trị chuyển nhượng, vị trí, tuổi, quốc tịch)

Ví dụ:
  python tm_scraper.py                              # EPL (mặc định)
  python tm_scraper.py --league LALIGA              # La Liga
  python tm_scraper.py --league BUNDESLIGA --limit 3  # Bundesliga, 3 đội
  python tm_scraper.py --metadata-only              # Chỉ team metadata
  python tm_scraper.py --list-leagues               # Xem tất cả giải
        """,
    )
    parser.add_argument(
        "--league",
        default="EPL",
        help=f"League ID (default: EPL). Choices: {', '.join(supported)}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Giới hạn số đội scrape (0 = tất cả)",
    )
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Chỉ scrape team metadata (bỏ market values)",
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
    try:
        league_check = get_league(league_id)
        if not league_check.has_transfermarkt:
            logger.error("League '%s' không có trên Transfermarkt. Dùng --list-leagues.", league_id)
            return
    except KeyError as e:
        logger.error(str(e))
        return

    asyncio.run(main(
        league_id=league_id,
        team_limit=args.limit,
        metadata_only=args.metadata_only,
    ))


if __name__ == "__main__":
    cli()
