# ============================================================
# live_match.py — Live Match Tracker (cập nhật mỗi 1-2 phút)
# ============================================================
"""
Theo dõi trận đấu trực tiếp qua SofaScore API.
Cập nhật liên tục: tỉ số, sự kiện, thống kê, heatmap.

Usage:
    # Theo dõi bằng event_id
    python live_match.py 12436870

    # Tìm trận đang diễn ra hôm nay
    python live_match.py --today

    # Tìm trận của đội cụ thể
    python live_match.py --team Arsenal

    # Tùy chỉnh interval
    python live_match.py 12436870 --interval 60

    # Lưu vào DB
    python live_match.py 12436870 --save-db

    # Xuất CSV snapshot
    python live_match.py 12436870 --save-csv
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Add project root to path
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "sofascore"))

import nodriver as uc

import sofascore.config_sofascore as cfg

logger = logging.getLogger("live_match")


# ────────────────────────────────────────────────────────────
# Terminal colors (Windows compatible via ANSI)
# ────────────────────────────────────────────────────────────

# Enable ANSI on Windows
if sys.platform == "win32":
    os.system("")  # noqa: S605 — enables ANSI escape sequences

class C:
    """ANSI color codes."""
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RED     = "\033[91m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    BLUE    = "\033[94m"
    CYAN    = "\033[96m"
    WHITE   = "\033[97m"
    BG_RED  = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_BLUE = "\033[44m"


# ────────────────────────────────────────────────────────────
# Data structures
# ────────────────────────────────────────────────────────────

@dataclass
class LiveMatchState:
    """Trạng thái hiện tại của trận đấu."""
    event_id: int = 0
    home_team: str = ""
    away_team: str = ""
    home_score: int = 0
    away_score: int = 0
    status: str = "notstarted"        # notstarted, inprogress, finished
    minute: int = 0
    period: str = ""                  # 1st half, 2nd half, halftime, etc.
    start_timestamp: int = 0

    # Incidents (goals, cards, subs)
    incidents: list[dict] = field(default_factory=list)

    # Match statistics
    statistics: dict[str, dict] = field(default_factory=dict)
    # Format: {"possession": {"home": 55, "away": 45}, ...}

    # Lineups
    home_lineup: list[dict] = field(default_factory=list)
    away_lineup: list[dict] = field(default_factory=list)

    # Metadata
    tournament_name: str = ""
    round_num: int = 0
    venue: str = ""
    last_updated: str = ""
    poll_count: int = 0


# ────────────────────────────────────────────────────────────
# SofaScore Live Client (reuses nodriver)
# ────────────────────────────────────────────────────────────

class LiveClient:
    """Lightweight SofaScore client optimized for live polling."""

    def __init__(self) -> None:
        self._browser: uc.Browser | None = None
        self._tab = None
        self._last_request: float = 0.0

    async def start(self) -> None:
        self._browser = await uc.start()
        self._tab = await self._browser.get("https://www.sofascore.com/")
        await asyncio.sleep(3)
        logger.debug("Browser started")

    async def stop(self) -> None:
        if self._browser:
            try:
                self._browser.stop()
            except Exception:
                pass

    async def get_json(self, endpoint: str) -> dict | list | None:
        """GET SofaScore API endpoint → JSON."""
        url = f"{cfg.SS_API_BASE}{endpoint}"

        # Rate limit — minimum 1.5s between requests
        elapsed = time.monotonic() - self._last_request
        if elapsed < 1.5:
            await asyncio.sleep(1.5 - elapsed)

        try:
            self._tab = await self._browser.get(url)
            self._last_request = time.monotonic()
            await asyncio.sleep(1.2)

            content = await self._tab.get_content()
            json_str = None

            if "<pre" in content:
                import re
                m = re.search(r"<pre[^>]*>(.*?)</pre>", content, re.DOTALL)
                if m:
                    json_str = m.group(1).strip()
            elif content.strip().startswith(("{", "[")):
                json_str = content.strip()

            if not json_str:
                try:
                    json_str = await self._tab.evaluate("document.body.innerText")
                except Exception:
                    pass

            if json_str:
                json_str = (
                    json_str
                    .replace("&amp;", "&").replace("&lt;", "<")
                    .replace("&gt;", ">").replace("&quot;", '"')
                )
                data = json.loads(json_str)
                if isinstance(data, dict) and data.get("error"):
                    code = data["error"].get("code", 0)
                    if code == 404:
                        return None
                return data

        except Exception as exc:
            logger.debug("Request failed: %s — %s", url, exc)
        return None


# ────────────────────────────────────────────────────────────
# API fetchers
# ────────────────────────────────────────────────────────────

async def fetch_match_info(client: LiveClient, event_id: int) -> dict | None:
    """GET /event/{id} — tỉ số, phút, trạng thái."""
    return await client.get_json(f"/event/{event_id}")


async def fetch_incidents(client: LiveClient, event_id: int) -> list[dict]:
    """GET /event/{id}/incidents — goals, cards, subs."""
    data = await client.get_json(f"/event/{event_id}/incidents")
    if not data:
        return []
    return data.get("incidents", [])


async def fetch_statistics(client: LiveClient, event_id: int) -> dict[str, dict]:
    """
    GET /event/{id}/statistics → parsed stats.

    Returns: {"possession": {"home": 55, "away": 45}, "totalShots": {"home": 12, "away": 8}, ...}
    """
    data = await client.get_json(f"/event/{event_id}/statistics")
    if not data:
        return {}

    stats: dict[str, dict] = {}
    # API returns {"statistics": [{"period": "ALL", "groups": [{"groupName": ..., "statisticsItems": [...]}]}]}
    stat_periods = data.get("statistics", [])
    for period in stat_periods:
        if period.get("period") != "ALL":
            continue
        for group in period.get("groups", []):
            for item in group.get("statisticsItems", []):
                key = item.get("name", "")
                stats[key] = {
                    "home": item.get("home", ""),
                    "away": item.get("away", ""),
                    "type": item.get("statisticsType", ""),
                }
    return stats


async def fetch_lineups(client: LiveClient, event_id: int) -> dict:
    """GET /event/{id}/lineups → simplified lineups."""
    data = await client.get_json(f"/event/{event_id}/lineups")
    if not data:
        return {"home": [], "away": []}

    result: dict[str, list] = {"home": [], "away": []}
    for side in ("home", "away"):
        lineup_data = data.get(side, {})
        for p in lineup_data.get("players", []):
            pi = p.get("player", {})
            stats = p.get("statistics", {})
            result[side].append({
                "player_id": pi.get("id"),
                "name": pi.get("name") or pi.get("shortName"),
                "position": pi.get("position"),
                "jersey": pi.get("jerseyNumber"),
                "rating": stats.get("rating"),
                "minutes": stats.get("minutesPlayed"),
                "substitute": p.get("substitute", False),
            })
    return result


async def fetch_todays_matches(client: LiveClient) -> list[dict]:
    """GET /sport/football/scheduled-events/{date} — trận hôm nay."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data = await client.get_json(f"/sport/football/scheduled-events/{today}")
    if not data:
        return []

    matches = []
    for ev in data.get("events", []):
        home = ev.get("homeTeam", {})
        away = ev.get("awayTeam", {})
        status = ev.get("status", {})
        tournament = ev.get("tournament", {})
        hs = ev.get("homeScore", {})
        aws = ev.get("awayScore", {})

        matches.append({
            "event_id": ev.get("id"),
            "home_team": home.get("name", "?"),
            "away_team": away.get("name", "?"),
            "home_score": hs.get("current"),
            "away_score": aws.get("current"),
            "status": status.get("type", ""),
            "status_desc": status.get("description", ""),
            "minute": status.get("description", ""),
            "tournament": tournament.get("name", ""),
            "start_timestamp": ev.get("startTimestamp", 0),
        })

    return matches


# ────────────────────────────────────────────────────────────
# Display — Terminal UI
# ────────────────────────────────────────────────────────────

def clear_screen():
    os.system("cls" if sys.platform == "win32" else "clear")  # noqa: S605


def _render_score_banner(state: LiveMatchState) -> str:
    """Render tỉ số lớn giữa màn hình."""
    status_color = {
        "inprogress": C.GREEN,
        "finished": C.RED,
        "notstarted": C.YELLOW,
    }.get(state.status, C.WHITE)

    status_text = {
        "inprogress": f"LIVE {state.minute}'",
        "finished": "FULL TIME",
        "notstarted": "NOT STARTED",
    }.get(state.status, state.status.upper())

    lines = []
    lines.append(f"{C.DIM}{'─' * 60}{C.RESET}")
    if state.tournament_name:
        lines.append(f"{C.DIM}  {state.tournament_name}  •  Round {state.round_num}{C.RESET}")
    lines.append("")
    lines.append(f"  {C.BOLD}{C.WHITE}{state.home_team:>25s}{C.RESET}"
                 f"  {C.BOLD}{status_color} {state.home_score} - {state.away_score} {C.RESET}"
                 f"  {C.BOLD}{C.WHITE}{state.away_team:<25s}{C.RESET}")
    lines.append("")
    lines.append(f"  {'':>25s}  {status_color}{C.BOLD}  {status_text}  {C.RESET}")
    lines.append(f"{C.DIM}{'─' * 60}{C.RESET}")
    return "\n".join(lines)


def _render_incidents(state: LiveMatchState) -> str:
    """Render danh sách sự kiện."""
    if not state.incidents:
        return f"\n{C.DIM}  Chưa có sự kiện{C.RESET}\n"

    lines = [f"\n{C.BOLD}  ⚽ SỰ KIỆN{C.RESET}", ""]

    # Sort by time
    sorted_inc = sorted(state.incidents, key=lambda i: i.get("time", 0))

    for inc in sorted_inc:
        inc_type = inc.get("incidentType", "")
        minute = inc.get("time", "")
        added_time = inc.get("addedTime")
        time_str = f"{minute}'" if minute else ""
        if added_time:
            time_str = f"{minute}+{added_time}'"

        player = inc.get("player", {}).get("name", "")
        is_home = inc.get("isHome", None)
        side = "home" if is_home else "away"

        if inc_type == "goal":
            icon = f"{C.GREEN}⚽{C.RESET}"
            detail = f"{C.BOLD}{player}{C.RESET}"
            # Check own goal
            if inc.get("incidentClass") == "ownGoal":
                detail += f" {C.RED}(OG){C.RESET}"
            elif inc.get("incidentClass") == "penalty":
                detail += f" {C.YELLOW}(pen){C.RESET}"
        elif inc_type == "card":
            card_type = inc.get("incidentClass", "")
            if "red" in card_type.lower():
                icon = f"{C.RED}🟥{C.RESET}"
            elif "yellow" in card_type.lower():
                icon = f"{C.YELLOW}🟨{C.RESET}"
            else:
                icon = "🃏"
            detail = player
        elif inc_type == "substitution":
            icon = f"{C.CYAN}🔄{C.RESET}"
            player_in = inc.get("playerIn", {}).get("name", "?")
            player_out = inc.get("playerOut", {}).get("name", "?")
            detail = f"{C.GREEN}{player_in}{C.RESET} ← {C.RED}{player_out}{C.RESET}"
        elif inc_type == "varDecision":
            icon = f"{C.BLUE}📺{C.RESET}"
            detail = f"VAR: {inc.get('incidentClass', '')}"
        elif inc_type == "period":
            period_name = inc.get("text", "")
            lines.append(f"  {C.DIM}{'─' * 40}  {period_name}{C.RESET}")
            continue
        else:
            continue

        if is_home:
            lines.append(f"  {C.DIM}{time_str:>6s}{C.RESET}  {icon}  {detail}")
        else:
            lines.append(f"  {C.DIM}{time_str:>6s}{C.RESET}  {'':>30s}  {icon}  {detail}")

    lines.append("")
    return "\n".join(lines)


def _render_statistics(state: LiveMatchState) -> str:
    """Render bảng thống kê."""
    if not state.statistics:
        return ""

    lines = [f"{C.BOLD}  📊 THỐNG KÊ{C.RESET}", ""]

    # Priority stats to show
    priority_keys = [
        "Ball possession", "Total shots", "Shots on target", "Shots off target",
        "Corner kicks", "Fouls", "Offsides",
        "Passes", "Accurate passes", "Expected goals",
        "Big chances", "Big chances missed",
        "Goalkeeper saves", "Free kicks", "Throw-ins",
    ]

    # Show priority stats first, then others
    shown = set()
    for key in priority_keys:
        if key in state.statistics:
            _render_stat_line(lines, key, state.statistics[key])
            shown.add(key)

    # Show remaining
    for key, val in state.statistics.items():
        if key not in shown:
            _render_stat_line(lines, key, val)

    lines.append("")
    return "\n".join(lines)


def _render_stat_line(lines: list[str], key: str, val: dict) -> None:
    """Render 1 dòng thống kê dạng bar chart."""
    home_val = val.get("home", "")
    away_val = val.get("away", "")

    # Try to make a bar for percentage-type stats
    try:
        h = float(str(home_val).replace("%", ""))
        a = float(str(away_val).replace("%", ""))
        total = h + a if h + a > 0 else 1
        h_pct = h / total
        bar_len = 20
        h_bar = int(h_pct * bar_len)
        a_bar = bar_len - h_bar

        h_color = C.GREEN if h > a else (C.RED if h < a else C.WHITE)
        a_color = C.GREEN if a > h else (C.RED if a < h else C.WHITE)

        lines.append(
            f"  {h_color}{str(home_val):>8s}{C.RESET}"
            f"  {C.GREEN}{'█' * h_bar}{C.RESET}{C.RED}{'█' * a_bar}{C.RESET}"
            f"  {a_color}{str(away_val):<8s}{C.RESET}"
            f"  {C.DIM}{key}{C.RESET}"
        )
    except (ValueError, TypeError):
        lines.append(
            f"  {str(home_val):>8s}"
            f"  {'':>20s}"
            f"  {str(away_val):<8s}"
            f"  {C.DIM}{key}{C.RESET}"
        )


def _render_lineup(state: LiveMatchState) -> str:
    """Render lineup (starters only, with ratings)."""
    if not state.home_lineup and not state.away_lineup:
        return ""

    lines = [f"{C.BOLD}  👥 ĐỘI HÌNH{C.RESET}", ""]

    home_starters = [p for p in state.home_lineup if not p.get("substitute")]
    away_starters = [p for p in state.away_lineup if not p.get("substitute")]

    max_rows = max(len(home_starters), len(away_starters))
    for i in range(max_rows):
        h = home_starters[i] if i < len(home_starters) else {}
        a = away_starters[i] if i < len(away_starters) else {}

        h_rating = h.get("rating")
        a_rating = a.get("rating")
        h_r_str = f"{h_rating:.1f}" if h_rating else "   "
        a_r_str = f"{a_rating:.1f}" if a_rating else "   "

        h_name = h.get("name", "")[:20] if h else ""
        a_name = a.get("name", "")[:20] if a else ""
        h_num = h.get("jersey", "")
        a_num = a.get("jersey", "")

        lines.append(
            f"  {C.DIM}{str(h_num):>3s}{C.RESET} {h_name:<20s} {C.CYAN}{h_r_str}{C.RESET}"
            f"  │  "
            f"{C.DIM}{str(a_num):>3s}{C.RESET} {a_name:<20s} {C.CYAN}{a_r_str}{C.RESET}"
        )

    lines.append("")
    return "\n".join(lines)


def render_full(state: LiveMatchState) -> str:
    """Render toàn bộ dashboard."""
    parts = [
        "",
        f"{C.BOLD}{C.CYAN}  VERTEX LIVE MATCH TRACKER{C.RESET}",
        _render_score_banner(state),
        _render_incidents(state),
        _render_statistics(state),
        _render_lineup(state),
        f"{C.DIM}  Updated: {state.last_updated}  |  Poll #{state.poll_count}  "
        f"|  Ctrl+C to stop{C.RESET}",
        f"{C.DIM}{'─' * 60}{C.RESET}",
    ]
    return "\n".join(parts)


# ────────────────────────────────────────────────────────────
# Update logic
# ────────────────────────────────────────────────────────────

async def update_match_state(client: LiveClient, state: LiveMatchState) -> bool:
    """
    Poll tất cả endpoints → cập nhật state.
    Returns True nếu trận vẫn đang diễn ra, False nếu kết thúc.
    """
    state.poll_count += 1
    state.last_updated = datetime.now().strftime("%H:%M:%S")

    # 1. Match info (score, status, minute)
    info = await fetch_match_info(client, state.event_id)
    if info:
        ev = info.get("event", info)
        home = ev.get("homeTeam", {})
        away = ev.get("awayTeam", {})
        hs = ev.get("homeScore", {})
        aws = ev.get("awayScore", {})
        status = ev.get("status", {})
        tournament = ev.get("tournament", {})

        state.home_team = home.get("name", state.home_team)
        state.away_team = away.get("name", state.away_team)
        state.home_score = hs.get("current", state.home_score) or 0
        state.away_score = aws.get("current", state.away_score) or 0
        state.status = status.get("type", state.status)
        state.period = status.get("description", "")
        state.tournament_name = tournament.get("name", "")
        state.round_num = ev.get("roundInfo", {}).get("round", 0)
        state.start_timestamp = ev.get("startTimestamp", 0)

        # Tính phút hiện tại
        if state.status == "inprogress" and state.start_timestamp:
            now_ts = int(datetime.now(timezone.utc).timestamp())
            elapsed_seconds = now_ts - state.start_timestamp
            state.minute = max(0, elapsed_seconds // 60)
            # Cap at reasonable values
            if state.minute > 120:
                state.minute = 90  # Fallback

    # 2. Incidents
    incidents = await fetch_incidents(client, state.event_id)
    if incidents:
        state.incidents = incidents

    # 3. Statistics
    stats = await fetch_statistics(client, state.event_id)
    if stats:
        state.statistics = stats

    # 4. Lineups (less frequent — every 3rd poll)
    if state.poll_count % 3 == 1:
        lineups = await fetch_lineups(client, state.event_id)
        if lineups:
            state.home_lineup = lineups.get("home", [])
            state.away_lineup = lineups.get("away", [])

    return state.status != "finished"


# ────────────────────────────────────────────────────────────
# CSV / DB save
# ────────────────────────────────────────────────────────────

def save_snapshot_csv(state: LiveMatchState, output_dir: Path) -> None:
    """Lưu snapshot hiện tại ra CSV."""
    import pandas as pd

    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Stats snapshot
    if state.statistics:
        rows = []
        for key, val in state.statistics.items():
            rows.append({
                "event_id": state.event_id,
                "minute": state.minute,
                "stat_name": key,
                "home_value": val.get("home", ""),
                "away_value": val.get("away", ""),
                "timestamp": state.last_updated,
            })
        df = pd.DataFrame(rows)
        path = output_dir / f"live_stats_{state.event_id}_{ts}.csv"
        df.to_csv(path, index=False)
        logger.info("  CSV saved: %s", path.name)

    # Incidents snapshot
    if state.incidents:
        rows = []
        for inc in state.incidents:
            if inc.get("incidentType") in ("goal", "card", "substitution", "varDecision"):
                rows.append({
                    "event_id": state.event_id,
                    "type": inc.get("incidentType"),
                    "minute": inc.get("time"),
                    "added_time": inc.get("addedTime"),
                    "player": inc.get("player", {}).get("name", ""),
                    "is_home": inc.get("isHome"),
                    "detail": inc.get("incidentClass", ""),
                })
        if rows:
            df = pd.DataFrame(rows)
            path = output_dir / f"live_incidents_{state.event_id}_{ts}.csv"
            df.to_csv(path, index=False)


def save_to_db(state: LiveMatchState) -> int:
    """Lưu snapshot + incidents vào PostgreSQL. Returns rows saved."""
    rows_saved = 0
    try:
        from db.config_db import get_connection
        conn = get_connection()
        cur = conn.cursor()

        # 1. Upsert live_snapshots
        cur.execute("""
            INSERT INTO live_snapshots
                (event_id, home_team, away_team, home_score, away_score,
                 status, minute, statistics_json, incidents_json, poll_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                status = EXCLUDED.status,
                minute = EXCLUDED.minute,
                statistics_json = EXCLUDED.statistics_json,
                incidents_json = EXCLUDED.incidents_json,
                poll_count = EXCLUDED.poll_count,
                loaded_at = NOW()
        """, (
            state.event_id, state.home_team, state.away_team,
            state.home_score, state.away_score,
            state.status, state.minute,
            json.dumps(state.statistics, ensure_ascii=False),
            json.dumps(state.incidents, ensure_ascii=False),
            state.poll_count,
        ))
        rows_saved += 1

        # 2. Upsert live_incidents (goals, cards, subs, VAR)
        for inc in state.incidents:
            inc_type = inc.get("incidentType", "")
            if inc_type not in ("goal", "card", "substitution", "varDecision"):
                continue
            player_name = inc.get("player", {}).get("name")
            player_in = inc.get("playerIn", {}).get("name")
            player_out = inc.get("playerOut", {}).get("name")
            cur.execute("""
                INSERT INTO live_incidents
                    (event_id, incident_type, minute, added_time,
                     player_name, player_in_name, player_out_name,
                     is_home, detail)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id, incident_type, minute, COALESCE(player_name, ''))
                DO UPDATE SET
                    added_time     = EXCLUDED.added_time,
                    player_in_name = EXCLUDED.player_in_name,
                    player_out_name= EXCLUDED.player_out_name,
                    is_home        = EXCLUDED.is_home,
                    detail         = EXCLUDED.detail,
                    loaded_at      = NOW()
            """, (
                state.event_id, inc_type,
                inc.get("time"), inc.get("addedTime"),
                player_name, player_in, player_out,
                inc.get("isHome"), inc.get("incidentClass", ""),
            ))
            rows_saved += 1

        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        logger.warning("  DB save failed: %s", exc)
    return rows_saved


# ────────────────────────────────────────────────────────────
# DB Query — xem dữ liệu live từ PostgreSQL
# ────────────────────────────────────────────────────────────

def query_live_from_db(event_id: int | None = None) -> None:
    """Truy vấn và hiển thị dữ liệu live từ DB."""
    from db.config_db import get_connection
    conn = get_connection()
    cur = conn.cursor()

    # If no event_id, show all tracked matches
    if event_id is None:
        cur.execute("""
            SELECT event_id, home_team, away_team, home_score, away_score,
                   status, minute, poll_count, loaded_at
            FROM live_snapshots
            ORDER BY loaded_at DESC
        """)
        rows = cur.fetchall()
        if not rows:
            print(f"\n{C.YELLOW}  No live data in DB yet.{C.RESET}\n")
            cur.close(); conn.close()
            return

        print(f"\n{C.BOLD}{C.CYAN}  LIVE MATCHES IN DATABASE{C.RESET}")
        print(f"{C.DIM}{'─' * 70}{C.RESET}\n")
        print(f"  {'ID':>10s}  {'Home':>18s}  {'Score':^5s}  {'Away':<18s}  {'Status':<12s}  {'Min':>3s}  {'Polls':>5s}  {'Updated'}")
        print(f"{C.DIM}  {'─'*10}  {'─'*18}  {'─'*5}  {'─'*18}  {'─'*12}  {'─'*3}  {'─'*5}  {'─'*19}{C.RESET}")
        for r in rows:
            eid, ht, at, hs, aws, st, mn, pc, upd = r
            st_color = C.GREEN if st == "inprogress" else (C.RED if st == "finished" else C.YELLOW)
            print(f"  {eid:>10d}  {ht:>18s}  {C.BOLD}{hs}-{aws}{C.RESET:^5s}  {at:<18s}  {st_color}{st:<12s}{C.RESET}  {mn:>3d}  {pc:>5d}  {upd}")
        print()
        cur.close(); conn.close()
        return

    # ── Detail view for specific event ──
    cur.execute("""
        SELECT event_id, home_team, away_team, home_score, away_score,
               status, minute, statistics_json, incidents_json, poll_count, loaded_at
        FROM live_snapshots WHERE event_id = %s
    """, (event_id,))
    row = cur.fetchone()
    if not row:
        print(f"\n{C.YELLOW}  Event {event_id} not found in DB.{C.RESET}\n")
        cur.close(); conn.close()
        return

    eid, ht, at, hs, aws_sc, st, mn, stats_json, inc_json, pc, upd = row

    # Reconstruct state for rendering
    state = LiveMatchState(
        event_id=eid, home_team=ht, away_team=at,
        home_score=hs or 0, away_score=aws_sc or 0,
        status=st or "", minute=mn or 0,
        poll_count=pc or 0,
        last_updated=str(upd),
    )
    if stats_json:
        state.statistics = stats_json if isinstance(stats_json, dict) else json.loads(stats_json)
    if inc_json:
        state.incidents = inc_json if isinstance(inc_json, list) else json.loads(inc_json)

    # Render full dashboard from DB data
    print(render_full(state))

    # ── Also show incidents from live_incidents table ──
    cur.execute("""
        SELECT incident_type, minute, added_time, player_name,
               player_in_name, player_out_name, is_home, detail, loaded_at
        FROM live_incidents
        WHERE event_id = %s
        ORDER BY minute, loaded_at
    """, (event_id,))
    inc_rows = cur.fetchall()

    if inc_rows:
        print(f"\n{C.BOLD}  DB: live_incidents table ({len(inc_rows)} rows){C.RESET}")
        print(f"  {'Type':<15s}  {'Min':>4s}  {'Player':<25s}  {'Detail':<15s}  {'Home?':>5s}  {'Saved at'}")
        print(f"{C.DIM}  {'─'*15}  {'─'*4}  {'─'*25}  {'─'*15}  {'─'*5}  {'─'*19}{C.RESET}")
        for ir in inc_rows:
            it, imin, iadd, pname, pin, pout, ihome, idet, iat = ir
            time_str = f"{imin}'" if imin else ""
            if iadd:
                time_str = f"{imin}+{iadd}'"
            pdisp = pname or ""
            if it == "substitution":
                pdisp = f"{pin or '?'} <- {pout or '?'}"
            side = "HOME" if ihome else "AWAY"
            print(f"  {it:<15s}  {time_str:>4s}  {pdisp:<25s}  {idet or '':<15s}  {side:>5s}  {iat}")
    else:
        print(f"\n{C.DIM}  DB: No incidents recorded yet.{C.RESET}")

    # ── Summary statistics from JSONB ──
    if state.statistics:
        print(f"\n{C.BOLD}  DB: statistics_json ({len(state.statistics)} stats){C.RESET}")
        priority = ["Ball possession", "Expected goals", "Total shots",
                     "Shots on target", "Corner kicks", "Fouls", "Passes"]
        for key in priority:
            if key in state.statistics:
                v = state.statistics[key]
                print(f"    {ht:>20s} {str(v.get('home','')):>8s}  |  {str(v.get('away','')):>8s} {at}")
        remaining = len(state.statistics) - len([k for k in priority if k in state.statistics])
        if remaining > 0:
            print(f"    {C.DIM}... and {remaining} more stats{C.RESET}")

    print(f"\n{C.DIM}  Data loaded_at: {upd}  |  poll_count: {pc}{C.RESET}\n")
    cur.close()
    conn.close()


# ────────────────────────────────────────────────────────────
# Today's matches — discovery
# ────────────────────────────────────────────────────────────

async def show_todays_matches(client: LiveClient, team_filter: str | None = None) -> int | None:
    """Hiển thị trận hôm nay, cho phép chọn. Returns event_id."""
    matches = await fetch_todays_matches(client)
    if not matches:
        print(f"\n{C.YELLOW}  Không tìm thấy trận nào hôm nay.{C.RESET}\n")
        return None

    # Filter by team name if specified
    if team_filter:
        tf = team_filter.lower()
        matches = [
            m for m in matches
            if tf in m["home_team"].lower() or tf in m["away_team"].lower()
        ]
        if not matches:
            print(f"\n{C.YELLOW}  Không tìm thấy trận nào có '{team_filter}'.{C.RESET}\n")
            return None

    # Sort: inprogress first, then by start time
    status_order = {"inprogress": 0, "notstarted": 1, "finished": 2}
    matches.sort(key=lambda m: (status_order.get(m["status"], 9), m["start_timestamp"]))

    print(f"\n{C.BOLD}{C.CYAN}  TRẬN ĐẤU HÔM NAY{C.RESET}")
    print(f"{C.DIM}{'─' * 60}{C.RESET}\n")

    for i, m in enumerate(matches, 1):
        status = m["status"]
        if status == "inprogress":
            st = f"{C.GREEN}● LIVE{C.RESET}"
        elif status == "finished":
            st = f"{C.RED}  FT{C.RESET}"
        else:
            # Format start time
            try:
                start_dt = datetime.fromtimestamp(m["start_timestamp"], tz=timezone.utc)
                st = f"{C.DIM}  {start_dt.strftime('%H:%M')}{C.RESET}"
            except Exception:
                st = f"{C.DIM}  --:--{C.RESET}"

        score_home = m.get("home_score")
        score_away = m.get("away_score")
        if score_home is not None and score_away is not None:
            score_str = f"{score_home}-{score_away}"
        else:
            score_str = "vs"

        tournament = m.get("tournament", "")
        print(
            f"  {C.BOLD}{i:>3d}{C.RESET}  "
            f"{st}  "
            f"{m['home_team']:>22s} {C.BOLD}{score_str:^5s}{C.RESET} {m['away_team']:<22s}"
            f"  {C.DIM}{tournament}{C.RESET}"
        )

    print(f"\n{C.DIM}{'─' * 60}{C.RESET}")

    # Let user choose
    if len(matches) == 1:
        return matches[0]["event_id"]

    try:
        choice = input(f"\n  Chọn trận (1-{len(matches)}): ").strip()
        idx = int(choice) - 1
        if 0 <= idx < len(matches):
            return matches[idx]["event_id"]
    except (ValueError, EOFError, KeyboardInterrupt):
        pass

    return None


# ────────────────────────────────────────────────────────────
# Main loop
# ────────────────────────────────────────────────────────────

async def live_track(
    event_id: int,
    *,
    interval: int = 60,
    save_csv: bool = False,
    save_db: bool = False,
) -> None:
    """Main loop: poll → render → wait → repeat."""
    state = LiveMatchState(event_id=event_id)
    output_dir = ROOT / "output" / "live"
    shutdown = asyncio.Event()

    # Handle Ctrl+C
    def _on_signal(*_):
        shutdown.set()

    if sys.platform != "win32":
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, _on_signal)
        loop.add_signal_handler(signal.SIGTERM, _on_signal)

    client = LiveClient()
    await client.start()

    try:
        while not shutdown.is_set():
            try:
                still_playing = await update_match_state(client, state)

                # Render
                clear_screen()
                print(render_full(state))

                # Save
                db_rows = 0
                if save_csv:
                    save_snapshot_csv(state, output_dir)
                if save_db:
                    db_rows = save_to_db(state)
                    print(f"  {C.GREEN}DB saved: {db_rows} rows upserted{C.RESET}")

                if not still_playing:
                    print(f"\n  {C.YELLOW}Match finished!{C.RESET}")
                    # Final save
                    if save_csv:
                        save_snapshot_csv(state, output_dir)
                    if save_db:
                        final_rows = save_to_db(state)
                        print(f"  {C.GREEN}Final DB save: {final_rows} rows{C.RESET}")
                    break

                # Wait interval (interruptible)
                logger.debug("Sleeping %ds...", interval)
                try:
                    await asyncio.wait_for(shutdown.wait(), timeout=interval)
                    break  # shutdown was set
                except asyncio.TimeoutError:
                    pass  # Normal — continue polling

            except KeyboardInterrupt:
                break
            except Exception as exc:
                logger.warning("Poll error: %s — retrying in %ds", exc, interval)
                try:
                    await asyncio.wait_for(shutdown.wait(), timeout=interval)
                    break
                except asyncio.TimeoutError:
                    pass

    finally:
        await client.stop()
        print(f"\n{C.DIM}  Tracker stopped.{C.RESET}\n")


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────

async def async_main() -> None:
    parser = argparse.ArgumentParser(
        description="Vertex Live Match Tracker -- Real-time match tracking via SofaScore",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python live_match.py 12436870                     # Track specific event_id
  python live_match.py --today                      # Show today's matches
  python live_match.py --team Arsenal               # Find Arsenal's match
  python live_match.py 12436870 --interval 120      # Update every 2 min
  python live_match.py 12436870 --save-db           # Save to PostgreSQL
  python live_match.py 12436870 --save-csv          # Export CSV snapshots
        """,
    )
    parser.add_argument("event_id", nargs="?", type=int, default=None,
                        help="SofaScore event ID")
    parser.add_argument("--today", action="store_true",
                        help="Show today's matches and pick one")
    parser.add_argument("--team", type=str, default=None,
                        help="Filter by team name (e.g. Arsenal)")
    parser.add_argument("--interval", type=int, default=60,
                        help="Poll interval in seconds (default: 60)")
    parser.add_argument("--save-csv", action="store_true",
                        help="Save CSV snapshot each poll")
    parser.add_argument("--save-db", action="store_true",
                        help="Save to live_snapshots table in PostgreSQL")
    parser.add_argument("--query", nargs="?", const="all", default=None,
                        metavar="EVENT_ID",
                        help="Query live data from DB (no EVENT_ID = list all)")

    args = parser.parse_args()

    # ── Query mode: read from DB, no scraping ──
    if args.query is not None:
        if args.query == "all":
            query_live_from_db()
        else:
            query_live_from_db(int(args.query))
        return

    event_id = args.event_id

    # If no event_id, discover from today's matches
    if event_id is None or args.today or args.team:
        client = LiveClient()
        await client.start()
        try:
            event_id = await show_todays_matches(client, team_filter=args.team)
        finally:
            await client.stop()

        if event_id is None:
            print(f"\n{C.DIM}  Không chọn trận nào. Thoát.{C.RESET}\n")
            return

    print(f"\n{C.CYAN}  Bắt đầu theo dõi event_id={event_id}"
          f" | interval={args.interval}s{C.RESET}\n")
    await asyncio.sleep(1)

    await live_track(
        event_id,
        interval=args.interval,
        save_csv=args.save_csv,
        save_db=args.save_db,
    )


def main() -> None:
    # Force UTF-8 on Windows
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    logging.basicConfig(
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
        level=logging.WARNING,
    )
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
