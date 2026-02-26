# ============================================================
# extractor.py – Logic bóc tách & chuyển đổi dữ liệu JSON
# ============================================================
"""
Module này chịu trách nhiệm:
  1. Parse JSON response từ Understat internal API.
  2. Flatten cấu trúc JSON lồng nhau thành dict phẳng.
  3. (Fallback) Dùng Regex trích xuất JSON từ HTML nếu API
     không khả dụng.

Kiến trúc dữ liệu Understat (2025+)
-------------------------------------
Understat sử dụng internal JSON API (AJAX endpoints):

  • ``GET /getMatchData/{match_id}``
    → Response JSON: ``{"rosters": {...}, "shots": {...}, "tmpl": "..."}``

  • ``GET /getLeagueData/{league}/{season}``
    → Response JSON: ``{"teams": {...}, "players": {...}, "dates": [...]}``

Frontend JS (``match.min.js``, ``league.min.js``) gọi các endpoint
này và render dữ liệu phía client.

Trang HTML vẫn nhúng ``match_info`` qua JSON.parse trong thẻ <script>,
nhưng shots & rosters data được tải riêng qua AJAX.

Fallback HTML Parsing
---------------------
Nếu vì lý do nào đó cần parse từ HTML (ví dụ match_info), ta vẫn
giữ lại logic Regex:

    var match_info = JSON.parse('\\x7B...\\x7D');

    Regex: var\\s+match_info\\s*=\\s*JSON\\.parse\\('(.+?)'\\)
    → capture group 1 → decode hex → json.loads()

Quy ước đặt tên hàm: ``extract_<loại_dữ_liệu>``
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
# PHẦN 1: XỬ LÝ JSON API RESPONSE (PRIMARY)
# ════════════════════════════════════════════════════════════

def parse_match_api_response(raw_text: str) -> dict[str, Any] | None:
    """
    Parse JSON text từ ``/getMatchData/{id}`` endpoint.

    API trả content-type ``text/javascript`` (không phải application/json),
    nên aiohttp ``response.json()`` sẽ raise ContentTypeError.
    → Ta dùng ``response.text()`` rồi ``json.loads()`` thủ công.

    Returns:
        dict với keys: "rosters", "shots", "tmpl"
        Hoặc None nếu parse thất bại.
    """
    try:
        data = json.loads(raw_text)
        if not isinstance(data, dict):
            logger.error("getMatchData response không phải dict: %s", type(data))
            return None
        return data
    except json.JSONDecodeError as exc:
        logger.error("Lỗi parse JSON từ getMatchData: %s", exc)
        return None


def parse_league_api_response(raw_text: str) -> dict[str, Any] | None:
    """
    Parse JSON text từ ``/getLeagueData/{league}/{season}`` endpoint.

    Returns:
        dict với keys: "teams", "players", "dates"
        Hoặc None nếu parse thất bại.
    """
    try:
        data = json.loads(raw_text)
        if not isinstance(data, dict):
            logger.error("getLeagueData response không phải dict: %s", type(data))
            return None
        return data
    except json.JSONDecodeError as exc:
        logger.error("Lỗi parse JSON từ getLeagueData: %s", exc)
        return None


# ════════════════════════════════════════════════════════════
# PHẦN 2: FLATTEN FUNCTIONS (chuyển nested → flat)
# ════════════════════════════════════════════════════════════

def flatten_shots(
    shots_data: dict[str, list[dict]],
    match_id: int | str,
) -> list[dict]:
    """
    Gộp shots đội nhà + đội khách thành một flat list.

    Input (từ API):
        {"h": [shot1, shot2], "a": [shot3]}
        Mỗi shot đã có đầy đủ fields: id, X, Y, xG, player, …

    Output:
        [shot1, shot2, shot3]  — đảm bảo match_id cho mỗi item.
    """
    if not isinstance(shots_data, dict):
        logger.warning("flatten_shots: expected dict, got %s", type(shots_data))
        return []

    flat: list[dict] = []
    for side in ("h", "a"):
        items = shots_data.get(side, [])
        if isinstance(items, dict):
            # Edge case: API trả dict thay vì list (keyed by id)
            items = list(items.values())
        if not isinstance(items, list):
            logger.warning("flatten_shots: side '%s' is %s, expected list", side, type(items))
            continue
        for shot in items:
            if isinstance(shot, dict):
                shot["match_id"] = str(match_id)
                flat.append(shot)
    return flat


def flatten_rosters(
    rosters_data: dict[str, dict[str, dict]],
    match_id: int | str,
) -> list[dict]:
    """
    Chuyển roster nested dict thành flat list.

    Input (từ API):
        {
            "h": {"player_id_1": {stats…}, "player_id_2": {stats…}},
            "a": {"player_id_3": {stats…}}
        }

    Output:
        [{stats… | match_id}, …]

    Lưu ý: API response đã chứa fields player_id, h_a, team_id
    trong mỗi player stats dict.
    """
    flat: list[dict] = []
    for side in ("h", "a"):
        roster = rosters_data.get(side, {})
        if isinstance(roster, dict):
            for player_id, stats in roster.items():
                if isinstance(stats, dict):
                    stats["match_id"] = str(match_id)
                    # Đảm bảo player_id tồn tại
                    if "player_id" not in stats:
                        stats["player_id"] = str(player_id)
                    flat.append(stats)
        elif isinstance(roster, list):
            # Một số edge case API trả list thay vì dict
            for stats in roster:
                if isinstance(stats, dict):
                    stats["match_id"] = str(match_id)
                    flat.append(stats)
    return flat


def flatten_league_match_entry(raw_entry: dict) -> dict:
    """
    Phẳng hóa một trận đấu từ ``dates`` array (league API response).

    Understat lồng tên đội trong dict con:
        {
            "id": "28778",
            "isResult": true,
            "h": {"id": "87", "title": "Liverpool", "short_title": "LIV"},
            "a": {"id": "73", "title": "Bournemouth", "short_title": "BOU"},
            "goals": {"h": "4", "a": "2"},
            "xG": {"h": "2.33007", "a": "1.57303"},
            "datetime": "2025-08-15 19:00:00",
            "forecast": {"w": "0.5498", "d": "0.2276", "l": "0.2226"}
        }

    Trả về dict phẳng:
        {
            "id": "28778", "isResult": true,
            "h": "Liverpool", "a": "Bournemouth",
            "goals_h": "4", "goals_a": "2",
            "xG_h": "2.33007", "xG_a": "1.57303",
            "datetime": "2025-08-15 19:00:00", …
        }
    """
    entry: dict = {}
    entry["id"] = raw_entry.get("id")
    entry["isResult"] = raw_entry.get("isResult", False)
    entry["datetime"] = raw_entry.get("datetime")

    # Home team – nested dict {"id": "87", "title": "Liverpool"}
    h_data = raw_entry.get("h", {})
    entry["h"] = h_data.get("title", "Unknown") if isinstance(h_data, dict) else str(h_data)

    goals = raw_entry.get("goals")
    entry["goals_h"] = (
        goals.get("h", 0) if isinstance(goals, dict) else 0
    )
    entry["goals_a"] = (
        goals.get("a", 0) if isinstance(goals, dict) else 0
    )

    xg = raw_entry.get("xG")
    entry["xG_h"] = xg.get("h") if isinstance(xg, dict) else None
    entry["xG_a"] = xg.get("a") if isinstance(xg, dict) else None

    # Away team
    a_data = raw_entry.get("a", {})
    entry["a"] = a_data.get("title", "Unknown") if isinstance(a_data, dict) else str(a_data)

    # Forecast probabilities
    forecast = raw_entry.get("forecast", {})
    if isinstance(forecast, dict):
        entry["forecast_w"] = forecast.get("w")
        entry["forecast_d"] = forecast.get("d")
        entry["forecast_l"] = forecast.get("l")

    return entry


# ════════════════════════════════════════════════════════════
# PHẦN 3: BUILD MATCH INFO TỪ API DATA
# ════════════════════════════════════════════════════════════

def build_match_info_from_api(
    match_id: int,
    shots_data: dict[str, list[dict]],
) -> dict:
    """
    Xây dựng match_info từ dữ liệu shots (không cần HTML).

    Shots data từ API đã chứa: h_team, a_team, h_goals, a_goals,
    date, season, match_id trong mỗi shot.  Ta lấy từ shot đầu tiên.
    """
    info: dict = {"match_id": match_id}

    # Lấy metadata từ shot đầu tiên (home hoặc away)
    sample_shot = None
    for side in ("h", "a"):
        shots = shots_data.get(side, [])
        if shots:
            sample_shot = shots[0]
            break

    if sample_shot:
        info["h_team"] = sample_shot.get("h_team", "Unknown")
        info["a_team"] = sample_shot.get("a_team", "Unknown")
        info["h_goals"] = sample_shot.get("h_goals", 0)
        info["a_goals"] = sample_shot.get("a_goals", 0)
        info["datetime_str"] = sample_shot.get("date")
        info["season"] = sample_shot.get("season")

        # Tính tổng xG từ toàn bộ shots
        h_xg = sum(float(s.get("xG", 0)) for s in shots_data.get("h", []))
        a_xg = sum(float(s.get("xG", 0)) for s in shots_data.get("a", []))
        info["h_xg"] = round(h_xg, 5)
        info["a_xg"] = round(a_xg, 5)
    else:
        info["h_team"] = "Unknown"
        info["a_team"] = "Unknown"
        info["h_goals"] = 0
        info["a_goals"] = 0
        info["h_xg"] = 0.0
        info["a_xg"] = 0.0

    return info


# ════════════════════════════════════════════════════════════
# PHẦN 4: HTML REGEX FALLBACK (match_info)
# ════════════════════════════════════════════════════════════
# match_info vẫn được nhúng trong HTML dưới dạng:
#   var match_info = JSON.parse('\x7B...\x7D');
#
# Ta giữ lại logic Regex để trích xuất khi cần metadata
# bổ sung (league_id, fid, team IDs…).

def _decode_hex_escapes(encoded: str) -> str:
    r"""
    Chuyển đổi chuỗi hex-escaped ``\xHH`` thành ký tự thực.

    Ví dụ:
        \x7B  →  {
        \x22  →  "
        \x2D  →  -

    Regex ``\\x([0-9a-fA-F]{2})`` bắt từng cặp hex:
      • ``\\x``             – literal "\x"
      • ``([0-9a-fA-F]{2})`` – capture 2 hex digits

    ``chr(int(m.group(1), 16))`` → chuyển hex → int → char.
    """
    return re.sub(
        r"\\x([0-9a-fA-F]{2})",
        lambda m: chr(int(m.group(1), 16)),
        encoded,
    )


# Compiled regex cho match_info (compile 1 lần, dùng lại nhiều lần)
_MATCH_INFO_PATTERN = re.compile(
    r"var\s+match_info\s*=\s*JSON\.parse\('(.+?)'\)",
    re.DOTALL,
)


def extract_match_info_from_html(html: str) -> dict | None:
    """
    Trích xuất ``match_info`` từ HTML match page (fallback).

    Trả về dict dạng:
        {
            "id": "28778", "fid": "...",
            "h": "87", "a": "73",
            "team_h": "Liverpool", "team_a": "Bournemouth",
            "h_goals": "4", "a_goals": "2",
            "h_xg": "2.33", "a_xg": "1.57",
            "league": "EPL", "season": "2025", …
        }
    """
    match = _MATCH_INFO_PATTERN.search(html)
    if match is None:
        logger.warning("Không tìm thấy match_info trong HTML.")
        return None

    raw_json = match.group(1)
    decoded = _decode_hex_escapes(raw_json)

    try:
        return json.loads(decoded)
    except json.JSONDecodeError as exc:
        logger.error("Lỗi parse match_info JSON: %s", exc)
        return None
