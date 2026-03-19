import sys
from pathlib import Path

# ── Paths ──
ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable


# ── SofaScore tournament IDs ──
TOURNAMENT_IDS = {
    "EPL": 17,
    "LALIGA": 8,
    "BUNDESLIGA": 35,
    "SERIEA": 23,
    "LIGUE1": 34,
    "UCL": 7,
    "UEL": 677,
    "EREDIVISIE": 37,
    "LIGA_PORTUGAL": 238,
    "RFPL": 203,
}

# ── Source support matrix ──
LEAGUE_SOURCES = {
    "EPL": {"understat": True, "fbref": True, "sofascore": True, "transfermarkt": True},
    "LALIGA": {"understat": True, "fbref": True, "sofascore": True, "transfermarkt": True},
    "BUNDESLIGA": {"understat": True, "fbref": True, "sofascore": True, "transfermarkt": True},
    "SERIEA": {"understat": True, "fbref": True, "sofascore": True, "transfermarkt": True},
    "LIGUE1": {"understat": True, "fbref": True, "sofascore": True, "transfermarkt": True},
    "UCL": {"understat": False, "fbref": True, "sofascore": True, "transfermarkt": True},
    "UEL": {"understat": False, "fbref": True, "sofascore": True, "transfermarkt": True},
    "EREDIVISIE": {"understat": False, "fbref": True, "sofascore": True, "transfermarkt": True},
    "LIGA_PORTUGAL": {"understat": False, "fbref": True, "sofascore": True, "transfermarkt": True},
    "RFPL": {"understat": True, "fbref": False, "sofascore": True, "transfermarkt": True},
}

# ── Retry / recycle config ──
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = [60, 300, 900]
BROWSER_RECYCLE_EVERY = 200  # recycle Chrome every N requests
