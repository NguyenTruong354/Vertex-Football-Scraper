from dataclasses import dataclass, field
from enum import Enum

class AntiBanState(Enum):
    NORMAL = "NORMAL"
    THROTTLED_429 = "THROTTLED_429"
    DEGRADED_403 = "DEGRADED_403"
    FALLBACK = "FALLBACK"  # intermediate after DEGRADED_403 timeout

# Tier C trigger types (plan_live Step 3)
_TIER_C_TRIGGERS = frozenset({"goal", "penalty", "varDecision"})

def is_tier_c_trigger(inc: dict) -> bool:
    """Return True if incident should trigger a Tier C lineup refresh."""
    inc_type = inc.get("incidentType", "")
    if inc_type in _TIER_C_TRIGGERS:
        return True
    # Red card or second-yellow-to-red
    if inc_type == "card" and inc.get("incidentClass") in ("red", "yellowRed"):
        return True
    # Late substitution (minute >= 70)
    if inc_type == "substitution" and (inc.get("time") or 0) >= 70:
        return True
    return False

@dataclass
class LiveMatchState:
    event_id: int = 0
    league: str = ""
    home_team: str = ""
    away_team: str = ""
    home_score: int = 0
    away_score: int = 0
    status: str = "notstarted"
    minute: int = 0
    incidents: list[dict] = field(default_factory=list)
    statistics: dict[str, dict] = field(default_factory=dict)
    insight_text: str = ""
    poll_count: int = 0
    last_updated: str = ""
    start_timestamp: int = 0
    last_drift_check: float = 0.0  # monotonic time of last drift check
    # Tier C tracking (plan_live Step 3)
    _last_tier_c_ts: float = 0.0
    _tier_c_pending: bool = False
    # Tier B tracking (statistics fetch interval)
    _last_tier_b_ts: float = 0.0
