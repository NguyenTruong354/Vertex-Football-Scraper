import logging
import time
from collections import deque

from core.models import AntiBanState

class AntiBanStateMachine:
    """Manages anti-ban runtime states with hard timeouts and exit evaluators."""

    # Hard timeout per state (seconds)
    _TIMEOUT = {
        AntiBanState.THROTTLED_429: 20 * 60,  # 20 min
        AntiBanState.DEGRADED_403: 45 * 60,  # 45 min
        AntiBanState.FALLBACK: 10 * 60,  # 10 min
    }

    # Tier intervals (seconds) per state
    TIER_A_INTERVAL = {
        AntiBanState.NORMAL: 60,
        AntiBanState.THROTTLED_429: 60,
        AntiBanState.DEGRADED_403: 105,  # 90-120 midpoint
        AntiBanState.FALLBACK: 120,
    }
    TIER_B_INTERVAL = {
        AntiBanState.NORMAL: 180,  # every 3rd poll ≈ 3×60
        AntiBanState.THROTTLED_429: 240,
        AntiBanState.DEGRADED_403: 300,
        AntiBanState.FALLBACK: 0,  # 0 = paused
    }
    TIER_C_INTERVAL = {
        AntiBanState.NORMAL: 300,
        AntiBanState.THROTTLED_429: 120,  # event-driven only, 120s cooldown
        AntiBanState.DEGRADED_403: 0,  # 0 = fully paused
        AntiBanState.FALLBACK: 0,  # 0 = fully paused
    }

    def __init__(self, log: logging.Logger):
        self.log = log
        self.state = AntiBanState.NORMAL
        self._entered_at: float = time.monotonic()
        # Rolling window of (timestamp, status_code, tier) tuples
        self._history: deque[tuple[float, int, str]] = deque(maxlen=100)
        # Consecutive counters
        self._consecutive_429: int = 0
        self._consecutive_403: int = 0
        self._consecutive_200_fallback: int = 0  # for FALLBACK exit
        self._recycle_requested: bool = False

    @property
    def needs_recycle(self) -> bool:
        """Check and clear the one-shot recycle flag."""
        if self._recycle_requested:
            self._recycle_requested = False
            return True
        return False

    @property
    def global_sleep_multiplier(self) -> float:
        """Extra jitter multiplier for throttled states."""
        if self.state == AntiBanState.THROTTLED_429:
            return 1.2  # +20%
        return 1.0

    def record_response(self, status_code: int, tier: str = "A") -> None:
        """Record an HTTP response and evaluate state transitions."""
        now = time.monotonic()
        self._history.append((now, status_code, tier))

        if status_code == 200:
            self._consecutive_429 = 0
            self._consecutive_403 = 0
            if self.state == AntiBanState.FALLBACK:
                self._consecutive_200_fallback += 1
        elif status_code == 429:
            self._consecutive_429 += 1
            self._consecutive_403 = 0
            self._consecutive_200_fallback = 0
        elif status_code == 403:
            self._consecutive_403 += 1
            self._consecutive_429 = 0
            self._consecutive_200_fallback = 0

        self._evaluate(now)

    def check_timeout(self) -> None:
        """Call periodically to check hard timeouts."""
        if self.state == AntiBanState.NORMAL:
            return
        now = time.monotonic()
        elapsed = now - self._entered_at
        timeout = self._TIMEOUT.get(self.state, 0)
        if timeout and elapsed >= timeout:
            if self.state == AntiBanState.DEGRADED_403:
                self._transition(
                    AntiBanState.FALLBACK, f"hard timeout after {elapsed / 60:.0f}m"
                )
            elif self.state == AntiBanState.FALLBACK:
                self._transition(
                    AntiBanState.NORMAL, f"FALLBACK expired after {elapsed / 60:.0f}m"
                )
            else:
                # THROTTLED_429 timeout → try NORMAL
                self._transition(
                    AntiBanState.NORMAL, f"hard timeout after {elapsed / 60:.0f}m"
                )

    def _evaluate(self, now: float) -> None:
        """Evaluate state transitions based on current counters and history."""
        # ── Entry conditions ──
        if self.state == AntiBanState.NORMAL:
            if self._consecutive_429 >= 3:
                self._transition(
                    AntiBanState.THROTTLED_429,
                    f"consecutive_429={self._consecutive_429}",
                )
            elif self._consecutive_403 >= 2:
                self._transition(
                    AntiBanState.DEGRADED_403,
                    f"consecutive_403={self._consecutive_403}",
                )
            return

        if self.state == AntiBanState.FALLBACK:
            # Re-enter DEGRADED on any 403
            if self._consecutive_403 >= 1:
                self._transition(AntiBanState.DEGRADED_403, "403 during FALLBACK")
                return
            # Exit FALLBACK → NORMAL after 5 consecutive 200 on Tier A
            if self._consecutive_200_fallback >= 5:
                self._transition(AntiBanState.NORMAL, "5 consecutive 200 in FALLBACK")
            return

        # ── Exit conditions for THROTTLED_429 ──
        if self.state == AntiBanState.THROTTLED_429:
            if self._check_mixed_exit(now):
                self._transition(
                    AntiBanState.NORMAL, "mixed-endpoint exit criteria met"
                )
            return

        # ── Exit conditions for DEGRADED_403 ──
        if self.state == AntiBanState.DEGRADED_403:
            window = 600  # 10 min
            recent = [
                (t, sc, tier) for t, sc, tier in self._history if now - t <= window
            ]
            ok_200 = [r for r in recent if r[1] == 200]
            any_403 = any(r[1] == 403 for r in recent)
            if len(ok_200) >= 8 and not any_403:
                self._transition(
                    AntiBanState.NORMAL, f"10min: {len(ok_200)} x 200, no 403"
                )

    def _check_mixed_exit(self, now: float) -> bool:
        """Check THROTTLED_429 exit: mixed endpoint 200s in rolling 10min window."""
        window = 600
        recent = [(t, sc, tier) for t, sc, tier in self._history if now - t <= window]
        ok_200 = [r for r in recent if r[1] == 200]
        if len(ok_200) < 5:
            return False
        has_tier_a = any(r[2] == "A" for r in ok_200)
        has_non_a = any(r[2] in ("B", "C") for r in ok_200)
        if not (has_tier_a and has_non_a):
            return False
        # No burst of 429 in last 2 minutes
        last_2min = [r for r in recent if now - r[0] <= 120 and r[1] == 429]
        return len(last_2min) < 2

    def _transition(self, new_state: AntiBanState, reason: str) -> None:
        old = self.state
        self.state = new_state
        self._entered_at = time.monotonic()
        self._consecutive_200_fallback = 0
        if new_state in (AntiBanState.DEGRADED_403, AntiBanState.FALLBACK):
            self._recycle_requested = True
        self.log.warning("⚡ State %s → %s (%s)", old.value, new_state.value, reason)

    def should_allow_fast_poll(self) -> bool:
        """Whether 0-15 min fast polling (45s) is allowed."""
        return self.state == AntiBanState.NORMAL

    def tier_c_cooldown(self) -> int:
        """Minimum seconds between Tier C fetches for a single match."""
        if self.state == AntiBanState.NORMAL:
            return 90
        elif self.state == AntiBanState.THROTTLED_429:
            return 120
        else:
            return 0  # 0 = Tier C paused

    def capacity_adjusted_interval(self, tier: str, active_matches: int) -> int:
        """Adjust tier interval by active match count (plan_live Step 5)."""
        base = {
            "A": self.TIER_A_INTERVAL[self.state],
            "B": self.TIER_B_INTERVAL[self.state],
            "C": self.TIER_C_INTERVAL[self.state],
        }.get(tier, 60)
        if active_matches <= 5:
            return base
        elif active_matches <= 10:
            if tier == "B":
                return max(base, 240)
            elif tier == "C":
                return max(base, 420)
        else:  # > 10
            if tier == "B":
                return max(base, 300)
            elif tier == "C":
                return max(base, 480)  # event-driven only, 8min hard cap
        return base


class LiveMetrics:
    """Lightweight counters emitted every 60s for observability."""

    def __init__(self, log: logging.Logger):
        self.log = log
        self._start = time.monotonic()
        self._last_emit: float = 0.0
        # Counters (reset each emit window)
        self.requests_total: int = 0
        self.tier_a_calls: int = 0
        self.tier_b_calls: int = 0
        self.tier_c_calls: int = 0
        self.skipped_polls: int = 0
        self.errors_429: int = 0
        self.errors_403: int = 0

    def record_request(self, tier: str = "A", status_code: int = 200) -> None:
        self.requests_total += 1
        if tier == "A":
            self.tier_a_calls += 1
        elif tier == "B":
            self.tier_b_calls += 1
        elif tier == "C":
            self.tier_c_calls += 1
        if status_code == 429:
            self.errors_429 += 1
        elif status_code == 403:
            self.errors_403 += 1

    def record_skip(self) -> None:
        self.skipped_polls += 1

    def maybe_emit(self, active_matches: int, state: AntiBanState) -> None:
        """Emit metric log line every 60s."""
        now = time.monotonic()
        if now - self._last_emit < 60:
            return
        elapsed = max(now - self._last_emit, 1) if self._last_emit else 60
        rpm = self.requests_total / (elapsed / 60)
        self.log.info(
            "📈 METRICS | rpm=%.1f | tier_a=%d tier_b=%d tier_c=%d | "
            "skip=%d | 429=%d 403=%d | active=%d | state=%s",
            rpm,
            self.tier_a_calls,
            self.tier_b_calls,
            self.tier_c_calls,
            self.skipped_polls,
            self.errors_429,
            self.errors_403,
            active_matches,
            state.value,
        )
        # Reset window counters
        self.requests_total = 0
        self.tier_a_calls = 0
        self.tier_b_calls = 0
        self.tier_c_calls = 0
        self.skipped_polls = 0
        self.errors_429 = 0
        self.errors_403 = 0
        self._last_emit = now
