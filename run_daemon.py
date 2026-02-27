# ============================================================
# run_daemon.py — Chế độ chạy liên tục 24/7
# ============================================================
"""
Chạy pipeline liên tục theo chu kỳ, tự động cập nhật dữ liệu.

Ba tầng tần suất:
  • TIER 1 — Understat + SofaScore: 30 phút (match hours) / 2 giờ (off hours)
  • TIER 2 — FBref:                 4 giờ
  • TIER 3 — Transfermarkt:         24 giờ
  • DB Load:                        sau mỗi lần scrape

Usage:
    python run_daemon.py                                # EPL, mặc định
    python run_daemon.py --league EPL LALIGA            # Nhiều league
    python run_daemon.py --match-hours 12-23             # Giờ thi đấu (UTC)
    python run_daemon.py --tier1-interval 1800           # 30 phút (giây)
    python run_daemon.py --tier1-off-interval 7200       # 2 giờ ngoài match hours
    python run_daemon.py --tier2-interval 14400          # 4 giờ
    python run_daemon.py --tier3-interval 86400          # 24 giờ
    python run_daemon.py --ss-match-limit 5              # SofaScore giới hạn
    python run_daemon.py --dry-run                       # Chỉ log, không chạy
"""

from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable
STATE_FILE = ROOT / "logs" / "daemon_state.json"

# ────────────────────────────────────────────────────────────
# Logging — file rotation + console
# ────────────────────────────────────────────────────────────

LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger = logging.getLogger("daemon")
logger.setLevel(logging.DEBUG)

# Console handler
_ch = logging.StreamHandler()
_ch.setLevel(logging.INFO)
_ch.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
))
logger.addHandler(_ch)

# File handler — 10 MB x 5 files rotation
_fh = logging.handlers.RotatingFileHandler(
    LOG_DIR / "daemon.log", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
))
logger.addHandler(_fh)


# ────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────

@dataclass
class DaemonConfig:
    """Cấu hình daemon."""
    leagues: list[str] = field(default_factory=lambda: ["EPL"])

    # Tier 1: Understat + SofaScore (near-realtime)
    tier1_interval: int = 1800        # 30 phút (giây) — match hours
    tier1_off_interval: int = 7200    # 2 giờ — ngoài match hours

    # Tier 2: FBref (medium frequency)
    tier2_interval: int = 14400       # 4 giờ

    # Tier 3: Transfermarkt (daily)
    tier3_interval: int = 86400       # 24 giờ

    # Match hours (UTC) — khoảng giờ có trận đấu
    match_hours_start: int = 11       # 11:00 UTC ≈ 18:00 UTC+7
    match_hours_end: int = 23         # 23:00 UTC ≈ 06:00 UTC+7

    # SofaScore match limit per cycle
    ss_match_limit: int = 5

    # Dry run — chỉ log, không chạy thực
    dry_run: bool = False


# ────────────────────────────────────────────────────────────
# Persistent state — nhớ last_run qua restart
# ────────────────────────────────────────────────────────────

class DaemonState:
    """Lưu trạng thái last_run cho từng task, survive restart."""

    def __init__(self, path: Path = STATE_FILE):
        self._path = path
        self._data: dict[str, float] = {}
        self._lock = threading.Lock()
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                with open(self._path, "r") as f:
                    self._data = json.load(f)
                logger.debug("State loaded: %s", self._data)
            except Exception:
                self._data = {}

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w") as f:
            json.dump(self._data, f, indent=2)

    def get_last_run(self, task_key: str) -> float:
        """Trả về timestamp lần chạy cuối (0 nếu chưa bao giờ chạy)."""
        with self._lock:
            return self._data.get(task_key, 0.0)

    def set_last_run(self, task_key: str, ts: float | None = None) -> None:
        """Ghi timestamp lần chạy cuối."""
        with self._lock:
            self._data[task_key] = ts or time.time()
            self._save()

    def get_run_count(self, task_key: str) -> int:
        """Đếm số lần chạy."""
        with self._lock:
            return self._data.get(f"{task_key}_count", 0)

    def inc_run_count(self, task_key: str) -> None:
        with self._lock:
            key = f"{task_key}_count"
            self._data[key] = self._data.get(key, 0) + 1
            self._save()


# ────────────────────────────────────────────────────────────
# Task runner — chạy subprocess
# ────────────────────────────────────────────────────────────

def _run_cmd(cmd: list[str], cwd: Path, label: str, *, dry_run: bool = False) -> bool:
    """Chạy command, trả về True nếu thành công."""
    if dry_run:
        logger.info("[DRY-RUN] %s — %s", label, " ".join(cmd))
        return True

    logger.info("▶ %s — %s", label, " ".join(cmd))
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd, cwd=str(cwd), capture_output=True, text=True,
            timeout=2 * 3600,
        )
        elapsed = time.perf_counter() - t0

        # Log subprocess output
        if proc.stdout.strip():
            for line in proc.stdout.strip().split("\n"):
                logger.debug("  [stdout] %s", line)
        if proc.stderr.strip():
            for line in proc.stderr.strip().split("\n"):
                logger.debug("  [stderr] %s", line)

        if proc.returncode == 0:
            logger.info("✓ %s — %.1fs", label, elapsed)
            return True
        else:
            logger.error("✗ %s — exit code %d (%.1fs)", label, proc.returncode, elapsed)
            return False
    except subprocess.TimeoutExpired:
        logger.error("✗ %s — TIMEOUT 2h", label)
        return False
    except Exception as e:
        logger.error("✗ %s — %s: %s", label, type(e).__name__, e)
        return False


# ────────────────────────────────────────────────────────────
# Scraping tasks
# ────────────────────────────────────────────────────────────

LEAGUE_SOURCES = {
    "EPL":           {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "LALIGA":        {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "BUNDESLIGA":    {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "SERIEA":        {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "LIGUE1":        {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "RFPL":          {"understat": True,  "fbref": False, "sofascore": True,  "transfermarkt": True},
    "UCL":           {"understat": False, "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "EREDIVISIE":    {"understat": False, "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "LIGA_PORTUGAL": {"understat": False, "fbref": True,  "sofascore": True,  "transfermarkt": True},
}


def run_tier1(league: str, cfg: DaemonConfig) -> bool:
    """Tier 1: Understat + SofaScore (near-realtime)."""
    sources = LEAGUE_SOURCES.get(league, {})
    ok = True

    if sources.get("understat"):
        r = _run_cmd(
            [PYTHON, "async_scraper.py", "--league", league],
            ROOT / "understat", f"T1/Understat [{league}]", dry_run=cfg.dry_run,
        )
        ok = ok and r

    if sources.get("sofascore"):
        r = _run_cmd(
            [PYTHON, "sofascore_client.py", "--league", league,
             "--match-limit", str(cfg.ss_match_limit)],
            ROOT / "sofascore", f"T1/SofaScore [{league}]", dry_run=cfg.dry_run,
        )
        ok = ok and r

    return ok


def run_tier2(league: str, cfg: DaemonConfig) -> bool:
    """Tier 2: FBref (medium frequency)."""
    sources = LEAGUE_SOURCES.get(league, {})
    if not sources.get("fbref"):
        return True

    return _run_cmd(
        [PYTHON, "fbref_scraper.py", "--league", league],
        ROOT / "fbref", f"T2/FBref [{league}]", dry_run=cfg.dry_run,
    )


def run_tier3(league: str, cfg: DaemonConfig) -> bool:
    """Tier 3: Transfermarkt (daily)."""
    sources = LEAGUE_SOURCES.get(league, {})
    if not sources.get("transfermarkt"):
        return True

    return _run_cmd(
        [PYTHON, "tm_scraper.py", "--league", league],
        ROOT / "transfermarkt", f"T3/Transfermarkt [{league}]", dry_run=cfg.dry_run,
    )


def run_db_load(league: str, cfg: DaemonConfig) -> bool:
    """Load CSV → PostgreSQL."""
    return _run_cmd(
        [PYTHON, "-m", "db.loader", "--league", league],
        ROOT, f"DB Load [{league}]", dry_run=cfg.dry_run,
    )


# ────────────────────────────────────────────────────────────
# Scheduler helpers
# ────────────────────────────────────────────────────────────

def _is_match_hours(cfg: DaemonConfig) -> bool:
    """Kiểm tra có đang trong giờ thi đấu không (UTC)."""
    hour = datetime.now(timezone.utc).hour
    if cfg.match_hours_start <= cfg.match_hours_end:
        return cfg.match_hours_start <= hour < cfg.match_hours_end
    else:
        # Wrap-around (VD: 20-06 → 20,21,...,23,0,1,...,5)
        return hour >= cfg.match_hours_start or hour < cfg.match_hours_end


def _is_due(state: DaemonState, task_key: str, interval: int) -> bool:
    """Task đã đến lúc chạy chưa?"""
    last = state.get_last_run(task_key)
    return (time.time() - last) >= interval


def _format_duration(seconds: float) -> str:
    """Chuyển giây → chuỗi đọc được."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.0f}m"
    else:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}h{m:02d}m"


def _next_run_in(state: DaemonState, task_key: str, interval: int) -> str:
    """Tính thời gian còn lại."""
    last = state.get_last_run(task_key)
    remaining = max(0, interval - (time.time() - last))
    return _format_duration(remaining)


# ────────────────────────────────────────────────────────────
# Main daemon loop
# ────────────────────────────────────────────────────────────

class Daemon:
    """Main daemon — chạy vòng lặp vô hạn."""

    def __init__(self, cfg: DaemonConfig):
        self.cfg = cfg
        self.state = DaemonState()
        self._shutdown = threading.Event()
        self._cycle = 0

    def _signal_handler(self, signum, frame):
        """Graceful shutdown khi nhận SIGINT/SIGTERM."""
        sig_name = signal.Signals(signum).name
        logger.info("Nhận signal %s — đang shutdown...", sig_name)
        self._shutdown.set()

    def _register_signals(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        # Windows: SIGBREAK (Ctrl+Break)
        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, self._signal_handler)

    def _print_status(self):
        """In trạng thái hiện tại."""
        now = datetime.now(timezone.utc)
        match_h = _is_match_hours(self.cfg)
        t1_int = self.cfg.tier1_interval if match_h else self.cfg.tier1_off_interval

        logger.info("─" * 60)
        logger.info("STATUS — Cycle #%d | %s UTC | Match hours: %s",
                     self._cycle, now.strftime("%H:%M"), "YES" if match_h else "NO")
        
        for league in self.cfg.leagues:
            t1_key = f"tier1_{league}"
            t2_key = f"tier2_{league}"
            t3_key = f"tier3_{league}"
            logger.info(
                "  %s: T1=%s | T2=%s | T3=%s",
                league,
                _next_run_in(self.state, t1_key, t1_int),
                _next_run_in(self.state, t2_key, self.cfg.tier2_interval),
                _next_run_in(self.state, t3_key, self.cfg.tier3_interval),
            )
        logger.info("─" * 60)

    def _run_cycle(self):
        """Kiểm tra và chạy các task đã đến lúc."""
        self._cycle += 1
        match_h = _is_match_hours(self.cfg)
        t1_interval = self.cfg.tier1_interval if match_h else self.cfg.tier1_off_interval

        for league in self.cfg.leagues:
            if self._shutdown.is_set():
                return

            any_scraped = False

            # ── Tier 1: Understat + SofaScore ────────────
            t1_key = f"tier1_{league}"
            if _is_due(self.state, t1_key, t1_interval):
                logger.info("⏰ TIER 1 due — %s (interval=%s, match_hours=%s)",
                            league, _format_duration(t1_interval), match_h)
                ok = run_tier1(league, self.cfg)
                self.state.set_last_run(t1_key)
                self.state.inc_run_count(t1_key)
                any_scraped = True
                if not ok:
                    logger.warning("⚠ Tier 1 có lỗi — %s", league)

            # ── Tier 2: FBref ────────────────────────────
            t2_key = f"tier2_{league}"
            if _is_due(self.state, t2_key, self.cfg.tier2_interval):
                logger.info("⏰ TIER 2 due — %s (interval=%s)",
                            league, _format_duration(self.cfg.tier2_interval))
                ok = run_tier2(league, self.cfg)
                self.state.set_last_run(t2_key)
                self.state.inc_run_count(t2_key)
                any_scraped = True
                if not ok:
                    logger.warning("⚠ Tier 2 có lỗi — %s", league)

            # ── Tier 3: Transfermarkt ────────────────────
            t3_key = f"tier3_{league}"
            if _is_due(self.state, t3_key, self.cfg.tier3_interval):
                logger.info("⏰ TIER 3 due — %s (interval=%s)",
                            league, _format_duration(self.cfg.tier3_interval))
                ok = run_tier3(league, self.cfg)
                self.state.set_last_run(t3_key)
                self.state.inc_run_count(t3_key)
                any_scraped = True
                if not ok:
                    logger.warning("⚠ Tier 3 có lỗi — %s", league)

            # ── DB Load: sau mỗi lần scrape ─────────────
            if any_scraped:
                run_db_load(league, self.cfg)

    def run(self):
        """Main loop — chạy cho đến khi nhận signal shutdown."""
        self._register_signals()

        logger.info("=" * 60)
        logger.info("DAEMON STARTED")
        logger.info("=" * 60)
        logger.info("  Leagues:         %s", ", ".join(self.cfg.leagues))
        logger.info("  Tier 1 interval: %s (match) / %s (off)",
                     _format_duration(self.cfg.tier1_interval),
                     _format_duration(self.cfg.tier1_off_interval))
        logger.info("  Tier 2 interval: %s", _format_duration(self.cfg.tier2_interval))
        logger.info("  Tier 3 interval: %s", _format_duration(self.cfg.tier3_interval))
        logger.info("  Match hours:     %02d:00–%02d:00 UTC",
                     self.cfg.match_hours_start, self.cfg.match_hours_end)
        logger.info("  SS match limit:  %d", self.cfg.ss_match_limit)
        logger.info("  Dry run:         %s", self.cfg.dry_run)
        logger.info("  PID:             %d", os.getpid())
        logger.info("  State file:      %s", STATE_FILE)
        logger.info("  Log file:        %s", LOG_DIR / "daemon.log")
        logger.info("=" * 60)
        logger.info("Nhấn Ctrl+C để dừng daemon.")
        logger.info("")

        # Check interval — polling mỗi 30 giây
        POLL_INTERVAL = 30

        while not self._shutdown.is_set():
            try:
                self._print_status()
                self._run_cycle()
            except Exception as e:
                logger.exception("Lỗi không mong đợi trong cycle #%d: %s", self._cycle, e)
                # Không crash — đợi rồi thử lại
                logger.info("Đợi 60s trước khi thử lại...")
                self._shutdown.wait(60)
                continue

            # Ngủ đến poll tiếp theo (chia nhỏ để responsive với shutdown)
            for _ in range(POLL_INTERVAL):
                if self._shutdown.is_set():
                    break
                time.sleep(1)

        logger.info("=" * 60)
        logger.info("DAEMON STOPPED — %d cycles completed", self._cycle)
        logger.info("=" * 60)


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────

def parse_match_hours(s: str) -> tuple[int, int]:
    """Parse '11-23' → (11, 23)."""
    parts = s.split("-")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(f"Format phải là START-END, VD: 11-23. Nhận: {s}")
    try:
        start, end = int(parts[0]), int(parts[1])
    except ValueError:
        raise argparse.ArgumentTypeError(f"Phải là số nguyên: {s}")
    if not (0 <= start <= 23 and 0 <= end <= 23):
        raise argparse.ArgumentTypeError(f"Giờ phải từ 0–23: {s}")
    return start, end


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Vertex Football Scraper — Daemon mode (24/7)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ:
  python run_daemon.py                             # EPL, cài đặt mặc định
  python run_daemon.py --league EPL LALIGA         # 2 league
  python run_daemon.py --match-hours 18-03         # Giờ thi đấu 18h-3h UTC
  python run_daemon.py --tier1-interval 900        # T1 mỗi 15 phút
  python run_daemon.py --dry-run                   # Chỉ log, không scrape

Tần suất mặc định:
  • Tier 1 (Understat + SofaScore):  30 phút (match hours) / 2 giờ (off)
  • Tier 2 (FBref):                  4 giờ
  • Tier 3 (Transfermarkt):          24 giờ

Match hours mặc định: 11:00–23:00 UTC
Nhấn Ctrl+C để dừng daemon.
        """,
    )
    parser.add_argument("--league", nargs="+", default=["EPL"],
                        help="League IDs (default: EPL)")
    parser.add_argument("--match-hours", type=str, default="11-23",
                        help="Giờ thi đấu UTC, format START-END (default: 11-23)")
    parser.add_argument("--tier1-interval", type=int, default=1800,
                        help="Tier 1 interval giây, match hours (default: 1800 = 30m)")
    parser.add_argument("--tier1-off-interval", type=int, default=7200,
                        help="Tier 1 interval giây, off hours (default: 7200 = 2h)")
    parser.add_argument("--tier2-interval", type=int, default=14400,
                        help="Tier 2 interval giây (default: 14400 = 4h)")
    parser.add_argument("--tier3-interval", type=int, default=86400,
                        help="Tier 3 interval giây (default: 86400 = 24h)")
    parser.add_argument("--ss-match-limit", type=int, default=5,
                        help="SofaScore match limit per cycle (default: 5)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Chỉ log schedule, không chạy scraper thật")

    args = parser.parse_args()
    mh_start, mh_end = parse_match_hours(args.match_hours)

    cfg = DaemonConfig(
        leagues=[l.upper() for l in args.league],
        tier1_interval=args.tier1_interval,
        tier1_off_interval=args.tier1_off_interval,
        tier2_interval=args.tier2_interval,
        tier3_interval=args.tier3_interval,
        match_hours_start=mh_start,
        match_hours_end=mh_end,
        ss_match_limit=args.ss_match_limit,
        dry_run=args.dry_run,
    )

    daemon = Daemon(cfg)
    daemon.run()


if __name__ == "__main__":
    main()
