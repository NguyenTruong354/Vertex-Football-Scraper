import sys

with open("scheduler_master.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

new_imports = """# ============================================================
# scheduler_master.py — Single-Browser Multi-League Daemon (Refactored)
# ============================================================
from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core.antiban import AntiBanStateMachine, LiveMetrics
from core.config import BROWSER_RECYCLE_EVERY, TOURNAMENT_IDS
from core.utils import Notifier, setup_logging
from network.http_client import CurlCffiClient
from scheduler.live_pool import LiveTrackingPool
from scheduler.maintenance import DailyMaintenance
from scheduler.manager import ScheduleManager
from scheduler.post_match import PostMatchWorker

from services import insight_worker

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "sofascore") not in sys.path:
    sys.path.insert(0, str(ROOT / "sofascore"))

"""

with open("scheduler_master.py", "w", encoding="utf-8") as f:
    f.write(new_imports)
    f.writelines(lines[2828:])

