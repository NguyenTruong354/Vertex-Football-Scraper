import sys

with open("scheduler_master.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

imports = """import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

from core.config import LEAGUE_SOURCES, ROOT, PYTHON
from core.runner import run_with_retry
from core.utils import Notifier
from services import insight_producer

"""

with open("scheduler/maintenance.py", "w", encoding="utf-8") as f:
    f.write(imports)
    f.writelines(lines[2265:2826]) # Lines 2266 to 2826

