import sys

with open("scheduler_master.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

imports = """import asyncio
import json
import logging
import time
from datetime import datetime, timezone

from core.models import LiveMatchState, is_tier_c_trigger
from network.http_client import CurlCffiClient
from core.utils import Notifier
from core.antiban import AntiBanState
from services import live_insight, insight_producer

"""

with open("scheduler/live_pool.py", "w", encoding="utf-8") as f:
    f.write(imports)
    # lines 1077 to 1845 are index 1076 to 1845
    # Wait, let's use actual lines:
    f.writelines(lines[1076:1845])

