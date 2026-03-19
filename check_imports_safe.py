import sys
import os
from pathlib import Path

# Add current dir to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    modules = [
        "core.config", "core.utils", "core.models", "core.antiban", "core.runner",
        "network.http_client", "scheduler.manager", "scheduler.live_pool",
        "scheduler.post_match", "scheduler.maintenance", "scheduler_master"
    ]
    for mod in modules:
        print(f"Testing {mod}...")
        __import__(mod)
    print("\nSUCCESS: All modules imported successfully!")
except Exception as e:
    print(f"\nFAILED: Import error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
