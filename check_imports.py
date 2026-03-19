import sys
import os
from pathlib import Path

# Add current dir to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    print("Testing core.config...")
    import core.config
    print("Testing core.utils...")
    import core.utils
    print("Testing core.models...")
    import core.models
    print("Testing core.antiban...")
    import core.antiban
    print("Testing core.runner...")
    import core.runner
    print("Testing network.http_client...")
    import network.http_client
    print("Testing scheduler.manager...")
    import scheduler.manager
    print("Testing scheduler.live_pool...")
    import scheduler.live_pool
    print("Testing scheduler.post_match...")
    import scheduler.post_match
    print("Testing scheduler.maintenance...")
    import scheduler.maintenance
    print("Testing scheduler_master...")
    import scheduler_master
    print("\n✅ All modules imported successfully without circular dependencies!")
except Exception as e:
    print(f"\n❌ Import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
