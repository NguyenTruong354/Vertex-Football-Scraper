import asyncio
import logging
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path and load .env
ROOT = Path(__file__).resolve().parent
sys.path.append(str(ROOT))
load_dotenv(ROOT / ".env")

from network.http_client import CurlCffiClient
from core.utils import Notifier

async def main():
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("TEST")
    notifier = Notifier(log)
    
    client = CurlCffiClient(log, notifier)
    await client.start()
    
    print("\n--- Testing Live Stealth Network ---")
    print(f"USE_STEALTH_NETWORK: {os.environ.get('USE_STEALTH_NETWORK')}")
    print(f"Bridges: {os.environ.get('STEALTH_BRIDGES')}")
    
    # Try fetching a simple tournament JSON
    endpoint = "/unique-tournament/17/seasons" # EPL Seasons List
    
    try:
        print(f"Firing request to SofaScore through bridges...")
        data = await client.get_json(endpoint)
        if data:
            print("SUCCESS: Data received from SofaScore via Stealth Bridge!")
            # print(data) # Optional: print first few keys
        else:
            print("FAILURE: No data received or fallback triggered.")
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(main())
