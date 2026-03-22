import asyncio
import sys
from curl_cffi.requests import AsyncSession

async def test_site(name, url):
    print(f"\n--- Testing {name} with curl_cffi ---")
    async with AsyncSession(impersonate="chrome120") as s:
        try:
            headers = {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            resp = await s.get(url, headers=headers, timeout=15)
            print(f"Status Code: {resp.status_code}")
            
            # Kiểm tra xem có bị kẹt ở màn hình chờ bot không
            if "captcha" in resp.text.lower() or "checking your browser" in resp.text.lower() or "bot" in resp.text.lower():
                print(f"❌ RESULT: {name} BLOCKED (Bot detection active)")
            elif resp.status_code == 200:
                print(f"✅ RESULT: {name} ACCESSIBLE (Surprising!)")
            else:
                print(f"⚠️ RESULT: {name} returned {resp.status_code}")
        except Exception as e:
            print(f"💥 ERROR: {e}")

async def run_all():
    # SofaScore API (Cấu hình lỏng)
    await test_site("SofaScore API", "https://api.sofascore.com/api/v1/sport/football/events/live")
    # FBref (Cấu hình chặt)
    await test_site("FBref", "https://fbref.com/en/comps/9/Premier-League-Stats")
    # Transfermarkt (Cấu hình chặt)
    await test_site("Transfermarkt", "https://www.transfermarkt.com/premier-league/startseite/wettbewerb/GB1")

if __name__ == "__main__":
    asyncio.run(run_all())
