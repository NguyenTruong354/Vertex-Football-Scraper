import asyncio
from playwright.async_api import async_playwright

async def test_playwright():
    print("\n--- Testing FBref with Playwright Headless ---")
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            url = "https://fbref.com/en/comps/9/Premier-League-Stats"
            print(f"Navigating to {url}...")
            
            # Đặt timeout ngắn
            response = await page.goto(url, wait_until="networkidle", timeout=30000)
            print(f"Status Code: {response.status}")
            
            title = await page.title()
            print(f"Page Title: {title}")
            
            content = await page.content()
            if "checking your browser" in content.lower() or "bot" in content.lower():
                print("❌ RESULT: Playwright Headless was DETECTED as a Bot.")
            else:
                print("✅ RESULT: Playwright Headless ACCESS successful (maybe).")
                
            await browser.close()
        except Exception as e:
            print(f"💥 ERROR: {e}")

if __name__ == "__main__":
    try:
        import playwright
        asyncio.run(test_playwright())
    except ImportError:
        print("❌ ERROR: Playwright is not installed in the environment.")
