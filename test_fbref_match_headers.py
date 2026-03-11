import asyncio
import re
import sys
sys.path.insert(0, "d:/Vertex_Football_Scraper2/fbref")
from bs4 import BeautifulSoup
from fbref_scraper import FBrefBrowser

async def test():
    url = "https://fbref.com/en/matches/cc5b4244/Manchester-United-Fulham-August-16-2024-Premier-League"
    
    async with FBrefBrowser() as browser:
        html = await browser.fetch(url)
        if not html:
            print("FAILED to fetch")
            return
        
        soup = BeautifulSoup(html, "html.parser")
        
        # Check all table IDs
        all_tables = soup.find_all("table")
        for t in all_tables:
            tid = t.get("id", "(no id)")
            print(f"Table: {tid}")
        
        # Let's inspect the first header of the summary table
        tbl = soup.find("table", id="stats_19538871_summary")
        if tbl:
            headers = [th.get("data-stat", "") for th in tbl.find_all("th")]
            print(f"Summary headers: {set(headers)}")

if __name__ == "__main__":
    asyncio.run(test())
