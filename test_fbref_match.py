import asyncio
import re
import sys
sys.path.insert(0, "d:/Vertex_Football_Scraper2/fbref")
from bs4 import BeautifulSoup, Comment
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
        print(f"\n=== Found {len(all_tables)} tables in DOM ===")
        for t in all_tables:
            tid = t.get("id", "(no id)")
            print(f"  Table: {tid}")
        
        # Check comments for hidden passing tables
        comments = soup.find_all(string=lambda t: isinstance(t, Comment))
        print(f"\n=== Found {len(comments)} HTML comments ===")
        passing_in_comments = 0
        for c in comments:
            cs = str(c)
            if "passing" in cs.lower():
                passing_in_comments += 1
                csoup = BeautifulSoup(cs, "html.parser")
                for tbl in csoup.find_all("table"):
                    tid = tbl.get("id", "(no id)")
                    print(f"  Comment table: {tid}")
        
        print(f"  Comments with 'passing': {passing_in_comments}")

if __name__ == "__main__":
    asyncio.run(test())
