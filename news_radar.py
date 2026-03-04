"""
Vertex Football - RSS News & Injury Radar

Scheduled background job to scrape major football RSS feeds.
Inserts headlines, links, and summaries into the `news_feed` table.
Runs periodically (e.g., every 30 minutes) to keep the platform alive
even when no matches are playing.
"""

import logging
import feedparser
from datetime import datetime, timezone, timedelta
from typing import List, Dict

log = logging.getLogger(__name__)

# List of RSS feeds to aggregate
RSS_FEEDS = [
    {
        "source": "BBC Sport",
        "url": "http://feeds.bbci.co.uk/sport/football/rss.xml"
    },
    {
        "source": "Sky Sports",
        "url": "https://www.skysports.com/rss/12040"
    }
]

def fetch_news() -> List[Dict]:
    """Fetch and parse RSS feeds, returning a list of news items."""
    all_news = []
    
    # Only keep news from the last 24 hours
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    
    for feed_info in RSS_FEEDS:
        source = feed_info["source"]
        url = feed_info["url"]
        log.info("📰 Fetching RSS feed from %s...", source)
        
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                # Attempt to parse publication date, fallback to now
                pub_date = datetime.now(timezone.utc)
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    try:
                        pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                    except Exception:
                        pass
                
                # Check cutoff
                if pub_date < cutoff:
                    continue
                
                title = entry.get('title', '').strip()
                link = entry.get('link', '').strip()
                summary = entry.get('summary', '').strip()
                
                if title and link:
                    all_news.append({
                        "title": title,
                        "link": link,
                        "summary": summary,
                        "published_at": pub_date,
                        "source": source
                    })
        except Exception as exc:
            log.warning("Failed to fetch RSS from %s: %s", source, exc)

    return all_news


def run_and_save() -> int:
    """Fetch news from RSS feeds and save to the database."""
    news_items = fetch_news()
    if not news_items:
        log.info("No recent news found.")
        return 0

    saved = 0
    try:
        from db.config_db import get_connection
        conn = get_connection()
        cur = conn.cursor()
        
        for item in news_items:
            # Using ON CONFLICT DO NOTHING to prevent duplicate links
            cur.execute("""
                INSERT INTO news_feed
                    (title, link, summary, published_at, source, league_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (link) DO NOTHING
            """, (
                item["title"],
                item["link"],
                item["summary"],
                item["published_at"],
                item["source"],
                "EPL"  # Defaulting to EPL for English news
            ))
            # rowcount is 1 if inserted, 0 if conflict
            saved += cur.rowcount

        conn.commit()
        cur.close()
        conn.close()
        
        if saved > 0:
            log.info("✓ Saved %d new RSS items to news_feed table", saved)
        else:
            log.info("✓ No new RSS items to save (all duplicates)")
    except Exception as exc:
        log.error("Failed to save news feed: %s", exc)
        return 0

    return saved

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_and_save()
