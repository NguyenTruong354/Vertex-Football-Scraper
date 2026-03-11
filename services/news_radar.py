"""
Vertex Football - RSS News & Injury Radar

Scheduled background job to scrape major football RSS feeds.
Inserts headlines, links, and summaries into the `news_feed` table.
Runs periodically (e.g., every 30 minutes) to keep the platform alive
even when no matches are playing.
"""

import logging
import feedparser
import os
import sys
import socket
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
from urllib import request as urllib_request

log = logging.getLogger(__name__)

_ERROR_COUNTS: Dict[str, int] = {}

def send_alert(message: str) -> None:
    webhook_url = os.environ.get("DISCORD_WEBHOOK")
    if not webhook_url:
        return
    try:
        payload = {"content": message}
        req = urllib_request.Request(
            webhook_url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "User-Agent": "NewsRadar/1.0"}
        )
        urllib_request.urlopen(req, timeout=5)
    except Exception as exc:
        log.warning("Failed to send discord alert: %s", exc)

def detect_league(title: str, summary: str) -> Optional[str]:
    text = f"{title} {summary}".lower()

    # 1. UCL / UEL Priority
    if any(k in text for k in ["champions league", "ucl"]):
        return "UCL"
    if any(k in text for k in ["europa league", "uel"]):
        return "UEL"

    # 2. League Rules
    if any(k in text for k in ["premier league", "epl", "arsenal", "chelsea", "liverpool", "manchester", "tottenham", "aston villa", "newcastle", "everton"]):
        return "EPL"
    if any(k in text for k in ["la liga", "laliga", "madrid", "barcelona", "atletico", "sevilla", "valencia"]):
        return "LALIGA"
    if any(k in text for k in ["bundesliga", "bayern", "dortmund", "leverkusen", "leipzig"]):
        return "BUNDESLIGA"
    if any(k in text for k in ["serie a", "juventus", "milan", "inter", "napoli", "roma", "lazio"]):
        return "SERIEA"
    if any(k in text for k in ["ligue 1", "psg", "marseille", "monaco", "lyon"]):
        return "LIGUE1"

    # 3. Fallback
    return None


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
            socket.setdefaulttimeout(15)
            feed = feedparser.parse(url)
            
            # Handle silent feed failure "bozo mode"
            if getattr(feed, 'bozo', False):
                _ERROR_COUNTS[source] = _ERROR_COUNTS.get(source, 0) + 1
                log.warning("Feed error from %s: %s", source, getattr(feed, 'bozo_exception', 'Unknown bozo error'))
                if _ERROR_COUNTS[source] >= 3:
                    send_alert(f"⚠️ **News Radar Alert**: `{source}` failed 3 consecutive times due to feed error.")
                    _ERROR_COUNTS[source] = 0  # Reset after spam
            else:
                _ERROR_COUNTS[source] = 0
                
            if not getattr(feed, 'entries', []):
                log.info("No entries from %s", source)
                continue

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
        finally:
            socket.setdefaulttimeout(None)

    return all_news


def run_and_save() -> int:
    """Fetch news from RSS feeds and save to the database."""
    news_items = fetch_news()
    if not news_items:
        log.info("No recent news found.")
        return 0

    saved = 0
    conn = None
    cur = None
    try:
        from db.config_db import get_connection
        conn = get_connection()
        cur = conn.cursor()
        
        for item in news_items:
            league_id = detect_league(item["title"], item["summary"])
            
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
                league_id
            ))
            # rowcount is 1 if inserted, 0 if conflict
            saved += cur.rowcount

        conn.commit()
        
        if saved > 0:
            log.info("✓ Saved %d new RSS items to news_feed table", saved)
        else:
            log.info("✓ No new RSS items to save (all duplicates)")
    except Exception as exc:
        log.error("Failed to save news feed: %s", exc)
        return 0
    finally:
        if cur: cur.close()
        if conn: conn.close()

    return saved

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    run_and_save()
