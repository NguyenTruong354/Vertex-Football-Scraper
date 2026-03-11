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

_STABLE_LEAGUE_KEYWORDS: dict[str, list[str]] = {
    "EPL":        ["premier league", "epl", "man united", "manchester united", 
                   "man city", "manchester city", "liverpool", "arsenal", "chelsea", "tottenham"],
    "LALIGA":     ["la liga", "laliga", "real madrid", "barcelona", "atletico madrid"],
    "BUNDESLIGA": ["bundesliga", "bayern munich", "borussia dortmund", "bvb"],
    "SERIEA":     ["serie a", "juventus", "inter milan", "ac milan", "napoli"],
    "LIGUE1":     ["ligue 1", "psg", "paris saint-germain", "marseille"],
    "UCL":        ["champions league", "ucl"],
    "UEL":        ["europa league", "uel"],
}

def _load_player_league_map() -> dict[str, str]:
    """Query DB lấy mapping {player_name_lower: league_id} mùa hiện tại."""
    try:
        from db.config_db import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (player_id) LOWER(player), league_id
            FROM player_match_stats
            ORDER BY player_id, match_id DESC
        """)
        res = {row[0]: row[1] for row in cur.fetchall()}
        conn.close()
        return res
    except Exception as exc:
        log.warning("Could not load player_league_map: %s", exc)
        return {}

def _detect_league_from_title(title: str, player_map: dict) -> str | None:
    t_lower = title.lower()
    # Lớp 1: UCL/UEL Priority
    if any(k in t_lower for k in ["champions league", "ucl"]): return "UCL"
    if any(k in t_lower for k in ["europa league", "uel"]): return "UEL"
    # Lớp 2: Stable keywords
    for lid, kws in _STABLE_LEAGUE_KEYWORDS.items():
        if any(kw in t_lower for kw in kws): return lid
    # Lớp 3: Dynamic player map
    for p_name, lid in player_map.items():
        if p_name in t_lower: return lid
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
    player_league_map = _load_player_league_map()
    
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
                    detected_league = _detect_league_from_title(title, player_league_map)
                    all_news.append({
                        "title": title,
                        "link": link,
                        "summary": summary,
                        "published_at": pub_date,
                        "source": source,
                        "league_id": detected_league
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
                item.get("league_id")
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
