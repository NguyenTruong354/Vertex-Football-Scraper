# Vertex Football - VM Daemon (Python) Features

This document outlines the advanced, background-processing features designed to run on the e2-micro Google Cloud VM. The Python Daemon acts as the "Data Factory" and "AI Journalist" of the platform, collecting raw data and turning it into digested, narrative insights before serving it to the database.

## 1. 🤖 Live Match Insight (Nhận định Thế Trận Trực Tiếp)
*   **Concept:** A real-time analytical algorithm that detects when a team is heavily dominating a match, translating raw stats into a human-readable insight (e.g., "Liverpool đang kiểm soát thế trận hoàn toàn ở hiệp 2"). This appeals to both casual fans and serious analysts.
*   **Mechanism:** During the live tracking cycle, the Python daemon analyzes the incoming SofaScore API JSON (`statistics`). It calculates a "Momentum Score" using a formula based on ball possession (>65%), shots in the last 10 minutes, and corner kicks.
*   **Output:** If the score exceeds a high threshold, it sets an insight flag/message in the `live_snapshots` database row. Spring Boot then reads this flag and displays a dynamic badge directly on the live match card.

## 2. 📰 RSS News & Injury Radar (Tin tức & Chấn thương)
*   **Concept:** Keep the platform alive and relevant even when no matches are playing by aggregating football news and injury reports.
*   **Mechanism:** A scheduled background thread (every 30 mins) uses `feedparser` or simple HTTP requests to scrape RSS feeds from major sports outlets (BBC Sport, The Athletic) or Twitter/X (Fabrizio Romano). 
*   **Output:** Inserts short headlines, links, and player injury statuses into a `news_feed` table in PostgreSQL.

## 3. ⚡ 30-Second Match Story (Tóm tắt trận đấu bằng AI)
*   **Concept:** Provide users who missed a match with a context-rich, narrative summary instead of just a raw list of highlights.
*   **Mechanism:** Implemented in `scheduler_master.py`'s PostMatchWorker. After a match ends, Python aggregates the scraped Understat xG data, SofaScore possession/red cards, and the final score. It sends this JSON payload to the **Google Gemini Free API** with a prompt to generate a 3-4 sentence narrative summary.
*   **Output:** The generated text is saved to the `match_summaries` table. This costs $0 and runs exactly once per match.

## 4. 📈 Player Performance Trend (Biểu đồ thăng trầm cầu thủ)
*   **Concept:** A quick-glance indicator showing if a player is currently in good form or in a slump.
*   **Mechanism:** A nightly cron job (e.g., at 4:00 AM AST) runs a script to analyze the `player_match_stats` table for the last 5 matches. It calculates moving averages for xG, xA, and goals. It passes these numbers to Gemini to generate a short, one-sequence explanation.
*   **Output:** Saves a status (🟢 GREEN for rising, 🔴 RED for falling) and a short text (e.g., "Ghi 3 bàn trong 4 trận gần nhất, phong độ hủy diệt") into a `player_insights` table for the frontend to display instantly upon page load.
