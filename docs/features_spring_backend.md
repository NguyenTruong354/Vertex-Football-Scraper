# Vertex Football - Spring Boot Backend Features

This document outlines the high-performance, user-facing features designed to be processed by the Spring Boot (Java) backend. The backend acts as the "Presenter," utilizing the rich database prepared by the Python daemon to deliver instant, scalable, and dynamic experiences to thousands of concurrent users.

## 1. ⏱️ Live Expected Goals (Live xG Chart)
*   **Concept:** A dynamic line chart updating in real-time during a match, showing the accumulation of Expected Goals (xG) to visualize which team is creating better chances.
*   **Mechanism:** The backend exposes a fast `GET /api/live/matches/{id}/xg` endpoint. It directly queries the `statistics_json` column injected by the Python daemon.
*   **Benefit:** Spring Boot handles the thousands of concurrent requests effortlessly using Connection Pools and in-memory caching (like Redis), ensuring the visualization renders instantly without touching the scraping logic.

## 2. ⭐ Match Watchability Score (Chỉ số Mức Độ Đáng Xem)
*   **Concept:** A clear 0-100 rating assigned to upcoming matches to help users decide which games are worth watching (e.g., "Arsenal vs Liverpool: 94/100").
*   **Mechanism:** Spring Boot dynamically calculates this score upon request when generating the fixtures list. The algorithm pulls variables from the database:
    *   League Position proximity (from `standings` table).
    *   Historical goal averages for the two teams (from `squad_stats`).
    *   Derby/Rivalry multiplier flags.
*   **Benefit:** Being purely mathematical, Spring Boot can compute this in milliseconds for a whole weekend of fixtures.

## 3. 🔍 Hidden Gem Radar (Radar Tìm Siêu Cò)
*   **Concept:** A spider/radar chart comparing lesser-known players to superstars using deep scouting data and market values.
*   **Mechanism:** When a user visits a player's profile, the backend exposes a `GET /api/players/{id}/radar` endpoint. It queries the `player_season_stats` (FBref passes, tackles, xA) and `market_values` (Transfermarkt) tables. It normalizes these stats into percentiles (0-100%) against the rest of the league.
*   **Benefit:** Spring Boot excels at these complex joins and mathematical normalizations across multiple tables, providing the frontend with clean, ready-to-plot coordinates.

## 4. 📲 Live Push Notifications (Thông báo Bàn thắng/Thẻ đỏ)
*   **Concept:** Instant push notifications or real-time UI updates when a goal is scored or a red card is given.
*   **Mechanism:** Using `Spring WebSocket` or `Server-Sent Events (SSE)`. The backend listens for updates in the `live_snapshots` or `live_incidents` database tables (possibly via Postgres NOTIFY/LISTEN or a Redis pub/sub channel published by the python daemon).
*   **Benefit:** Allows the mobile app or web frontend to update instantly without the client constantly polling the server (saving immense bandwidth and client battery life).
