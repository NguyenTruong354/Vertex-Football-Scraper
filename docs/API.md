# Football Website — API Reference (v1)

## Overview
- **Base URL:** `https://api.yoursite.com/api/v1`
- **Authentication:** Bearer Token (JWT)
- **Response format:** JSON
- **Pagination:** `?page=1&limit=20`
- **Date format:** ISO 8601 (`YYYY-MM-DD`)
- **Request tracing:** Every response includes `X-Request-ID` header
- **Idempotency:** POST/PUT/PATCH support `Idempotency-Key` header (UUID v4, TTL 24h, stored in Redis)

---

## Security & Anti-Scraping Measures

### 1. Strict CORS Policy
The API **only** accepts requests from our official frontend domain (`https://vertex-football.com`). Cross-origin requests from tools like Postman or unknown domains are blocked for public endpoints.

Each access tier has a distinct CORS policy:
| Tier | Allowed Origins |
|------|----------------|
| Public (Guest) | `https://vertex-football.com` |
| Authenticated (User) | `https://vertex-football.com` |
| Admin / Service Daemon | Internal network only (IP whitelist) |

### 2. Rate Limiting (Throttling)
IP-based and User-based rate limiting via Redis. Limits by tier:
| Tier | Limit | Window | On Exceed |
|------|-------|--------|-----------|
| Public (unauthenticated) | 30 req | 1 min | HTTP 429, `Retry-After` header |
| Authenticated User | 100 req | 1 min | HTTP 429 |
| Admin / Service Daemon | 500 req | 1 min | HTTP 429 |

Rate limit headers included in every response:
```
X-Rate-Limit-Limit: 100
X-Rate-Limit-Remaining: 87
X-Rate-Limit-Reset: 1714568400
```

### 3. Hard Pagination Limits
`limit` is capped at `50` for all endpoints. A request with `?limit=10000` is immediately rejected with `400 Bad Request`.

### 4. Input Validation & Injection Prevention
- All `sort_by` parameters are **enum-whitelisted** in the controller. Arbitrary strings are rejected with `422 Unprocessable Entity`.
- All `league_id`, `season`, `status` parameters are validated against known enum values.
- `Content-Type: application/json` is **enforced** on all POST/PUT/PATCH requests. Non-JSON bodies return `415 Unsupported Media Type`.

### 5. JWT & API Keys
| Role | Access | Token Type |
|------|--------|------------|
| **Guest** | Limited read — basic stats, standings, fixtures | None (public) |
| **User** | Deeper insights, AI stories, player trends | JWT (access + refresh) |
| **Admin** | Full CRUD, admin endpoints, view refresh | Admin-role JWT |
| **Service (Python Daemon)** | Internal write operations | `SERVICE_API_KEY` (IP-whitelisted, rotatable) |

### 6. Idempotency
All write operations (POST/PUT/PATCH) support the `Idempotency-Key` header:
```http
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000
```
Duplicate requests with the same key within 24h return the cached response without re-executing the operation.

### 7. Audit Logging
All Admin write operations (POST/PUT/DELETE) are logged with:
- `X-Request-ID` (UUID, echoed from request or auto-generated)
- Actor identity (user_id from JWT)
- Timestamp, IP address, endpoint, payload hash

### 8. WAF (Web Application Firewall)
API is placed behind Cloudflare to automatically block known scraping bots, headless browsers, and malicious data-mining traffic.

---

## Table of Contents
- [Authentication](#authentication)
- [Leagues](#leagues)
- [Teams](#teams)
- [Players](#players)
- [Matches & Fixtures](#matches--fixtures)
- [Match Detail — Shots, Lineups, Heatmaps](#match-detail--shots-lineups-heatmaps)
- [Match Events & Live Data](#match-events--live-data)
- [Live Match Stream (SSE)](#live-match-stream-sse)
- [Standings](#standings)
- [Statistics](#statistics)
- [AI Insights & News](#ai-insights--news)
- [Health & Admin](#health--admin)
- [Error Codes](#error-codes)

---

## Authentication

### `POST /api/v1/auth/login`
**Description:** Authenticate with email/password. Returns access token and refresh token.

**Request Body:**
```json
{
  "email": "user@example.com",
  "password": "s3cr3t"
}
```

**Response:**
```json
{
  "data": {
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "refresh_token": "dGhpcyBpcyBhIHJlZnJlc2ggdG9rZW4...",
    "expires_in": 3600,
    "token_type": "Bearer"
  }
}
```

---

### `POST /api/v1/auth/refresh`
**Description:** Exchange a valid refresh token for a new access token.

**Request Body:**
```json
{
  "refresh_token": "dGhpcyBpcyBhIHJlZnJlc2ggdG9rZW4..."
}
```

**Response:**
```json
{
  "data": {
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "expires_in": 3600
  }
}
```

---

### `POST /api/v1/auth/logout`
**Description:** Revoke the current refresh token. Requires valid JWT.

**Example Request:**
```http
POST /api/v1/auth/logout
Authorization: Bearer <access_token>
```

---

## Leagues

### `GET /api/v1/leagues`
**Description:** List all leagues currently tracked by the system. Used to validate `league_id` values.
**Auth:** Public

**Example Response:**
```json
{
  "data": [
    { "league_id": "EPL",   "name": "English Premier League", "country": "England" },
    { "league_id": "LL",    "name": "La Liga",                 "country": "Spain"   },
    { "league_id": "BL1",   "name": "Bundesliga",              "country": "Germany" },
    { "league_id": "SA",    "name": "Serie A",                 "country": "Italy"   },
    { "league_id": "FL1",   "name": "Ligue 1",                 "country": "France"  }
  ]
}
```

---

## Teams

The Teams domain exposes squad metadata, general club information, and related players derived from `team_metadata`, `squad_stats`, `squad_rosters`, and `mv_team_profiles`.

### `GET /api/v1/teams`
**Description:** Retrieve a paginated list of teams.
**Auth:** Public

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| page | integer | No | Page number, default 1 |
| limit | integer | No | Items per page, default 20, max 50 |
| league_id | string | No | Filter by league (e.g., `EPL`) |
| season | string | No | Filter by season (e.g., `2024-2025`) |

**Example Request:**
```http
GET /api/v1/teams?league_id=EPL&season=2024-2025&page=1&limit=20
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "team_id": "822bd0ba",
      "team_name": "Arsenal",
      "league_id": "EPL",
      "logo_url": "https://cdn.yoursite.com/logos/arsenal.png",
      "manager_name": "Mikel Arteta",
      "stadium_name": "Emirates Stadium",
      "match_confidence": 1.0
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 20,
    "total": 20
  }
}
```

---

### `GET /api/v1/teams/:id`
**Description:** Retrieve comprehensive details and metadata for a specific team (from `team_metadata` + `mv_team_profiles`).
**Auth:** Public

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | string | Team ID (FBref team ID) |

**Example Request:**
```http
GET /api/v1/teams/822bd0ba
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": {
    "team_id": "822bd0ba",
    "team_name": "Arsenal",
    "league_id": "EPL",
    "season": "2024-2025",
    "logo_url": "https://cdn.yoursite.com/logos/arsenal.png",
    "stadium_name": "Emirates Stadium",
    "stadium_capacity": "60704",
    "manager_name": "Mikel Arteta",
    "manager_since": "2019-12-20",
    "manager_contract_until": "2027-06-30",
    "squad_size": 25,
    "avg_age": 25.4,
    "num_foreigners": 17,
    "total_market_value": "€1.10bn",
    "formation": "4-3-3"
  }
}
```

---

### `GET /api/v1/teams/:id/stats`
**Description:** Retrieve aggregate team statistics for a season (from `squad_stats`).
**Auth:** Public

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| season | string | No | Season filter, default current season |

**Example Request:**
```http
GET /api/v1/teams/822bd0ba/stats?season=2024-2025
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": {
    "team_id": "822bd0ba",
    "team_name": "Arsenal",
    "season": "2024-2025",
    "players_used": 28,
    "avg_age": 25.4,
    "possession": 56.2,
    "matches_played": 38,
    "goals": 91,
    "assists": 71,
    "yellow_cards": 42,
    "red_cards": 2,
    "goals_per90": 2.39,
    "assists_per90": 1.87
  }
}
```

---

### `GET /api/v1/teams/:id/players`
**Description:** Get all players currently rostered for the specified team (from `squad_rosters` + `mv_player_profiles`).
**Auth:** Public

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| season | string | No | Season filter |
| position | string | No | Filter by position (`GK`, `DF`, `MF`, `FW`) |

**Example Request:**
```http
GET /api/v1/teams/822bd0ba/players?season=2024-2025
Authorization: Bearer <token>
```

---

### `POST /api/v1/teams`
**Description:** Create a new team record.
**Auth:** Admin

**Example Request:**
```http
POST /api/v1/teams
Authorization: Bearer <admin_token>
Content-Type: application/json
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000

{
  "team_id": "new_team",
  "team_name": "FC Example",
  "league_id": "EPL",
  "manager_name": "John Doe",
  "stadium_name": "Example Park"
}
```

---

### `PUT /api/v1/teams/:id`
**Description:** Fully replace an existing team's metadata.
**Auth:** Admin

---

### `PATCH /api/v1/teams/:id`
**Description:** Partially update an existing team's metadata.
**Auth:** Admin

**Example Request:**
```http
PATCH /api/v1/teams/822bd0ba
Authorization: Bearer <admin_token>
Content-Type: application/json
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440001

{
  "manager_name": "New Manager"
}
```

---

### `DELETE /api/v1/teams/:id`
**Description:** Delete a team record. Cascades to related squad statistics and rosters.
**Auth:** Admin

---

## Players

The Players domain handles player demographic information, market values, and aggregated multi-source IDs derived from `squad_rosters`, `player_season_stats`, `market_values`, `player_crossref`, and `mv_player_profiles`.

### `GET /api/v1/players`
**Description:** Retrieve a paginated list of players.
**Auth:** Public

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| page | integer | No | Page number, default 1 |
| limit | integer | No | Items per page, default 20, max 50 |
| team_id | string | No | Filter by team ID |
| league_id | string | No | Filter by league ID |
| season | string | No | Filter by season |
| position | string | No | Filter by position (`GK`, `DF`, `MF`, `FW`) |

**Example Request:**
```http
GET /api/v1/players?team_id=822bd0ba&position=FW
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "player_id": "a23b4c5d",
      "player_name": "Bukayo Saka",
      "nationality": "ENG",
      "position": "FW",
      "age": 22,
      "team_name": "Arsenal",
      "player_image_url": "https://cdn.yoursite.com/players/saka.png",
      "market_value_numeric": 130000000
    }
  ],
  "pagination": { "page": 1, "limit": 20, "total": 6 }
}
```

---

### `GET /api/v1/players/search`
**Description:** Full-text search for players by name. Uses PostgreSQL trigram index (`pg_trgm`) for fuzzy matching — works with partial names, abbreviations, or accented variants.
**Auth:** Public

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| q | string | **Yes** | Search query (min 2 characters) |
| league_id | string | No | Narrow results to a specific league |
| season | string | No | Season filter |
| limit | integer | No | Max results, default 10, max 50 |

**Example Request:**
```http
GET /api/v1/players/search?q=saka&league_id=EPL
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "player_id": "a23b4c5d",
      "player_name": "Bukayo Saka",
      "team_name": "Arsenal",
      "position": "FW",
      "nationality": "ENG",
      "player_image_url": "https://cdn.yoursite.com/players/saka.png",
      "similarity": 0.92
    }
  ]
}
```

---

### `GET /api/v1/players/compare`
**Description:** Compare 2–3 players side by side using the `mv_player_complete_stats` materialized view. Returns all stats needed for a comparison table or radar chart.
**Auth:** User

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| ids | string | **Yes** | Comma-separated player IDs (2–3 players) |
| season | string | No | Season filter, default current season |

**Example Request:**
```http
GET /api/v1/players/compare?ids=a23b4c5d,b34c5d6e&season=2024-2025
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "player_id": "a23b4c5d",
      "player_name": "Bukayo Saka",
      "team_name": "Arsenal",
      "position": "FW",
      "minutes_90s": 31.2,
      "goals": 14,
      "assists": 11,
      "goals_per90": 0.45,
      "total_xg": 11.23,
      "xg_per90": 0.36,
      "xg_overperformance": 2.77,
      "progressive_carries": 78,
      "take_ons_won_pct": 54.2,
      "tackles": 28,
      "interceptions": 15,
      "market_value_numeric": 130000000,
      "player_image_url": "https://cdn.yoursite.com/players/saka.png"
    }
  ]
}
```

---

### `GET /api/v1/players/:id`
**Description:** Retrieve full details for a player, including market values and cross-reference IDs.
**Auth:** Public

**Example Request:**
```http
GET /api/v1/players/a23b4c5d
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": {
    "player_id": "a23b4c5d",
    "player_name": "Bukayo Saka",
    "nationality": "ENG",
    "position": "FW",
    "age": 22,
    "team_name": "Arsenal",
    "player_image_url": "https://cdn.yoursite.com/players/saka.png",
    "market_value": "€130.00m",
    "market_value_numeric": 130000000,
    "crossref": {
      "understat_id": 8260,
      "fbref_id": "a23b4c5d"
    }
  }
}
```

---

### `GET /api/v1/players/:id/stats`
**Description:** Aggregated season statistics for a player (from `player_season_stats`).
**Auth:** Public

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| season | string | No | Season filter |

**Example Request:**
```http
GET /api/v1/players/a23b4c5d/stats?season=2024-2025
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": {
    "player_id": "a23b4c5d",
    "player_name": "Bukayo Saka",
    "season": "2024-2025",
    "matches_played": 35,
    "starts": 34,
    "minutes": 2888,
    "minutes_90s": 32.1,
    "goals": 14,
    "assists": 11,
    "goals_per90": 0.44,
    "assists_per90": 0.34,
    "shots": 89,
    "shots_on_target_pct": 42.7,
    "yellow_cards": 3,
    "red_cards": 0
  }
}
```

---

### `GET /api/v1/players/:id/stats/advanced`
**Description:** Full advanced statistics for a player in a season, combining defensive actions (`player_defensive_stats`), possession/progression (`player_possession_stats`), and Understat xG data via crossref. For goalkeepers, also includes `gk_stats`.
**Auth:** User

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| season | string | No | Season filter |

**Example Request:**
```http
GET /api/v1/players/a23b4c5d/stats/advanced?season=2024-2025
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": {
    "player_id": "a23b4c5d",
    "season": "2024-2025",
    "understat": {
      "total_xg": 11.23,
      "xg_per90": 0.36,
      "goals_from_shots": 14,
      "xg_overperformance": 2.77
    },
    "defensive": {
      "tackles": 28,
      "tackles_won": 19,
      "interceptions": 15,
      "blocks": 6,
      "clearances": 4,
      "pressures": 187,
      "pressure_regains": 53,
      "pressure_regain_pct": 28.3
    },
    "possession": {
      "touches": 1842,
      "progressive_carries": 78,
      "carries_into_final_third": 41,
      "carries_into_penalty_area": 18,
      "take_ons": 94,
      "take_ons_won": 51,
      "take_ons_won_pct": 54.2,
      "progressive_passes_received": 112
    },
    "goalkeeping": null
  }
}
```

---

### `GET /api/v1/players/:id/stats/shots`
**Description:** Per-match Understat xG timeline for a player — individual shot data across all matches in a season.
**Auth:** User

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| season | string | No | Season filter |

**Example Request:**
```http
GET /api/v1/players/a23b4c5d/stats/shots?season=2024-2025
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "match_id": 12345,
      "date": "2024-10-05",
      "h_team": "Arsenal",
      "a_team": "Chelsea",
      "minute": 34,
      "result": "Goal",
      "situation": "OpenPlay",
      "shot_type": "RightFoot",
      "xg": 0.21,
      "x": 0.87,
      "y": 0.52
    }
  ]
}
```

---

### `GET /api/v1/players/:id/stats/passing`
**Description:** Per-match passing statistics aggregated from SofaScore (`match_passing_stats`).
**Auth:** User

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| season | string | No | Season filter |

**Example Request:**
```http
GET /api/v1/players/a23b4c5d/stats/passing?season=2024-2025
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "event_id": 12345678,
      "match_date": "2024-10-05",
      "home_team": "Arsenal",
      "away_team": "Chelsea",
      "minutes_played": 90,
      "total_pass": 42,
      "accurate_pass": 37,
      "key_pass": 3,
      "total_long_balls": 4,
      "accurate_long_balls": 3,
      "total_cross": 6,
      "accurate_cross": 2,
      "expected_assists": 0.45,
      "goal_assist": 1
    }
  ]
}
```

---

### `POST /api/v1/players`
**Description:** Create a new player record.
**Auth:** Admin

**Example Request:**
```http
POST /api/v1/players
Authorization: Bearer <admin_token>
Content-Type: application/json
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440002

{
  "player_id": "new_player",
  "player_name": "John Football",
  "team_id": "822bd0ba",
  "nationality": "ENG",
  "position": "FW"
}
```

---

### `PUT /api/v1/players/:id`
**Description:** Fully replace an existing player's record.
**Auth:** Admin

---

### `PATCH /api/v1/players/:id`
**Description:** Partially update an existing player's record.
**Auth:** Admin

**Example Request:**
```http
PATCH /api/v1/players/new_player
Authorization: Bearer <admin_token>
Content-Type: application/json
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440003

{
  "position": "MF"
}
```

---

### `DELETE /api/v1/players/:id`
**Description:** Delete a player record.
**Auth:** Admin

---

## Matches & Fixtures

The Matches domain covers both the fixture schedule and match results, derived from `fixtures`, `match_stats`, and `ss_events`.

### `GET /api/v1/fixtures`
**Description:** Retrieve the match schedule (upcoming and past fixtures) from FBref. Use this for calendars and scheduling views.
**Auth:** Public

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| page | integer | No | Page number, default 1 |
| limit | integer | No | Items per page, default 20, max 50 |
| league_id | string | No | Filter by league ID |
| season | string | No | Filter by season |
| team_id | string | No | Filter by home or away team ID |
| date | string | No | Exact date filter (ISO 8601) |
| date_from | string | No | Date range start |
| date_to | string | No | Date range end |

**Example Request:**
```http
GET /api/v1/fixtures?league_id=EPL&season=2024-2025&date_from=2025-01-01
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "match_id": "fixture_001",
      "gameweek": 20,
      "date": "2025-01-04",
      "start_time": "15:00",
      "home_team": "Arsenal",
      "home_team_id": "822bd0ba",
      "away_team": "Chelsea",
      "away_team_id": "cff3d9bb",
      "score": null,
      "home_xg": null,
      "away_xg": null,
      "venue": "Emirates Stadium",
      "referee": null
    }
  ],
  "pagination": { "page": 1, "limit": 20, "total": 190 }
}
```

---

### `GET /api/v1/matches`
**Description:** Retrieve played match results with xG data (from `match_stats`).
**Auth:** Public

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| page | integer | No | Page number, default 1 |
| limit | integer | No | Items per page, default 20, max 50 |
| league_id | string | No | Filter by league ID |
| season | integer | No | Season year (e.g., `2024`) |
| team | string | No | Filter by team name |

**Example Request:**
```http
GET /api/v1/matches?league_id=EPL&season=2024
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "match_id": 12345,
      "h_team": "Arsenal",
      "a_team": "Chelsea",
      "h_goals": 3,
      "a_goals": 1,
      "h_xg": 2.54,
      "a_xg": 0.83,
      "datetime_str": "2024-05-01 16:30:00",
      "league": "EPL",
      "season": 2024
    }
  ],
  "pagination": { "page": 1, "limit": 20, "total": 380 }
}
```

---

### `GET /api/v1/matches/:id`
**Description:** Get detailed match info combining FBref fixture data with Understat xG stats.
**Auth:** Public

**Example Request:**
```http
GET /api/v1/matches/12345
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": {
    "match_id": "12345",
    "h_team": "Arsenal",
    "a_team": "Chelsea",
    "h_goals": 3,
    "a_goals": 1,
    "h_xg": 2.54,
    "a_xg": 0.83,
    "referee": "Michael Oliver",
    "venue": "Emirates Stadium",
    "attendance": "60341",
    "date": "2024-05-01",
    "gameweek": 36
  }
}
```

---

### `POST /api/v1/matches`
**Description:** Manually insert a new fixture or match record.
**Auth:** Admin

**Example Request:**
```http
POST /api/v1/matches
Authorization: Bearer <admin_token>
Content-Type: application/json
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440004

{
  "match_id": "new_match",
  "h_team": "Arsenal",
  "a_team": "Chelsea",
  "datetime_str": "2025-08-15 15:00:00",
  "league_id": "EPL",
  "season": 2025
}
```

---

### `PUT /api/v1/matches/:id`
**Description:** Fully replace a match record.
**Auth:** Admin

---

### `PATCH /api/v1/matches/:id`
**Description:** Partially update match fields (e.g., after a live result is confirmed).
**Auth:** Admin

**Example Request:**
```http
PATCH /api/v1/matches/12345
Authorization: Bearer <admin_token>
Content-Type: application/json
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440005

{
  "h_goals": 4,
  "h_xg": 3.10
}
```

---

### `DELETE /api/v1/matches/:id`
**Description:** Delete a match from the schedule / records.
**Auth:** Admin

---

## Match Detail — Shots, Lineups, Heatmaps

### `GET /api/v1/matches/:id/shots`
**Description:** Retrieve all spatial shot tracking data for a given match (from `shots` table). Returns x/y coordinates, xG values, shot types, and results for pitch visualization.
**Auth:** User

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| team | string | No | Filter by `home` or `away` |

**Example Request:**
```http
GET /api/v1/matches/12345/shots
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "id": 1001,
      "player": "Bukayo Saka",
      "player_id": 8260,
      "minute": 34,
      "result": "Goal",
      "situation": "OpenPlay",
      "shot_type": "RightFoot",
      "xg": 0.21,
      "x": 0.87,
      "y": 0.52,
      "h_a": "h"
    }
  ]
}
```

---

### `GET /api/v1/matches/:id/lineups`
**Description:** Retrieve confirmed or predicted lineups for both teams. Combines `match_lineups` (formation, starting XI, subs) with `player_avg_positions` (avg_x, avg_y) for pitch rendering.
**Auth:** User

**Example Request:**
```http
GET /api/v1/matches/12345678/lineups
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": {
    "home": {
      "team_name": "Arsenal",
      "formation": "4-3-3",
      "starters": [
        {
          "player_id": 1234567,
          "player_name": "David Raya",
          "position": "G",
          "jersey_number": 22,
          "avg_x": 50.0,
          "avg_y": 5.2,
          "minutes_played": 90,
          "rating": 7.1
        }
      ],
      "substitutes": [
        {
          "player_id": 2345678,
          "player_name": "Oleksandr Zinchenko",
          "position": "D",
          "jersey_number": 35,
          "is_substitute": true,
          "minutes_played": null,
          "rating": null
        }
      ]
    },
    "away": { ... }
  }
}
```

---

### `GET /api/v1/matches/:id/heatmaps`
**Description:** Retrieve localized SofaScore player heatmap coordinates for a specific match.
**Auth:** User

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| player_id | integer | No | Filter to a specific player's heatmap |
| team | string | No | Filter by `home` or `away` |

**Example Request:**
```http
GET /api/v1/matches/12345678/heatmaps?player_id=8260
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "player_id": 8260,
      "player_name": "Bukayo Saka",
      "team_name": "Arsenal",
      "position": "F",
      "num_points": 48,
      "avg_x": 74.2,
      "avg_y": 33.1,
      "heatmap_points": [
        { "x": 68.2, "y": 28.4, "v": 3 },
        { "x": 80.1, "y": 40.0, "v": 7 }
      ]
    }
  ]
}
```

---

## Match Events & Live Data

Handles real-time game polling and atomic incidents (goals, cards, substitutions) via `live_incidents` and `live_match_state`.

### `GET /api/v1/events`
**Description:** Retrieve a list of match incidents (goals, cards, substitutions, VAR decisions).
**Auth:** Public

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| event_id | integer | No | Filter by SofaScore match ID |
| incident_type | string | No | One of: `goal`, `card`, `substitution`, `varDecision` |
| page | integer | No | Page number, default 1 |
| limit | integer | No | Max 50 |

**Example Request:**
```http
GET /api/v1/events?event_id=101&incident_type=goal
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "id": 1,
      "event_id": 101,
      "incident_type": "goal",
      "minute": 45,
      "added_time": 2,
      "player_name": "Bukayo Saka",
      "detail": "penalty",
      "is_home": true
    }
  ],
  "pagination": { "page": 1, "limit": 20, "total": 4 }
}
```

---

### `GET /api/v1/events/:id`
**Description:** Retrieve a single detailed match incident.
**Auth:** Public

---

### `POST /api/v1/events`
**Description:** Dispatch and log a new event/incident.
**Auth:** Admin / Service Daemon

**Example Request:**
```http
POST /api/v1/events
Authorization: Bearer <service_token>
Content-Type: application/json
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440006

{
  "event_id": 101,
  "incident_type": "card",
  "detail": "yellow",
  "player_name": "John Doe",
  "minute": 55,
  "is_home": false
}
```

---

### `PUT /api/v1/events/:id`
**Description:** Fully replace a match event record.
**Auth:** Admin

---

### `PATCH /api/v1/events/:id`
**Description:** Correct fields within an existing event (e.g., upgrade a yellow card to red after VAR review).
**Auth:** Admin

**Example Request:**
```http
PATCH /api/v1/events/1
Authorization: Bearer <admin_token>
Content-Type: application/json
Idempotency-Key: 550e8400-e29b-41d4-a716-446655440007

{
  "detail": "red"
}
```

---

### `DELETE /api/v1/events/:id`
**Description:** Remove a logged incident.
**Auth:** Admin

---

### `GET /api/v1/live/:match_id/state`
**Description:** Get the current live match state — score, minute, status, and core stats summary. Lightweight polling endpoint backed by `live_match_state`.
**Auth:** Public

**Example Request:**
```http
GET /api/v1/live/12345678/state
```

**Example Response:**
```json
{
  "data": {
    "event_id": 12345678,
    "home_team": "Arsenal",
    "away_team": "Chelsea",
    "home_score": 2,
    "away_score": 1,
    "status": "inprogress",
    "minute": 67,
    "poll_count": 134,
    "insight_text": "Arsenal đang kiểm soát hoàn toàn với 68% kiểm soát bóng và 3 cú sút trúng đích trong 15 phút qua.",
    "stats_core": {
      "home_possession": 68,
      "away_possession": 32,
      "home_shots": 9,
      "away_shots": 4,
      "home_shots_on_target": 4,
      "away_shots_on_target": 1
    }
  }
}
```

---

## Live Match Stream (SSE)

Server-Sent Events (SSE) push real-time match updates to the client without polling. Significantly reduces server load and improves latency compared to HTTP polling.

### `GET /api/v1/live/stream/:match_id`
**Description:** Open a persistent SSE connection for a live match. The server pushes events whenever the match state changes — new incidents, score updates, minute ticks, and insight refreshes.
**Auth:** Public (no JWT required; rate-limited by IP)

**Request:**
```http
GET /api/v1/live/stream/12345678
Accept: text/event-stream
Cache-Control: no-cache
```

**Event Types pushed by server:**

| Event | Payload | Description |
|-------|---------|-------------|
| `incident` | `{ incident_type, minute, player_name, detail }` | Goal, card, substitution, VAR |
| `score` | `{ home_score, away_score, minute }` | Score update |
| `stats` | `{ stats_core_json }` | Match stats refresh |
| `insight` | `{ insight_text }` | New AI insight badge |
| `status` | `{ status, minute }` | Half-time, full-time, extra time |
| `heartbeat` | `{}` | Keep-alive every 30s |

**Example SSE stream:**
```
event: incident
data: {"incident_type":"goal","minute":45,"player_name":"Bukayo Saka","detail":"penalty","is_home":true}

event: score
data: {"home_score":3,"away_score":1,"minute":45}

event: insight
data: {"insight_text":"Arsenal áp đảo hoàn toàn trong 15 phút cuối hiệp một."}

event: heartbeat
data: {}
```

**Connection notes:**
- Client should implement auto-reconnect with exponential backoff on disconnect.
- Connection is automatically closed by the server when `status = finished`.
- For matches with `status = notstarted`, the server begins streaming from kick-off time.

---

## Standings

Table data presenting ranking systems via the `standings` schema.

### `GET /api/v1/standings`
**Description:** Retrieve the current points table for a specified league and season.
**Auth:** Public

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| league_id | string | **Yes** | Filter by league ID (e.g., `EPL`) |
| season | string | **Yes** | Filter by season (e.g., `2024-2025`) |

**Example Request:**
```http
GET /api/v1/standings?league_id=EPL&season=2024-2025
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "position": 1,
      "team_id": "822bd0ba",
      "team_name": "Arsenal",
      "logo_url": "https://cdn.yoursite.com/logos/arsenal.png",
      "matches_played": 38,
      "wins": 28,
      "draws": 5,
      "losses": 5,
      "goals_for": 91,
      "goals_against": 29,
      "goal_difference": 62,
      "points": 89,
      "points_avg": 2.34,
      "form_last5": "WWWDW",
      "top_scorer": "Bukayo Saka"
    }
  ]
}
```

---

### `GET /api/v1/standings/:league_id/:season/:team_id`
**Description:** Retrieve single-team standing position.
**Auth:** Public

**Example Request:**
```http
GET /api/v1/standings/EPL/2024-2025/822bd0ba
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": {
    "position": 1,
    "team_id": "822bd0ba",
    "team_name": "Arsenal",
    "points": 89,
    "wins": 28,
    "draws": 5,
    "losses": 5,
    "goal_difference": 62
  }
}
```

---

### `POST /api/v1/standings`
**Description:** Add a new standing snapshot record.
**Auth:** Admin / Service Daemon

---

### `PUT /api/v1/standings/:league_id/:season/:team_id`
**Description:** Replace all variables in a standings record.
**Auth:** Admin

---

### `PATCH /api/v1/standings/:league_id/:season/:team_id`
**Description:** Partially update points/tally for a team.
**Auth:** Admin

**Example Request:**
```http
PATCH /api/v1/standings/EPL/2024-2025/822bd0ba
Authorization: Bearer <admin_token>
Content-Type: application/json

{
  "points": 92,
  "wins": 29
}
```

---

### `DELETE /api/v1/standings/:league_id/:season/:team_id`
**Description:** Remove a team's standings record (triggers cascades).
**Auth:** Admin

---

## Statistics

Aggregated statistics from `squad_stats`, `player_season_stats`, `gk_stats`, and `mv_player_complete_stats`.

### `GET /api/v1/statistics`
**Description:** Obtain ranked aggregate statistics across a league.
**Auth:** Public

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| type | string | **Yes** | One of: `player`, `team`, `gk` |
| league_id | string | No | League filter |
| season | string | No | Season filter |
| sort_by | string | No | **Whitelisted enum** — see allowed values below |
| page | integer | No | Page number, default 1 |
| limit | integer | No | Max 50 |

**Allowed `sort_by` values by type:**

| `type=player` | `type=team` | `type=gk` |
|---------------|-------------|-----------|
| `goals`, `assists`, `minutes`, `xg`, `shots`, `yellow_cards`, `red_cards`, `goals_per90` | `goals`, `assists`, `possession`, `matches_played`, `goals_per90` | `gk_saves`, `gk_save_pct`, `gk_clean_sheets`, `gk_goals_against` |

**Example Request:**
```http
GET /api/v1/statistics?type=player&league_id=EPL&sort_by=goals&season=2024-2025
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "player_name": "Erling Haaland",
      "player_id": "b34c5d6e",
      "team_name": "Manchester City",
      "goals": 27,
      "assists": 5,
      "shots_on_target_pct": 55.4,
      "goals_per90": 0.97
    }
  ],
  "pagination": { "page": 1, "limit": 20, "total": 398 }
}
```

---

### `GET /api/v1/statistics/:id`
**Description:** Get an individual statistical summary for a player or team.
**Auth:** Public

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| type | string | **Yes** | `player`, `team`, or `gk` |
| season | string | No | Season filter |

**Example Request:**
```http
GET /api/v1/statistics/a23b4c5d?type=player&season=2024-2025
Authorization: Bearer <token>
```

---

### `POST /api/v1/statistics`
**Description:** Inject standalone aggregate measures.
**Auth:** Admin / Service Daemon

---

### `PUT /api/v1/statistics/:id`
**Description:** Hard-overwrite metrics values.
**Auth:** Admin

---

### `PATCH /api/v1/statistics/:id`
**Description:** Incremental statistical record modifier.
**Auth:** Admin

---

### `DELETE /api/v1/statistics/:id`
**Description:** Delete aggregated numbers for an entity.
**Auth:** Admin

---

## AI Insights & News

Narrative AI-generated content and aggregated RSS news created by the background Python Daemon. Covers live badges, match stories, player trends, and news radar.

### `GET /api/v1/news`
**Description:** Retrieve the freshest football news from the `news_feed` table (aggregated from BBC Sport / Sky Sports).
**Auth:** Public

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| limit | integer | No | Items per page, default 10, max 50 |
| league_id | string | No | Filter by league, default `EPL` |
| team_id | string | No | Filter news by team |

**Example Request:**
```http
GET /api/v1/news?limit=5&league_id=EPL
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "id": 1,
      "title": "Arsenal sign new striker",
      "link": "https://www.bbc.co.uk/sport/football/...",
      "summary": "Mikel Arteta confirms the club has agreed terms...",
      "published_at": "2024-05-01T12:00:00Z",
      "source": "BBC Sport",
      "league_id": "EPL"
    }
  ]
}
```

---

### `GET /api/v1/insights/live/:match_id`
**Description:** Retrieve the latest AI momentum insight for an ongoing match (from `live_match_state.insight_text`).
**Auth:** Public

**Example Request:**
```http
GET /api/v1/insights/live/12345678
```

**Example Response:**
```json
{
  "data": {
    "event_id": 12345678,
    "home_team": "Arsenal",
    "away_team": "Chelsea",
    "home_score": 2,
    "away_score": 1,
    "minute": 67,
    "status": "inprogress",
    "insight_text": "Arsenal đang kiểm soát thế trận hoàn toàn ở hiệp 2, liên tục nhồi bóng bổng vào vòng cấm.",
    "updated_at": "2024-05-01T21:12:44Z"
  }
}
```

---

### `GET /api/v1/insights/story/:match_id`
**Description:** Retrieve the post-match AI narrative summary (from `match_summaries`). Available in Vietnamese and English.
**Auth:** User

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| lang | string | No | Language: `vi` (default) or `en` |

**Example Request:**
```http
GET /api/v1/insights/story/12345678?lang=vi
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": {
    "event_id": 12345678,
    "home_team": "Arsenal",
    "away_team": "Chelsea",
    "home_score": 3,
    "away_score": 1,
    "summary_text": "Trận cầu đinh kết thúc với Arsenal giành chiến thắng thuyết phục. Arsenal áp đảo xG nhưng Chelsea vươn lên dẫn trước nhờ khoảnh khắc lóe sáng của Cole Palmer trước khi Saka lập cú đúp ấn định kết quả.",
    "created_at": "2024-05-01T22:30:00Z"
  }
}
```

---

### `GET /api/v1/insights/players`
**Description:** Retrieve all player performance trend alerts (Rising / Falling form) from `player_insights`.
**Auth:** User

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| league_id | string | **Yes** | League ID (e.g., `EPL`) |
| trend_type | string | No | Filter: `GREEN`, `RED`, or `NEUTRAL` |
| lang | string | No | Language: `vi` (default) or `en` |
| limit | integer | No | Max 50 |

**Example Request:**
```http
GET /api/v1/insights/players?league_id=EPL&trend_type=GREEN
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "player_id": 8260,
      "player_name": "Bukayo Saka",
      "league_id": "EPL",
      "trend": "GREEN",
      "trend_score": 85,
      "insight_text": "Bukayo Saka đang thăng hoa với nền tảng thể lực sung mãn, đóng góp 3 bàn trong 2 trận gần nhất.",
      "updated_at": "2024-05-01T06:00:00Z"
    }
  ]
}
```

---

### `POST /api/v1/insights/feedback`
**Description:** Submit quality feedback on an AI insight (upvote, downvote, flag as irrelevant). Writes to `ai_insight_feedback` table to improve future AI pipeline quality.
**Auth:** User

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| job_id | integer | **Yes** | ID of the `ai_insight_jobs` record |
| event_id | integer | No | Match event ID (if applicable) |
| feedback_type | string | **Yes** | One of: `upvote`, `downvote`, `duplicate`, `irrelevant`, `too_generic` |
| score | integer | No | Quality score, range -2 to +2 |
| comment | string | No | Optional free-text comment |

**Example Request:**
```http
POST /api/v1/insights/feedback
Authorization: Bearer <token>
Content-Type: application/json

{
  "job_id": 1042,
  "event_id": 12345678,
  "feedback_type": "upvote",
  "score": 2,
  "comment": "Very accurate insight"
}
```

**Example Response:**
```json
{
  "data": {
    "id": 55,
    "job_id": 1042,
    "feedback_type": "upvote",
    "created_at": "2024-05-01T22:35:00Z"
  }
}
```

---

## Health & Admin

Internal and monitoring endpoints. Admin endpoints require an Admin-role JWT.

### `GET /api/v1/health`
**Description:** Basic health check for uptime monitoring. Returns overall status and dependency health.
**Auth:** Public

**Example Response:**
```json
{
  "status": "ok",
  "timestamp": "2025-01-04T10:00:00Z",
  "dependencies": {
    "database": "ok",
    "redis": "ok",
    "python_daemon": "ok"
  }
}
```

---

### `GET /api/v1/admin/circuit-breakers`
**Description:** View current circuit breaker states from `cb_state_log`. Useful for diagnosing when the AI pipeline or live scraper is degraded.
**Auth:** Admin

**Example Response:**
```json
{
  "data": [
    {
      "breaker_name": "ai_insight_worker",
      "old_state": "closed",
      "new_state": "open",
      "reason": "Failure rate exceeded 50% in last 60s",
      "logged_at": "2025-01-04T09:58:32Z"
    }
  ]
}
```

---

### `POST /api/v1/admin/refresh-views`
**Description:** Trigger a `REFRESH MATERIALIZED VIEW CONCURRENTLY` for one or all materialized views. Non-blocking — existing read queries continue while the refresh runs.
**Auth:** Admin

**Request Body:**
| Field | Type | Required | Description |
|-------|------|----------|-------------|
| view | string | No | View name to refresh. If omitted, refreshes all views in dependency order. |

**Allowed values for `view`:** `mv_tm_player_candidates`, `mv_player_profiles`, `mv_team_profiles`, `mv_shot_agg`, `mv_player_complete_stats`

**Refresh order (automatic when `view` is omitted):**
1. `mv_tm_player_candidates`
2. `mv_player_profiles`
3. `mv_team_profiles`
4. `mv_shot_agg`
5. `mv_player_complete_stats`

**Example Request:**
```http
POST /api/v1/admin/refresh-views
Authorization: Bearer <admin_token>
Content-Type: application/json

{
  "view": "mv_player_complete_stats"
}
```

**Example Response:**
```json
{
  "data": {
    "view": "mv_player_complete_stats",
    "status": "refreshed",
    "duration_ms": 4820
  }
}
```

---

## Error Codes

| Code | Meaning | Notes |
|------|---------|-------|
| 400 | Bad Request | Malformed request, invalid parameters |
| 401 | Unauthorized | Missing or expired JWT token |
| 403 | Forbidden | Valid JWT but insufficient role/permissions |
| 404 | Not Found | Resource does not exist |
| 409 | Conflict | Resource with this ID already exists (use `Idempotency-Key` for safe retries) |
| 415 | Unsupported Media Type | POST/PUT/PATCH requires `Content-Type: application/json` |
| 422 | Validation Error | Request body or query params failed validation (e.g., invalid `sort_by`) |
| 429 | Too Many Requests | Rate limit exceeded — check `Retry-After` header |
| 500 | Internal Server Error | Unexpected server-side error |
| 503 | Service Unavailable | Circuit breaker open or dependency down — check `Retry-After` header |

### Standard error response body:
```json
{
  "error": {
    "code": 422,
    "message": "Invalid sort_by value: 'player_name'. Allowed values: goals, assists, xg, minutes, yellow_cards, red_cards, goals_per90",
    "request_id": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```