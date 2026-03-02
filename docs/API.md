---
# Football Website — API Reference

## Overview
- Base URL: `https://api.yoursite.com/v1`
- Authentication: Bearer Token (JWT)
- Response format: JSON
- Pagination: `?page=1&limit=20`
- Date format: ISO 8601 (`YYYY-MM-DD`)

## Table of Contents
- [Teams](#teams)
- [Players](#players)
- [Matches](#matches)
- [Match Events & Live Data](#match-events--live-data)
- [Standings](#standings)
- [Statistics](#statistics)
- [Error Codes](#error-codes)

---

## Teams
The Teams domain exposes squad metadata, general club information, and related players derived from `team_metadata`, `squad_stats`, and `squad_rosters`.

### `GET /api/teams`
**Description:** Retrieve a paginated list of teams.
**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| page | integer | No | Page number, default 1 |
| limit | integer | No | Items per page, default 20 |
| league_id | string | No | Filter by league (e.g., 'EPL') |
| season | string | No | Filter by season (e.g., '2024-2025') |

**Example Request:**
```http
GET /api/teams?league_id=EPL&season=2024-2025&page=1&limit=20
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
      "logo_url": "https://...",
      "manager_name": "Mikel Arteta"
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

### `GET /api/teams/:id`
**Description:** Retrieve comprehensive details and metadata for a specific team.
**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | string | Team ID |

**Example Request:**
```http
GET /api/teams/822bd0ba
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": {
    "team_name": "Arsenal",
    "team_id": "822bd0ba",
    "league_id": "EPL",
    "season": "2024-2025",
    "stadium_name": "Emirates Stadium",
    "manager_name": "Mikel Arteta",
    "squad_size": 25,
    "total_market_value": "€1.10bn"
  }
}
```

---

### `POST /api/teams`
**Description:** Create a new team record.
**Example Request:**
```http
POST /api/teams
Authorization: Bearer <token>
Content-Type: application/json

{
  "team_id": "new_team",
  "team_name": "FC Example",
  "league_id": "EPL",
  "manager_name": "John Doe",
  "stadium_name": "Example Park"
}
```

---

### `PUT /api/teams/:id`
**Description:** Fully update an existing team's metadata.
**Example Request:**
```http
PUT /api/teams/new_team
Authorization: Bearer <token>
Content-Type: application/json

{
  "team_id": "new_team",
  "team_name": "FC Example Updated",
  "league_id": "EPL",
  "manager_name": "Jane Doe",
  "stadium_name": "Example Park 2"
}
```

---

### `PATCH /api/teams/:id`
**Description:** Partially update an existing team's metadata.
**Example Request:**
```http
PATCH /api/teams/new_team
Authorization: Bearer <token>
Content-Type: application/json

{
  "manager_name": "Mike Manager"
}
```

---

### `DELETE /api/teams/:id`
**Description:** Delete a team record and cascade delete related squad statistics and rosters.
**Example Request:**
```http
DELETE /api/teams/new_team
Authorization: Bearer <token>
```

---

### `GET /api/teams/:id/players`
**Description:** Get all players currently rostered for the specified team.
**Example Request:**
```http
GET /api/teams/822bd0ba/players
Authorization: Bearer <token>
```

---

## Players
The Players domain handles player demographic information, market values, and aggregated multi-source IDs derived from `squad_rosters`, `player_season_stats`, `market_values`, and `player_crossref`.

### `GET /api/players`
**Description:** Retrieve a paginated list of players.
**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| page | integer | No | Page number, default 1 |
| limit | integer | No | Items per page, default 20 |
| team_id | string | No | Filter by team ID |
| position | string | No | Filter by position (e.g., 'FW') |

**Example Request:**
```http
GET /api/players?team_id=822bd0ba&position=FW
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
      "team_name": "Arsenal"
    }
  ],
  "pagination": { ... }
}
```

---

### `GET /api/players/:id`
**Description:** Retrieve full details for a player, including market values and aliases.
**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | string | Player ID |

**Example Request:**
```http
GET /api/players/a23b4c5d
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
    "market_value": "€130.00m",
    "crossref": {
      "understat_id": 8260,
      "fbref_id": "a23b4c5d"
    }
  }
}
```

---

### `POST /api/players`
**Description:** Create a new player record.
**Example Request:**
```http
POST /api/players
Authorization: Bearer <token>
Content-Type: application/json

{
  "player_id": "new_player",
  "player_name": "John Football",
  "team_id": "team_1",
  "nationality": "ENG"
}
```

---

### `PUT /api/players/:id`
**Description:** Fully update an existing player's record.
**Example Request:**
```http
PUT /api/players/new_player
...
```

---

### `PATCH /api/players/:id`
**Description:** Partially update an existing player's record.
**Example Request:**
```http
PATCH /api/players/new_player
Authorization: Bearer <token>
Content-Type: application/json

{
  "position": "MF"
}
```

---

### `DELETE /api/players/:id`
**Description:** Delete a player record.
**Example Request:**
```http
DELETE /api/players/new_player
Authorization: Bearer <token>
```

---

### `GET /api/players/:id/stats`
**Description:** Get aggregated season statistics and per-match performance for a player.
**Example Request:**
```http
GET /api/players/a23b4c5d/stats
Authorization: Bearer <token>
```

---

## Matches
The Matches domain tracks upcoming properties, match outcomes, and aggregate stats derived from `fixtures`, `match_stats`, and `ss_events`.

### `GET /api/matches`
**Description:** Retrieve a paginated list of fixtures and played match records.
**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| page | integer | No | Page number, default 1 |
| limit | integer | No | Items per page, default 20 |
| league_id | string | No | Filter by league ID |
| status | string | No | Filter by status (e.g., 'finished') |
| date | string | No | Match date filter (ISO 8601) |

**Example Request:**
```http
GET /api/matches?league_id=EPL&status=finished
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "match_id": "12345",
      "home_team": "Arsenal",
      "away_team": "Chelsea",
      "score": "3-1",
      "date": "2024-05-01"
    }
  ],
  "pagination": { ... }
}
```

---

### `GET /api/matches/:id`
**Description:** Get specific fixture info, match statistics, and result tracking for a match ID.
**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | string | Match ID |

**Example Request:**
```http
GET /api/matches/12345
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": {
    "match_id": "12345",
    "home_team": "Arsenal",
    "away_team": "Chelsea",
    "h_goals": 3,
    "a_goals": 1,
    "h_xg": 2.5,
    "a_xg": 0.8,
    "referee": "Michael Oliver",
    "venue": "Emirates Stadium"
  }
}
```

---

### `POST /api/matches`
**Description:** Manually insert a new fixture or match record.
**Example Request:**
```http
POST /api/matches
Authorization: Bearer <token>
Content-Type: application/json

{
  "match_id": "new_match",
  "home_team": "Arsenal",
  "away_team": "Chelsea",
  "date": "2025-01-01"
}
```

---

### `PUT /api/matches/:id`
**Description:** Replace an entire match record completely.
**Example Request:**
```http
PUT /api/matches/new_match
...
```

---

### `PATCH /api/matches/:id`
**Description:** Minor updates to match fields.
**Example Request:**
```http
PATCH /api/matches/12345
Authorization: Bearer <token>
Content-Type: application/json

{
  "h_goals": 4,
  "h_xg": 3.1
}
```

---

### `DELETE /api/matches/:id`
**Description:** Delete a match from the schedule / records.
**Example Request:**
```http
DELETE /api/matches/12345
Authorization: Bearer <token>
```

---

### `GET /api/matches/:id/shots`
**Description:** Retrieve all spatial shot tracking data (`shots` table) for a given match.
**Example Request:**
```http
GET /api/matches/12345/shots
Authorization: Bearer <token>
```

---

### `GET /api/matches/:id/heatmaps`
**Description:** Retrieve localized Sofascore player heatmap coordinates for a specific match.
**Example Request:**
```http
GET /api/matches/12345/heatmaps
Authorization: Bearer <token>
```

---

## Match Events & Live Data
Handles real-time game polling and atomic incidents (e.g., goals, substitutions) via `live_snapshots` and `live_incidents`.

### `GET /api/events`
**Description:** Retrieve isolated list of match events (cards, points, substitutions).
**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| event_id | integer | No | Specific SofaScore event/match ID |
| incident_type | string | No | Type (e.g., 'goal', 'card') |

**Example Request:**
```http
GET /api/events?event_id=101&incident_type=goal
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
      "player_name": "Bukayo Saka",
      "detail": "penalty"
    }
  ],
  "pagination": { ... }
}
```

---

### `GET /api/events/:id`
**Description:** Retrieve a single detailed match incident.
**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | integer | Incident ID |

**Example Request:**
```http
GET /api/events/1
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": {
    "id": 1,
    "event_id": 101,
    "incident_type": "goal",
    "minute": 45,
    "player_name": "Bukayo Saka",
    "is_home": true
  }
}
```

---

### `POST /api/events`
**Description:** Dispatch and log a new event/incident.
**Example Request:**
```http
POST /api/events
Authorization: Bearer <token>
Content-Type: application/json

{
  "event_id": 101,
  "incident_type": "card",
  "detail": "yellow",
  "player_name": "John Doe",
  "minute": 55
}
```

---

### `PUT /api/events/:id`
**Description:** Update a match event completely.
**Example Request:**
```http
PUT /api/events/1
...
```

---

### `PATCH /api/events/:id`
**Description:** Correct fields within an executed event.
**Example Request:**
```http
PATCH /api/events/1
Authorization: Bearer <token>
Content-Type: application/json

{
  "detail": "red"
}
```

---

### `DELETE /api/events/:id`
**Description:** Purge a logged incident. 
**Example Request:**
```http
DELETE /api/events/1
Authorization: Bearer <token>
```

---

## Standings
Table data presenting ranking systems via the `standings` schema.

### `GET /api/standings`
**Description:** Retrieve current points table format for specified league and season.
**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| league_id | string | Yes | Filter by league ID |
| season | string | Yes | Filter by season |

**Example Request:**
```http
GET /api/standings?league_id=EPL&season=2024-2025
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "position": 1,
      "team_name": "Arsenal",
      "points": 89,
      "wins": 28,
      "draws": 5,
      "losses": 5,
      "goals_for": 91,
      "goals_against": 29
    }
  ]
}
```

---

### `GET /api/standings/:league_id/:season/:team_id`
**Description:** Retrieve single-team positioning.
**Example Request:**
```http
GET /api/standings/EPL/2024-2025/822bd0ba
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": {
    "position": 1,
    "team_id": "822bd0ba",
    "points": 89
  }
}
```

---

### `POST /api/standings`
**Description:** Add a new table snapshot record.
**Example Request:**
```http
POST /api/standings
Authorization: Bearer <token>
Content-Type: application/json

{
  "league_id": "EPL",
  "season": "2024-2025",
  "team_id": "team_1",
  "points": 0,
  "position": 20
}
```

---

### `PUT /api/standings/:league_id/:season/:team_id`
**Description:** Replace all variables in a standings record.
**Example Request:**
```http
PUT /api/standings/EPL/2024-2025/team_1
...
```

---

### `PATCH /api/standings/:league_id/:season/:team_id`
**Description:** Partially update points/tally for a team.
**Example Request:**
```http
PATCH /api/standings/EPL/2024-2025/822bd0ba
Authorization: Bearer <token>
Content-Type: application/json

{
  "points": 92,
  "wins": 29
}
```

---

### `DELETE /api/standings/:league_id/:season/:team_id`
**Description:** Remove a team's standings metrics (triggers cascades).
**Example Request:**
```http
DELETE /api/standings/EPL/2024-2025/822bd0ba
Authorization: Bearer <token>
```

---

## Statistics
Accumulated team metrics (`squad_stats`) and player performance averages (`player_season_stats`, `gk_stats`).

### `GET /api/statistics`
**Description:** Obtain aggregate statistics queries.
**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| type | string | Yes | `player`, `team`, or `gk` |
| league_id | string | No | League filter |
| sort_by | string | No | Variable to order by (e.g., `goals`) |

**Example Request:**
```http
GET /api/statistics?type=player&league_id=EPL&sort_by=goals
Authorization: Bearer <token>
```

**Example Response:**
```json
{
  "data": [
    {
      "player_name": "Erling Haaland",
      "goals": 27,
      "assists": 5,
      "shots_on_target": 55.4
    }
  ],
  "pagination": { ... }
}
```

---

### `GET /api/statistics/:id`
**Description:** Get an individual statistical summary (by player_id or team_id depending on context parameters).
**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| id | string | Resource ID |

**Example Request:**
```http
GET /api/statistics/a23b4c5d?type=player
Authorization: Bearer <token>
```

---

### `POST /api/statistics`
**Description:** Inject standalone aggregate measures.
**Example Request:**
```http
POST /api/statistics
Authorization: Bearer <token>
Content-Type: application/json

{
  "type": "team",
  "team_id": "team_1",
  "goals": 50,
  "possession": 55.5
}
```

---

### `PUT /api/statistics/:id`
**Description:** Hard-overwrite metrics values.
**Example Request:**
```http
PUT /api/statistics/team_1?type=team
...
```

---

### `PATCH /api/statistics/:id`
**Description:** Incremental statistical record modifier.
**Example Request:**
```http
PATCH /api/statistics/a23b4c5d?type=player
Authorization: Bearer <token>
Content-Type: application/json

{
  "goals": 28
}
```

---

### `DELETE /api/statistics/:id`
**Description:** Delete aggregated numbers for an entity.
**Example Request:**
```http
DELETE /api/statistics/a23b4c5d?type=player
Authorization: Bearer <token>
```

---

## Error Codes
| Code | Meaning |
|------|---------|
| 400 | Bad Request |
| 401 | Unauthorized |
| 403 | Forbidden |
| 404 | Not Found |
| 422 | Validation Error |
| 500 | Internal Server Error |
