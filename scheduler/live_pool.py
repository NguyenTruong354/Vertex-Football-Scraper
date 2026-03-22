import asyncio
import json
import logging
import time
from datetime import datetime, timezone

from core.models import LiveMatchState, is_tier_c_trigger
from network.http_client import CurlCffiClient
from core.utils import Notifier
from core.antiban import AntiBanState
from services import live_insight, insight_producer

class LiveTrackingPool:
    """Tracks multiple concurrent matches via round-robin polling on HTTP client."""

    _DRIFT_INTERVAL = 300  # 5 minutes between drift checks

    def __init__(
        self,
        browser: CurlCffiClient,
        log: logging.Logger,
        notifier: Notifier,
        *,
        dry_run: bool = False,
    ):
        self.browser = browser
        self.log = log
        self.notifier = notifier
        self.dry_run = dry_run
        self._matches: dict[int, LiveMatchState] = {}
        self.live_drift_mismatch_count: int = 0

    @property
    def is_empty(self) -> bool:
        return len(self._matches) == 0

    @property
    def active_count(self) -> int:
        return len(self._matches)

    def add_match(self, match: dict) -> None:
        eid = match["event_id"]
        if eid in self._matches:
            return
        self._matches[eid] = LiveMatchState(
            event_id=eid,
            league=match.get("league", ""),
            home_team=match["home_team"],
            away_team=match["away_team"],
            status=match["status"],
            start_timestamp=match.get("kickoff_ts", 0),
        )
        self.log.info(
            "➕ Added to pool: [%s] %s vs %s (event=%d)",
            match.get("league", "?"),
            match["home_team"],
            match["away_team"],
            eid,
        )
        self.notifier.send(
            "match_start",
            f"[{match.get('league', '?')}] {match['home_team']} vs {match['away_team']} — tracking started",
        )

    async def poll_all(self) -> list[dict]:
        """
        Poll all active matches once (round-robin).
        Returns list of matches that just finished.
        """
        finished = []
        event_ids = list(self._matches.keys())

        for eid in event_ids:
            state = self._matches.get(eid)
            if not state:
                continue

            still_playing = await self._poll_one(state)

            if not still_playing:
                self.log.info(
                    "🏁 Match finished: [%s] %s %d-%d %s",
                    state.league,
                    state.home_team,
                    state.home_score,
                    state.away_score,
                    state.away_team,
                )
                self.notifier.send(
                    "match_end",
                    f"[{state.league}] {state.home_team} {state.home_score}-{state.away_score} {state.away_team}",
                )

                # ── Post-match flush (plan_live Step 4) ──
                flush_ok = await self._final_flush(state)
                self._save_to_db(state, flush_incomplete=not flush_ok)

                finished.append(
                    {
                        "event_id": eid,
                        "league": state.league,
                        "home_team": state.home_team,
                        "away_team": state.away_team,
                    }
                )
                del self._matches[eid]

            # Smooth pacing: spread polling evenly across 60 seconds
            if len(event_ids) > 1:
                per_match_interval = max(60.0 / len(event_ids), 3.0)
                await asyncio.sleep(per_match_interval)

        return finished

    async def _final_flush(self, state: LiveMatchState) -> bool:
        """Final pull sequence on match finish (plan_live Step 4).
        Returns True if all endpoints fetched successfully."""
        _RETRY_DELAYS = (10, 30, 60)
        endpoints = {
            "event": f"/event/{state.event_id}",
            "incidents": f"/event/{state.event_id}/incidents",
            "statistics": f"/event/{state.event_id}/statistics",
            "lineups": f"/event/{state.event_id}/lineups",
        }
        failed = set(endpoints.keys())

        for delay_idx in range(len(_RETRY_DELAYS) + 1):
            still_failed = set()
            for name in list(failed):
                tier = (
                    "C" if name == "lineups" else ("B" if name == "statistics" else "A")
                )
                data = await self.browser.get_json(endpoints[name], tier=tier)
                if data:
                    # Apply data to state
                    if name == "event":
                        ev = data.get("event", data)
                        state.home_score = (
                            ev.get("homeScore", {}).get("current", state.home_score)
                            or 0
                        )
                        state.away_score = (
                            ev.get("awayScore", {}).get("current", state.away_score)
                            or 0
                        )
                        state.status = ev.get("status", {}).get("type", state.status)
                    elif name == "incidents":
                        state.incidents = data.get("incidents", state.incidents)
                    elif name == "statistics":
                        stats = {}
                        for period in data.get("statistics", []):
                            if period.get("period") != "ALL":
                                continue
                            for group in period.get("groups", []):
                                for item in group.get("statisticsItems", []):
                                    stats[item.get("name", "")] = {
                                        "home": item.get("home", ""),
                                        "away": item.get("away", ""),
                                    }
                        if stats:
                            state.statistics = stats
                    elif name == "lineups":
                        await self._upsert_lineup_from_data(state, data)
                else:
                    still_failed.add(name)

            failed = still_failed
            if not failed:
                self.log.info("  ✅ Final flush complete for event %d", state.event_id)
                return True
            if delay_idx < len(_RETRY_DELAYS):
                delay = _RETRY_DELAYS[delay_idx]
                self.log.info(
                    "  ⏳ Final flush retry in %ds (missing: %s)",
                    delay,
                    ", ".join(sorted(failed)),
                )
                await asyncio.sleep(delay)

        self.log.warning(
            "  ⚠ Final flush incomplete for event %d (missing: %s)",
            state.event_id,
            ", ".join(sorted(failed)),
        )
        return False

    async def _poll_one(self, state: LiveMatchState) -> bool:
        """Poll a single match. Returns True if still in progress."""
        state.poll_count += 1
        state.last_updated = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

        if self.dry_run:
            self.log.info(
                "[DRY-RUN] Poll #%d: %s vs %s",
                state.poll_count,
                state.home_team,
                state.away_team,
            )
            return True

        # ── Resolve anti-ban state machine (may be None) ──
        ab = self.browser.antiban
        metrics = self.browser.metrics
        now_mono = time.monotonic()

        # ── 0-15 min fast-poll guard ──
        if state.start_timestamp:
            elapsed_match_min = max(
                0,
                (int(datetime.now(timezone.utc).timestamp()) - state.start_timestamp)
                // 60,
            )
        else:
            elapsed_match_min = state.minute

        # In first 15 min: if antiban says no fast poll, skip every other poll
        if elapsed_match_min < 15 and ab and not ab.should_allow_fast_poll():
            if state.poll_count % 2 == 0:
                self.log.debug(
                    "⏸️ 0-15min guard: skipping poll #%d for %s vs %s",
                    state.poll_count,
                    state.home_team,
                    state.away_team,
                )
                if metrics:
                    metrics.record_skip()
                return True

        # 1. Match info (Tier A — always)
        info = await self.browser.get_json(f"/event/{state.event_id}", tier="A")
        if info:
            ev = info.get("event", info)
            hs = ev.get("homeScore", {})
            aws = ev.get("awayScore", {})
            st = ev.get("status", {})

            old_score = (state.home_score, state.away_score)
            state.home_team = ev.get("homeTeam", {}).get("name", state.home_team)
            state.away_team = ev.get("awayTeam", {}).get("name", state.away_team)
            state.home_score = hs.get("current", state.home_score) or 0
            state.away_score = aws.get("current", state.away_score) or 0
            state.status = st.get("type", state.status)
            state.start_timestamp = ev.get("startTimestamp", state.start_timestamp)

            new_score = (state.home_score, state.away_score)
            if new_score != old_score and state.poll_count > 1:
                self.notifier.send(
                    "goal",
                    f"[{state.league}] {state.home_team} {state.home_score}-{state.away_score} {state.away_team}",
                )

            # Calculate minute
            if state.status == "inprogress" and state.start_timestamp:
                now_ts = int(datetime.now(timezone.utc).timestamp())
                state.minute = max(0, min(120, (now_ts - state.start_timestamp) // 60))

        # 2. Incidents (Tier A — always)
        inc_data = await self.browser.get_json(
            f"/event/{state.event_id}/incidents", tier="A"
        )
        # FIX Issue #1: Save old_incidents BEFORE updating state.incidents
        # so both Discord alerts AND Tier C detection use the correct old list.
        old_incidents = state.incidents  # snapshot before overwrite

        if inc_data:
            current_incidents = inc_data.get("incidents", [])

            # Detect new incidents for Discord alerts (using old_incidents)
            if state.poll_count > 1 and old_incidents:
                old_ids = {i.get("id") for i in old_incidents if i.get("id")}
                for inc in current_incidents:
                    iid = inc.get("id")
                    if iid and iid not in old_ids:
                        inc_type = inc.get("incidentType", "")
                        inc_class = inc.get("incidentClass", "")
                        player = inc.get("player", {}).get("name", "")
                        time_str = f"{inc.get('time', '?')}'"
                        if inc.get("addedTime"):
                            time_str += f"+{inc.get('addedTime')}'"

                        if inc_type == "card" and inc_class in ("red", "yellowRed"):
                            self.notifier.send(
                                "match_event",
                                f"🟥 **RED CARD** [{state.league}] {state.home_team} vs {state.away_team} | {player} ({time_str})",
                            )
                        elif inc_type == "varDecision":
                            self.notifier.send(
                                "match_event",
                                f"📺 **VAR DECISION** [{state.league}] {state.home_team} vs {state.away_team} | {time_str}",
                            )
                        elif inc_type == "penalty":
                            self.notifier.send(
                                "match_event",
                                f"🎯 **PENALTY** [{state.league}] {state.home_team} vs {state.away_team} | {time_str}",
                            )

            state.incidents = current_incidents

        # 3. Statistics — Tier B (interval driven by anti-ban state + capacity)
        n_active = len(self._matches)
        tier_b_interval = ab.capacity_adjusted_interval("B", n_active) if ab else 180
        should_fetch_stats = (tier_b_interval > 0) and (
            now_mono - state._last_tier_b_ts >= tier_b_interval
        )
        if should_fetch_stats:
            stat_data = await self.browser.get_json(
                f"/event/{state.event_id}/statistics", tier="B"
            )
            if stat_data:
                stats = {}
                for period in stat_data.get("statistics", []):
                    if period.get("period") != "ALL":
                        continue
                    for group in period.get("groups", []):
                        for item in group.get("statisticsItems", []):
                            stats[item.get("name", "")] = {
                                "home": item.get("home", ""),
                                "away": item.get("away", ""),
                            }
                state.statistics = stats
            state._last_tier_b_ts = now_mono

        # 3b. Lineups — Tier C (event-driven with cooldown, plan_live Step 3)
        tier_c_cooldown = ab.tier_c_cooldown() if ab else 90
        tier_c_fetched = False
        if tier_c_cooldown > 0:  # 0 = Tier C paused
            # FIX Issue #1: Detect triggers using old_incidents (saved before overwrite)
            if inc_data and state.poll_count > 1 and old_incidents:
                old_ids = {i.get("id") for i in old_incidents if i.get("id")}
                for inc in inc_data.get("incidents") or []:
                    iid = inc.get("id")
                    if iid and iid not in old_ids and is_tier_c_trigger(inc):
                        state._tier_c_pending = True
                        break

            # Respect per-match cooldown
            elapsed_c = now_mono - state._last_tier_c_ts
            # Capacity hard cap for >10 matches
            if ab and n_active > 10:
                tier_c_cooldown = max(tier_c_cooldown, 480)
            if state._tier_c_pending and elapsed_c >= tier_c_cooldown:
                lineup_data = await self.browser.get_json(
                    f"/event/{state.event_id}/lineups", tier="C"
                )
                if lineup_data:
                    await self._upsert_lineup_from_data(state, lineup_data)
                    tier_c_fetched = True
                state._tier_c_pending = False
                state._last_tier_c_ts = now_mono

        # 4. Momentum Analysis & Insights
        # Generate insight only if we just fetched fresh stats
        if should_fetch_stats and state.statistics:
            score, insight = live_insight.analyze(
                home_team=state.home_team,
                away_team=state.away_team,
                minute=state.minute,
                home_score=state.home_score,
                away_score=state.away_score,
                statistics=state.statistics,
                incidents=state.incidents,
            )
            if insight:
                # Log insight on discord if it changes
                if insight != state.insight_text:
                    self.notifier.send(
                        "live",
                        f"💡 [INSIGHT] {state.league} ({state.home_team} vs {state.away_team}): {insight}",
                    )
                state.insight_text = insight

            # Phase B (shadow): enqueue live_badge job for AI pipeline
            if score > 0:
                try:
                    insight_producer.enqueue_live_badge(
                        event_id=state.event_id,
                        league_id=state.league,
                        home_team=state.home_team,
                        away_team=state.away_team,
                        home_score=state.home_score,
                        away_score=state.away_score,
                        minute=state.minute,
                        statistics=state.statistics,
                        incidents=state.incidents,
                        momentum_score=score,
                    )
                except Exception as exc:
                    self.log.debug("Insight producer error: %s", exc)

        # 5. Save to DB every poll
        await self._save_to_db(state)

        # 6. Drift guard: compare live_match_state vs live_snapshots
        drift_now = time.monotonic()
        should_check_drift = False
        if state.status == "finished":
            should_check_drift = True  # mandatory at finish
        elif state.status == "inprogress":
            if drift_now - state.last_drift_check >= self._DRIFT_INTERVAL:
                should_check_drift = True
        if should_check_drift:
            await self._check_drift(state)
            state.last_drift_check = drift_now

        # 7. Metrics emit (every 60s)
        if metrics:
            ab_state = ab.state if ab else AntiBanState.NORMAL
            metrics.maybe_emit(len(self._matches), ab_state)

        self.log.info(
            "  📊 [%s] %s %d-%d %s | %s' | poll #%d | %s",
            state.league,
            state.home_team,
            state.home_score,
            state.away_score,
            state.away_team,
            state.minute,
            state.poll_count,
            ab.state.value if ab else "NORMAL",
        )

        return state.status != "finished"

    async def _check_drift(self, state: LiveMatchState) -> None:
        """Compare live_match_state vs live_snapshots for key fields.
        Emits ERROR log + increments metric counter on mismatch.
        Never raises — polling loop must stay alive."""
        if not getattr(self, 'db_pool', None):
            return

        try:
            row = await self.db_pool.fetchrow(
                """
                SELECT ls.home_score, ls.away_score, ls.status, ls.minute,
                       lm.home_score, lm.away_score, lm.status, lm.minute
                FROM live_snapshots ls
                JOIN live_match_state lm ON lm.event_id = ls.event_id
                WHERE ls.event_id = $1
            """,
                state.event_id,
            )
            if row is None:
                return  # one side missing, skip (first write race)
            ls_hs, ls_as, ls_st, ls_min = row[0], row[1], row[2], row[3]
            lm_hs, lm_as, lm_st, lm_min = row[4], row[5], row[6], row[7]
            diffs = []
            if ls_hs != lm_hs:
                diffs.append(f"home_score snap={ls_hs} state={lm_hs}")
            if ls_as != lm_as:
                diffs.append(f"away_score snap={ls_as} state={lm_as}")
            if ls_st != lm_st:
                diffs.append(f"status snap={ls_st} state={lm_st}")
            if ls_min != lm_min:
                diffs.append(f"minute snap={ls_min} state={lm_min}")
            if diffs:
                self.live_drift_mismatch_count += 1
                self.log.error(
                    "DRIFT MISMATCH event=%d [%s] %s vs %s: %s (total_mismatches=%d)",
                    state.event_id,
                    state.league,
                    state.home_team,
                    state.away_team,
                    "; ".join(diffs),
                    self.live_drift_mismatch_count,
                )
        except Exception as exc:
            self.log.warning("Drift check failed for event %d: %s", state.event_id, exc)

    async def _save_to_db(
        self, state: LiveMatchState, *, flush_incomplete: bool = False
    ) -> None:
        """Upsert to live_snapshots + live_match_state + live_incidents (dual-write).

        Transaction strategy:
          Phase 1: live_snapshots + live_incidents -> COMMIT (always succeeds)
          Phase 2: auto-seed ss_events + live_match_state -> COMMIT (separate)
        This ensures core live data is never lost if live_match_state FK fails.
        """
        if not getattr(self, 'db_pool', None):
            return

        # == Phase 1: live_snapshots + live_incidents (critical path) ==
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        """
                        INSERT INTO live_snapshots
                            (event_id, home_team, away_team, home_score, away_score,
                             status, minute, statistics_json, incidents_json, insight_text, poll_count)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        ON CONFLICT (event_id) DO UPDATE SET
                            home_score = EXCLUDED.home_score,
                            away_score = EXCLUDED.away_score,
                            status = EXCLUDED.status,
                            minute = EXCLUDED.minute,
                            statistics_json = EXCLUDED.statistics_json,
                            incidents_json = EXCLUDED.incidents_json,
                            insight_text = EXCLUDED.insight_text,
                            poll_count = EXCLUDED.poll_count,
                            loaded_at = NOW()
                    """,
                        state.event_id,
                        state.home_team,
                        state.away_team,
                        state.home_score,
                        state.away_score,
                        state.status,
                        state.minute,
                        json.dumps(state.statistics, ensure_ascii=False),
                        json.dumps(state.incidents, ensure_ascii=False),
                        state.insight_text,
                        state.poll_count,
                    )

                    inc_rows = []
                    for inc in state.incidents:
                        inc_type = inc.get("incidentType", "")
                        if inc_type not in ("goal", "card", "substitution", "varDecision"):
                            continue
                        inc_rows.append(
                            (
                                state.event_id,
                                inc_type,
                                inc.get("time"),
                                inc.get("addedTime"),
                                inc.get("player", {}).get("name"),
                                inc.get("playerIn", {}).get("name"),
                                inc.get("playerOut", {}).get("name"),
                                inc.get("isHome"),
                                inc.get("incidentClass", ""),
                            )
                        )
                    
                    if inc_rows:
                        await conn.executemany(
                            """
                            INSERT INTO live_incidents
                                (event_id, incident_type, minute, added_time,
                                 player_name, player_in_name, player_out_name,
                                 is_home, detail)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT (event_id, incident_type, minute,
                                         COALESCE(added_time, -1),
                                         COALESCE(player_name, ''), is_home)
                            DO UPDATE SET
                                added_time     = EXCLUDED.added_time,
                                player_in_name = EXCLUDED.player_in_name,
                                player_out_name= EXCLUDED.player_out_name,
                                is_home        = EXCLUDED.is_home,
                                detail         = EXCLUDED.detail,
                                loaded_at      = NOW()
                        """,
                            inc_rows
                        )
        except Exception as exc:
            self.log.warning("DB save Phase 1 failed for event %d: %s", state.event_id, exc)
            return

        # == Phase 2: live_match_state (separate transaction) ==
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    # Auto-seed ss_events to satisfy FK constraint
                    await conn.execute(
                        """
                        INSERT INTO ss_events (event_id, league_id, match_date, home_team, away_team)
                        VALUES ($1, $2, CURRENT_DATE, $3, $4)
                        ON CONFLICT (event_id, league_id) DO NOTHING
                        """,
                        state.event_id, state.league, state.home_team, state.away_team
                    )

                    stats_core = {}
                    _CORE_KEYS = (
                        "Ball possession",
                        "Shots on target",
                        "Expected goals",
                        "Dangerous attacks",
                    )
                    for key in _CORE_KEYS:
                        if key in state.statistics:
                            stats_core[key] = state.statistics[key]
                    stats_core_json = (
                        json.dumps(stats_core, ensure_ascii=False) if stats_core else None
                    )

                    row = await conn.fetchrow(
                        "SELECT MAX(seq) FROM live_incidents WHERE event_id = $1",
                        state.event_id,
                    )
                    max_seq = row[0] if row else None

                    await conn.execute(
                        """
                        INSERT INTO live_match_state
                            (event_id, league_id, home_team, away_team, home_score, away_score,
                             status, minute, poll_count, insight_text, stats_core_json,
                             last_processed_seq, flush_incomplete)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                        ON CONFLICT (event_id) DO UPDATE SET
                            home_score         = EXCLUDED.home_score,
                            away_score         = EXCLUDED.away_score,
                            status             = EXCLUDED.status,
                            minute             = EXCLUDED.minute,
                            poll_count         = EXCLUDED.poll_count,
                            insight_text       = EXCLUDED.insight_text,
                            stats_core_json    = EXCLUDED.stats_core_json,
                            last_processed_seq = EXCLUDED.last_processed_seq,
                            flush_incomplete   = EXCLUDED.flush_incomplete,
                            loaded_at          = NOW()
                    """,
                        state.event_id,
                        state.league,
                        state.home_team,
                        state.away_team,
                        state.home_score,
                        state.away_score,
                        state.status,
                        state.minute,
                        state.poll_count,
                        state.insight_text,
                        stats_core_json,
                        max_seq,
                        flush_incomplete,
                    )
        except Exception as exc:
            self.log.warning("DB save Phase 2 (live_match_state) failed for event %d: %s", state.event_id, exc)

    async def _upsert_lineup_from_data(self, state: LiveMatchState, data: dict) -> None:
        """Upsert lineup rows from Tier C /lineups response (plan_live Step 3)."""
        if not getattr(self, 'db_pool', None):
            return
            
        try:
            rows = []
            for side in ("home", "away"):
                lineup_data = data.get(side, {})
                formation = lineup_data.get("formation")
                team_name = state.home_team if side == "home" else state.away_team
                for p in lineup_data.get("players", []):
                    pi = p.get("player", {})
                    stats = p.get("statistics", {})
                    pid = pi.get("id")
                    if not pid:
                        continue
                    rows.append(
                        (
                            state.event_id,
                            pid,
                            pi.get("name") or pi.get("shortName"),
                            side,
                            team_name,
                            pi.get("position"),
                            pi.get("jerseyNumber"),
                            p.get("substitute", False),
                            stats.get("minutesPlayed"),
                            stats.get("rating"),
                            formation,
                            "confirmed",
                            state.league,
                            "",
                        )
                    )
            if not rows:
                return

            sql = """
                INSERT INTO match_lineups
                    (event_id, player_id, player_name, team_side, team_name,
                     position, jersey_number, is_substitute, minutes_played,
                     rating, formation, status, league_id, season)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (event_id, player_id, league_id) DO UPDATE SET
                    minutes_played = COALESCE(EXCLUDED.minutes_played, match_lineups.minutes_played),
                    rating = COALESCE(EXCLUDED.rating, match_lineups.rating),
                    is_substitute = EXCLUDED.is_substitute,
                    formation = EXCLUDED.formation,
                    loaded_at = NOW()
            """
            await self.db_pool.executemany(sql, rows)
            
            self.log.info(
                "  📋 Tier C lineup refresh: %s vs %s — %d players",
                state.home_team,
                state.away_team,
                len(rows),
            )
        except Exception as exc:
            self.log.warning(
                "Tier C lineup upsert failed for event %d: %s", state.event_id, exc
            )


