"""
AI Insight Pipeline — Phase C Cutover Utilities

Pre-cutover checklist (plan §9 Phase C):
1. Drain queue: ensure no live_badge jobs remain in queued/running.
2. Optional cooldown anchor backfill: set recent N succeeded live_badge
   jobs per event from shadow window to is_published=TRUE.
3. Verify cooldown state: last published row exists for active events.

Usage:
  python -m services.insight_cutover --check
  python -m services.insight_cutover --backfill --n=3
  python -m services.insight_cutover --drain --timeout=60
"""

import argparse
import logging
import time

log = logging.getLogger(__name__)


def check_queue_drained() -> dict:
    """Check if queue is drained (no queued/running live_badge jobs).
    Returns {drained: bool, queued: int, running: int}."""
    from db.config_db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT status, COUNT(*)
            FROM ai_insight_jobs
            WHERE job_type = 'live_badge'
              AND status IN ('queued', 'running')
            GROUP BY status
        """)
        counts = dict(cur.fetchall())
        queued = counts.get("queued", 0)
        running = counts.get("running", 0)
        return {
            "drained": queued == 0 and running == 0,
            "queued": queued,
            "running": running,
        }
    finally:
        conn.close()


def drain_queue(timeout_seconds: int = 60) -> bool:
    """Wait for queue to drain. Returns True if drained within timeout."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        result = check_queue_drained()
        if result["drained"]:
            log.info("Queue drained: no active live_badge jobs.")
            return True
        log.info("Waiting for drain: queued=%d running=%d",
                 result["queued"], result["running"])
        time.sleep(5)
    log.warning("Drain timeout after %ds. Queue not empty.", timeout_seconds)
    return False


def backfill_cooldown_anchors(n_per_event: int = 3) -> int:
    """Backfill: set recent N succeeded live_badge jobs per event
    from shadow window (is_published=FALSE) to published.
    Returns total rows updated."""
    from db.config_db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor()
        # Get distinct events with un-published succeeded jobs
        cur.execute("""
            UPDATE ai_insight_jobs
            SET is_published = TRUE,
                published_at = finished_at
            WHERE id IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY event_id
                               ORDER BY finished_at DESC
                           ) AS rn
                    FROM ai_insight_jobs
                    WHERE job_type = 'live_badge'
                      AND status = 'succeeded'
                      AND is_published = FALSE
                      AND finished_at IS NOT NULL
                ) ranked
                WHERE rn <= %s
            )
        """, (n_per_event,))
        updated = cur.rowcount
        conn.commit()
        log.info("Backfill: updated %d rows (n_per_event=%d)",
                 updated, n_per_event)
        return updated
    except Exception as exc:
        conn.rollback()
        log.error("Backfill failed: %s", exc)
        return 0
    finally:
        conn.close()


def verify_cooldown_state() -> dict:
    """Verify cooldown anchor state: check how many active events have
    at least one published live_badge job.
    Returns {events_with_anchor: int, events_without: int, detail: list}."""
    from db.config_db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor()
        # Find events with recent succeeded jobs but no published anchor
        cur.execute("""
            SELECT DISTINCT j.event_id, j.league_id
            FROM ai_insight_jobs j
            WHERE j.job_type = 'live_badge'
              AND j.status = 'succeeded'
              AND j.finished_at > NOW() - INTERVAL '24 hours'
              AND NOT EXISTS (
                  SELECT 1 FROM ai_insight_jobs p
                  WHERE p.event_id = j.event_id
                    AND p.job_type = 'live_badge'
                    AND p.status = 'succeeded'
                    AND p.is_published = TRUE
              )
        """)
        without_anchor = cur.fetchall()

        cur.execute("""
            SELECT COUNT(DISTINCT event_id)
            FROM ai_insight_jobs
            WHERE job_type = 'live_badge'
              AND status = 'succeeded'
              AND is_published = TRUE
              AND finished_at > NOW() - INTERVAL '24 hours'
        """)
        with_anchor = cur.fetchone()[0]

        return {
            "events_with_anchor": with_anchor,
            "events_without": len(without_anchor),
            "events_needing_backfill": [
                {"event_id": r[0], "league_id": r[1]}
                for r in without_anchor
            ],
        }
    finally:
        conn.close()


def run_pre_cutover_checklist(backfill_n: int = 3,
                              drain_timeout: int = 60) -> bool:
    """Run full pre-cutover checklist. Returns True if all checks pass."""
    print("=" * 60)
    print("Phase C Pre-Cutover Checklist")
    print("=" * 60)

    # Step 1: Drain
    print("\n[1/3] Checking queue drain status...")
    drain_result = check_queue_drained()
    if not drain_result["drained"]:
        print(f"  WARNING: Queue not drained - "
              f"queued={drain_result['queued']} running={drain_result['running']}")
        print("  Run with --drain to wait for drain.")
        return False
    print("  OK: Queue is drained.")

    # Step 2: Backfill (informational)
    print(f"\n[2/3] Cooldown anchor state...")
    state = verify_cooldown_state()
    print(f"  Events with published anchor: {state['events_with_anchor']}")
    print(f"  Events needing backfill:      {state['events_without']}")
    if state["events_without"] > 0:
        print(f"  Run with --backfill --n={backfill_n} to set anchors.")

    # Step 3: Final verification
    print(f"\n[3/3] Final status...")
    post_state = verify_cooldown_state()
    all_ok = drain_result["drained"] and post_state["events_without"] == 0
    if all_ok:
        print("  ALL CHECKS PASSED — Safe to flip to Phase C.")
    else:
        print("  Some checks need attention before cutover.")
    print("=" * 60)
    return all_ok


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Phase C cutover utilities")
    parser.add_argument("--check", action="store_true",
                        help="Run pre-cutover checklist")
    parser.add_argument("--drain", action="store_true",
                        help="Wait for queue to drain")
    parser.add_argument("--backfill", action="store_true",
                        help="Backfill cooldown anchors")
    parser.add_argument("--n", type=int, default=3,
                        help="N per event for backfill (default: 3)")
    parser.add_argument("--timeout", type=int, default=60,
                        help="Drain timeout in seconds (default: 60)")
    args = parser.parse_args()

    if args.drain:
        drain_queue(timeout_seconds=args.timeout)
    elif args.backfill:
        backfill_cooldown_anchors(n_per_event=args.n)
    elif args.check:
        run_pre_cutover_checklist(backfill_n=args.n,
                                 drain_timeout=args.timeout)
    else:
        run_pre_cutover_checklist()
