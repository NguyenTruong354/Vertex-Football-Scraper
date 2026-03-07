"""
AI Insight Pipeline — Feedback Ingestion

Provides functions to record feedback on AI-generated insights
into the ai_insight_feedback table.

Usage:
    from services.insight_feedback import record_feedback
    record_feedback(
        job_id=42,
        feedback_type="duplicate",
        score=-1,
        channel="discord",
        comment="Same insight as 5 minutes ago",
    )
"""

import logging
from typing import Optional

log = logging.getLogger(__name__)

VALID_TYPES = ("upvote", "downvote", "duplicate", "irrelevant", "too_generic")


def record_feedback(
    job_id: int,
    feedback_type: str,
    *,
    event_id: int = None,
    league_id: str = None,
    channel: str = None,
    score: int = None,
    tags: list = None,
    comment: str = None,
    created_by: str = None,
) -> Optional[int]:
    """
    Record feedback for an AI insight job.

    Args:
        job_id: The ai_insight_jobs.id being reviewed
        feedback_type: One of VALID_TYPES
        event_id: Optional event context
        league_id: Optional league context
        channel: Source channel (discord, api, manual_review)
        score: -2 to +2 (optional)
        tags: List of string tags (optional)
        comment: Free-text comment (optional)
        created_by: Identifier of who created the feedback

    Returns:
        feedback ID if inserted, None on error.
    """
    if feedback_type not in VALID_TYPES:
        log.error("Invalid feedback_type '%s'. Must be one of %s",
                  feedback_type, VALID_TYPES)
        return None

    if score is not None and not (-2 <= score <= 2):
        log.error("Invalid score %d. Must be between -2 and 2", score)
        return None

    from db.config_db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ai_insight_feedback
                (job_id, event_id, league_id, channel, feedback_type,
                 score, tags, comment, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            job_id, event_id, league_id, channel, feedback_type,
            score, tags, comment, created_by,
        ))
        row = cur.fetchone()
        conn.commit()
        feedback_id = row[0]
        log.info("Feedback recorded: id=%d job_id=%d type=%s score=%s",
                 feedback_id, job_id, feedback_type, score)
        return feedback_id
    except Exception as exc:
        conn.rollback()
        log.error("Failed to record feedback for job_id=%d: %s",
                  job_id, exc)
        return None
    finally:
        conn.close()


def get_quality_summary(days: int = 7) -> dict:
    """
    Get a quality summary of AI insights over the last N days.
    Returns dict with counts by feedback_type and average scores.
    """
    from db.config_db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Feedback distribution
        cur.execute("""
            SELECT feedback_type, COUNT(*), AVG(score)
            FROM ai_insight_feedback
            WHERE created_at > NOW() - make_interval(days => %s)
            GROUP BY feedback_type
            ORDER BY COUNT(*) DESC
        """, (days,))
        type_stats = {}
        for fb_type, count, avg_score in cur.fetchall():
            type_stats[fb_type] = {
                "count": count,
                "avg_score": round(float(avg_score), 2) if avg_score else None,
            }

        # Job outcome distribution
        cur.execute("""
            SELECT status, COUNT(*)
            FROM ai_insight_jobs
            WHERE created_at > NOW() - make_interval(days => %s)
            GROUP BY status
        """, (days,))
        job_stats = dict(cur.fetchall())

        # Duplicate/drop rates
        cur.execute("""
            SELECT reason_code, COUNT(*)
            FROM ai_insight_jobs
            WHERE status = 'dropped'
              AND created_at > NOW() - make_interval(days => %s)
            GROUP BY reason_code
        """, (days,))
        drop_reasons = dict(cur.fetchall())

        return {
            "period_days": days,
            "feedback_by_type": type_stats,
            "jobs_by_status": job_stats,
            "drops_by_reason": drop_reasons,
        }
    finally:
        conn.close()
