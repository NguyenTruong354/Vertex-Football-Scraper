import asyncio
import json
from db.config_db import get_async_pool

async def verify():
    pool = await get_async_pool()
    async with pool.acquire() as conn:
        print("--- 1. AI Job Status Summary ---")
        rows = await conn.fetch("SELECT status, count(*) as cnt FROM ai_insight_jobs GROUP BY status ORDER BY cnt DESC")
        for r in rows:
            print(f"{r['status']}: {r['cnt']}")

        print("\n--- 2. Historical Gate Check ---")
        cnt = await conn.fetchval("SELECT count(*) FROM ai_insight_jobs WHERE reason_code = 'historical_match'")
        print(f"Jobs skipped by Historical Gate: {cnt}")

        print("\n--- 3. Job Table: EPL Notable Players Check ---")
        cnt = await conn.fetchval("SELECT count(*) FROM ai_insight_jobs WHERE league_id = 'EPL' AND job_type = 'player_trend' AND status = 'queued'")
        print(f"Queued deep AI jobs for EPL players: {cnt}")

        print("\n--- 4. Success Sample (Detailed) ---")
        rows = await conn.fetch("""
            SELECT league_id, job_type, result_text_en, is_published, finished_at 
            FROM ai_insight_jobs 
            WHERE status = 'succeeded' 
            ORDER BY finished_at DESC LIMIT 5
        """)
        for r in rows:
            pub = "[SHADOW]" if not r['is_published'] else "[LIVE]"
            print(f"{r['finished_at']} {pub} {r['league_id']} {r['job_type']}: {r['result_text_en'][:50]}...")


    await pool.close()

if __name__ == "__main__":
    asyncio.run(verify())
