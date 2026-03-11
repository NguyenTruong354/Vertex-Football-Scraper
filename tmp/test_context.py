import logging
import sys
from pathlib import Path
import json

# Setup environment
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.live_insight import analyze
from services.prompt_registry import get_prompt

# Mock data
statistics = {
    "Ball possession": {"home": 70, "away": 30},
    "Shots on target": {"home": 8, "away": 1},
    "Expected goals": {"home": 2.50, "away": 0.40}
}
incidents = []

def test_context_awareness():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    
    print("\n" + "="*70)
    print("🧪 TESTING CONTEXT AWARENESS (ANTI-REPETITION) 🧪")
    print("="*70)

    # 1. First Run: No historical context (mocking first insight)
    print("\n[RUN 1] No historical context...")
    # Note: result_text in DB for event -1 must be empty or non-existent to test 'no history' properly
    # For local test, analyze will just find whatever is in DB for event_id=-1
    # We use a likely unused event_id
    TEST_EVENT_ID = 999999999
    
    score1, insight1 = analyze(
        "Liverpool", "Chelsea", 35, 1, 0, 
        statistics, incidents, event_id=TEST_EVENT_ID
    )
    print(f"Momentum: {score1}")
    print(f"✨ Insight 1: {insight1}")

    # 2. Second Run: To test if it REALLY avoids repetition, we'd need a real DB record.
    # Since we can't easily insert and then query in one script without side effects,
    # let's look at what the prompt looks like internally by mocking the DB function.
    
    print("\n[RUN 2] Checking prompt injection logic...")
    import services.live_insight as li
    
    # Mocking the DB fetch to return insight1
    def mock_get_last(eid):
        return insight1
    
    original_get_last = li._get_last_published_insight
    li._get_last_published_insight = mock_get_last
    
    # We'll just run analyze again and see if the LLM output changes or is distinct
    score2, insight2 = analyze(
        "Liverpool", "Chelsea", 40, 1, 0, 
        statistics, incidents, event_id=TEST_EVENT_ID
    )
    
    print(f"✨ Insight 2 (Context Aware): {insight2}")
    
    if insight1 == insight2:
        print("\n⚠️ WARNING: Insights are identical. Repeat detection might be failing or LLM is stubborn.")
    else:
        print("\n✅ SUCCESS: Insights are different! The 'avoid_repeat_block' worked.")

    # Restore original function
    li._get_last_published_insight = original_get_last
    print("="*70 + "\n")

if __name__ == "__main__":
    test_context_awareness()
