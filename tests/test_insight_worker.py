import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

from services.insight_worker import (
    _is_historical_match, 
    _build_pipeline_context, 
    run_worker_cycle
)

@pytest.fixture
def mock_payload():
    return {
        "home_team": "Arsenal",
        "away_team": "Chelsea",
        "home_score": 1,
        "away_score": 0,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        # Use current year so it's not detected as an old season
        "season": datetime.now(timezone.utc).year if datetime.now(timezone.utc).month >= 7 else datetime.now(timezone.utc).year - 1,
        "statistics": {"Expected goals": {"home": 2.8, "away": 0.4}},
    }

def test_tc01_historical_gate_drop_old(mock_payload):
    """TC-01: Historical Gate — match cũ 20 ngày bị skip"""
    mock_payload["finished_at"] = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat()
    assert _is_historical_match(mock_payload) is True

def test_tc02_historical_gate_allow_recent(mock_payload):
    """TC-02: Historical Gate — match hiện tại (2 ngày) không bị skip"""
    mock_payload["finished_at"] = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    assert _is_historical_match(mock_payload) is False

def test_tc03_season_gate_drop_old_season(mock_payload):
    """TC-03: Season gate — backfill trận mới timestamp nhưng season cũ thì bỏ qua"""
    mock_payload["finished_at"] = datetime.now(timezone.utc).isoformat()
    mock_payload["season"] = 2022
    assert _is_historical_match(mock_payload) is True

@patch('services.insight_worker.SKIP_HISTORICAL_LLM', False)
def test_tc06_gate_toggle(mock_payload):
    """TC-06: Xác nhận gate bị vô hiệu hóa khi env bar false"""
    # 20 days ago (Normally True)
    mock_payload["finished_at"] = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat()
    assert _is_historical_match(mock_payload) is False

def test_tc05_payload_mapping(mock_payload):
    """TC-05: Payload mapping verify keys into ctx"""
    ctx = _build_pipeline_context("match_story", mock_payload)
    assert "stats" in ctx
    assert "incidents" in ctx
    assert "momentum_score" in ctx
    assert ctx["stats"]["expected_goals_home"] == 2.8
    assert ctx["stats"]["goals_home"] == 1
    assert ctx["stats"]["goals_away"] == 0
    assert ctx["momentum_score"] == 0.5

@patch('services.insight_worker.pick_job')
@patch('services.insight_worker._get_orchestrator')
@patch('services.insight_worker.recover_expired_leases')
def test_tc04_async_bridge_safety(mock_recover, mock_get_orch, mock_pick):
    """TC-04: Async bridge safety — đảm bảo asyncio.run_until_complete() không dính Runtime Error."""
    # Mock return 0 processing
    mock_pick.return_value = None
    mock_recover.return_value = 0

    mock_orch = MagicMock()
    mock_orch.llm.async_groq_client_1 = True
    mock_orch.llm.cb_groq_1.can_execute.return_value = True
    mock_get_orch.return_value = mock_orch

    # Loop injection test
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # Calling run_worker_cycle should not raise "Event loop is already running"
    processed = loop.run_until_complete(run_worker_cycle(shadow_mode=True, max_jobs=2))
    assert processed == 0
