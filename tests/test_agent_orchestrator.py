import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from services.agent_orchestrator import AgentOrchestrator
from services.agents.editor_in_chief_agent import EditorOutput

@pytest.fixture
def mock_llm_client():
    client = MagicMock()
    client.async_generate_for_agent = AsyncMock()
    return client

@pytest.fixture
def orchestrator(mock_llm_client):
    orch = AgentOrchestrator(mock_llm_client)
    orch.data_miner.analyze = AsyncMock(return_value="Miner Insight")
    orch.tactical_analyst.analyze = AsyncMock(return_value="Analyst Insight")
    orch.scout.analyze = AsyncMock(return_value="Scout Insight")
    orch.editor.synthesize = AsyncMock()
    return orch

@pytest.mark.asyncio
async def test_retry_scenario_uses_simplified_context(orchestrator):
    # First call fails verification, second call succeeds
    fail_output = EditorOutput(factual_verification_passed=False, dominant_narrative="NONE", confidence_score=0.9, rejected_reason="Test fail")
    success_output = EditorOutput(factual_verification_passed=True, dominant_narrative="STATISTICAL", confidence_score=0.9)
    orchestrator.editor.synthesize.side_effect = [fail_output, success_output]
    
    ctx = {"stats": {}, "incidents": [], "standings": {}, "momentum_score": 0.5}
    res = await orchestrator.run_pipeline("live_badge", ctx, max_retries=1)
    
    assert res is not None
    assert orchestrator.editor.synthesize.call_count == 2
    # Verify second call argument has simplified context
    second_call_args = orchestrator.editor.synthesize.call_args_list[1][0][0]
    assert "N/A" in second_call_args["analyst_insight"]

@pytest.mark.asyncio
async def test_both_agent_failure(orchestrator):
    orchestrator.data_miner.analyze.side_effect = Exception("Miner Fail")
    orchestrator.tactical_analyst.analyze.side_effect = Exception("Analyst Fail")
    
    ctx = {}
    res = await orchestrator.run_pipeline("live_badge", ctx)
    
    assert res is None
    orchestrator.editor.synthesize.assert_not_called()

@pytest.mark.asyncio
async def test_degraded_mode_1_fail_1_ok(orchestrator):
    # DataMiner fails, TacticalAnalyst succeeds
    orchestrator.data_miner.analyze.side_effect = Exception("Miner Fail")
    success_output = EditorOutput(factual_verification_passed=True, dominant_narrative="TACTICAL", confidence_score=0.9)
    orchestrator.editor.synthesize.return_value = success_output
    
    ctx = {}
    res = await orchestrator.run_pipeline("live_badge", ctx)
    
    # Should return dict containing success_output
    assert res == success_output.model_dump()
    orchestrator.editor.synthesize.assert_called_once()
    
    first_call_args = orchestrator.editor.synthesize.call_args_list[0][0][0]
    assert first_call_args["miner_insight"] is None
    assert first_call_args["analyst_insight"] == "Analyst Insight"

@pytest.mark.asyncio
async def test_confidence_filter(orchestrator):
    # Output confidence 0.4 which is below the 0.6 threshold
    low_conf_output = EditorOutput(factual_verification_passed=True, dominant_narrative="STATISTICAL", confidence_score=0.4)
    orchestrator.editor.synthesize.return_value = low_conf_output
    
    ctx = {}
    res = await orchestrator.run_pipeline("live_badge", ctx)
    
    assert res is None

@pytest.mark.asyncio
async def test_global_timeout(orchestrator):
    async def slow_miner(*args, **kwargs):
        await asyncio.sleep(2)
        return "Slow Miner"
    
    orchestrator.data_miner.analyze.side_effect = slow_miner
    
    with patch("services.agent_orchestrator.PIPELINE_CONFIG", {"live_badge": {"agents": ["data_miner"], "timeout": 0.5}}):
        ctx = {}
        res = await orchestrator.run_pipeline("live_badge", ctx)
        assert res is None

@pytest.mark.asyncio
async def test_routing_player_trend(orchestrator):
    success_output = EditorOutput(factual_verification_passed=True, dominant_narrative="STATISTICAL", confidence_score=0.9)
    orchestrator.editor.synthesize.return_value = success_output
    
    ctx = {}
    res = await orchestrator.run_pipeline("player_trend", ctx)
    
    assert res is not None
    orchestrator.data_miner.analyze.assert_called_once()
    orchestrator.tactical_analyst.analyze.assert_not_called()
    orchestrator.scout.analyze.assert_not_called()
