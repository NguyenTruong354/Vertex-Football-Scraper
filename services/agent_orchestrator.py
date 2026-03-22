import os
import asyncio
from typing import Dict, Any, Optional
import logging

from services.llm_client import LLMClient
from services.agents.data_miner_agent import DataMinerAgent
from services.agents.tactical_analyst_agent import TacticalAnalystAgent
from services.agents.scout_agent import ScoutAgent
from services.agents.editor_in_chief_agent import EditorInChiefAgent

logger = logging.getLogger(__name__)

# Configurable confidence threshold
INSIGHT_CONFIDENCE_THRESHOLD = float(os.getenv("CONFIDENCE_THRESHOLD", "0.6"))

PIPELINE_CONFIG = {
    "live_badge":   {"agents": ["data_miner", "tactical_analyst"], "timeout": 8},
    "match_story":  {"agents": ["data_miner", "tactical_analyst", "scout"], "timeout": 30},
    "player_trend": {"agents": ["data_miner"], "timeout": 20}
}

class AgentOrchestrator:
    def __init__(self, llm_client: LLMClient):
        self.llm = llm_client
        self.data_miner = DataMinerAgent(self.llm)
        self.tactical_analyst = TacticalAnalystAgent(self.llm)
        self.scout = ScoutAgent(self.llm)
        self.editor = EditorInChiefAgent(self.llm)
        
    async def run_pipeline(self, insight_type: str, ctx: Dict[str, Any], max_retries=1, priority: int = 100) -> Optional[Dict[str, Any]]:
        """
        Runs the specified pipeline variant within a global timeout constraint.
        """
        config = PIPELINE_CONFIG.get(insight_type)
        if not config:
            logger.error(f"Unknown insight type config: {insight_type}")
            return None
            
        try:
            return await asyncio.wait_for(
                self._execute_pipeline(config, ctx, max_retries, priority),
                timeout=config["timeout"]
            )
        except asyncio.TimeoutError:
            logger.warning(f"Pipeline {insight_type} timed out after {config['timeout']}s.")
            return None
            
    async def _execute_pipeline(self, config: Dict[str, Any], ctx: Dict[str, Any], max_retries: int, priority: int) -> Optional[Dict[str, Any]]:
        tasks = []
        agent_names = config["agents"]
        
        # Extract inputs from context
        stats = ctx.get("stats", {})
        incidents = ctx.get("incidents", [])
        standings = ctx.get("standings", {})
        momentum = ctx.get("momentum_score", 0.0)
        
        # Dispatch parallel tasks conditionally based on PIPELINE_CONFIG
        async def dummy_task(): return None

        tasks.append(self.data_miner.analyze(stats) if "data_miner" in agent_names else dummy_task())
        tasks.append(self.tactical_analyst.analyze(incidents, standings, momentum) if "tactical_analyst" in agent_names else dummy_task())
        tasks.append(self.scout.analyze(ctx) if "scout" in agent_names else dummy_task())

        # Gather with return_exceptions=True to implement Degraded Mode logic
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        miner_res = results[0] if not isinstance(results[0], Exception) else None
        analyst_res = results[1] if not isinstance(results[1], Exception) else None
        scout_res = results[2] if not isinstance(results[2], Exception) else None
        
        if not miner_res and not analyst_res and not scout_res:
            logger.error("Both (All) agents failed (e.g. TimeoutError). Aborting pipeline.")
            return None
            
        editor_ctx = {
            "miner_insight": miner_res,
            "analyst_insight": analyst_res,
            "scout_insight": scout_res
        }
        
        # Synthesis and Verification Loop
        for attempt in range(max_retries + 1):
            editor_output = await self.editor.synthesize(editor_ctx, priority=priority)
            
            if editor_output.factual_verification_passed:
                if editor_output.confidence_score >= INSIGHT_CONFIDENCE_THRESHOLD:
                    return editor_output.model_dump()
                else:
                    logger.warning(f"Insight discarded - Confidence ({editor_output.confidence_score}) < Threshold ({INSIGHT_CONFIDENCE_THRESHOLD})")
                    return None
            
            if attempt < max_retries:
                # Retry with simplified context (strip tactical info)
                logger.info(f"Verification failed: {editor_output.rejected_reason}. Retrying with simplified context.")
                editor_ctx["analyst_insight"] = "N/A - Focus purely on DataMiner stats to reduce conflict."
            else:
                logger.error(f"Insight skipped after max retries: {editor_output.rejected_reason}")
                return None
