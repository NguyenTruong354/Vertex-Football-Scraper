import json
from typing import Dict, Any, List
from services.llm_client import LLMClient

class TacticalAnalystAgent:
    def __init__(self, llm_client: LLMClient):
        self.llm = llm_client
        
    async def analyze(self, incidents: List[Dict[str, Any]], standings: Dict[str, Any], momentum_score: float) -> str:
        """Analyzes tactical flow based on momentum, standings, and incidents."""
        prompt = (
            "Analyze the tactical momentum and shifts in gameplay.\n"
            f"Momentum Score (Intensity): {momentum_score}\n"
            f"Standings Context: {json.dumps(standings)}\n"
            f"Recent Match Incidents: {json.dumps(incidents[-15:])}\n\n"
            "Correlate the momentum shifts with the incidents (e.g., parking the bus after a red card, turning up pressure). "
            "Output a concise bullet list of 1-3 tactical insights."
        )
        system = "You are a TacticalAnalyst agent. Focus on match dynamics, game flow, pressing, and tactical causality."
        return await self.llm.async_generate_for_agent("tactical_analyst", prompt, system)
