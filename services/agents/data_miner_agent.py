import json
from dataclasses import dataclass
from typing import Dict, Any, List
from services.llm_client import LLMClient

@dataclass
class Hook:
    type: str
    value: float
    description: str
    
    def to_dict(self):
        return {"type": self.type, "value": self.value, "description": self.description}

class DataMinerAgent:
    def __init__(self, llm_client: LLMClient):
        self.llm = llm_client
        
    async def analyze(self, match_stats: Dict[str, Any]) -> str:
        """Hybrid approach: Python rule-based first, LLM if >2 hooks."""
        hooks = self._detect_hooks(match_stats)
        
        if len(hooks) > 2:
            return await self._prioritize_hooks(hooks, match_stats)
        
        if hooks:
            lines = [f"- [{h.type}] {h.description} (Value: {h.value})" for h in hooks]
            return "\n".join(lines)
        
        return "No statistical anomalies."
        
    def _detect_hooks(self, stats: Dict[str, Any]) -> List[Hook]:
        hooks = []
        home_xg = float(stats.get("expected_goals_home", 0))
        home_g = int(stats.get("goals_home", 0))
        home_poss = float(stats.get("possession_home", 50))
        
        if home_xg - home_g > 1.5:
            hooks.append(Hook("XG_UNDERPERFORM_HOME", home_xg, f"Home team underperforming xG {home_xg} vs goals {home_g}"))
        if home_poss > 70:
            hooks.append(Hook("DOMINANCE_HOME", home_poss, f"Home team dominating possession {home_poss}%"))
            
        away_xg = float(stats.get("expected_goals_away", 0))
        away_g = int(stats.get("goals_away", 0))
        away_poss = float(stats.get("possession_away", 100 - home_poss))
        
        if away_xg - away_g > 1.5:
            hooks.append(Hook("XG_UNDERPERFORM_AWAY", away_xg, f"Away team underperforming xG {away_xg} vs goals {away_g}"))
        if away_poss > 70:
            hooks.append(Hook("DOMINANCE_AWAY", away_poss, f"Away team dominating possession {away_poss}%"))
            
        return hooks

    async def _prioritize_hooks(self, hooks: List[Hook], stats: Dict[str, Any]) -> str:
        prompt = (
            f"Detected multiple statistical hooks: {json.dumps([h.to_dict() for h in hooks])}\n"
            f"Raw stats: {json.dumps(stats)}\n\n"
            "Select and describe the most compelling 1-2 statistical hooks that define this match. Keep it very concise and analytical."
        )
        system = "You are a DataMiner agent focusing purely on statistical facts and anomalies. Return a short bulleted list."
        return await self.llm.async_generate_for_agent("data_miner", prompt, system)
