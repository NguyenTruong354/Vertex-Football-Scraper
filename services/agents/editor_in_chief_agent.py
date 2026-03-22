import json
from typing import Dict, Any, Optional
from pydantic import BaseModel
from services.llm_client import LLMClient

class EditorOutput(BaseModel):
    factual_verification_passed: bool
    dominant_narrative: str
    confidence_score: float
    rejected_reason: Optional[str] = None
    final_insight_vi: Optional[str] = None
    final_insight_en: Optional[str] = None

class EditorInChiefAgent:
    def __init__(self, llm_client: LLMClient):
        self.llm = llm_client

    async def synthesize(self, ctx: Dict[str, Any]) -> EditorOutput:
        prompt = (
            "You are the EditorInChief. Synthesize the provided agent outputs into a compelling, 30-40 word broadcast-style summary in both Vietnamese and English.\n"
            "First, fact-check the data (is it logically consistent?). If consistent, factual_verification_passed = true.\n"
            "If contradictory (e.g. Miner says dominating, but Analyst says they are crushed 3-0), determine the dominant_narrative "
            "('STATISTICAL', 'TACTICAL', or 'BALANCED') and explain the flow. "
            "If it's absurdly wrong or lacks logical ground, set factual_verification_passed = false and provide a rejected_reason.\n"
            "Evaluate your confidence_score (0.0 to 1.0).\n\n"
            f"DataMiner Output: {ctx.get('miner_insight', 'None')}\n"
            f"TacticalAnalyst Output: {ctx.get('analyst_insight', 'None')}\n"
            f"Scout Output: {ctx.get('scout_insight', 'None')}\n\n"
            "Output valid JSON."
        )
        system = (
            "You are the EditorInChief for a football analysis system. You must output valid JSON matching this schema:\n"
            "{\n"
            "  \"factual_verification_passed\": bool,\n"
            "  \"dominant_narrative\": \"STATISTICAL\" | \"TACTICAL\" | \"BALANCED\",\n"
            "  \"confidence_score\": float,\n"
            "  \"rejected_reason\": str | null,\n"
            "  \"final_insight_vi\": str | null,\n"
            "  \"final_insight_en\": str | null\n"
            "}"
        )
        
        res = await self.llm.async_generate_for_agent(
            "editor_in_chief", 
            prompt, 
            system, 
            response_format={"type": "json_object"}
        )
        
        if not res:
            return EditorOutput(
                factual_verification_passed=False,
                dominant_narrative="NONE",
                confidence_score=0.0,
                rejected_reason="LLM returned empty response"
            )

        try:
            data = json.loads(res)
            return EditorOutput(**data)
        except Exception as e:
            return EditorOutput(
                factual_verification_passed=False,
                dominant_narrative="NONE",
                confidence_score=0.0,
                rejected_reason=f"JSON Parsing Error: {e}"
            )
