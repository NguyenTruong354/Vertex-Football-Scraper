"""
Vertex Football — Prompt Registry

Single source of truth cho tất cả system prompts theo version.
Dùng get_prompt(job_type, version) thay vì hardcode SYSTEM_PROMPT trong từng file.

Cách dùng:
    from services.prompt_registry import get_prompt, get_active_version
    prompt = get_prompt("live_badge", "v2")
    active = get_active_version("live_badge")  # → "v1" hoặc "v2"
"""

import logging

log = logging.getLogger(__name__)

# ── Active versions (thay đổi ở đây để flip production) ──
ACTIVE_VERSIONS: dict[str, str] = {
    "live_badge":    "v1",
    "match_story":   "v1",
    "player_trend":  "v1",
}

# ── Prompt Registry ──
_PROMPTS: dict[str, dict[str, str]] = {

    "live_badge": {
        "v1": (
            "You are a live football data analyst.\n"
            "Analyze the match statistics and provide exactly ONE punchy insight.\n"
            "You MUST return a JSON object with two keys: 'en' (English) and 'vi' (Vietnamese).\n"
            "Each value should be max 25 words. Make it engaging, coherent, showing both expertise and emotion.\n"
            "NO emojis. NO markdown blocks. ONLY output the raw JSON."
        ),
        "v2": (
            "You are a top-tier live football commentator.\n"
            "Write exactly ONE sharp insight about the match progress.\n"
            "Prioritize: xG data, red cards, momentum of the dominating team.\n"
            "You MUST return a JSON object with two keys: 'en' (English) and 'vi' (Vietnamese).\n"
            "Each value should be max 20 words.\n"
            "NO emojis. NO markdown blocks. NO explanations. ONLY output the raw JSON."
        ),
    },

    "match_story": {
        "v1": (
            "You are a top-tier football commentator, "
            "known for sharp, emotional, and tactical writing.\n"
            "Task: Write a brief match summary (3-4 sentences, max 80 words per language).\n"
            "- First sentence: score and context\n"
            "- Middle sentence(s): analyze reasons (xG, possession, red cards, standout players)\n"
            "- Last sentence: overall assessment\n"
            "You MUST return a JSON object with two keys: 'en' (English) and 'vi' (Vietnamese).\n"
            "NO emojis, bullet points, or markdown blocks. ONLY output the raw JSON."
        ),
    },

    "player_trend": {
        "v1": (
            "You are a football player form analyst.\n"
            "Task: Write ONE concise comment about the player's recent form.\n"
            "- Concise, sharp, using commentator language\n"
            "- If rising form: emphasize strengths\n"
            "- If falling form: point out issues objectively\n"
            "You MUST return a JSON object with two keys: 'en' (English) and 'vi' (Vietnamese).\n"
            "Each value should be max 20 words.\n"
            "NO emojis, bullet points, or markdown blocks. ONLY output the raw JSON."
        ),
    },
}


def get_prompt(job_type: str, version: str | None = None) -> str:
    """
    Lấy system prompt theo job_type và version.
    Nếu version=None thì dùng active version.
    Nếu không tìm thấy thì raise ValueError.
    """
    if version is None:
        version = get_active_version(job_type)

    prompts_for_type = _PROMPTS.get(job_type)
    if not prompts_for_type:
        raise ValueError(f"Unknown job_type '{job_type}' in prompt registry")

    prompt = prompts_for_type.get(version)
    if not prompt:
        raise ValueError(
            f"Prompt version '{version}' not found for job_type '{job_type}'. "
            f"Available: {list(prompts_for_type.keys())}"
        )
    return prompt


def get_active_version(job_type: str) -> str:
    """Lấy active version cho một job_type."""
    version = ACTIVE_VERSIONS.get(job_type)
    if not version:
        raise ValueError(f"No active version configured for job_type '{job_type}'")
    return version


def list_versions(job_type: str) -> list[str]:
    """Liệt kê tất cả versions có sẵn cho một job_type."""
    return list(_PROMPTS.get(job_type, {}).keys())
