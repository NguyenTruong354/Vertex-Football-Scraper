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
            "Bạn là một chuyên gia phân tích dữ liệu bóng đá trực tiếp.\n"
            "Dựa vào thống kê trận đấu truyền vào, hãy viết MỘT câu nhận định "
            "(tối đa 25 từ) bằng tiếng Việt.\n"
            "Câu này sẽ được hiển thị dạng thẻ Badge 'Live Insight' trên App.\n"
            "Hãy viết thật thu hút, mạch lạc, vừa có chuyên môn vừa có cảm xúc.\n"
            "KHÔNG dùng emoji. KHÔNG dùng gạch đầu dòng. CHỈ trả lời đúng 1 câu."
        ),
        "v2": (
            "Bạn là bình luận viên bóng đá trực tiếp hàng đầu Việt Nam.\n"
            "Viết đúng 1 câu nhận định sắc bén (tối đa 20 từ) về diễn biến trận đấu.\n"
            "Ưu tiên: số liệu xG, thẻ đỏ, momentum đội đang thống trị.\n"
            "KHÔNG emoji. KHÔNG markdown. KHÔNG giải thích. CHỈ 1 câu duy nhất."
        ),
    },

    "match_story": {
        "v1": (
            "Bạn là chuyên gia bình luận bóng đá hàng đầu Việt Nam, "
            "nổi tiếng với lối viết sắc bén, giàu cảm xúc và có chiều sâu chiến thuật.\n"
            "Nhiệm vụ: Viết một đoạn tóm tắt trận đấu ngắn gọn (3-4 câu, tối đa 80 từ) "
            "bằng tiếng Việt.\n"
            "- Câu mở đầu nêu tỷ số và bối cảnh\n"
            "- Câu giữa phân tích lý do (xG, kiểm soát bóng, thẻ đỏ, cầu thủ nổi bật)\n"
            "- Câu cuối đánh giá tổng thể\n"
            "KHÔNG dùng emoji, gạch đầu dòng, hay markdown."
        ),
    },

    "player_trend": {
        "v1": (
            "Bạn là chuyên gia phân tích phong độ cầu thủ bóng đá.\n"
            "Nhiệm vụ: Viết MỘT câu nhận xét ngắn gọn (tối đa 20 từ) bằng tiếng Việt "
            "về phong độ gần đây của cầu thủ.\n"
            "- Ngắn gọn, sắc bén, dùng ngôn ngữ bình luận viên\n"
            "- KHÔNG dùng emoji, gạch đầu dòng, hay markdown\n"
            "- Nếu thăng hoa: nhấn mạnh điểm mạnh\n"
            "- Nếu sa sút: chỉ ra vấn đề khách quan\n"
            "CHỈ trả lời đúng 1 câu."
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
