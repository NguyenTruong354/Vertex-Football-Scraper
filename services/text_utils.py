"""
Vertex Football — Text Post-Processing Utilities

Dùng để clean output từ LLM trước khi lưu vào DB hoặc hiển thị.
"""

import re


def clean_insight(text: str, max_sentences: int = 1) -> str:
    """
    Clean LLM output:
    - Lấy N câu đầu tiên
    - Xóa markdown (**bold**, _italic_, `, #, -, •)
    - Xóa gạch đầu dòng
    - Đảm bảo kết thúc bằng dấu chấm câu
    - Strip whitespace thừa
    """
    if not text:
        return ""

    text = text.strip()

    # Lấy dòng đầu tiên (bỏ multi-line)
    text = text.split("\n")[0].strip()

    # Xóa markdown
    text = re.sub(r"\*{1,2}|_{1,2}|`|#{1,6}", "", text)
    text = re.sub(r"\s*[-–•]\s*", "", text)
    text = text.strip()

    # Lấy N câu đầu
    if max_sentences == 1:
        # Cắt tại dấu câu kết thúc đầu tiên
        match = re.search(r"[.!?]", text)
        if match:
            text = text[:match.end()].strip()
    
    # Đảm bảo có dấu câu kết thúc
    if text and text[-1] not in ".!?":
        text += "."

    return text


def truncate_to_words(text: str, max_words: int) -> str:
    """Cắt text nếu vượt quá max_words, thêm dấu ... nếu bị cắt."""
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]) + "..."
