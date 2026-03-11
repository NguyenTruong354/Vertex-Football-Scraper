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
        # Cắt tại dấu câu kết thúc đầu tiên (sau dấu câu phải là khoảng trắng hoặc hết chuỗi)
        match = re.search(r"[.!?](?:\s|$)", text)
        if match:
            text = text[:match.end()].strip()
    elif max_sentences > 1:
        # Tạm thời chỉ split theo newline cho multi-sentences, regex chia câu chuẩn có thể phức tạp.
        # Ở đây ta ưu tiên lấy số dòng.
        pass
    
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


def _detect_text_lang(text: str) -> str:
    """Heuristic đơn giản: đếm ký tự có dấu tiếng Việt."""
    vi_chars = set("àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ")
    count = sum(1 for c in text.lower() if c in vi_chars)
    return "vi" if count > 2 else "en"


def clean_json_insight(raw: str, max_sentences: int = 1) -> dict:
    import json
    if not raw:
        return {"en": "", "vi": ""}

    # Strip markdown code blocks
    text = raw.strip()
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    
    if text.endswith("```"):
        text = text[:-3]
        
    text = text.strip()

    try:
        data = json.loads(text)
        en_text = clean_insight(data.get("en", ""), max_sentences=max_sentences)
        vi_text = clean_insight(data.get("vi", ""), max_sentences=max_sentences)
        return {"en": en_text, "vi": vi_text}
    except json.JSONDecodeError:
        lang = _detect_text_lang(raw)
        other = "en" if lang == "vi" else "vi"
        cleaned = clean_insight(raw, max_sentences=max_sentences)
        return {lang: cleaned, other: cleaned}
