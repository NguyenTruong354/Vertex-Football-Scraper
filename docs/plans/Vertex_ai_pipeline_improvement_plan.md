# VERTEX FOOTBALL — AI PIPELINE: KẾ HOẠCH HOÀN THIỆN CHI TIẾT
> Dành cho local model. Mỗi PHẦN có: vấn đề cụ thể, file cần sửa, đoạn code cần tìm, code thay thế.
> Làm theo đúng thứ tự. Không được tự thêm logic ngoài những gì được ghi.

---

# PHẦN 1 — SỬA BUG THỰC SỰ (BẮT BUỘC LÀM TRƯỚC)

---

## 1.1 — Fix bug ngôn ngữ trong live_insight.py

**File:** `live_insight.py`

**Vấn đề:** `system_prompt` bảo model viết bằng tiếng Anh, nhưng toàn bộ context và dữ liệu
là tiếng Việt. Model bị confused, output không ổn định — đôi khi ra tiếng Anh, đôi khi tiếng Việt.

**Tìm đoạn này:**
```python
    system_prompt = (
        "Bạn là một chuyên gia phân tích dữ liệu bóng đá trực tiếp (Live Data Analyst). "
        "Dựa vào thống kê trận đấu truyền vào, hãy viết MỘT câu nhận định (tối đa 25 từ) bằng tiếng Anh."
        "Câu này sẽ được hiển thị dạng thẻ Badge 'Live Insight' trên App. "
        "Hãy viết thật thu hút, mạch lạc, vừa có chuyên môn vừa có cảm xúc (không dùng emoji, không gạch đầu dòng)."
    )
```

**Thay bằng:**
```python
    system_prompt = (
        "Bạn là một chuyên gia phân tích dữ liệu bóng đá trực tiếp.\n"
        "Dựa vào thống kê trận đấu truyền vào, hãy viết MỘT câu nhận định (tối đa 25 từ) bằng tiếng Việt.\n"
        "Câu này sẽ được hiển thị dạng thẻ Badge 'Live Insight' trên App.\n"
        "Hãy viết thật thu hút, mạch lạc, vừa có chuyên môn vừa có cảm xúc.\n"
        "KHÔNG dùng emoji. KHÔNG dùng gạch đầu dòng. CHỈ trả lời đúng 1 câu."
    )
```

---

## 1.2 — Fix redundant logic trong insight_producer.py

**File:** `insight_producer.py`

**Vấn đề:** Đoạn check `minute >= 80` trong `_has_priority_bypass` là dead code vì
nếu incidents đã có goal thì vòng loop đầu tiên đã `return True` rồi.
Đoạn này làm code khó đọc và có thể gây nhầm lẫn khi maintain.

**Tìm đoạn này:**
```python
    # Late-goal check (minute >= 80)
    minute = payload.get("minute", 0)
    if minute >= 80:
        for inc in incidents:
            if inc.get("incidentType") == "goal" and (inc.get("time", 0) >= 80):
                return True
    return False
```

**Thay bằng:**
```python
    return False
```

*(Xóa hoàn toàn đoạn late-goal check vì đã được xử lý bởi loop goal ở trên)*

---

## 1.3 — Fix news_radar.py: League Tagging (Stable + Dynamic)

**File:** `news_radar.py`

**Vấn đề:** `league_id` đang bị hardcode là "EPL" hoặc dùng heuristic keyword thuần túy. Cần tách thành 2 lớp: Keywords tĩnh (giải/CLB) và Mapping động (tên cầu thủ từ DB mùa hiện tại).

**Hướng sửa:**

1. **Thêm các hằng số và hàm load map vào news_radar.py:**
```python
_STABLE_LEAGUE_KEYWORDS: dict[str, list[str]] = {
    "EPL":        ["premier league", "epl", "man united", "manchester united", 
                   "man city", "manchester city", "liverpool", "arsenal", "chelsea", "tottenham"],
    "LALIGA":     ["la liga", "laliga", "real madrid", "barcelona", "atletico madrid"],
    "BUNDESLIGA": ["bundesliga", "bayern munich", "borussia dortmund", "bvb"],
    "SERIEA":     ["serie a", "juventus", "inter milan", "ac milan", "napoli"],
    "LIGUE1":     ["ligue 1", "psg", "paris saint-germain", "marseille"],
    "UCL":        ["champions league", "ucl"],
    "UEL":        ["europa league", "uel"],
}

def _load_player_league_map() -> dict[str, str]:
    """Query DB lấy mapping {player_name_lower: league_id} mùa hiện tại."""
    try:
        from db.config_db import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(\"\"\"
            SELECT DISTINCT ON (player_id) LOWER(player), league_id
            FROM player_match_stats
            ORDER BY player_id, match_id DESC
        \"\"\")
        res = {row[0]: row[1] for row in cur.fetchall()}
        conn.close()
        return res
    except Exception as exc:
        log.warning("Could not load player_league_map: %s", exc)
        return {}
```

2. **Viết lại hàm detect:**
```python
def _detect_league_from_title(title: str, player_map: dict) -> str | None:
    t_lower = title.lower()
    # Lớp 1: UCL/UEL Priority
    if any(k in t_lower for k in ["champions league", "ucl"]): return "UCL"
    if any(k in t_lower for k in ["europa league", "uel"]): return "UEL"
    # Lớp 2: Stable keywords
    for lid, kws in _STABLE_LEAGUE_KEYWORDS.items():
        if any(kw in t_lower for kw in kws): return lid
    # Lớp 3: Dynamic player map
    for p_name, lid in player_map.items():
        if p_name in t_lower: return lid
    return None
```

3. **Cập nhật `fetch_news` để gọi map 1 lần duy nhất ở đầu hàm.**
Tìm dòng đầu tiên trong `fetch_news()`:
```python
    all_news = []
```

Thêm ngay bên dưới:
```python
    player_league_map = _load_player_league_map()
```

---

## 1.4 — Fix Circuit Breaker HALF_OPEN không release sau success

**File:** `llm_client.py`

**Vấn đề:** Khi circuit ở `HALF_OPEN`, request test được gửi đi. Nếu thành công thì
`record_success()` set state về `CLOSED` — đúng. Nhưng trong `can_execute()`, đoạn check
`HALF_OPEN` luôn return `False` mà không check xem request test đã complete chưa.
Trong môi trường single-threaded (nightly job) thì ổn, nhưng trong live pipeline
(nhiều events cùng lúc) thì request ngay sau request test sẽ bị block dù circuit đã về CLOSED.

**Tìm đoạn này trong `can_execute`:**
```python
        if self.state == "HALF_OPEN":
            # The test request was already granted during the transition from OPEN -> HALF_OPEN.
            # All subsequent requests should be blocked until the test request completes.
            return False
```

**Thay bằng:**
```python
        if self.state == "HALF_OPEN":
            # Nếu đã quá 30s kể từ lần chuyển sang HALF_OPEN mà vẫn chưa có kết quả
            # (tức là request test bị treo/timeout), cho phép retry để tránh bị kẹt mãi.
            if current_time >= self.next_retry_time + 30:
                logger.warning(
                    "[CircuitBreaker-%s] HALF_OPEN test timed out after 30s. Allowing retry.",
                    self.name
                )
                return True
            return False
```

---

# PHẦN 2 — ĐƯA player_trend VÀ match_story VÀO QUEUE

**Đây là refactor quan trọng nhất về kiến trúc.**

---

## 2.1 — Tạo hàm enqueue trong insight_producer.py (đã có sẵn)

Kiểm tra lại: `insight_producer.py` đã có `enqueue_player_trend()` và sẽ cần thêm
`enqueue_match_story()`. Đây là bước nối `player_trend.py` và `match_story.py` vào queue
thay vì gọi LLM trực tiếp.

**Không cần sửa `insight_producer.py` — `enqueue_player_trend()` đã đúng.**

---

## 2.2 — Sửa player_trend.py: thay direct LLM call bằng enqueue

**File:** `player_trend.py`

**Vấn đề:** Phase 2 đang gọi `_generate_insight_text()` trực tiếp — bypass hoàn toàn queue,
không có retry, không có audit trail, không có dedup protection.

**Tìm hàm `analyze_all_players`, tìm toàn bộ Phase 2:**
```python
        # Phase 2: Only call LLM for top 15 GREEN + top 15 RED (saves API quota)
        LLM_BUDGET = 15
        green_top = sorted([p for p in all_players if p["trend"] == "GREEN"], key=lambda x: -x["score"])[:LLM_BUDGET]
        red_top = sorted([p for p in all_players if p["trend"] == "RED"], key=lambda x: x["score"])[:LLM_BUDGET]
        llm_candidates = {p["player_id"] for p in green_top + red_top}

        log.info("  🤖 Generating AI insights for %d notable players...", len(llm_candidates))

        insights = []
        for p in all_players:
            if p["player_id"] in llm_candidates:
                p["insight_text"] = _generate_insight_text(
                    p["player_name"], p["trend"],
                    p["goals"], p["assists"], p["xg_arr"], p["xa_arr"], p["match_count"]
                )
            else:
                # Static fallback for non-notable players
                p["insight_text"] = _static_insight(p)

            insights.append({
                "player_id": p["player_id"],
                "player_name": p["player_name"],
                "league_id": p["league_id"],
                "trend": p["trend"],
                "score": p["score"],
                "insight_text": p["insight_text"],
            })

        return insights
```

**Thay bằng:**
```python
        # Phase 2: Enqueue top players vào queue thay vì gọi LLM trực tiếp
        LLM_BUDGET = 15
        green_top = sorted(
            [p for p in all_players if p["trend"] == "GREEN"],
            key=lambda x: -x["score"]
        )[:LLM_BUDGET]
        red_top = sorted(
            [p for p in all_players if p["trend"] == "RED"],
            key=lambda x: x["score"]
        )[:LLM_BUDGET]
        llm_candidates = {p["player_id"] for p in green_top + red_top}

        log.info("  📥 Enqueueing %d notable players into job queue...", len(llm_candidates))

        from services.insight_producer import enqueue_player_trend

        enqueued_count = 0
        insights = []
        for p in all_players:
            # Với notable players: enqueue vào queue để worker xử lý async
            if p["player_id"] in llm_candidates:
                job_id = enqueue_player_trend(
                    league_id=p["league_id"],
                    player_id=p["player_id"],
                    player_name=p["player_name"],
                    trend=p["trend"],
                    trend_score=p["score"],
                    goals=p["goals"],
                    assists=p["assists"],
                    xg_arr=p["xg_arr"],
                    xa_arr=p["xa_arr"],
                    match_count=p["match_count"],
                )
                if job_id:
                    enqueued_count += 1
                # Dùng static insight tạm thời — worker sẽ update sau khi LLM xong
                p["insight_text"] = _static_insight(p)
            else:
                p["insight_text"] = _static_insight(p)

            insights.append({
                "player_id": p["player_id"],
                "player_name": p["player_name"],
                "league_id": p["league_id"],
                "trend": p["trend"],
                "score": p["score"],
                "insight_text": p["insight_text"],
            })

        log.info("  ✓ Enqueued %d player_trend jobs for async LLM processing", enqueued_count)
        return insights
```

---

## 2.3 — Sửa insight_worker.py: thêm handler cho job_type player_trend

**File:** `insight_worker.py`

Bạn cần xem file này trước. Tìm đoạn dispatch theo `job_type` (thường là `if/elif` chain
hoặc dict mapping). Thêm case `player_trend` vào.

**Tìm đoạn dispatch (ví dụ dạng):**
```python
        if job["job_type"] == "live_badge":
            result = _handle_live_badge(job)
        elif job["job_type"] == "match_story":
            result = _handle_match_story(job)
```

**Thêm vào:**
```python
        if job["job_type"] == "live_badge":
            result = _handle_live_badge(job)
        elif job["job_type"] == "match_story":
            result = _handle_match_story(job)
        elif job["job_type"] == "player_trend":
            result = _handle_player_trend(job)
```

**Thêm hàm `_handle_player_trend` vào worker:**
```python
def _handle_player_trend(job: dict) -> str:
    """
    Worker handler cho player_trend jobs.
    Đọc payload từ job, generate insight bằng LLM,
    rồi update player_insights table.
    """
    payload = job["payload_json"]
    if isinstance(payload, str):
        import json
        payload = json.loads(payload)

    player_name = payload["player_name"]
    trend = payload["trend"]
    goals = payload["goals"]
    assists = payload["assists"]
    xg_arr = payload["xg_arr"]
    xa_arr = payload["xa_arr"]
    match_count = payload["match_count"]
    league_id = job["league_id"]
    player_id = payload["player_id"]

    # Import trực tiếp hàm generate từ player_trend module
    from services.player_trend import _generate_insight_text
    insight_text = _generate_insight_text(
        player_name, trend, goals, assists, xg_arr, xa_arr, match_count
    )

    if not insight_text:
        raise ValueError(f"LLM returned empty insight for player {player_name}")

    # Update player_insights với AI-generated text
    from db.config_db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE player_insights
            SET insight_text = %s,
                loaded_at = NOW()
            WHERE player_id = %s AND league_id = %s
        """, (insight_text, player_id, league_id))
        conn.commit()
    finally:
        conn.close()

    return insight_text
```

---

# PHẦN 3 — PROMPT VERSIONING + A/B TESTING

---

## 3.1 — Thêm prompt_version vào tất cả các enqueue call

**Vấn đề:** `prompt_version` đang hardcode `'v1'` ở mọi chỗ. Không thể chạy A/B test.

**File:** `insight_producer.py`

**Tìm TẤT CẢ các dòng:**
```python
                 %s, %s, %s, 'v1')
```

**Sửa tất cả thành nhận `prompt_version` như một parameter:**

Trong hàm `enqueue_live_badge`, tìm signature:
```python
def enqueue_live_badge(
    event_id: int,
    league_id: str,
    ...
    momentum_score: int,
) -> Optional[int]:
```

**Thay bằng:**
```python
def enqueue_live_badge(
    event_id: int,
    league_id: str,
    ...
    momentum_score: int,
    prompt_version: str = "v1",
) -> Optional[int]:
```

Làm tương tự cho `enqueue_match_story` và `enqueue_player_trend` — thêm
`prompt_version: str = "v1"` vào cuối parameter list mỗi hàm.

Sau đó tìm trong mỗi hàm dòng SQL hardcode `'v1'`:
```python
                 %s, %s, %s, 'v1')
```

**Thay bằng:**
```python
                 %s, %s, %s, %s)
```

Và thêm `prompt_version` vào tuple params của `cur.execute(...)` tương ứng.

---

## 3.2 — Thêm hàm query A/B result vào insight_feedback.py

**File:** `insight_feedback.py`

**Thêm hàm mới vào cuối file (trước EOF):**
```python
def get_prompt_version_stats(job_type: str = "live_badge", days: int = 7) -> list[dict]:
    """
    So sánh performance giữa các prompt versions.
    Dùng để quyết định promote version nào lên production.

    Returns list of dicts:
        [{prompt_version, job_count, feedback_count, avg_score, upvote_rate}, ...]
    """
    from db.config_db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                j.prompt_version,
                COUNT(DISTINCT j.id)                                    AS job_count,
                COUNT(f.id)                                             AS feedback_count,
                ROUND(AVG(f.score)::numeric, 2)                        AS avg_score,
                ROUND(
                    100.0 * SUM(CASE WHEN f.feedback_type = 'upvote' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(f.id), 0), 1
                )                                                       AS upvote_rate
            FROM ai_insight_jobs j
            LEFT JOIN ai_insight_feedback f ON f.job_id = j.id
            WHERE j.job_type = %s
              AND j.status = 'succeeded'
              AND j.created_at > NOW() - make_interval(days => %s)
            GROUP BY j.prompt_version
            ORDER BY avg_score DESC NULLS LAST
        """, (job_type, days))

        results = []
        for row in cur.fetchall():
            results.append({
                "prompt_version": row[0],
                "job_count": row[1],
                "feedback_count": row[2],
                "avg_score": float(row[3]) if row[3] else None,
                "upvote_rate": float(row[4]) if row[4] else None,
            })
        return results
    finally:
        conn.close()
```

---

## 3.3 — Tạo file mới: services/prompt_registry.py

**Đây là trung tâm quản lý tất cả prompts theo version.**
Tạo file mới `/services/prompt_registry.py` với nội dung sau:

```python
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
```

---

## 3.4 — Migrate live_insight.py và match_story.py sang dùng prompt_registry

**File:** `live_insight.py`

**Tìm đoạn:**
```python
    system_prompt = (
        "Bạn là một chuyên gia phân tích dữ liệu bóng đá trực tiếp.\n"
        ...
    )
```

**Thay bằng:**
```python
    from services.prompt_registry import get_prompt
    system_prompt = get_prompt("live_badge")
```

**File:** `match_story.py`

**Tìm đoạn:**
```python
SYSTEM_PROMPT = (
    "Bạn là chuyên gia bình luận bóng đá hàng đầu Việt Nam, "
    ...
)
```

**Thay bằng:**
```python
from services.prompt_registry import get_prompt
SYSTEM_PROMPT = get_prompt("match_story")
```

**File:** `player_trend.py`

**Tìm:**
```python
SYSTEM_PROMPT = (
    "Bạn là chuyên gia phân tích phong độ cầu thủ bóng đá.\n"
    ...
)
```

**Thay bằng:**
```python
from services.prompt_registry import get_prompt
SYSTEM_PROMPT = get_prompt("player_trend")
```

---

# PHẦN 4 — CONTEXT AWARENESS: TRÁNH LẶP NỘI DUNG

---

## 4.1 — Inject last insight vào prompt của live_badge

**File:** `live_insight.py`

**Vấn đề:** Mỗi lần generate là stateless. Model không biết câu trước nói gì,
dễ sinh ra nội dung trùng lặp dù `insight_producer.py` đã có delta gate.

**Thêm hàm `_get_last_published_insight` vào `live_insight.py`
(thêm vào trước hàm `analyze`):**
```python
def _get_last_published_insight(event_id: int) -> str | None:
    """
    Lấy insight text của live_badge job được publish gần nhất cho event này.
    Dùng để inject vào prompt tránh lặp nội dung.
    Returns insight_text string hoặc None nếu không có.
    """
    try:
        from db.config_db import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT result_text
            FROM ai_insight_jobs
            WHERE event_id = %s
              AND job_type = 'live_badge'
              AND status = 'succeeded'
              AND is_published = TRUE
            ORDER BY published_at DESC
            LIMIT 1
        """, (event_id,))
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None
```

**Tìm đoạn build user_prompt trong hàm `analyze`:**
```python
    user_prompt = f"""
Trận đấu phút {minute}. Tỷ số hiện tại: {home_team} {home_score}-{away_score} {away_team}.
...
Viết một câu nhận định siêu ngắn gọn (Dưới 25 chữ).
"""
```

**Thay bằng:**
```python
    # Lấy insight trước đó để tránh lặp
    last_insight = _get_last_published_insight(event_id)
    avoid_repeat_block = (
        f"\nInsight VỪA ĐƯỢC PHÁT trước đó: \"{last_insight}\"\n"
        f"KHÔNG được lặp lại ý này. Hãy nhấn mạnh khía cạnh KHÁC."
        if last_insight else ""
    )

    user_prompt = f"""
Trận đấu phút {minute}. Tỷ số hiện tại: {home_team} {home_score}-{away_score} {away_team}.
Thống kê chính:
- Tỷ lệ cầm bóng: {home_team} {possession_home}% - {possession_away}% {away_team}
- Sút trúng đích: {home_team} {shots_on_home} - {shots_on_away} {away_team}
- xG (Bàn thắng kỳ vọng): {home_team} {xg_home:.2f} - {xg_away:.2f} {away_team}
- Điểm nhấn trận đấu: {context_trigger}
{avoid_repeat_block}
Viết một câu nhận định siêu ngắn gọn (Dưới 25 chữ).
"""
```

**Tiếp theo, sửa signature hàm `analyze` để nhận `event_id`:**

Tìm:
```python
def analyze(
    home_team: str,
    away_team: str,
    minute: int,
    home_score: int,
    away_score: int,
    statistics: Dict[str, Dict[str, Any]],
    incidents: list[dict]
) -> Tuple[int, str]:
```

Thay bằng:
```python
def analyze(
    home_team: str,
    away_team: str,
    minute: int,
    home_score: int,
    away_score: int,
    statistics: Dict[str, Dict[str, Any]],
    incidents: list[dict],
    event_id: int | None = None,       # THÊM MỚI
) -> Tuple[int, str]:
```

> *(Không cần thêm gì sau dòng `if minute < 15: return 0, ""` — tiếp tục logic bình thường của hàm.)*

---

# PHẦN 5 — HARDENING: BẢO VỆ PRODUCTION

---

## 5.1 — Post-process output LLM trong tất cả các module

**Tạo file mới: `services/text_utils.py`**

```python
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
    text = re.sub(r"^\s*[-–•]\s*", "", text)
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
```

**Sau đó áp dụng trong `live_insight.py`:**

Tìm:
```python
    insight_text = llm.generate_insight(user_prompt, system_instruction=system_prompt)
    if not insight_text:
        # Fallback static text if all LLMs fail
        insight_text = context_trigger

    return momentum_score, insight_text
```

Thay bằng:
```python
    from services.text_utils import clean_insight
    insight_text = llm.generate_insight(user_prompt, system_instruction=system_prompt)
    if not insight_text:
        insight_text = context_trigger
    else:
        insight_text = clean_insight(insight_text, max_sentences=1)

    return momentum_score, insight_text
```

**Làm tương tự trong `player_trend.py`, hàm `_generate_insight_text`:**

Tìm:
```python
    text = llm.generate_insight(prompt, system_instruction=SYSTEM_PROMPT)
    if not text:
        text = f"{player_name} {trend_label} với {total_goals} bàn trong {match_count} trận gần nhất."
    return text
```

Thay bằng:
```python
    from services.text_utils import clean_insight
    text = llm.generate_insight(prompt, system_instruction=SYSTEM_PROMPT)
    if not text:
        text = f"{player_name} {trend_label} với {total_goals} bàn trong {match_count} trận gần nhất."
        return text
    return clean_insight(text, max_sentences=1)
```

---

## 5.2 — Thêm monitoring log tập trung

**File:** `llm_client.py`

**Tìm hàm `generate_insight`, tìm đoạn return sau khi Groq 1 thành công:**
```python
                res = self._call_groq(self.groq_client_1, prompt, system_instruction)
                self.cb_groq_1.record_success()
                return res
```

**Thay bằng:**
```python
                res = self._call_groq(self.groq_client_1, prompt, system_instruction)
                self.cb_groq_1.record_success()
                logger.debug(
                    "LLM OK | provider=groq_1 | words=%d | preview=%.60s",
                    len(res.split()), res
                )
                return res
```

**Tìm đoạn return Groq 2:**
```python
                res = self._call_groq(self.groq_client_2, prompt, system_instruction)
                self.cb_groq_2.record_success()
                return res
```

**Thay bằng:**
```python
                res = self._call_groq(self.groq_client_2, prompt, system_instruction)
                self.cb_groq_2.record_success()
                logger.debug(
                    "LLM OK | provider=groq_2_fallback | words=%d | preview=%.60s",
                    len(res.split()), res
                )
                return res
```

---

# KIỂM TRA CUỐI

Sau khi hoàn thành tất cả các phần, chạy lệnh sau để kiểm tra không có lỗi syntax:

```bash
python -m py_compile services/prompt_registry.py && echo "prompt_registry OK"
python -m py_compile services/text_utils.py && echo "text_utils OK"
python -m py_compile services/live_insight.py && echo "live_insight OK"
python -m py_compile services/player_trend.py && echo "player_trend OK"
python -m py_compile services/match_story.py && echo "match_story OK"
python -m py_compile services/insight_producer.py && echo "insight_producer OK"
python -m py_compile services/insight_feedback.py && echo "insight_feedback OK"
python -m py_compile services/llm_client.py && echo "llm_client OK"
python -m py_compile services/news_radar.py && echo "news_radar OK"
```

---

# TỔNG HỢP — THỨ TỰ LÀM THEO PRIORITY

| Phần | Task | Mức độ |
|------|------|--------|
| 1.1 | Fix language bug live_insight | ✅ DONE |
| 1.2 | Fix redundant logic producer | ✅ DONE |
| 1.3 | Fix news tagging (Dynamic) | ✅ DONE |
| 1.4 | Fix Circuit Breaker HALF_OPEN | ✅ DONE |
| 5.1 | Tạo text_utils + áp dụng | ✅ DONE |
| 5.2 | Monitoring log LLM | ✅ DONE |
| 3.3 | Tạo prompt_registry.py | ✅ DONE |
| 3.1 | Thêm prompt_version param | ✅ DONE |
| 3.2 | Thêm get_prompt_version_stats | ✅ DONE |
| 3.4 | Migrate sang prompt_registry | ✅ DONE |
| 4.1 | Context awareness live_badge | ✅ DONE |
| 2.2 | Enqueue player_trend | ✅ DONE |
| 2.3 | Thêm handler worker | ✅ DONE |

**File mới cần tạo:** `services/prompt_registry.py`, `services/text_utils.py`

**File cần sửa:** `live_insight.py`, `insight_producer.py`, `news_radar.py`,
`llm_client.py`, `player_trend.py`, `match_story.py`, `insight_feedback.py`, `insight_worker.py`