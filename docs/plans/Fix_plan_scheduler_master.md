# Fix Plan — `scheduler_master.py`

> **Mục tiêu:** Ghi lại toàn bộ vấn đề đã tìm ra, mức độ ưu tiên, nguyên nhân gốc, và hướng sửa cụ thể.
> **Phân loại:** 🔴 Critical · 🟠 High · 🟡 Medium · 🔵 Low

---

## Tóm tắt

| # | Mức độ | Vấn đề | File / Vị trí |
|---|--------|---------|---------------|
| 1 | ✅ ~~🔴 Critical~~ | ~~Tier C trigger không bao giờ kích hoạt~~ | `_poll_one()` | **DONE 2026-03-10** |
| 2 | ✅ ~~🔴 Critical~~ | ~~`asyncio.Lock` giữ suốt retry loop — block toàn bộ polling~~ | `CurlCffiClient.get_json()` | **DONE 2026-03-10** |
| 3 | ✅ ~~🔴 Critical~~ | ~~`post_match.run()` block async event loop~~ | `MasterScheduler._cycle()` | **DONE 2026-03-10** |
| 4 | 🟠 High | Daily scrapes toàn bộ mùa giải thay vì chỉ hôm qua/hôm nay | `DailyMaintenance.run()` |
| 5 | 🟠 High | DB connection leak trong `_check_drift()` | `LiveTrackingPool._check_drift()` |
| 6 | 🟡 Medium | `AttributeError` khi chạy `--test-notify` | `main()` |
| 7 | 🟡 Medium | Standings update fire-and-forget — lỗi im lặng hoàn toàn | `PostMatchWorker.run()` |
| 8 | ✅ ~~🔵 Low~~ | ~~`_last_tier_b_ts` khởi tạo bằng `hasattr` ngoài dataclass~~ | `_poll_one()` | **DONE 2026-03-10** |

---

## Chi tiết từng issue

---

### ✅ Issue #1 — [HOÀN THÀNH] Tier C trigger không bao giờ kích hoạt

**Vị trí:** `LiveTrackingPool._poll_one()` — block xử lý incidents và Tier C detection

**Mô tả vấn đề:**

Trong `_poll_one()`, luồng xử lý incidents diễn ra theo thứ tự sau:

```python
# Bước 1: Fetch incidents mới
inc_data = await self.browser.get_json(f"/event/{state.event_id}/incidents", tier="A")
if inc_data:
    current_incidents = inc_data.get("incidents", [])
    
    # Bước 2: Discord alert — dùng state.incidents làm old list ✅ (đúng)
    if state.poll_count > 1 and state.incidents:
        old_ids = {i.get("id") for i in state.incidents if i.get("id")}
        ...

    state.incidents = current_incidents  # ← Bước 3: UPDATE state.incidents

# Bước 4: Tier C detection — dùng state.incidents làm old list ❌ (SAI)
# Lúc này state.incidents đã là current_incidents → old_ids == current_ids
# → Không bao giờ tìm thấy incident mới → Tier C không bao giờ set pending
if inc_data and state.poll_count > 1 and state.incidents:
    old_ids = {i.get("id") for i in state.incidents if i.get("id")}
    for inc in (inc_data.get("incidents") or []):
        if _is_tier_c_trigger(inc) and inc.get("id") not in old_ids:
            state._tier_c_pending = True  # ← Không bao giờ chạy đến đây
```

**Hậu quả:** Lineup refresh sau goal / penalty / thẻ đỏ / thay người muộn bị vô hiệu hóa hoàn toàn. `state._tier_c_pending` không bao giờ được set `True`.

**Cách sửa:**

```python
if inc_data:
    current_incidents = inc_data.get("incidents", [])
    old_incidents = state.incidents  # ← Lưu old list TRƯỚC KHI update

    # Discord alert detection (dùng old_incidents)
    if state.poll_count > 1 and old_incidents:
        old_ids = {i.get("id") for i in old_incidents if i.get("id")}
        for inc in current_incidents:
            iid = inc.get("id")
            if iid and iid not in old_ids:
                inc_type = inc.get("incidentType", "")
                # ... gửi Discord alert ...

    state.incidents = current_incidents  # Update state SAU KHI đã dùng xong old list

# Tier C detection — dùng old_incidents (đã lưu từ trước)
if inc_data and state.poll_count > 1:
    old_ids = {i.get("id") for i in old_incidents if i.get("id")}
    for inc in (inc_data.get("incidents") or []):
        iid = inc.get("id")
        if iid and iid not in old_ids and _is_tier_c_trigger(inc):
            state._tier_c_pending = True
            break
```

---

### 🔴 Issue #2 — `asyncio.Lock` giữ suốt retry loop

**Vị trí:** `CurlCffiClient.get_json()` và `CurlCffiClient.get_schedule_json()`

**Vấn đề cũ:** Lock bao trùm toàn bộ retry loop và toàn bộ thời gian sleep. Khi bị 429 với backoff 60s, toàn bộ Polling của các trận đấu khác bị đóng băng hoàn toàn.

**Giải pháp mới (Narrow Scope + Anti-Bot Jitter):**
Chúng ta vẫn giữ tính chất **Serialized Requests** (1 request tại 1 thời điểm để mô phỏng người dùng thật) nhưng thu hẹp phạm vi của Lock và thêm khoảng nghỉ tự nhiên (Jitter) **trước khi** vào Lock.

```python
async def get_json(self, endpoint, *, tier="A"):
    url = f"{cfg.SS_API_BASE}{endpoint}"
    for attempt in range(self.MAX_RETRIES):
        # 1. JITTER NGOÀI LOCK: Tránh việc nhiều match cùng xếp hàng 
        # và bắn request liên tiếp ngay khi Lock mở.
        # Điều này tạo ra khoảng cách tự nhiên giữa các request (như người thật).
        jitter = random.uniform(1.0, 2.5) * (self.antiban.global_sleep_multiplier if self.antiban else 1.0)
        await asyncio.sleep(jitter)

        # 2. LOCK SCOPE THU HẸP: Chỉ bọc đúng hành động Networking
        async with self._lock: 
            try:
                if not self._session: await self.start()
                self._last_request = time.monotonic()
                resp = await self._session.get(url, timeout=15)
                sc = resp.status_code
                
                # ... cập nhật antiban, metrics ...
                
                if sc == 200:
                    return resp.json()
            except Exception as exc:
                self.log.debug("Fail: %s", exc)

        # 3. BACKOFF NGOÀI LOCK: Nếu bị 429/403, đi ngủ mà KHÔNG giữ Lock,
        # cho phép các match khác vẫn có cửa Polling (nhưng vẫn phải xếp hàng qua Lock).
        if sc in (429, 403) and attempt < self.MAX_RETRIES - 1:
            backoff = self.RETRY_BACKOFF[attempt] * (3 if sc == 429 else 2)
            await asyncio.sleep(backoff)
            
    return None
```

**Lợi ích:**
- **Không còn Bottleneck:** Một trận bị 429 không làm "chết chùm" các trận khác.
- **Anti-ban tốt hơn:** Các request bắn ra có độ trễ ngẫu nhiên (Jitter trước Lock), mô phỏng hoàn hảo hành vi duyệt web thủ công.
- **Duy trì Serialized:** Không bao giờ có 2 request bắn ra đồng thời từ cùng một Client.

> **Lưu ý:** Áp dụng tương tự cho `get_schedule_json()`.

---

### ✅ Issue #3 — [HOÀN THÀNH] post_match.run() block async event loop

**Vị trí:** `MasterScheduler._cycle()` — vòng lặp `for fm in finished_matches`

**Mô tả vấn đề:**

```python
# _cycle() là async coroutine
async def _cycle(self) -> None:
    ...
    for fm in finished_matches:
        self.post_match.run(fm)  # ← Synchronous! Chạy nhiều subprocess, mỗi cái vài phút
```

`post_match.run()` là hàm **synchronous** gọi nhiều `run_with_retry()` có thể mất 5-15 phút tổng cộng. Gọi trực tiếp trong `async _cycle()` sẽ **block toàn bộ event loop** — mọi coroutine khác (kể cả `poll_all()`) bị đóng băng.

**Hậu quả:** Nếu 2-3 match kết thúc cùng lúc, live polling dừng 20-30 phút.

**Cách sửa:**

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Trong MasterScheduler.__init__():
self._executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="post_match")

# Trong _cycle():
for fm in finished_matches:
    # Chạy non-blocking trong thread pool, không block event loop
    asyncio.get_event_loop().run_in_executor(
        self._executor,
        self.post_match.run,
        fm
    )
    # Không cần await — fire-and-forget trong thread riêng

# Trong run() khi shutdown:
finally:
    await self.browser.stop()
    self._executor.shutdown(wait=False)  # Không block shutdown
```

---

### 🟠 Issue #4 — Daily scrapes toàn bộ mùa giải

**Vị trí:** `DailyMaintenance.run()` — build commands cho `fbref_scraper.py` và `tm_scraper.py`

**Mô tả vấn đề:**

```python
fbref_cmd = [PYTHON, "fbref_scraper.py", "--league", league]
# Không có --since-date → scraper quét từ đầu mùa giải
```

Với FBref và Transfermarkt, mỗi lần chạy sẽ crawl lại toàn bộ match reports của cả mùa — hàng trăm HTTP request, có thể mất vài tiếng mỗi đêm và tăng nguy cơ bị rate-limit/ban.

**Cách sửa:**

**Phần 1 — Trong `DailyMaintenance.run()`:**

```python
from datetime import datetime, timezone, timedelta

def run(self) -> bool:
    # Tính ngày hôm qua để giới hạn scrape
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    for league in self.leagues:
        if sources.get("fbref"):
            fbref_cmd = [PYTHON, "fbref_scraper.py", "--league", league,
                         "--since-date", yesterday]  # ← CHỈ quét từ hôm qua
            # Giữ các flags khác như --standings-only, --limit, v.v.
            if self.fbref_standings_only:
                fbref_cmd.append("--standings-only")
            ...

        if sources.get("transfermarkt"):
            tm_cmd = [PYTHON, "tm_scraper.py", "--league", league,
                      "--since-date", yesterday]  # ← Tương tự
```

**Phần 2 — Trong `fbref_scraper.py` và `tm_scraper.py` cần thêm:**

```python
import argparse
from datetime import datetime

parser.add_argument("--since-date", type=str, default=None,
                    help="Chỉ scrape matches có date >= YYYY-MM-DD")

args = parser.parse_args()
since_dt = datetime.strptime(args.since_date, "%Y-%m-%d") if args.since_date else None

# Khi iterate qua match list, filter theo date:
for match in all_matches:
    if since_dt and match.date < since_dt:
        continue  # Bỏ qua match cũ
    scrape_match(match)
```

**Hiệu quả:** Giảm 90%+ thời gian chạy daily. Từ vài giờ → vài phút.

---

### 🟠 Issue #5 — DB connection leak trong `_check_drift()`

**Vị trí:** `LiveTrackingPool._check_drift()`

**Mô tả vấn đề:**

```python
def _check_drift(self, state):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT ...")
        row = cur.fetchone()
        cur.close()

        if row is None:
            return  # ← conn không được close!
        
        # ... xử lý ...
        conn.close()
    except Exception as exc:
        self.log.warning(...)
        # conn cũng không được close nếu exception xảy ra trước conn.close()
```

Với drift check chạy 5 phút/lần trên mỗi match đang live, connection pool sẽ cạn kiệt sau vài ngày → toàn bộ DB calls fail.

**Cách sửa:**

```python
def _check_drift(self, state: LiveMatchState) -> None:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT ls.home_score, ls.away_score, ls.status, ls.minute,
                   lm.home_score, lm.away_score, lm.status, lm.minute
            FROM live_snapshots ls
            JOIN live_match_state lm ON lm.event_id = ls.event_id
            WHERE ls.event_id = %s
        """, (state.event_id,))
        row = cur.fetchone()
        cur.close()

        if row is None:
            return  # ← conn sẽ được close trong finally

        ls_hs, ls_as, ls_st, ls_min = row[0], row[1], row[2], row[3]
        lm_hs, lm_as, lm_st, lm_min = row[4], row[5], row[6], row[7]
        diffs = []
        if ls_hs != lm_hs: diffs.append(f"home_score snap={ls_hs} state={lm_hs}")
        if ls_as != lm_as: diffs.append(f"away_score snap={ls_as} state={lm_as}")
        if ls_st != lm_st: diffs.append(f"status snap={ls_st} state={lm_st}")
        if ls_min != lm_min: diffs.append(f"minute snap={ls_min} state={lm_min}")
        if diffs:
            self.live_drift_mismatch_count += 1
            self.log.error("DRIFT MISMATCH event=%d: %s", state.event_id, "; ".join(diffs))
    except Exception as exc:
        self.log.warning("Drift check failed for event %d: %s", state.event_id, exc)
    finally:
        if conn:
            conn.close()  # ← Luôn được gọi dù return sớm hay exception
```

---

### 🟡 Issue #6 — `AttributeError` khi chạy `--test-notify`

**Vị trí:** `main()` — block `if args.test_notify`

**Mô tả vấn đề:**

```python
if args.test_notify:
    log = setup_logging()
    n = Notifier(log)
    if not n.webhook_url:   # ← AttributeError: Notifier không có attribute 'webhook_url'
        ...                 #   Chỉ có 'default_webhook'
```

**Cách sửa:**

```python
if args.test_notify:
    log = setup_logging()
    n = Notifier(log)
    if not n.is_enabled:   # ← Dùng property is_enabled đã có sẵn trong class
        print("Không có DISCORD_WEBHOOK nào được cấu hình trong .env")
        sys.exit(1)
    n.send("info", "🧪 Test từ scheduler_master.py — OK!")
    print("✓ Đã gửi")
    sys.exit(0)
```

---

### 🟡 Issue #7 — Standings update fire-and-forget không có error boundary

**Vị trí:** `PostMatchWorker.run()` — block `asyncio.ensure_future()`

**Mô tả vấn đề:**

```python
if loop.is_running():
    asyncio.ensure_future(
        self._update_standings_from_sofascore(league, tournament_id)
    )
    # Không track result → exception/lỗi mạng/DB sẽ biến mất hoàn toàn
```

Nếu standings update fail, không có log, không có retry, không có Discord alert — standings có thể sai mà không ai biết.

**Cách sửa:**

```python
async def _run_standings_safe(self, league: str, tournament_id: int) -> None:
    """Wrapper để log lỗi khi standings update fail."""
    try:
        await self._update_standings_from_sofascore(league, tournament_id)
    except Exception as exc:
        self.log.warning("Standings update failed for %s: %s", league, exc)
        # Tuỳ chọn: gửi Discord alert nếu muốn
        # self.notifier.send("error", f"Standings update failed: {league} — {exc}")

# Trong run():
if loop.is_running():
    task = asyncio.ensure_future(self._run_standings_safe(league, tournament_id))
    # Gắn callback để prevent "Task exception was never retrieved" warning
    task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
```

---

### ✅ Issue #8 — [HOÀN THÀNH] _last_tier_b_ts khởi tạo bằng hasattr ngoài dataclass

**Vị trí:** `LiveTrackingPool._poll_one()` — đầu block Statistics (Tier B)

**Mô tả vấn đề:**

```python
# Pattern anti-pattern: thêm attribute vào instance NGOÀI class definition
if not hasattr(state, '_last_tier_b_ts'):
    state._last_tier_b_ts = 0.0
```

`LiveMatchState` là dataclass nhưng `_last_tier_b_ts` không được khai báo trong đó. Dễ gây bug khi serialize state, reset, hoặc thêm logic mới.

**Cách sửa:**

```python
@dataclass
class LiveMatchState:
    event_id: int = 0
    league: str = ""
    home_team: str = ""
    away_team: str = ""
    home_score: int = 0
    away_score: int = 0
    status: str = "notstarted"
    minute: int = 0
    incidents: list[dict] = field(default_factory=list)
    statistics: dict[str, dict] = field(default_factory=dict)
    insight_text: str = ""
    poll_count: int = 0
    last_updated: str = ""
    start_timestamp: int = 0
    last_drift_check: float = 0.0
    _last_tier_c_ts: float = 0.0
    _tier_c_pending: bool = False
    _last_tier_b_ts: float = 0.0    # ← Thêm vào đây, xoá hasattr check trong _poll_one()
```

Sau đó trong `_poll_one()`, xoá block `hasattr`:

```python
# XOÁ:
# if not hasattr(state, '_last_tier_b_ts'):
#     state._last_tier_b_ts = 0.0

# Dùng trực tiếp — đã có trong dataclass:
should_fetch_stats = (tier_b_interval > 0) and (now_mono - state._last_tier_b_ts >= tier_b_interval)
```

---

## Thứ tự ưu tiên thực hiện

```
Tuần 1 (Critical — ảnh hưởng trực tiếp đến production):
  ✅ #1  Sửa Tier C trigger (bug logic) — DONE 2026-03-10
       → Lưu old_incidents trước khi update state.incidents
       → Cả Discord alerts và Tier C detection đều dùng old_incidents
  ⬜ #5  Sửa connection leak _check_drift (24/7 daemon — rò rỉ chậm nhưng chắc)
  ✅ #2  Refactor asyncio.Lock scope — DONE 2026-03-10
       → Lock chỉ bọc HTTP call, jitter TRƯỚC lock, backoff SAU lock
       → Áp dụng cho cả get_json() và get_schedule_json()
  ✅ #3  Thread pool cho post_match.run() — DONE 2026-03-10
       → ThreadPoolExecutor(max_workers=3) trong MasterScheduler
       → run_in_executor() thay vì gọi trực tiếp

Tuần 2 (High — hiệu năng và ổn định):
  ⬜ #4  Thêm --since-date vào daily scrapers (cần sửa fbref + tm scraper)
  ⬜ #6  Sửa --test-notify AttributeError
  ⬜ #7  Wrap standings update với error boundary

Tuần 3 (Low — code quality):
  ✅ #8  Chuyển _last_tier_b_ts vào dataclass — DONE 2026-03-10
       → Thêm field vào LiveMatchState, xóa hasattr check trong _poll_one()
```

---

*Tạo ngày: 2026-03-10 · Dựa trên phân tích tĩnh `scheduler_master.py`*
*Cập nhật: 2026-03-10 · Issues #1, #2, #3, #8 — DONE*