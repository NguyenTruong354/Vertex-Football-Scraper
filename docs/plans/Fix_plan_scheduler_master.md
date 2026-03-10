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
| 4 | ✅ ~~🟠 High~~ | ~~Daily scrapes toàn bộ mùa giải thay vì giới hạn 3 ngày gần nhất~~ | `DailyMaintenance.run()` | **DONE 2026-03-11** |
| 5 | ✅ ~~🔴 Critical (slow burn)~~ | ~~DB connection leak trong `_check_drift()`~~ | `LiveTrackingPool._check_drift()` | **DONE 2026-03-11** |
| 6 | ✅ ~~🟡 Medium~~ | ~~`AttributeError` khi chạy `--test-notify`~~ | `main()` | **DONE 2026-03-11** |
| 7 | ✅ ~~🟡 Medium~~ | ~~Standings update fire-and-forget — lỗi im lặng hoàn toàn~~ | `PostMatchWorker.run()` | **DONE 2026-03-11** |
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

### ✅ Issue #2 — [HOÀN THÀNH] `asyncio.Lock` giữ suốt retry loop

**Vị trí:** `CurlCffiClient.get_json()` và `CurlCffiClient.get_schedule_json()`

**Vấn đề cũ:** Lock bao trùm toàn bộ retry loop và toàn bộ thời gian sleep. Khi bị 429 với backoff 60s, toàn bộ polling của các trận đấu khác bị đóng băng hoàn toàn.

**Giải pháp (Narrow Scope + Anti-Bot Jitter):**
Vẫn giữ tính chất **Serialized Requests** (1 request tại 1 thời điểm để mô phỏng người dùng thật) nhưng thu hẹp phạm vi của Lock và thêm khoảng nghỉ tự nhiên (Jitter) **trước khi** vào Lock.

```python
async def get_json(self, endpoint, *, tier="A"):
    url = f"{cfg.SS_API_BASE}{endpoint}"
    for attempt in range(self.MAX_RETRIES):
        # 1. JITTER NGOÀI LOCK: Tránh nhiều match cùng xếp hàng và bắn
        #    request liên tiếp ngay khi Lock mở — mô phỏng hành vi người thật.
        jitter = random.uniform(1.0, 2.5) * (self.antiban.global_sleep_multiplier if self.antiban else 1.0)
        await asyncio.sleep(jitter)

        # 2. LOCK SCOPE THU HẸP: Chỉ bọc đúng hành động networking
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

        # 3. BACKOFF NGOÀI LOCK: sleep mà KHÔNG giữ Lock — các match khác
        #    vẫn có thể poll trong thời gian này.
        if sc in (429, 403) and attempt < self.MAX_RETRIES - 1:
            backoff = self.RETRY_BACKOFF[attempt] * (3 if sc == 429 else 2)
            await asyncio.sleep(backoff)

    return None
```

> Áp dụng tương tự cho `get_schedule_json()`.

---

### ✅ Issue #3 — [HOÀN THÀNH] `post_match.run()` block async event loop

**Vị trí:** `MasterScheduler._cycle()` — vòng lặp `for fm in finished_matches`

**Vấn đề cũ:**

```python
async def _cycle(self) -> None:
    ...
    for fm in finished_matches:
        self.post_match.run(fm)  # ← Synchronous! Có thể mất 5-15 phút
```

`post_match.run()` là hàm **synchronous** gọi nhiều `run_with_retry()`. Gọi trực tiếp trong `async _cycle()` block toàn bộ event loop — mọi coroutine khác (kể cả `poll_all()`) bị đóng băng. Nếu 2-3 match kết thúc cùng lúc, live polling dừng 20-30 phút.

**Cách sửa:**

```python
# Trong MasterScheduler.__init__():
self._executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="post_match")

# Trong _cycle():
for fm in finished_matches:
    asyncio.get_event_loop().run_in_executor(
        self._executor,
        self.post_match.run,
        fm
    )

# Trong run() khi shutdown:
finally:
    await self.browser.stop()
    self._executor.shutdown(wait=False)
```

---

### ✅ Issue #4 — [HOÀN THÀNH] Daily scrapes toàn bộ mùa giải

**Vị trí:** `DailyMaintenance.run()` — build commands cho `fbref_scraper.py` và `tm_scraper.py`

**Những gì đã implement (2026-03-11):**

**Vấn đề cũ:**

```python
fbref_cmd = [PYTHON, "fbref_scraper.py", "--league", league]
# Không có --since-date → STEP 4 quét lại toàn bộ match reports cả mùa
```

Với FBref, mỗi lần chạy STEP 4 crawl lại toàn bộ match reports của cả mùa — hàng trăm HTTP request, có thể mất vài tiếng mỗi đêm và tăng nguy cơ bị rate-limit/ban.

**Thay đổi 1 — `fbref_scraper.py`:**

Thêm param `since_date: str | None = None` vào `main()`. Trong STEP 4, filter `matches_with_reports` theo ngày **trước khi** apply `match_limit` (để limit đếm trên set đã filtered):

```python
async def main(
    ...,
    since_date: str | None = None,   # ← thêm mới
) -> dict[str, list]:

    # STEP 4: Match Reports
    matches_with_reports = [
        f for f in all_fixtures if f.match_report_url and f.match_id
    ]

    # Filter by since_date: FBref date format "YYYY-MM-DD"
    # → so sánh string ISO hoạt động đúng vì lexicographic = chronological.
    if since_date:
        before_count = len(matches_with_reports)
        matches_with_reports = [
            f for f in matches_with_reports
            if f.date and f.date >= since_date
        ]
        logger.info(
            "--since-date %s: %d → %d match reports (skipped %d older)",
            since_date, before_count, len(matches_with_reports),
            before_count - len(matches_with_reports),
        )

    if match_limit > 0:
        matches_with_reports = matches_with_reports[:match_limit]
```

CLI: thêm `--since-date YYYY-MM-DD` argument, pass vào `asyncio.run(main(..., since_date=args.since_date))`.

**Thay đổi 2 — Thêm Understat + SofaScore vào daily maintenance:**

**Vấn đề phát hiện:** Daily maintenance chỉ chạy FBref + TM, bỏ qua Understat + SofaScore. Nếu daemon restart/down → missed matches không được compensate tự động (chỉ có post-match worker chạy SAU trận kết thúc).

**Fix:** Thêm Understat + SofaScore vào đầu vòng lặp league trong `DailyMaintenance.run()`, TRƯỚC FBref:

```python
for league in self.leagues:
    sources = LEAGUE_SOURCES.get(league, {})
    
    # Understat: limit 10 trận gần nhất (compensate missed matches)
    if sources.get("understat"):
        run_with_retry(
            [PYTHON, "async_scraper.py", "--league", league, "--limit", "10"],
            ROOT / "understat", f"Daily/Understat [{league}]", ...
        )
    
    # SofaScore: limit 10 trận (lineup + passing + advanced stats)
    if sources.get("sofascore"):
        run_with_retry(
            [PYTHON, "sofascore_client.py", "--league", league, "--match-limit", "10"],
            ROOT / "sofascore", f"Daily/SofaScore [{league}]", ...
        )
```

**Thay đổi 3 — `DailyMaintenance.run()` — FBref + TM optimization:**

`--since-date` không áp dụng được cho Transfermarkt vì TM scrape current state (không có match-level data). Thay vào đó: daily TM dùng `--metadata-only` để chỉ scrape team overview pages (bỏ kader/market value pages). Market values được cập nhật ~1 lần/tuần qua `run_pipeline.py`.

```python
def run(self) -> bool:
    # Window 3 ngày: safety margin.
    _LOOKBACK_DAYS = 3
    since_date = (
        datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)
    ).strftime("%Y-%m-%d")
    self.log.info("  since_date: %s (lookback=%d days)", since_date, _LOOKBACK_DAYS)

    for league in self.leagues:
        if sources.get("fbref"):
            fbref_cmd = [PYTHON, "fbref_scraper.py", "--league", league]
            if self.fbref_standings_only:
                fbref_cmd.append("--standings-only")
                # --standings-only không chạy STEP 4 → --since-date không cần
            else:
                if self.fbref_no_match_passing:
                    fbref_cmd.append("--no-match-passing")
                else:
                    fbref_cmd.extend(["--since-date", since_date])  # ← thêm mới
                ...

        if sources.get("transfermarkt"):
            # --metadata-only: bỏ kader pages (20 đội × 30+ cầu thủ)
            # Market values → run_pipeline.py weekly
            run_with_retry(
                [PYTHON, "tm_scraper.py", "--league", league, "--metadata-only"],
                ...
            )
```

**Hiệu quả:**
- **Understat**: daily compensate 10 trận gần nhất (thay vì chỉ rely vào post-match). Nếu daemon down → tự catch up.
- **SofaScore**: daily compensate 10 trận lineup/passing/advanced (tương tự Understat).
- **FBref**: từ vài trăm → ~3-10 match reports/ngày. Từ vài giờ → vài phút.
- **TM**: bỏ kader pages, chỉ scrape 20 team overview pages. Nhanh hơn ~10×.
- Window 3 ngày (FBref) đảm bảo không miss data khi FBref cập nhật muộn.
- **Tổng thể**: Daily maintenance từ vài giờ → ~10-20 phút. Tự động catch up missed matches.

---

### ✅ Issue #5 — [HOÀN THÀNH] DB connection leak trong `_check_drift()` *(Critical — slow burn)*

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

**Tại sao là Critical (dù không crash ngay):**

Đây là **slow burn** — daemon sẽ chạy bình thường trong 1-2 ngày đầu, sau đó DB connection pool cạn dần. Khi pool cạn, exception bắt đầu xuất hiện ở nhiều chỗ cùng lúc trông giống nhiều loại bug khác nhau (timeout, DB unreachable, query fail...) — không trỏ thẳng vào connection leak. Debug lúc đó rất tốn thời gian.

**Quy mô rò rỉ thực tế:**
- Drift check chạy mỗi **5 phút** × mỗi **trận đang live**
- 10 trận live × 12 checks/giờ × 24 giờ = **~2,880 leaked connections/ngày**
- PostgreSQL default `max_connections=100` → pool cạn trong **vài giờ** nếu có nhiều trận live cùng lúc

**Cách sửa — `try-finally` đảm bảo luôn close:**

```python
def _check_drift(self, state: LiveMatchState) -> None:
    conn = None
    cur = None
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

        if row is None:
            return  # finally sẽ close conn

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
        if cur:
            cur.close()   # Close cursor trước
        if conn:
            conn.close()  # Luôn được gọi dù return sớm hay exception
```

---

### ✅ Issue #6 — [HOÀN THÀNH] `AttributeError` khi chạy `--test-notify`

**Vị trí:** `main()` — block `if args.test_notify`

**Mô tả vấn đề:**
**Vấn đề cũ:**

```python
if args.test_notify:
    log = setup_logging()
    n = Notifier(log)
    if not n.webhook_url:   # ← AttributeError: Notifier không có attribute 'webhook_url'
        ...                 #   Chỉ có property 'is_enabled'
```

**Đã fix (2026-03-11):**

```python
if args.test_notify:
    log = setup_logging()
    n = Notifier(log)
    if not n.is_enabled:   # ← Dùng property is_enabled đã có sẵn trong class
        print("❌ Không có DISCORD_WEBHOOK nào được cấu hình trong .env")
        sys.exit(1)
    n.send("test", "🧪 Test notification from scheduler_master.py — OK!")
    print("✅ Test notification sent successfully!")
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

Nếu standings update fail, không có log, không có Discord alert — standings có thể sai mà không ai biết.

**Thiết kế giải pháp:**

Standings **không phải real-time critical** — sai 1 trận hay delay vài tiếng không ảnh hưởng đến live tracking. Thêm retry tự động vào một `ensure_future` task sẽ tạo complexity không cần thiết và có thể góp thêm request pressure ngay lúc hệ thống đang bận nhất (ngay sau khi nhiều trận kết thúc cùng lúc). Thay vào đó: **log rõ + Discord alert** để người vận hành biết và manual trigger lại nếu cần.

**Đã implement (2026-03-11):**

**Bước 1 — Thêm `_run_standings_safe()` wrapper:**

```python
async def _run_standings_safe(self, league: str, tournament_id: int) -> None:
    """
    Safe wrapper cho standings update với error boundary.
    Log rõ + Discord alert thay vì để exception biến mất im lặng.
    """
    try:
        await self._update_standings_from_sofascore(league, tournament_id)
        self.log.info("✓ Standings updated: %s", league)
    except Exception as exc:
        # Log đủ thông tin để debug: league, tournament_id, error message
        self.log.error(
            "⚠ Standings update FAILED — league=%s tournament_id=%d — %s",
            league, tournament_id, exc
        )
        # Discord alert để người vận hành biết cần manual check
        self.notifier.send(
            "error",
            f"⚠ Standings update failed: `{league}` (tournament={tournament_id})\n"
            f"```{exc}```\n"
            f"Manual trigger: `python run_pipeline.py --league {league} --load-only`"
        )
```

**Bước 2 — Update `run()` để dùng wrapper + callback:**

```python
# Trong PostMatchWorker.run():
if loop.is_running():
    # Fire-and-forget với error boundary wrapper
    task = asyncio.ensure_future(
        self._run_standings_safe(league, tournament_id)
    )
    
    # Prevent "Task exception was never retrieved" warning.
    # Dùng named function: t.exception() trong lambda sẽ re-raise,
    # named function chỉ mark as retrieved mà không re-raise.
    def _swallow_task_exc(t: asyncio.Task) -> None:
        if not t.cancelled():
            t.exception()  # mark as retrieved, không re-raise
    
    task.add_done_callback(_swallow_task_exc)
else:
    loop.run_until_complete(
        self._run_standings_safe(league, tournament_id)
    )
```

**Hiệu quả:**
- Exception trong standings update không còn "biến mất" — log ERROR rõ ràng
- Discord alert kèm lệnh manual trigger để ops dễ dàng re-run
- Không block post-match worker — vẫn là fire-and-forget
- Không retry tự động — tránh spam requests lúc hệ thống bận

---

### ✅ Issue #8 — [HOÀN THÀNH] `_last_tier_b_ts` khởi tạo bằng `hasattr` ngoài dataclass

**Vị trí:** `LiveTrackingPool._poll_one()` — đầu block Statistics (Tier B)

**Vấn đề cũ:**

```python
# Anti-pattern: thêm attribute vào instance NGOÀI class definition
if not hasattr(state, '_last_tier_b_ts'):
    state._last_tier_b_ts = 0.0
```

`LiveMatchState` là dataclass nhưng `_last_tier_b_ts` không được khai báo trong đó — dễ gây bug khi serialize state, reset, hoặc thêm logic mới.

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
    _last_tier_b_ts: float = 0.0    # ← Đã thêm vào đây, xoá hasattr check trong _poll_one()
```

---

## Thứ tự ưu tiên thực hiện

```
TUẦN NÀY — Fix trước khi deploy:

  ✅ #1  Tier C trigger logic          — DONE 2026-03-10
  ✅ #2  asyncio.Lock narrow scope     — DONE 2026-03-10
  ✅ #3  post_match thread pool        — DONE 2026-03-10
  ✅ #8  _last_tier_b_ts vào dataclass — DONE 2026-03-10

  ✅ #5  DB connection leak            — DONE 2026-03-11
         try-finally trong _check_drift(): conn=None/cur=None trước try,
         xoá manual close() trong try body, finally luôn close cả cur lẫn conn.

TUẦN SAU — Optimize và polish:

  ✅ #4  Daily scraper --since-date    — DONE 2026-03-11
         fbref_scraper.py: thêm since_date param + filter STEP 4 trước match_limit.
         scheduler_master.py DailyMaintenance: thêm Understat/SofaScore (--limit 10),
         FBref since_date=now-3d, TM --metadata-only.
         Tổng daily time: vài giờ → 10-20 phút, tự catch up missed matches.

  ✅ #6  --test-notify AttributeError  — DONE 2026-03-11
         Dùng n.is_enabled thay vì n.webhook_url (không tồn tại).
  
  ✅ #7  Standings error boundary      — DONE 2026-03-11
         Thêm _run_standings_safe() wrapper: log.error + Discord alert,
         callback _swallow_task_exc() prevent asyncio warning.
         Không retry tự động — manual trigger nếu cần.
```

---

*Tạo ngày: 2026-03-10 · Dựa trên phân tích tĩnh `scheduler_master.py`*
*Cập nhật: 2026-03-10 · Issues #1, #2, #3, #8 — DONE*
*Cập nhật: 2026-03-11 · #5 lên 🔴 Critical (slow burn), #4 window 3 ngày, #7 bỏ retry giữ log+alert*
*Cập nhật: 2026-03-11 · Issue #5 — DONE (try-finally guarantee trong _check_drift)*
*Cập nhật: 2026-03-11 · Issue #4 — DONE (--since-date fbref + Understat/SofaScore vào daily + TM metadata-only)*
*Cập nhật: 2026-03-11 · Issues #6, #7 — DONE (test-notify fix + standings error boundary)*