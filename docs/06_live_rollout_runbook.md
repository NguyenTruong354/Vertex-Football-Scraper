# Live Pipeline Rollout Runbook (Step 6-7)

Version: v1.0  
Date: 2026-03-08  
Scope: `scheduler_master.py` anti-ban upgrades (Step 1-5 already coded)

## 1. Muc tieu

Runbook nay dung de thuc hien 2 buoc cuoi trong `docs/plans/plan_live.txt`:
- Step 6: Dry-run + baseline + calibration threshold
- Step 7: Rollout theo tung giai doan, uu tien an toan

Muc tieu van hanh:
- Khong tang sustained 403/429 sau rollout
- Do tuoi Tier A on dinh (60-90s theo state)
- Queue lag khong keo dai
- Co nguong canh bao ro rang (`threshold_for_env`)

## 2. Pham vi code da co san

Code da ho tro san:
- State machine: `NORMAL`, `THROTTLED_429`, `DEGRADED_403`, `FALLBACK`
- Tier policy A/B/C + capacity policy theo so match active
- Metrics line moi 60s:
  - `rpm`, `tier_a`, `tier_b`, `tier_c`, `skip`, `429`, `403`, `active`, `state`
- Post-match final flush + `flush_incomplete` trong `live_match_state`

Log file chinh:
- `logs/scheduler_master.log`

## 3. Dieu kien truoc khi chay Step 6

Checklist:
1. Da pull dung code moi nhat tren branch production.
2. Python env san sang (`.venv`).
3. DB schema da co cot `flush_incomplete`:
   - `live_match_state.flush_incomplete BOOLEAN NOT NULL DEFAULT FALSE`
4. Webhook Discord (neu dung alert) da cau hinh:
   - `DISCORD_WEBHOOK_LIVE`
   - `DISCORD_WEBHOOK_ERROR`
   - `DISCORD_WEBHOOK_INFO`
5. Xoa/rotate log cu de de baseline:
   - Backup `logs/scheduler_master.log` truoc khi bat dau ca quan sat.

## 4. Step 6A - Dry-run smoke (10-30 phut)

Muc dich: xac nhan process khoi dong on dinh, khong crash, co log METRICS.

Command (Git Bash):

```bash
cd d:/Vertex_Football_Scraper2
.venv/Scripts/python scheduler_master.py --dry-run --leagues EPL
```

Ky vong:
- Co banner start
- Co dong `Anti-ban: NORMAL`
- Co dong `METRICS` dinh ky
- Khong co traceback

## 5. Step 6B - 24h baseline (1 league live)

Muc dich: do baseline thuc te truoc khi rollout all leagues.

### 5.1 Chay 1 league live

```bash
cd d:/Vertex_Football_Scraper2
.venv/Scripts/python scheduler_master.py --leagues EPL
```

Thoi gian quan sat de nghi: 24h.

### 5.2 Thu thap metrics tu log

Loc metrics:

```bash
cd d:/Vertex_Football_Scraper2
rg "METRICS \|" logs/scheduler_master.log > logs/metrics_epl_24h.log
```

Loc state transitions:

```bash
rg "State .* ->|State .* →" logs/scheduler_master.log > logs/state_transitions_epl_24h.log
```

Dem 403/429:

```bash
rg "HTTP 403|HTTP 429" logs/scheduler_master.log > logs/http_errors_epl_24h.log
```

### 5.3 Tinh p50/p95 rpm

Dung Python snippet (chay trong repo root):

```bash
.venv/Scripts/python - << 'PY'
import re
from pathlib import Path

p = Path('logs/metrics_epl_24h.log')
vals = []
pat = re.compile(r"rpm=([0-9]+(?:\.[0-9]+)?)")
for line in p.read_text(encoding='utf-8', errors='ignore').splitlines():
    m = pat.search(line)
    if m:
        vals.append(float(m.group(1)))

if not vals:
    print('No rpm values found')
    raise SystemExit(1)

vals.sort()

def pct(a, q):
    i = int(round((len(a)-1) * q))
    return a[max(0, min(i, len(a)-1))]

p50 = pct(vals, 0.50)
p95 = pct(vals, 0.95)
threshold = min(45.0, p95 * 1.10)

print(f'samples={len(vals)}')
print(f'p50={p50:.2f}')
print(f'p95={p95:.2f}')
print(f'threshold_for_env={threshold:.2f}')
PY
```

### 5.4 Chot nguong

Ap dung quy tac:
- `threshold_for_env = min(45, p95 + 10%)`

Khuyen nghi:
- Ghi nguong da chot vao runbook team hoac env config de giu co dinh trong rollout.

## 6. Step 6C - Danh gia Go/No-Go

Go neu tat ca dieu kien duoi dat:
1. Khong co crash loop process.
2. Khong co burst 403/429 keo dai (nhieu chu ky lien tiep).
3. `queue_lag_seconds` (neu da co feed) khong vuot 60s trong >5% chu ky.
4. Ty le `flush_incomplete` thap va co compensation.

No-Go neu:
- 403 tang ro ret so voi baseline cu
- State bi kẹt `DEGRADED_403`/`FALLBACK` qua lau
- RPM vuot nguong 3 chu ky lien tiep

## 7. Step 7 - Rollout all leagues (giai doan)

### 7.1 Giai doan de nghi

1. Giai doan 1: `EPL` (da xong baseline)
2. Giai doan 2: `EPL + LALIGA`
3. Giai doan 3: all leagues

Moi giai doan giu toi thieu 6-12h quan sat truoc khi len tiep.

### 7.2 Command rollout

2 leagues:

```bash
cd d:/Vertex_Football_Scraper2
.venv/Scripts/python scheduler_master.py --leagues EPL LALIGA
```

All leagues (default):

```bash
cd d:/Vertex_Football_Scraper2
.venv/Scripts/python scheduler_master.py
```

## 8. Monitoring dashboard toi thieu (log-based)

Theo doi cac chuoi sau trong `logs/scheduler_master.log`:
- `METRICS | rpm=`
- `State NORMAL`
- `State THROTTLED_429`
- `State DEGRADED_403`
- `State FALLBACK`
- `HTTP 403`
- `HTTP 429`
- `Final flush incomplete`
- `Compensation`

Lenh tail nhanh:

```bash
tail -f logs/scheduler_master.log | rg "METRICS|State|HTTP 403|HTTP 429|flush"
```

## 9. Rollback policy

Rollback ngay khi gap mot trong cac dau hieu:
1. 403 sustained tang manh va state khong recover.
2. Queue lag cao lien tuc (neu co metric feed).
3. Match data freshness khong dat (Tier A cham ro ret).

Huong rollback:
- Dung process hien tai.
- Chay lai phien ban scheduler on dinh truoc do.
- Giu log va snapshot 24h de phan tich root cause.

## 10. SQL kiem tra sau rollout

Kiem tra match con flush incomplete:

```sql
SELECT event_id, league_id, status, flush_incomplete, loaded_at
FROM live_match_state
WHERE flush_incomplete = TRUE
ORDER BY loaded_at DESC;
```

Kiem tra tan suat theo ngay:

```sql
SELECT DATE(loaded_at) AS d, COUNT(*) AS n
FROM live_match_state
WHERE flush_incomplete = TRUE
GROUP BY 1
ORDER BY 1 DESC;
```

Kiem tra duplicate incident (sanity):

```sql
SELECT event_id, incident_type, minute,
       COALESCE(added_time, -1) AS added_time_key,
       COALESCE(player_name, '') AS player_name_key,
       is_home, COUNT(*)
FROM live_incidents
GROUP BY 1,2,3,4,5,6
HAVING COUNT(*) > 1;
```

## 11. Mau bao cao ngay (copy/paste)

- Window: `YYYY-MM-DD HH:MM` -> `YYYY-MM-DD HH:MM UTC`
- League scope: `EPL / EPL+LALIGA / ALL`
- Uptime: `...`
- State distribution: `NORMAL x%, THROTTLED x%, DEGRADED x%, FALLBACK x%`
- RPM: `p50=..., p95=..., threshold=...`
- HTTP errors: `403=..., 429=...`
- Flush incomplete: `... match(es)`
- Decision: `GO / HOLD / ROLLBACK`
- Notes: `...`

## 12. Ghi chu van hanh

- Step 6-7 khong phai bo qua, day la pha can thiet de xac nhan tinh on dinh production.
- Neu can, co the nang cap runbook nay thanh script tu dong tong hop so lieu log theo ngay.
