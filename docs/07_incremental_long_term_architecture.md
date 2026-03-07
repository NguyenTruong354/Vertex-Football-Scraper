# Incremental Long-Term Architecture (Draft)

Version: v0.1  
Date: 2026-03-08  
Scope: Daily ingestion strategy for low-resource VM (e2-micro) while preserving data quality

## 1. Muc tieu

Muc tieu dai han:
- Giam tai cho daily run, tranh recrawl full season moi ngay.
- Giu do tin cay cua du lieu bang co che periodic backfill.
- Tach ro trach nhiem giua live tracking, daily incremental, va deep backfill.
- Van phu hop ha tang nho (1 vCPU, 1GB RAM + swap).

## 2. Van de hien tai

Hien trang (tom tat):
- Daily job co xu huong refresh theo season-level cho mot so bo du lieu.
- Trang thai run chu yeu nam trong process memory, chua co checkpoint ben DB cho tung task.
- Khi restart process, mot so logic de-bi “lam lai” nhieu hon muc can thiet.

He qua:
- CPU/network peak khong can thiet.
- Tang risk bi throttle khi trung gio co nhieu tran.
- Khong toi uu cho VM nho.

## 3. Kien truc de xuat (Hybrid)

Kien truc de xuat gom 3 plane:

1. Live Plane (real-time)
- `scheduler_master.py` live pool.
- Uu tien freshness, anti-ban state machine da co.
- Khong can quet lich su rong.

2. Daily Incremental Plane (lightweight)
- Chay theo ngay, chi lay phan du lieu thay doi ke tu checkpoint cuoi.
- Neu endpoint khong ho tro incremental, dung strategy "windowed refresh" (chi N ngay gan nhat).

3. Periodic Backfill Plane (heavy but rare)
- Chay theo lich (weekly/monthly) de sua drift va bo sung du lieu thieu.
- Tach khoi daily de khong anh huong operation moi ngay.

## 4. Nguyen tac thiet ke

- Checkpoint-first: moi task phai co checkpoint ben DB.
- Idempotent writes: tiep tuc du dung `UPSERT` theo khoa on dinh.
- Bounded workload: daily chi duoc phep xu ly trong budget thoi gian/tai nguyen ro rang.
- Degraded-safe: neu qua budget, uu tien bo qua phan non-critical, de lai cho backfill.

## 5. DB thay doi toi thieu

De xuat them bang checkpoint tong quat:

```sql
CREATE TABLE IF NOT EXISTS ingestion_checkpoint (
  source TEXT NOT NULL,             -- fbref / sofascore / understat / transfermarkt
  task_name TEXT NOT NULL,          -- fixtures / standings / squads / player_stats / ...
  league_id INTEGER NOT NULL,
  season TEXT,                      -- nullable neu task khong theo season
  cursor_key TEXT,                  -- event_id / date / page / hash marker
  cursor_value TEXT,
  last_success_at TIMESTAMPTZ,
  last_attempt_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'idle',  -- idle/running/success/failed
  error_count INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (source, task_name, league_id, COALESCE(season, ''))
);
```

Luu y:
- Neu muon giu PK clean, co the thay `season` nullable bang `season_key TEXT NOT NULL DEFAULT ''`.
- `cursor_*` linh hoat cho nhieu kieu endpoint.

## 6. Chien luoc incremental theo nhom du lieu

1. Fixtures/Results
- Daily: lay cac tran trong cua so `today-2` den `today+7`.
- Update nhung tran co status thay doi hoac chua final.
- Backfill: quet lai theo vong 7-14 ngay 1 lan.

2. Standings
- Daily: chi refresh khi co tran vua ket thuc trong league do.
- Neu khong co tran moi -> skip.

3. Squad rosters
- Daily: skip mac dinh.
- Weekly: refresh 1 lan, hoac bat buoc khi gap player unknown trong live.

4. Player season stats / squad stats / gk stats
- Daily: chi cap nhat doi/match vua da xong (delta update) neu endpoint cho phep.
- Neu endpoint chi tra full season: gioi han theo "active leagues + recent matches".
- Monthly backfill full de reconcile.

5. Match report / incidents / lineups
- Live + post-match flush la nguon chinh.
- Daily chi dung de compensation cho match co `flush_incomplete = TRUE` hoac missing data signal.

## 7. Scheduler policy de xuat

1. Lich chay
- Live loop: lien tuc (nhu hien tai).
- Daily incremental: 1 lan/ngay (gio off-peak).
- Backfill: 1 lan/tuan (hoac 2 tuan), co throttle chat.

2. Time budget
- Daily job co hard budget (vi du 20-30 phut tren e2-micro).
- Qua budget: dung task non-critical, ghi checkpoint de chay tiep lan sau.

3. Concurrency budget
- Limit worker theo source (vi du 1-2 concurrent requests/source).
- Uu tien endpoint critical truoc (fixtures/result > standings > stats).

## 8. Monitoring va SLO

KPI nen theo doi:
- `daily_duration_seconds`
- `daily_tasks_completed/total`
- `checkpoint_staleness_hours`
- `backfill_lag_days`
- `% matches with complete post-match data within 15m`
- `http_403_rate`, `http_429_rate`

Canh bao goi y:
- checkpoint stale > 48h voi task critical.
- daily fail 2 lan lien tiep.
- backfill lag > 14 ngay.

## 9. Rollout theo phase

Phase 1 (safe foundation)
- Them `ingestion_checkpoint` + helper read/write checkpoint.
- Chua doi logic fetch nhieu, chi bat dau ghi checkpoint.

Phase 2 (daily light)
- Chuyen fixtures/results sang windowed incremental.
- Standings refresh co dieu kien (match-finished trigger).

Phase 3 (stats optimization)
- Giam full-season stats refresh trong daily.
- Day monthly backfill lo phan reconcile.

Phase 4 (hardening)
- Them dashboard/log metrics cho checkpoint staleness.
- Chot threshold rollback/no-go.

## 10. Backward compatibility

- Khong pha vo pipeline hien tai: neu checkpoint null -> fallback ve mode cu.
- Tat ca write van idempotent (`UPSERT`).
- Co co che disable incremental bang env flag neu can emergency rollback.

## 11. Risks va giai phap

Risk:
- Checkpoint sai lam bo sot du lieu.

Mitigation:
- Dinh ky backfill reconcile.
- Them query audit dem row truoc/sau theo league/season.

Risk:
- Them bang checkpoint nhung khong duoc cap nhat dung luc process crash.

Mitigation:
- Ghi `last_attempt_at` truoc task, `last_success_at` sau task.
- Task restart doc status `running` qua timeout thi cho retry an toan.

## 12. Decision can chot trong buoi ban tiep theo

1. Cua so incremental cho fixtures/results: `[-2, +7]` hay `[-3, +10]`?
2. Tan suat backfill: weekly hay bi-weekly?
3. Hard budget cho daily tren e2-micro: 20p hay 30p?
4. Nhom du lieu nao bat buoc daily, nhom nao cho phep weekly?
5. Co bat buoc dashboard ngoai log file ngay tu dau khong?

## 13. Tieu chi thanh cong

- Giam ro rang thoi gian daily run so voi baseline hien tai.
- Khong giam freshness o live.
- Khong tang sustained 403/429.
- Ty le du lieu thieu sau tran khong tang.
- Van hanh on dinh >= 2 tuan tren production.
