# pipeline_report

Remote Resolve Pipeline ၏ daily report ([pipeline-daily-report-v2.md](../noc_automation/logs/mockups/pipeline-daily-report-v2.md) / `.html` / `.jpg`) ကို database မှ live ဖတ်ပြီး web UI အဖြစ် ပြသော Go program ဖြစ်သည်။ `pipeline_runs`, `pipeline_run_events`, `retry_jobs` ကို **ဖတ်ရုံသာ** ဖတ်ပြီး ကိုယ်ပိုင် DB connection pool ဖြင့် သီးသန့် ချိတ်သည်။ အခြား service (noc_automation, rt_web_ui, rtutil, rtdatacore) ကို import မလုပ်ပါ။

## Run ပုံ

```bash
cd pipeline_report
cp .env.example .env          # DB setting ဖြည့်ပါ
make run                      # http://localhost:8090

# noc_automation ၏ .env ကို တိုက်ရိုက် သုံးလည်း ရသည် (MYSQL_DB_* key တူသည်)
make run ENV=../noc_automation/.env
```

Module သည် `../go.work` ထဲတွင် မပါသဖြင့် Makefile က `GOWORK=off` ဖြင့် run သည်။ `go` command ကို ကိုယ်တိုင် ခေါ်ပါက `GOWORK=off go run . --env .env` ဟု ခေါ်ပါ။

| Make target | |
|---|---|
| `make run` | Server run (`ENV=` ဖြင့် config file ပြောင်းနိုင်) |
| `make build` | `bin/pipeline-report` binary ထုတ် |
| `make test` | `go vet` + unit test |

## Config

| Key | Default | |
|---|---|---|
| `MYSQL_DB_HOST` / `_PORT` / `_USERNAME` / `_PASSWORD` / `_DATABASE` | — / `3306` | Database (read-only user ဖြင့် လုံလောက်) |
| `REPORT_HTTP_ADDR` | `:8090` | Listen address |
| `REPORT_TZ` | `Local` | Service များ DATETIME ရေးသော timezone (ဥပမာ `Asia/Yangon`) |
| `REPORT_NOC_QUEUE` | `Network Operation Center (NOC)` | Success run ၏ `before_queue` နှင့် `target_queue` နှစ်ခုလုံး ဤ queue ဖြစ်လျှင် "Kept in NOC" |
| `REPORT_STUCK_MINUTES` | `10` | Retry မဟုတ်ဘဲ state မပြောင်းသော မိနစ် — Stuck |
| `REPORT_DB_MAX_OPEN` | `5` | Report ၏ connection pool အရွယ် |

Environment variable သည် `.env` ထက် ဦးစားပေးသည်။

### Timezone

Pipeline service များ (rtdatacore DSN `loc=Local`) သည် DATETIME ကို process ၏ local time ဖြင့် timezone မပါဘဲ သိမ်းသည်။ Local MariaDB server သည် UTC ဖြင့် run နေသဖြင့် SQL တွင် `CURDATE()` / `NOW()` ကို **မသုံးပါ** — "Today" (00:00 – မနက်ဖြန် 00:00)၊ `now` နှင့် Stuck threshold ကို Go က `REPORT_TZ` ဖြင့် တွက်ပြီး parameter အဖြစ် ပို့သည်။ ထို့ကြောင့် `REPORT_TZ` သည် service များ၏ timezone နှင့် တူရမည်။

## HTTP

| Path | |
|---|---|
| `GET /` | Report UI (5 စက္ကန့်တစ်ကြိမ် refresh) |
| `GET /api/v1/pipeline-runs/today?filter=&limit=` | Summary + live list ကို အချိန်တစ်ခုတည်းဖြင့် — UI က ဤ endpoint ကို poll လုပ်သည် |
| `GET /api/v1/pipeline-runs/today?case=&limit=` | Summary + card တစ်ခု၏ run list (`runs`) — state, queue, BCS, reason ပါသည်။ case: `received`, `success`, `remote_resolved`, `transferred`, `kept_in_noc`, `bcs_ok`, `bcs_failed`, `not_eligible`, `manual_check` (မပါ/`in_progress` ဆိုလျှင် live list) |
| `GET /api/v1/pipeline-runs/today/summary` | Spec ၏ Q1 (§10 JSON) |
| `GET /api/v1/pipeline-runs/today/in-progress?filter=all\|retrying\|stuck&limit=` | Spec ၏ Q2 (limit default 50, max 200) |
| `GET /api/v1/pipeline-runs/today/export.csv?case=&filter=` | ယနေ့ run + bucket (Export CSV ခလုတ်)။ case မပါလျှင် run အားလုံး၊ case ပါလျှင် ထို card ၏ run များသာ (`in_progress` တွင် filter ကိုလည်း လိုက်သည်) |
| `GET /healthz` | DB ping |

## UI feature အလိုက် SQL

SQL အားလုံးသည် [store.go](store.go) တွင် ရှိသည်။

### အခြေခံ

- **Today window** — query တိုင်းတွင် `pr.created_at >= :start AND pr.created_at < :end` (ယနေ့ 00:00 – မနက်ဖြန် 00:00၊ `REPORT_TZ` ဖြင့်) ပါသည်။ ယနေ့ *ဖွင့်သော* run များကိုသာ ရေတွက်သည်။ မနေ့က ဖွင့်ပြီး ယနေ့ ပြီးသော run မပါပါ။
- **Snapshot** — refresh တစ်ကြိမ်၏ query အားလုံးကို read-only `REPEATABLE READ` transaction တစ်ခုတည်းဖြင့် run သည်။
- **Card condition တစ်နေရာတည်း** — card ဂဏန်း (`SUM(<cond>)`) နှင့် card ကို နှိပ်လျှင် ပေါ်သော list (`WHERE <cond>`) သည် `cond*` constant တစ်ခုတည်းကို သုံးသဖြင့် ဂဏန်းနှင့် list အရေအတွက် အမြဲ ညီသည်။ Condition ပြင်လျှင် ထို constant ကိုသာ ပြင်ပါ။
- **`rej` join** — `pipeline_runs` တွင် http status မရှိသဖြင့် API_REJECTED run ၏ `pipeline_run_events` (`event = 'api_rejected' AND to_state = 'API_REJECTED'`) ၏ `detail` JSON မှ `$.http_status` ကို `rej.http_status` အဖြစ် join သည်။

  ```sql
  LEFT JOIN (
    SELECT e.run_id, MAX(CAST(JSON_VALUE(e.detail, '$.http_status') AS UNSIGNED)) AS http_status
    FROM pipeline_runs p
    JOIN pipeline_run_events e
      ON e.run_id = p.run_id AND e.to_state = 'API_REJECTED' AND e.event = 'api_rejected'
    WHERE p.created_at >= :start AND p.created_at < :end AND p.current_state = 'API_REJECTED'
    GROUP BY e.run_id
  ) rej ON rej.run_id = pr.run_id
  ```

### KPI card များ

Card ဂဏန်းအားလုံးကို `FROM pipeline_runs pr <rej join> WHERE <today>` query တစ်ခုတည်း (row တစ်ကြောင်း) မှ `COALESCE(SUM(<condition>), 0)` ဖြင့် ရေတွက်သည်။

| Card | Condition (`pr.` ချန်ထားသည်) | Meta ၏ % ပိုင်းခြေ |
|---|---|---|
| Received | `COUNT(*)` — state မရွေး | — |
| In progress | `completed_at IS NULL` | — |
| ↳ retrying | `completed_at IS NULL AND current_state IN ('CPE_FETCH_RETRY_SCHEDULED','KAFKA_PUBLISH_RETRY_SCHEDULED')` | — |
| ↳ stuck | `completed_at IS NULL AND COALESCE(current_state,'') NOT IN (<retry states>) AND COALESCE(updated_at, created_at) < :now − REPORT_STUCK_MINUTES` | — |
| Success | `current_state = 'COMPLETED'` | finished |
| Remote resolved | `COMPLETED AND is_remote_resolved = 1` | Success |
| Not eligible | `current_state IN ('NOT_ELIGIBLE','ALREADY_PROCESSED')` | Received |
| Manual check | အောက်ပါ sub-bucket ၈ ခု ပေါင်းလဒ် | Received |

- **finished** = `current_state IN ('COMPLETED','SKIPPED','CONSUME_VALIDATION_FAILED','FAILED_PERMANENT','CPE_NOT_FOUND','RT_UPDATE_FAILED','NORMALIZE_FAILED')` — API လက်ခံပြီးမှ ပြီးသော run များ။ NOT_ELIGIBLE / API_REJECTED / API_UNREACHABLE မပါ။
- Retry state ရှိ run သည် တမင် စောင့်နေခြင်းဖြစ်၍ stuck ထဲ မပါ။ State မရှိသေးသော run (`''`) ကား ပါသည်။

### Success panel ("Of N successful runs")

| နေရာ | Condition |
|---|---|
| Remote resolved | `COMPLETED AND is_remote_resolved = 1` |
| Kept in NOC | `COMPLETED AND COALESCE(is_remote_resolved,0) = 0 AND TRIM(before_queue) = :noc AND TRIM(target_queue) = :noc` |
| Transferred | summary တွင် (not remote resolved) − (kept in NOC)။ List တွင် `COMPLETED AND COALESCE(is_remote_resolved,0) = 0 AND NOT COALESCE(TRIM(before_queue) = :noc AND TRIM(target_queue) = :noc, FALSE)` |
| BCS updated | `COMPLETED AND is_remote_resolved = 1 AND is_bcs_success = 1` |
| Resolved, BCS failed | `COMPLETED AND is_remote_resolved = 1 AND is_bcs_success = 0` |

- `:noc` = `REPORT_NOC_QUEUE`။
- Transferred list ၏ `COALESCE(..., FALSE)` ကြောင့် queue တစ်ခုခု NULL ဖြစ်သော run သည် summary ၏ နုတ်တွက်နည်းကဲ့သို့ပင် transferred ထဲ ကျသည်။
- `is_bcs_success` NULL ဖြစ်သော remote resolved run သည် BCS updated / BCS failed နှစ်ခုလုံးတွင် မပါ။

### Manual check

Card တစ်ခုတည်း ပြသော်လည်း summary API တွင် ခွဲ၍ ရေတွက်သည်။

| Sub-bucket (API field) | Condition |
|---|---|
| `validation_failed.api_400` | `current_state = 'API_REJECTED' AND rej.http_status = 400` |
| `validation_failed.rtutil` | `current_state = 'CONSUME_VALIDATION_FAILED'` |
| `api_error.http_409` | `current_state = 'API_REJECTED' AND rej.http_status = 409` |
| `api_error.other` | `current_state = 'API_REJECTED' AND COALESCE(rej.http_status,0) NOT IN (400,409)` — event row မရှိလျှင်လည်း ဤထဲ |
| `api_error.unreachable` | `current_state = 'API_UNREACHABLE'` |
| `failed.failed_permanent` | `current_state IN ('FAILED_PERMANENT','NORMALIZE_FAILED')` (NORMALIZE_FAILED = legacy) |
| `failed.cpe_not_found` | `current_state = 'CPE_NOT_FOUND'` |
| `failed.rt_update_failed` | `current_state = 'RT_UPDATE_FAILED'` |

API_REJECTED ၏ 400 / 409 / other ပေါင်းလျှင် API_REJECTED အားလုံး ဖြစ်သဖြင့် list က `current_state IN ('API_REJECTED','API_UNREACHABLE','CONSUME_VALIDATION_FAILED','FAILED_PERMANENT','NORMALIZE_FAILED','CPE_NOT_FOUND','RT_UPDATE_FAILED')` ကို သုံးပြီး card ဂဏန်းနှင့် ညီသည် (`TestCaseConditions` ဖြင့် စစ်ထားသည်)။

### "In progress now" table

```sql
SELECT pr.run_id, pr.ticket_id, pr.ticket_no, pr.cpe_id, pr.township, COALESCE(pr.current_state, ''),
       pr.created_at, COALESCE(pr.updated_at, pr.created_at),
       rj.attempt_count + 1, rj.max_attempts, rj.next_retry_at
FROM pipeline_runs pr
LEFT JOIN retry_jobs rj ON rj.id = pr.active_retry_job_id
WHERE <today> AND <tab condition>
ORDER BY pr.created_at ASC
LIMIT :limit          -- default 50, max 200
```

- All / Retrying / Stuck tab ၏ condition သည် KPI card ၏ in progress / retrying / stuck condition နှင့် တူသည်။
- Retry column — "2 / 3" = နောက် run မည့် attempt (`attempt_count + 1`) / `max_attempts`၊ "next hh:mm" = `next_retry_at`။
- Age = now − `created_at`၊ Stuck pill ၏ idle = now − `updated_at`။
- Stage stepper နှင့် owner ကို SQL မဟုတ်ဘဲ Go ([report.go](report.go)) ၏ `current_state` map မှ တွက်သည်။

### Card ကို နှိပ်လျှင် ပေါ်သော list

```sql
SELECT pr.run_id, pr.ticket_id, pr.ticket_no, pr.cpe_id, pr.township, COALESCE(pr.current_state, ''),
       pr.before_queue, pr.target_queue, rej.http_status, pr.is_remote_resolved, pr.is_bcs_success,
       pr.bcs_status_message, pr.last_state_reason, pr.created_at, pr.updated_at, pr.completed_at
FROM pipeline_runs pr <rej join>
WHERE <today> AND <case condition>
ORDER BY pr.created_at DESC
LIMIT :limit
```

- Case condition သည် အထက်ပါ card condition များ ဖြစ်သည် (`received` = `TRUE`)။ URL `#case=<case>` ဖြင့် ရွေးထားသော card ကို reload / share လုပ်နိုင်သည်။
- **State** — `current_state` နှင့် Go ၏ `bucket()` (validation failed / API error / failed …) tag။
- **Queue** — `before_queue → target_queue`။
- **BCS** — `is_bcs_success`။ Failed ဖြစ်လျှင် `bcs_status_message` (CPEMS/BCS error) ကိုပါ ပြသည်။
- **Reason** — `last_state_reason`။ API_REJECTED ဖြစ်လျှင် ရှေ့တွင် `HTTP <rej.http_status>`။
- **Completed** — `completed_at` နှင့် ကြာချိန် (`completed_at − created_at`)။

### Export CSV

- `case` မပါလျှင် — အထက်ပါ SELECT ကို `WHERE <today>` သာဖြင့် `ORDER BY created_at ASC`၊ LIMIT မပါဘဲ ထုတ်သည်။
- `?case=<case>` ပါလျှင် — ထို case ၏ condition ကို ထပ်ထည့်သည်။ `case=in_progress` တွင် `&filter=` ကိုလည်း လိုက်သည်။ UI ၏ Export CSV ခလုတ်သည် ရွေးထားသော card / tab ကို အလိုအလျောက် ထည့်ပေးသည်။
- `card` / `bucket` column ကို SQL မဟုတ်ဘဲ Go ၏ `bucket()` / `card()` မှ တွက်သည်။

### Footnote

- **Skipped (N today)** — `SUM(current_state = 'SKIPPED')`။ Card မပြသော်လည်း finished (Success % ပိုင်းခြေ) ထဲ ပါသည်။
- **Unclassified** — `completed_at IS NOT NULL AND COALESCE(current_state,'') NOT IN (<classified terminal states>)`။ State အသစ် ထည့်ပြီး report ကို update မလုပ်ရသေးမှုကို ဖမ်းရန်ဖြစ်သည်။ 0 မဟုတ်လျှင် CSV ဖြင့် ဘယ် state ဖြစ်သည်ကို စစ်ပါ။

## Spec နှင့် ကွာခြားချက်

- Spec ၏ Q4 (reconciliation) ကို summary ထဲ `unclassified` အဖြစ် ထည့်ထားသည်။ 0 မဟုတ်ပါက UI footnote တွင် အနီရောင်ဖြင့် သတိပေးသည် (rtdatacore တွင် state အသစ် ထည့်ပြီး report ကို update မလုပ်ရသေး)။
- Stage index / owner / retrying / stuck ကို SQL `CASE` အစား Go ([report.go](report.go)) တွင် တွက်သည်။
- Request တစ်ခု၏ query များ (Q1 + Q2) ကို read-only `REPEATABLE READ` transaction တစ်ခုတည်းဖြင့် run ပြီး `now` တစ်ခုတည်းကို သုံးသဖြင့် count နှင့် row များ အမြဲ ညီသည်။ Report သည် DB သို့ ဘယ်တော့မှ မရေးနိုင်ပါ။
- `updated_at` NULL ဖြစ်သော run ကို Stuck စစ်ရာတွင် `created_at` ဖြင့် တွက်သည်။
- "Eligible for automation" card နှင့် API ၏ `eligible` field ကို ဖြုတ်ထားသည် (Not eligible card ၏ "% of received" ဖြင့် သိနိုင်သည်)။
- Success panel ၏ "Queue transfer only" ကို **Transferred** (အခြား queue သို့ ရွှေ့) နှင့် **Kept in NOC** (`before_queue` = `target_queue` = `REPORT_NOC_QUEUE`၊ NOC က ဆက်ကိုင်ရမည်) ဟု ခွဲပြသည်။ Queue ကို နာမည်ဖြင့် နှိုင်းယှဉ်သဖြင့် RT ၏ queue နာမည်နှင့် `REPORT_NOC_QUEUE` တူရမည်။ CSV တွင် `before_queue` / `target_queue` column ပါသည်။
- Validation failed / API error / Failed card များကို ဖြုတ်ပြီး **Manual check** card တစ်ခုတည်း (ပေါင်းလဒ်သာ၊ အပိုင်းခွဲ မပြ) အဖြစ် ပြသည်။ Summary API တွင် `manual_check` (= ၃ ခု ပေါင်းလဒ်) ပါပြီး အပိုင်းအလိုက် object များ (`validation_failed`, `api_error`, `failed`) ကိုလည်း ဆက်ပေးသည်။ CSV တွင် `card` column (`manual_check` စသည်) နှင့် အသေးစိတ် `bucket` column နှစ်ခုလုံး ပါသည်။
- API error meta ကို spec ၏ အကြံပြုချက်အတိုင်း "5xx/other" ဟု ရေးသည်။
- Export CSV ၏ column များကို spec တွင် မသတ်မှတ်ထားသဖြင့် run တစ်ခုချင်း၏ state၊ bucket၊ API http status၊ timestamp များကို ထုတ်သည်။
- Q3 (Not eligible gate breakdown) ကို v2.0 UI တွင် မပြသဖြင့် မထည့်ရသေးပါ။
