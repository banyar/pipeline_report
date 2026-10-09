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
