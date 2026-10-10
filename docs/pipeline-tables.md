# Pipeline Monitoring Tables — Column ရှင်းလင်းချက်

Oct 10, 2026 · @Ba Nyar Si Thu

Ticket တစ်ခု remote-resolve pipeline ထဲ ဝင်တိုင်း **run** တစ်ခု (UUID `run_id`) ဖြစ်လာပြီး ဒီ run ကို table သုံးခုမှာ မှတ်ပါတယ်။ အချက်အလက်များကို local DB ရဲ့ `SHOW CREATE TABLE`၊ table data နဲ့ table တွေကို ရေးတဲ့ Go code (rtdatacore, noc\_automation, rtutil consumer, rt\_web\_ui) ကို ဖတ်ပြီး စုထားပါတယ်။

## အခြေခံ သဘောတရား

| Table | ဘာကို မှတ်လဲ | ပုံစံ |
| --- | --- | --- |
| `pipeline_runs` | Run တစ်ခုရဲ့ **လက်ရှိ state** နဲ့ **နောက်ဆုံး business ရလဒ်** | Run တစ်ခု = row တစ်ကြောင်း၊ update ဆက်လုပ်တယ် |
| `pipeline_run_events` | State ပြောင်းတိုင်း၊ step တစ်ခုချင်း ဖြစ်တိုင်း **သမိုင်းကြောင်း** | Append-only (ထည့်ပဲ ထည့်၊ မပြင်၊ မဖျက်) |
| `retry_jobs` | ကျသွားတဲ့ step ကို **နောက်မှ ပြန်ကြိုးစားဖို့ queue** | Retry schedule တစ်ခါ = row တစ်ကြောင်း |

Table တွေကို ရေးတဲ့ component ၅ ခု (event ရဲ့ `component` column တန်ဖိုးများ) –

| Component | တာဝန် |
| --- | --- |
| `rt_web_ui` | RT ticket ကို eligibility gate နဲ့ စစ်ပြီး noc\_automation API ဆီ ပို့တယ် |
| `remote_resolve_service` | noc\_automation API – Node-RED ကနေ CPE data ယူ၊ workflow တွက်၊ Kafka ပို့တယ် |
| `retry_worker` | `retry_jobs` ကို poll လုပ်ပြီး ကျသွားတဲ့ step ကို ပြန် run တယ် |
| `publish_timeout_sweeper` | Kafka ပို့ပြီး `PUBLISHED_TIMEOUT_MINUTES` (default 5) ကျော်ထိ consume မဖြစ်တဲ့ run ကို ပိတ်တယ် |
| `rtutil_consumer` | Kafka ကနေ ဖတ်ပြီး BCS နဲ့ RT ticket ကို update လုပ်တယ် |

State ပြောင်းတာ (`pipeline_runs`) နဲ့ event ထည့်တာ (`pipeline_run_events`) ကို transaction တစ်ခုတည်းမှာ လုပ်လို့ နှစ်ခု ဘယ်တော့မှ မကွဲပါဘူး။

## ၁။ pipeline\_runs

Run တစ်ခု = row တစ်ကြောင်း ဖြစ်ပြီး pipeline ရဲ့ step တိုင်းမှာ update လုပ်ပါတယ်။ Pipeline report ရဲ့ card အားလုံးကို ဒီ table တစ်ခုတည်းကနေ ရေတွက်ပါတယ်။

### Identity / RT ticket data

Run စဖွင့်ချိန်မှာ RT webhook payload ကနေ ကူးထည့်ပါတယ်။ နောက်ပိုင်း RT မှာ ticket ပြောင်းသွားရင်တောင် pipeline စဝင်ချိန်က အခြေအနေကို သိနိုင်ဖို့ ဖြစ်ပါတယ်။

| Column | ရည်ရွယ်ချက် / ထည့်ပုံ | Reporting အသုံး |
| --- | --- | --- |
| `id` | Auto-increment primary key။ Internal အတွက်ပဲ သုံးပါတယ်။ | — |
| `run_id` | Run ရဲ့ UUID။ Table သုံးခုလုံးကို ချိတ်တဲ့ key ဖြစ်ပါတယ် (unique index)။ | Event နဲ့ retry ကို join လုပ်ဖို့၊ CSV ရဲ့ row ID |
| `ticket_id` | RT ticket ID။ | Ticket တစ်ခုကို run ဘယ်နှခါ ဝင်လဲ ရေတွက်ဖို့၊ RT link လုပ်ဖို့ |
| `ticket_no` | RT custom field (ဥပမာ `Aug26-TKT-2851212`)။ | Report list မှာ ပြပါတယ်။ NOC က ဒီနံပါတ်နဲ့ ရှာပါတယ်။ |
| `service_area`, `township` | Ticket ရဲ့ ဒေသ။ | ဒေသအလိုက် success/fail ခွဲကြည့်ဖို့ (`township` မှာ index ရှိ) |
| `ticket_created_at` | RT မှာ ticket ဖွင့်တဲ့ အချိန်။ Parse မရရင် NULL။ | Ticket ဖွင့်ပြီး pipeline ဝင်တဲ့အထိ ကြာချိန် |
| `ticket_problem` | RT ရဲ့ problem custom field။ | Problem အမျိုးအစားအလိုက် remote-resolve ရာခိုင်နှုန်း |
| `ticket_status` | စဝင်ချိန်မှာ RT status (new/open…)။ `COMPLETED` ဖြစ်ချိန်မှာ RT update ပြီးနောက် status နဲ့ **ပြန်ရေးပါတယ်**။ | နောက်ဆုံး ticket status |
| `tags` | ရေးတဲ့ code မရှိပါ။ အမြဲ NULL။ | အသုံးမဝင်သေးပါ |

### Device / CPE data

| Column | ရည်ရွယ်ချက် / ထည့်ပုံ | Reporting အသုံး |
| --- | --- | --- |
| `cpe_id` | Process လုပ်မယ့် CPE။ စဝင်ချိန်မှာ ထည့်ပါတယ်။ မရှိရင် `NOT_ELIGIBLE`။ | CPE တစ်ခုကို ticket ထပ်ခါထပ်ခါ ဝင်လား စစ်ဖို့ (index ရှိ) |
| `local_service_id` | Customer ရဲ့ service ID (ဥပမာ `2084145-001`)။ Eligibility gate လည်း ဖြစ်ပါတယ်။ | Customer အလိုက် ခွဲကြည့်ဖို့ |
| `onu_serial`, `olt_hostname`, `ca1`, `uplink` | Node-RED ကနေ ရလာတဲ့ network topology။ `WORKFLOW_EVALUATED` ဖြစ်ချိန်မှာ ထည့်ပါတယ်။ | OLT/uplink တစ်ခုထဲမှာ ticket စုနေလား (outage ရှာဖို့) |
| `ref_bcs_process_id`, `before_bcs_channel`, `target_bcs_channel` | နောက်လာမယ့် BCS stage အတွက် ချန်ထားတာ။ အခု ရေးတဲ့ code မရှိပါ။ | WiFi channel ပြောင်းတာကို ခြေရာခံဖို့ (နောင်တွင်) |

### State / Processing

| Column | ရည်ရွယ်ချက် / ထည့်ပုံ | Reporting အသုံး |
| --- | --- | --- |
| `current_state` | Pipeline ရဲ့ လက်ရှိ state။ State ပြောင်းတိုင်း event row နဲ့ transaction တစ်ခုတည်းမှာ update လုပ်ပါတယ်။ | **Report ရဲ့ အဓိက column**။ Card တိုင်း (Success, Manual check, Not eligible…) ကို ဒီ column နဲ့ ခွဲပါတယ်။ |
| `last_state_reason` | နောက်ဆုံး state ဘာကြောင့် ပြောင်းလဲ (ဥပမာ `RT External API returned 400: ...`)။ `PUBLISHED` သို့မဟုတ် terminal state ကို reason မပါဘဲ ရောက်ရင် ရှင်းပစ်ပါတယ်။ | Manual check list မှာ ဘာကြောင့် fail လဲ ပြဖို့ |
| `active_retry_job_id` | လက်ရှိ retry job ရဲ့ id။ Retry schedule လုပ်ချိန် ထည့်ပြီး retry ပြီးရင် NULL ပြန်ထားပါတယ်။ | In-progress list မှာ `retry_jobs` နဲ့ join ပြီး "attempt 2/3၊ next retry ..." ပြဖို့ |

### Business ရလဒ်

| Column | ရည်ရွယ်ချက် / ထည့်ပုံ | Reporting အသုံး |
| --- | --- | --- |
| `is_remote_resolved` | Workflow ရဲ့ target status က "Resolved" ဖြစ်ရင် 1။ `WORKFLOW_EVALUATED` ဖြစ်ချိန်မှာ ဆုံးဖြတ်ပါတယ်။ | **Remote resolved** / **Not remote resolved** card |
| `is_bcs_success` | BCS ရလဒ် (1 = အောင်၊ 0 = ကျ၊ NULL = မလုပ်ရ/မရှိ)။ Remote resolved ticket အတွက်ပဲ `rtutil_consumer` က ရေးပါတယ်။ BCS ကျလည်း RT update ဆက်လုပ်ပါတယ်။ | **BCS OK / BCS failed** card |
| `bcs_status_message` | BCS ကနေ ပြန်လာတဲ့ message/error။ | BCS failed ရဲ့ အကြောင်းရင်း |
| `final_message` | RT ticket မှာ ရေးမယ့် workflow comment။ | Customer/NOC က ဘာ message မြင်ရလဲ စစ်ဖို့ |
| `before_queue` | Pipeline စဝင်ချိန်က RT queue။ | **Kept in NOC** / **Transferred** ခွဲဖို့ |
| `target_queue` | Workflow က ရှွေ့ခိုင်းတဲ့ queue။ | `before_queue` = `target_queue` = NOC ဆိုရင် "Kept in NOC"။ မတူရင် ဘယ် queue ကို ရှွေ့လဲ ခွဲကြည့်ပါတယ်။ |
| `rt_request_snapshot` | RT ကနေ ရလာတဲ့ မူရင်း JSON payload အပြည့်အစုံ။ | Debug နဲ့ dispute ဖြစ်ရင် "အဲ့ချိန်က data ဘာလဲ" သက်သေပြဖို့ |

### Timestamp များ

| Column | ရည်ရွယ်ချက် | Reporting အသုံး |
| --- | --- | --- |
| `created_at` | Run စဖွင့်တဲ့ အချိန်။ | **"Today" window** (`created_at >= 00:00 AND < မနက်ဖြန်`)။ Query တိုင်းမှာ သုံးပြီး index ရှိပါတယ်။ |
| `updated_at` | နောက်ဆုံး update အချိန်။ | **Stuck** – `updated_at` က N မိနစ်ထက် ကြာနေပြီး retry လည်း မဟုတ်ရင် stuck |
| `completed_at` | Terminal state ရောက်တဲ့ အချိန်။ တစ်ခါ ထည့်ပြီးရင် ပြန်မဖျက်ပါ။ | `IS NULL` = **In progress**။ `completed_at - created_at` = run တစ်ခုလုံး ကြာချိန် (SLA) |

### current\_state တန်ဖိုးများ

ဆက်လုပ်နေဆဲ (non-terminal) state တွေရဲ့ ပုံမှန်လမ်းကြောင်းက `SUBMITTED_TO_API → RECEIVED → FETCHING_CPE_STATUS → WORKFLOW_EVALUATED → PUBLISHED → CONSUMED → (BCS_UPDATING →) RT_UPDATING` ဖြစ်ပါတယ်။ `CPE_FETCH_RETRY_SCHEDULED` နဲ့ `KAFKA_PUBLISH_RETRY_SCHEDULED` ကို report မှာ "Retrying" လို့ ပြပြီး stuck အဖြစ် မရေတွက်ပါ။

Terminal state ရောက်ရင် `completed_at` ထည့်ပါတယ် –

| State | ဘယ်သူ သတ်မှတ်လဲ | Report card |
| --- | --- | --- |
| `COMPLETED` | rtutil\_consumer (RT update အောင်မြင်) | Success |
| `SKIPPED` | rtutil\_consumer | Finished |
| `NOT_ELIGIBLE` | rt\_web\_ui (gate မအောင်) | Not eligible |
| `ALREADY_PROCESSED` | (ရေးတဲ့ code မရှိသေးပါ) | Not eligible |
| `API_REJECTED` | rt\_web\_ui (API က 4xx/5xx ပြန်) | Manual check |
| `API_UNREACHABLE` | rt\_web\_ui (API ကို ဆက်သွယ်လို့ မရ) | Manual check |
| `CPE_NOT_FOUND` | remote\_resolve\_service / retry\_worker | Manual check |
| `FAILED_PERMANENT` | Retry ကုန် / normalize fail / publish timeout | Manual check |
| `CONSUME_VALIDATION_FAILED` | rtutil\_consumer | Manual check |
| `RT_UPDATE_FAILED` | rtutil\_consumer | Manual check |

## ၂။ pipeline\_run\_events

`pipeline_runs` က လက်ရှိ state ကိုပဲ ပြပါတယ်။ ဘယ်လမ်းကြောင်းကနေ ရောက်လာလဲ၊ step တစ်ခုချင်းစီ ဘယ်လောက် ကြာလဲ၊ ဘယ် component က ဘာလုပ်လဲဆိုတာ ဒီ table မှာပဲ ရှိပါတယ်။ Code က `Create` ပဲ လုပ်ပြီး update/delete ဘယ်တော့မှ မလုပ်ပါဘူး (append-only)။

| Column | ရည်ရွယ်ချက် | Reporting အသုံး |
| --- | --- | --- |
| `id` | Primary key။ ဖြစ်တဲ့ အစဉ်အတိုင်း တိုးသွားပါတယ်။ | အစဉ်လိုက် စီဖို့ |
| `run_id` | ဘယ် run ရဲ့ event လဲ။ | Run တစ်ခုရဲ့ timeline ကို ပြန်ဆွဲဖို့ (index `(run_id, occurred_at)`) |
| `from_state` / `to_state` | ဘယ် state ကနေ ဘယ် state ကို ပြောင်းလဲ။ State မပြောင်းဘဲ မှတ်ချက်ပဲ ထည့်တဲ့ event (`AppendEvent`) ဆိုရင် နှစ်ခုလုံး တူပါတယ် (ဥပမာ `api_accepted`၊ `bcs_update_failed`)။ | Funnel analysis – step တစ်ခုချင်းစီကို ဘယ်နှခု ရောက်လဲ၊ ဘယ်မှာ ကျလဲ |
| `event` | ဖြစ်ခဲ့တဲ့ အကြောင်းအရာ (ဥပမာ `kafka_published`၊ `retry_attempts_exhausted`)။ | ကျတဲ့ အကြောင်းရင်း ခွဲဖို့ – ဥပမာ NOT\_ELIGIBLE ဖြစ်ရတဲ့ gate (`not_in_noc_queue`၊ `cpe_or_local_service_id_missing` …) |
| `component` | ဘယ် service က ရေးလဲ။ | ဘယ် component ကြောင့် fail လဲ သိဖို့ |
| `detail` | JSON အပိုအချက်အလက် (အောက်က table ကို ကြည့်ပါ)။ | Pipeline report က `api_rejected` ရဲ့ `http_status` ကို ယူပြီး **400 (validation)**၊ **409 (duplicate)**၊ **တခြား** လို့ ခွဲပါတယ်။ |
| `occurred_at` | ဖြစ်တဲ့ အချိန် (millisecond)။ | Step တစ်ခုချင်းစီ ကြာချိန် – ဥပမာ `kafka_published → kafka_consumed` = Kafka lag |

### အဓိက event များနဲ့ detail JSON

| Component | Event | `detail` keys |
| --- | --- | --- |
| rt\_web\_ui | `not_in_noc_queue`, `ticket_status_not_allowed`, `cpe_or_local_service_id_missing`, `opi_customer_excluded`, `service_type_not_eligible` | `gate` |
| rt\_web\_ui | `payload_submitted` | `url`, `bytes` |
| rt\_web\_ui | `api_accepted`, `api_rejected` | `http_status`, `message` |
| rt\_web\_ui | `api_unreachable` | `error` |
| remote\_resolve\_service | `api_received`, `node_red_fetch_started`, `cpe_not_found`, `normalize_failed`, `kafka_published` | — |
| remote\_resolve\_service | `workflow_evaluated` | `node_red_response`, `workflow_decision` |
| remote\_resolve\_service | `cpe_fetch_retry_scheduled`, `kafka_publish_retry_scheduled` | `error`, `max_attempts` |
| retry\_worker | `retry_claimed`, `retry_attempt_failed` | — (error က `last_state_reason` ထဲ ရောက်ပါတယ်) |
| retry\_worker | `retry_attempts_exhausted`, `cpe_not_found_on_retry`, `normalize_failed_on_retry` | `error`, `attempt`, `max_attempts` |
| publish\_timeout\_sweeper | `publish_consume_timeout` | `published_at`, `timeout_minutes` |
| rtutil\_consumer | `kafka_consumed` | `topic`, `partition`, `offset` |
| rtutil\_consumer | `consume_validation_failed`, `rt_ticket_fetch_failed`, `ticket_skipped`, `bcs_update_started/succeeded/failed`, `rt_update_started`, `rt_update_failed`, `rt_updated` | — |

## ၃။ retry\_jobs

Node-RED သို့မဟုတ် Kafka ခဏ ကျနေတဲ့အခါ run ကို ချက်ချင်း fail မလုပ်ဘဲ backoff schedule (default **1m → 3m → 5m**၊ အများဆုံး ၃ ကြိမ်၊ `RETRY_WORKER_BACKOFF_SCHEDULE`) နဲ့ ပြန်ကြိုးစားဖို့ပါ။ Job ကို DB ထဲမှာ သိမ်းလို့ service restart ဖြစ်လည်း retry မပျောက်ပါဘူး။

| Column | ရည်ရွယ်ချက် / ထည့်ပုံ | Reporting အသုံး |
| --- | --- | --- |
| `id` | Primary key။ `pipeline_runs.active_retry_job_id` က ဒီကို ညွှန်ပါတယ်။ | Run နဲ့ join ဖို့ |
| `run_id` | ဘယ် run အတွက်လဲ။ Logical link ပဲ (FK မရှိ)။ Run မရှိတော့ရင် job ကို `FAILED_PERMANENT` လုပ်ပါတယ်။ | Run တစ်ခုရဲ့ retry history (job တစ်ခုထက် ပိုရှိနိုင်) |
| `job_type` | `NODE_RED_FETCH` (CPE data ယူတာ ကျ) သို့မဟုတ် `KAFKA_PUBLISH` (Kafka ပို့တာ ကျ)။ | ဘယ် dependency က မကြာခဏ ကျလဲ |
| `ticket_id`, `cpe_id` | Run ကို join မလုပ်ဘဲ တန်းကြည့်လို့ရအောင် ကူးထားတာ။ | — |
| `request_payload` | ပြန်ကြိုးစားဖို့ လိုတဲ့ input (NODE\_RED\_FETCH = မူရင်း request၊ KAFKA\_PUBLISH = ပို့ရမယ့် payload)။ | Debug |
| `last_error` | နောက်ဆုံး ကြိုးစားတုန်းက error။ Resolve ဖြစ်ရင် ရှင်းပစ်ပါတယ်။ | Retry ဘာကြောင့် ကျနေလဲ |
| `attempt_count` / `max_attempts` | ကြိုးစားပြီး အကြိမ်ရေ / အများဆုံး ကြိုးစားခွင့် (schedule အရှည် = 3)။ | In-progress list က "attempt `attempt_count+1` / `max_attempts`" လို့ ပြပါတယ်။ နောက်ဆုံးအကြိမ် ကျရင် count မတိုးလို့ fail ဖြစ်ပြီးသား row မှာ 2 ပဲ ပြပါမယ်။ |
| `status` | `PENDING` (စောင့်နေ) → `RETRYING` (worker ယူထားပြီ) → `RESOLVED` / `FAILED_PERMANENT` | Retry resolve rate |
| `next_retry_at` | နောက်တစ်ခါ ကြိုးစားမယ့် အချိန်။ | In-progress list မှာ "next retry in …" |
| `locked_by` / `locked_at` | Worker က job ကို ယူထားတဲ့ lease (`hostname-pid`)။ Worker ကျသွားရင် `LOCK_TTL` (default 5m) ကျော်ရင် တခြား worker က ပြန်ယူပါတယ်။ | Lock ကြာနေရင် worker ကျနေတဲ့ လက္ခဏာ |
| `created_at` / `updated_at` | Job ဖွင့်ချိန် / နောက်ဆုံး update။ | — |
| `resolved_at` | အောင်မြင်ရင်ပဲ ထည့်ပါတယ်။ Fail ဆိုရင် NULL။ | Retry ဘယ်လောက်ကြာမှ ပြန်ကောင်းလဲ |

### Run ပေါ် သက်ရောက်ပုံ

1. Step ကျရင် `remote_resolve_service` က job ဖွင့်ပြီး run ကို `CPE_FETCH_RETRY_SCHEDULED` သို့မဟုတ် `KAFKA_PUBLISH_RETRY_SCHEDULED` သို့ ပို့ပါတယ်။ `active_retry_job_id` ထည့်ပါတယ်။
2. `retry_worker` က 30 စက္ကန့်တိုင်း poll လုပ်ပြီး `next_retry_at` ရောက်တဲ့ job တွေကို ယူပါတယ် (`SELECT … FOR UPDATE`၊ တစ်ကြိမ် 20 ခု)။
3. အောင်ရင် job = `RESOLVED`၊ run က ရှေ့ step ဆက်သွားပြီး `active_retry_job_id` = NULL။
4. NODE\_RED\_FETCH အောင်ပြီး Kafka ပို့တာ ထပ်ကျရင် transaction တစ်ခုထဲမှာ job ဟောင်းကို resolve၊ KAFKA\_PUBLISH job အသစ် ဖွင့်၊ `active_retry_job_id` ကို အသစ်ဆီ ပြောင်းပါတယ်။
5. အကြိမ်ရေ ကုန်ရင် job နဲ့ run နှစ်ခုလုံး `FAILED_PERMANENT`၊ event `retry_attempts_exhausted`၊ `completed_at` ထည့်၊ `active_retry_job_id` = NULL။
6. Retry အတွင်း `CPE_NOT_FOUND` ဖြစ်ရင် ကျန်အကြိမ်ရေ မစောင့်ဘဲ run ကို ချက်ချင်း ပိတ်ပါတယ်။

## သတိပြုရန်

- **`refine_job` table မရှိပါဘူး။** Code နဲ့ doc အားလုံးမှာ ရှာကြည့်တော့ job table က `retry_jobs` တစ်ခုပဲ ရှိပါတယ်။
- **Schema မတူတာ ရှိပါတယ်။** `Final/pipeline_runs.sql` မှာ `last_error` လို့ ရေးထားပေမဲ့ တကယ့် column က `last_state_reason` ဖြစ်ပါတယ်။ `local_service_id`၊ `ref_bcs_process_id` ကလည်း file ထဲမှာ မပါပါဘူး။ Table ကို noc\_automation startup မှာ GORM AutoMigrate က ဆောက်တာမို့ DB ကပဲ မှန်ပါတယ်။ AutoMigrate က column မဖျက်လို့ DB ဟောင်းမှာ `last_error` ကျန်နေနိုင်ပါတယ်။
- **အမြဲ NULL ဖြစ်နေတဲ့ column တွေ** – `tags`၊ `ref_bcs_process_id`၊ `before_bcs_channel`၊ `target_bcs_channel`။ Report မှာ မသုံးပါနဲ့။
- **`ALREADY_PROCESSED`** state ကို ရေးတဲ့ code မရှိသေးပါဘူး။
- Local DB မှာ `retry_jobs` row မရှိလို့ retry ပိုင်းကို code ကနေပဲ စစ်ခဲ့ရပါတယ်။ Data နဲ့ မစစ်ရသေးပါ။



tail ~/db_dumps/rt5_progress.log            # ဘယ် table ရောက်နေလဲ (OK / FAILED)
cat ~/db_dumps/rt5.status                   # ရှိရင် ပြီးပါပြီ (exit=0 ဆို အောင်မြင်)
systemctl --user stop rt5-dump              # ရပ်ချင်ရင်


tail -f .run/pipeline_report.log
