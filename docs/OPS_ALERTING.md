# SchoolNossa operations alerting

Failure and usage emails for SchoolNossa, modelled on StoryTeller's
`functions_alerts/` (see "Differences from StoryTeller" below). Built 2026-09-13.

All emails go to `ALERT_EMAIL_TO` (von.roth@gmail.com) from
`SchoolNossa Alerts <alerts@schoolnossa.com>` via Resend.

## What arrives in the inbox

| Email | When |
|---|---|
| 🔴 `PERMANENT · {component} · {error_code}` | Immediately, once per distinct bug (fingerprint) per 30 min; repeats are counted into the next email |
| 🎉 New signup · 🧪 Trial started · 💶 New paid · 🔁 Renewed · ⚠️ Cancelled / expired / billing issue · 💬 Feedback | Immediately, one per event |
| 🟠 `TRANSIENT elevated` | > 10 transient failures with the same code in 60 min (max once per 2 h per code) |
| 🟡 `DEGRADED` | ≥ 5 fallback events in 60 min (max once per 2 h) |
| `Daily report · N active · N signups · N paid · N failures` | 08:00 Europe/Berlin — accounts, activity, trials/revenue, feedback, failures, alerter health. ⚠️ prefix = the sweep heartbeat is stale |
| 🛑 Alert email cap reached | > 30 instant emails in one clock hour; further instant alerts wait for the daily report |

There is deliberately no "no traffic" alert — at current volume it would just be spam
(StoryTeller lesson). If the daily report stops arriving, pg_cron or the report is broken.

## Architecture (Lovable Cloud / Supabase `whzvzoumldeqgyrqlilt`)

```
edge functions ──withFailureReporting──┐
web app  ──┐                           ▼
mobile   ──┼─ POST client-errors ──► failure_events ──(PERMANENT, trigger + pg_net)──► alert-dispatch ──► Resend
pipeline ──┘                           │                                                   ▲
auth.users / user_access / app_feedback triggers ──► usage_events ──(trigger + pg_net)─────┘
revenuecat-webhook (cancel/expire/billing) ─────────┘
pg_cron */15 ──► alert-sweep   (heartbeat, retry undelivered, transient/degraded rate alerts)
pg_cron 06:00+07:00 UTC ──► usage-report (sends only when Berlin hour = 8)
pg_cron 03:30 UTC ──► retention (failure_events 30 d, usage_events 180 d, keyed on expires_at)
```

- Tables: `failure_events`, `failure_state` (per-fingerprint cooldown + `_heartbeat`,
  `_email_budget`, `_rate_*` rows), `usage_events`. RLS on; service role writes; admins read.
- `claim_failure_alert()` makes the cooldown decision atomically, so a burst of
  identical failures sends exactly one email.
- Internal functions (`alert-dispatch`, `alert-sweep`, `usage-report`) require the
  `x-alerts-secret` header (`ALERTS_INTERNAL_SECRET`, also in Vault as
  `alerts_internal_secret` for the triggers and cron).
- Classification lives in `supabase/functions/_shared/failure-reporter.ts` (app repo):
  PERMANENT / TRANSIENT / DEGRADED + error code; unmatched → PERMANENT `unclassified`.

## The `client-errors` contract (fixed)

`POST https://whzvzoumldeqgyrqlilt.supabase.co/functions/v1/client-errors`
with `apikey` + `Authorization: Bearer <anon key or user JWT>`:

```json
{ "component": "web|mobile|pipeline", "message": "…", "error_type": "…", "stack": "…",
  "stage": "…", "severity": "PERMANENT|TRANSIENT|DEGRADED", "error_code": "snake_case",
  "app_version": "…", "platform": "…", "url": "…", "context": {} }
```

→ `202 {"ok": true}`. Rate limit 20/min per IP; body ≤ 32 KB. `pipeline` may set
severity; `web`/`mobile` may only downgrade the server's classification. The user id
comes from the JWT, never from the body.

## Where each client lives

| Client | Code |
|---|---|
| Edge functions | app repo `supabase/functions/_shared/failure-reporter.ts`; every function wrapped in `withFailureReporting` (5xx + uncaught), explicit reports for OTP email, payments, RevenueCat |
| Web app | app repo `src/lib/errorReporting.ts`, `src/components/AppErrorBoundary.tsx`, handlers in `src/main.tsx` (off in dev unless `localStorage.sn_report_errors_in_dev = '1'`) |
| Mobile app | `schoolnossa-mobile/lib/core/error_reporting.dart` (FlutterError + PlatformDispatcher handlers, route observer, handled reports at sign-in/social auth/purchase; off in debug builds) |
| Data pipeline | this repo, `scripts_shared/failure_reporter.py`; every city orchestrator's `__main__` runs `run_with_failure_reporting(main, pipeline=…)` |

### Pipeline behaviour

- Crash, non-zero exit, or an orchestrator ERROR line containing "fail"
  (e.g. `Phase 5 failed: …`) → PERMANENT → instant email.
- Success but enrichment modules logged ERROR lines → DEGRADED → daily report only.
- Ctrl+C → nothing. Exit code and traceback are preserved.
- Opt out for experiments: `SCHOOLNOSSA_ALERTS=0 python scripts_x/…orchestrator.py`.

## Operating it

- Test a report now: SQL `select net.http_post(url := '…/functions/v1/usage-report', body := '{"force": true}'::jsonb, headers := jsonb_build_object('Content-Type','application/json','x-alerts-secret',(select decrypted_secret from vault.decrypted_secrets where name='alerts_internal_secret')))` via the Lovable SQL tool.
- Inspect: `select severity, component, error_code, message, notified, notify_error, created_at from failure_events order by created_at desc limit 20;`
- Change recipient: edge function secret `ALERT_EMAIL_TO`.
- Pause everything: `select cron.unschedule('alert-sweep');` etc.; instant alerts stop if
  the `dispatch_failure_event` / `dispatch_usage_event` triggers are disabled.

## Differences from StoryTeller

| StoryTeller (Firebase) | SchoolNossa (Supabase) |
|---|---|
| Firestore `failure_events` + onCreate function | Postgres table + trigger → pg_net → edge function |
| Firestore transaction for cooldown | `claim_failure_alert()` single SQL function |
| Firestore TTL (once deleted everything within minutes) | pg_cron delete on `expires_at` |
| 6-hourly production report | Daily 08:00 Berlin report (traffic is far lower) |
| No usage emails | Instant signup / trial / paid / churn / feedback emails |
| Cloud Monitoring alert on `ALERT_EMAIL_FAILED` | Sweep retries undelivered alerts; daily report shows pending/failed deliveries and heartbeat age |
