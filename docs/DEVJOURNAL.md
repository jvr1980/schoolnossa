# SchoolNossa Development Journal

## 2026-10-30 — Landing page: connector announcement for Claude + ChatGPT (in preview, not published)

**Why:** to tell visitors that paid subscribers can use SchoolNossa inside Claude and ChatGPT, with real screenshots of the connector.

**What** (app repo 9d5a655, pushed to main; Lovable shows it as a developer update):
- **Section:** new `src/components/landing/ConnectorShowcase.tsx` on the home page after "So funktioniert's / How It Works".
  - Contents: badge, title, 3 highlights, the two screenshots side by side (stacked on phones), a highlighted "Nur für zahlende Abonnenten. / For paying subscribers only." notice and a button to `/connectors`.
  - The notice says the trial doesn't include the connectors and that a Claude or ChatGPT plan with custom connectors is needed.
  - Copy in DE + EN: `connectorsTeaser.*` keys in `src/lib/translations.ts`.
- **Screenshots:** `public/screenshots/connector-claude.jpg` and `connector-chatgpt.jpg`, 1200 px wide, about 120 KB each.
  - Query: Gymnasien in Steglitz-Zehlendorf, in German in Claude and in English in ChatGPT. Both rendered the map card with numbered pins and the Arndt-Gymnasium Dahlem card.
  - Taken in the user's signed-in Chrome at 200% CSS zoom with the sidebar hidden in that tab only, then stitched from 3 frames and cropped to the chat column.
  - No name, initials, avatar or email in either image. The crop ends above the card description, so the unverified founding year isn't shown.
- **Verified:** `tsc` and `vite build` pass. In the Lovable preview the section renders in DE + EN, the images load and the button opens `/connectors`.

**Open:**
- Publishing needs the user's OK. It also ships connector 0.5.3 (UTM tags + visit counter, Lovable 164382f), which is still unpublished.
- The two test chats are still in the user's Claude and ChatGPT history.
- The screenshots show 2025/26 figures and the 2026-02 application window; retake them after the next data refresh.

## 2026-10-29 — Admission top-up: fresh crawl of schools without upcoming open days; 3 city rules published (live)

**Why:** the 10-03 refresh ran before most schools announced their November–January open days. On 10-29 only 467/1,183 German secondary and 376/1,639 primary schools showed an upcoming event, which is low for the start of the open-day season.

**What:**
- **Targets:** 1,909 German schools with a website and no open day dated today or later (676 secondary, 1,233 primary).
- **Reading the rows:** the anon key can no longer read `schools`/`primary_schools` (RLS lock 10-25), and there's no service-role key. A temporary `security definer` RPC `_admission_topup_rows(tok, off)`, guarded by a one-time token, returned the rows; it was dropped right after.
- **Script:** `refresh_admission_open_days.py` has two new options. `--rows` takes the rows from that JSON file, and `--cache-dir data_shared/cache/school_site_pages_admission_2026-10-29` forces a fresh crawl. The extraction, quote check and `implausible()` filter are unchanged.
- **Promotion:** same `_admission_staging` token pattern as 10-03. The UPDATE is guarded: only schools that still have no upcoming event. Criteria, window and notes use COALESCE. `last_open_day_seen` also takes the latest past date from the old `open_days`.
  - Backup: `_admission_backup_20261029` (1,685 rows).
  - Staging rows deleted afterwards; no policies left.

**Results:**
- **Run:** 1,628 ok, 57 nothing on the site, 218 sites unreadable, 6 extraction failures. 1,685 schools promoted (597 secondary, 1,088 primary). Cost ≈ $21 (30.9M input / 1.7M output tokens, Gemini 3 Flash).
- **Schools with an upcoming event (live):** secondary 467 → 647, primary 376 → 553.
  - New events by month: Nov 321, Dec 85, Jan 119, Feb–Mar 42, later 47.
  - Still thin: München secondary 43/129, Leipzig 36/94, Düsseldorf primary 12/88. Bavarian schools hold open days in Feb–Mar.

**City rules (`city_admission_rules`):** 17/20 now `published`. Backup: `_city_admission_rules_backup_20261029`.
- Berlin secondary: Klasse 7 registration 3–11 Mar 2027; Probeunterricht registration 8/9 Feb 2027 (berlin.de).
- Düsseldorf secondary: 15–17 Feb 2027, appointment only; notices from 24 Feb (duesseldorf.de).
- Bremen secondary: form due at the Grundschule by 8 Feb 2027 (2027 flyer on bildung.bremen.de).
- Still a pattern: Köln secondary and Stuttgart primary (2027 dates not published yet), and Frankfurt primary (its 2027 intake registered in spring 2026).

**Next:**
- Early January 2027: the same top-up for late publishers (Munich, Leipzig), then the full re-run.
- Update Köln secondary and Stuttgart primary once those cities publish their dates.

## 2026-10-28 — Connector 0.5.3: UTM-tagged links + anonymous visit counter (built, not yet published)

**Why:** to measure the traffic ChatGPT and Claude send to schoolnossa.com. Neither platform shares revenue with connector developers, so the value of the connector has to show up as our own traffic. Lovable analytics only reports referrer sources (on 10-21–10-28: chatgpt.com 1, claude.ai 1), so we count tagged visits ourselves.

**What** (Lovable 164382f; spec `lovable_connector_v053_utm_message.md`):
- **Link helper:** `src/lib/mcp/links.ts`. `trackedUrl` adds `utm_source=chatgpt|claude|ai_assistant`, `utm_medium=ai_connector`, `utm_campaign=mcp` and `utm_content` to schoolnossa.com links only. `tagText` tags links inside message texts.
- **Source:** `public.mcp_client_source()` (SECURITY DEFINER, authenticated only) maps the JWT `client_id` → `auth.oauth_clients.client_name`. It's called once per tool call and stored in the request's AsyncLocalStorage scope.
  - Checked: `mcp_usage.client_id` joins to `oauth_clients.id` (ChatGPT 31 rows, Claude 19).
- **Content values:** answer, card_open, card_more, card_map, card_places, card_footer, app, connectors, message, attribution.
  - "mehr in SchoolNossa" is now a link.
- **Counter:** table `connector_visits` (source, content, path; RLS with admin-only SELECT). RPC `log_connector_visit` checks values against an allow-list and is callable by anon.
  - `src/lib/connectorVisit.ts` runs in `main.tsx` before the router mounts, then strips `utm_*` with `replaceState`.
  - No user id, IP, cookies or device storage (§25 TTDDDG).
- **Migration:** 0006_connector_visits_and_client_source. Version 0.5.3, so the card URI is `ui://schoolnossa/schools.v0.5.3.html`. 96 tests pass.
- **Known gaps:** the subscription-required card tags /connectors as `ai_assistant`. The server `instructions` links are untagged.

**Query:** `select created_at::date, source, content, count(*) from connector_visits where created_at >= now() - interval '30 days' group by 1,2,3 order by 1 desc, 4 desc;`

**Open:**
- Publishing needs the user's click: the auto-mode classifier blocks production deploys.
- Then: Refresh tools in ChatGPT and Claude; test that a click lands in `connector_visits`.

## 2026-10-28 — CARTO basemap key, connector 0.5.2 card map, iOS 1.1.1 submitted

**Why:** since 2026-09-23 CARTO basemaps need an API key. Without it, every tile is an "API KEY REQUIRED" watermark, so the iPhone search map (and the web dark style) showed no map.

**What:**
- **Key:** a CARTO Basemaps key on the free commercial tier (1M requests/month), requested under jose.vonroth@schoolnossa.com. It is public (it travels in every tile URL); attribution is mandatory.
- **Web** (app repo 3b1a4ec): new `src/lib/mapTiles.ts` (`CARTO_BASEMAPS_KEY`, `cartoTileUrl(style)`, `CARTO_ATTRIBUTION`). The OverviewMap dark style uses it; the standard style stays on OSM.
- **Connector 0.5.2** (published): a Leaflet map block in the list views, above the cards:
  - numbered pins in card order, with the top 3 highlighted
  - the link "Open the full map in SchoolNossa" → /menu
  - CSP `resourceDomains` https://basemaps.cartocdn.com
  - card URI `ui://schoolnossa/schools.v0.5.2.html`
  - Verified in Claude: tiles, pins and attribution render.
- **iOS 1.1.1 (29)** (mobile `fix/carto-api-key` 6e205df): `search_map.dart` uses `rastertiles/voyager/…{r}.png?key=…` with `RetinaMode.isHighDensity`, and no subdomains.
  - Uploaded via altool; processed (VALID).
  - Version 1.1.1 created via the ASC API; release notes "Fehlerbehebung: Die Karte wird wieder angezeigt." / "Bug fix: the map is displayed again."
  - Release: AFTER_APPROVAL, the same as 1.1.0.
  - Submitted for review: reviewSubmission → WAITING_FOR_REVIEW.

**Verified in ChatGPT** (after "Refresh tools"): Steglitz-Zehlendorf Gymnasien → map with 10 numbered pins (top 3 highlighted), CARTO attribution, 3 rich cards with card-only texts (description, languages, contact, admission criteria), "+7 more in SchoolNossa".
- First test ran with "Enforce CSP for custom apps" off. The re-test with it on (the user switched it on) passed: Pankow → CARTO tiles, pins and rich cards all load, no CSP badge.
- Re-test detail: Our CSP is declared both ways: `_meta.ui.csp` and `openai/widgetCSP`.
- ChatGPT notes "private tools can't render their widgets: card_details"; expected for the app-only tool.

**Open:**
- Check the web dark map live (the key is in the live bundle; a tile returns 200).
- Restrict the CARTO key to schoolnossa.com and the app in the CARTO dashboard.

## 2026-10-26 — Connector 0.5 / 0.5.1 live: top-3 full details, travel times, card-only texts, Google hardening

**What** (Lovable 0208e3d + d464c8f, published):
- **Top-3 expansion:** the 3 expanded schools count as `detail`, with a slim fallback when the allowance is used up.
- **`commute_times`:** uses only the caller's saved `user_pois` (no typed addresses), through google-directions. It returns labels + times, never addresses or coordinates. New `commute` bucket, 20 per 24 h (migration 0005).
- **Card-only long texts:** descriptions, special features, admission criteria/notes and contact details go only in the tool result `_meta["schoolnossa/card"]`. The model sees key facts only.
  - 0.5 bug: the wrapper read the AsyncLocalStorage scope before the streamed tool finished, so `_meta` was never attached in either host. 0.5.1 reads the reply inside the scope.
  - Also new: the app-only `card_details` tool (`_meta.ui.visibility ["app"]`) as a fallback.
- **Versioned card URI** (`ui://schoolnossa/schools.v0.5.1.html`): ChatGPT had cached the 0.4 HTML.
- **Hardening:** google-directions and google-places-autocomplete/details now require a JWT + `has_app_access` (they were open to anyone with the anon key).
- **Texts:** /terms bans AI training on connector content; /connectors tip to turn off training in ChatGPT/Claude; privacy §8 sentence on travel times (labels only).

**Verified live:**
- Claude: Pankow → 3 rich cards (description, languages, address, contact, admission criteria) + "+6 more"; travel times from "Home" (e.g. 18 min / 2.2 km); `card_details` hidden from Claude's tool list.
- ChatGPT (after "Refresh tools"): Amsterdam VWO → rich cards with Dutch track chips.

**Found:** founding years are still in many descriptions (secondary: en 178, de 126; primary: en 162, de 68), and they're inconsistent. E.g. Carl-von-Ossietzky: description_en says 1909, the app modal showed 1908. That's a follow-up for the description audit.

## 2026-10-26 — Privacy AI section, dashboard-reset fix, Overview tab hidden, non-operating schools removed

**What (all published):**
- **Privacy:** /privacy section 8 "Nutzung über KI-Assistenten / Use through AI assistants" in DE + EN (old 8 → 9, "Oktober 2026"). Logging made to match the text:
  - pg_cron `mcp-usage-retention` deletes `mcp_usage` rows older than 90 days (daily 03:45)
  - semantic-search and the Google Places functions now log only the query length and scope, via `_shared/search-log.ts`
- **Dashboard reset fix** (found by Lovable's issue scan; a regression from the stage-1 gate):
  - the access re-check (every 60 s + on focus) unmounted the dashboard, wiping filters, search, open details and pins
  - now the spinner shows only before the first decision
  - background checks keep the UI; on focus at most every 5 min; an explicit refresh after payment
  - errors keep the last decision
- **Overview tab hidden** (user request); Search is the default tab. The code is kept (`src/lib/dashboardTabs.ts`).
- **Data:**
  - deleted Mentora Gymnasium (04P35) and NEWSchool ISS (09P21), both "KEIN Schulbetrieb"; 21 similarity rows cascaded; rollback in `data_shared/supabase_sql/data_fixes_2026-10/`
  - `insert_new_schools_to_supabase.py` now skips such rows and stops when RLS returns no existing rows
  - Utrecht "Amadeus Lyceum" ×2 = two real locations (NL-00RO00 / NL-00RO03), not a duplicate
  - NL has 221 secondary and 35 primary same-name groups per municipality; display names could get a location suffix (open)
- **Found:** google-directions and the Google Places functions don't check the caller; hardening is in the 0.5 spec.

## 2026-10-25 — Connector 0.3 (limits, AI search, attribution, deep links) + 0.4 (MCP App cards) live

**What.**
- **0.3** (Lovable 93374f1, migrations 0002 + 0003):
  - results default 5, max 10; `search_schools` requires a city; slim list fields
  - 60 searches and 30 detail lookups per user per 24 h through `mcp_consume()` + `mcp_usage`; admins exempt
  - `mcp_limit_reached` goes to the daily report only; Lovable had to widen the `usage_events` type check
  - new `find_schools_by_description`, which reuses the app's semantic-search with the caller's token
  - every school gets `url` (`/school/<id>?cat=`) + `source`, and every result an attribution block
  - new `/school/:id` deep link, and AppAccessGate now passes `?next=`
  - /terms bans bulk extraction
  - Verified live: limit 50 rejected; 5 slim results; call logged as OAuth; deep link opens the detail modal; the AI search works in Claude.
- **0.4** (Lovable 145d2a4 + build fix):
  - MCP App with branded cards: list / detail / limit or subscription message / empty; DE/EN; light/dark; links only to schoolnossa.com
  - implemented as a wrapper around the mcp-js handler, so OAuth and the challenge are unchanged; resources/* are authenticated by replaying the request as a ping
  - Verified rendering in Claude ("public Gymnasien in Berlin-Pankow" → 3 cards + "+6 more") and ChatGPT (Amsterdam VWO → cards with track chips). Both needed a tool-list refresh in their settings.
- **AI search diagnosis:**
  - the key and model are fine (gemini-embedding-001, 768 d, both sides)
  - the flat scores come from very similar school embeddings: Berlin pairwise cosine averages 0.84
  - mean-centering widens the p10–p90 spread from 0.065 to 0.219 and brings science-profile schools to the top (tested in SQL with a real query embedding)
  - **Shipped** (Lovable 390d7a3, migration 0004, published):
    - `semantic_rank_schools()` (service_role only) ranks in the DB with a country-wide centroid and excludes "KEIN Schulbetrieb" rows
    - `similarity = (centered + 1) / 2`, so app sorting is unchanged
    - language boost 0.15; lowConfidence when the best centered score < 0.10
    - optional `schoolType` filter, also in the MCP tool
  - **Result** for the bilingual + science query in Berlin:
    - before, the top 8 sat within 0.006 of each other
    - after, the top ranks are Willi-Graf, Marie-Curie, Rückert, Manfred-von-Ardenne, Isaac-Newton, Nelson-Mandela (bilingual), Heinrich-Hertz, Ernst-Abbe
    - p10–p90 spread 0.119 (Berlin-only centroid would give 0.219)
    - 1.4–3.3 s per call
  - The 30–60 s seen through the connector wasn't reproducible in the function itself.
- **Data notes:**
  - 2 Berlin secondary rows are "KEIN Schulbetrieb" (Mentora Gymnasium, NEWSchool ISS) and still show in the app
  - Utrecht has duplicate "Amadeus Lyceum" rows, one without students (probably a second location)

## 2026-10-25 — ChatGPT + Claude connectors tested; discovery files live; /connectors page

**What.**
- **Connector tests:** the MCP server works end to end with the admin (paid) account.
  - Claude: custom connector; auth and DCR detected automatically; query "covered cities + Gymnasien in Berlin-Pankow".
  - ChatGPT: Plugins → Add custom MCP server; query "VWO schools in Utrecht".
  - `auth.oauth_clients` now holds "Claude" and "ChatGPT" (dynamic).
- **Published** (Lovable 44ea15e + 31da2ba + bd4f0d6):
  - robots.txt lets the AI search/user fetchers (OAI-SearchBot, ChatGPT-User, Claude-SearchBot, Claude-User) read public pages; training bots stay blocked.
  - llms.txt rewritten in DE + EN with the current scope, access rules and connector URL.
  - Meta and homepage descriptions changed from "600 Schulen" to "über 10.000 Schulen in Deutschland und den Niederlanden".
  - New public page /connectors (setup in Claude and ChatGPT, requirements, privacy, disconnect).
  - Explicit `destructiveHint: false` on the read-only MCP tools (ChatGPT requires all three hints).
- **Submission pack:** `docs/connector_listing/AI_CONNECTOR_LISTINGS.md` (listing texts, reviewer steps, example prompts, compliance notes, draft privacy section) plus icons.

**Blockers for the directory submissions:**
- **Reviewer login:** web sign-in needs an emailed 6-digit code after the password, and there is no bypass; OpenAI forbids 2FA barriers for reviewers.
- **Privacy policy:** it has no AI-connector section (draft awaits approval).
- **OpenAI:** organization verification.
- **Connector 0.3 proposed:** result caps (5 by default, max 10), daily per-user limits, call logging, an AI/semantic search tool reusing `semantic-search`, and per-school deep links + `source`. Optional 0.4: branded MCP App cards.

**Process note:** when pasting into Lovable, copy with `LANG=en_US.UTF-8 pbcopy`; without it the umlauts arrived garbled (the agent repaired llms.txt itself). The auto-mode check also blocked the `--no-ff` merge to main, so it is still pending the user.

## 2026-10-25 — Access model live: 48 h trial, one trial per email, paid-only connectors, school tables locked

**Why.** The user asked how to open SchoolNossa to ChatGPT/Claude without breaking the paywall. An audit showed there was no paywall below the UI:
- `schools`, `primary_schools` and `school_similarities` were readable by anyone with the anon key (`USING (true)`).
- `/dashboard` only checked sign-in; FreeTrialBlocker was mounted nowhere; expired paid accounts were never blocked; a missing `user_access` row counted as allowed.
- 14 of 46 accounts (Google/Apple sign-ins) had no row at all, and the client chose its own trial row. The INSERT policy allowed any `test_ended_at`.
- The re-trial guard (`trial_used_emails` + `check_trial_used`) was checked client-side on web email signup only. iOS never checked it, and anon could call the RPC to probe whether an email had an account.
- The web "Delete account" didn't delete the account.
- The MCP server only checked `isAuthenticated()`.
- `semantic-search` used the service role with no caller check.
- `verify-payment` silently granted nothing to accounts without a row.

**User decisions.** No free/premium column split. New users get a 48 h trial, then pay. Deleting the account and signing up again gives no new trial. ChatGPT/Claude connectors need a paid licence. The 44 existing non-paying accounts get one fresh 48 h window at launch.

**What.** SQL, specs, rollback and a local Postgres test harness are in `data_shared/supabase_sql/access_model_2026-10/`. All assertions passed on postgres:15 before anything touched production.
- **Stage 1** (Lovable a8620bf, 12.4 credits; `drizzle/migrations/0000_server_access_stage_1.sql`):
  - a trigger on `auth.users` grants the trial server-side for every provider (`app_settings.trial_hours` = 48)
  - `trial_claims` keeps one row per normalized email (lowercase, +tag, Gmail dots), recorded at grant time and never deleted; all 46 existing emails were seeded
  - `ensure_user_access()` self-heals a missing row
  - `has_app_access()` = trial, paid or admin; tokens with an OAuth `client_id` (ChatGPT/Claude) need `has_paid_access()`
  - client INSERT policy dropped, `check_trial_used` closed, `public_school_stats()` added for the landing page and /data-coverage
  - frontend: `AppAccessGate` on /menu and /dashboard, hours countdown, 48 h copy, real deletion via `delete-account`
  - MCP (all 6 tools) and WebMCP paid-only; `semantic-search` requires a JWT with access; `verify-payment` upserts the row and extends from the later of now and the current end
- **Go-live backfill** (SQL tool, right after the user published): 14 rows created and 30 expired trials extended, all ending 2026-10-27 15:21 UTC. Usage events were suppressed (`app.suppress_usage_events`), so no alert spam. The 2 paid rows were untouched.
- **Stage 2** (Lovable `0001_access_stage_2_lock_school_tables.sql`): the school tables are SELECT-only for `authenticated` with `has_app_access()`; `school_city_counts` runs as definer; `trial_claims` privileges were reduced to SELECT and DELETE for authenticated.

**Results** (live role-switched probes):

| Caller | Secondary schools | Primary schools | City counts |
|---|---|---|---|
| anon | 0 | 0 | 299 rows |
| trial, first-party | 2,814 | 7,699 | ok |
| trial with an OAuth token | 0 | 0 | ok |
| paid with an OAuth token | 2,814 | 7,699 | ok |

The landing page, /data-coverage and the admin dashboard were confirmed by Lovable. iOS 1.1.0 (28) uses `ensure_user_access()` (other session).

**Process note.** Auto mode blocks `mcp__lovable__send_message` for production changes. At the user's request, both stages were sent through the Lovable chat in Chrome; Lovable turns long pastes into an attachment.

**Still open:** listing the connector in Claude/ChatGPT (directory submission; ChatGPT forbids upsell links), public discovery pages plus a robots.txt split for AI search bots, and an end-to-end test of re-signup after deletion with a disposable account.

**Side effect on the pipeline** (found by the other session after stage 2): four data-repo scripts read `schools`/`primary_schools` with the anon key, and since the lock those reads return 0 rows without an error:
- `scripts_shared/upload_to_supabase.py`
- `scripts_shared/upload_embeddings_to_supabase.py`
- `scripts_shared/upload_admission_open_days_to_supabase.py`
- `scripts_munich/enrichment/munich_recover_missing_metadata.py`

My pre-lock check missed them because they build the URL as `SUPABASE_URL + '/' + table`. A separate task makes them fail loudly; nothing runs them before the next refresh (~Jan 2027). Reads need an authenticated session with access, or the Lovable SQL tool.

## 2026-10-25 — Netherlands public: website published, iOS 1.1.0 in review, 1.0.1 live

**What.**
- **iOS 1.0.1:** live since the morning; it carries the crime redesign and the login/usage fix.
- **Website:** published by the user, with live-bundle checks for `school_city_counts`, Praktijkonderwijs and the launch banner. It brings:
  - country → province → municipality navigation, up to 10 Dutch municipalities combined (Lovable 9a1f525 + 2535dfa)
  - OR-matched track filter
  - launch banner/hero (8f677467)
  - the other session's 48 h trial / access gating (a8620bf)
- **iOS 1.1.0 (28):** the Netherlands with multi-municipality, plus a server-side access row (`ensure_user_access`) and a 48 h countdown. Submitted to App Store review at 15:20 UTC with an updated DE/EN listing.

**Still open.**
- The other session's go-live backfill and stage-2 table lock.
- NL descriptions with approximate student counts (5,298 of 7,689; user paused this).
- Announcement email/social: drafts in `docs/LAUNCH_ANNOUNCEMENT_2026-10.md`; channel and timing need the user's OK.

## 2026-10-24 — iOS 1.0.1 submitted, App Store badge live, NL branch merged, NL navigation started

**What.**
- **iOS:** 1.0 (build 20) had been on sale since 2026-09-15 without anyone noticing. 1.0.1 (25) was submitted to App Store review on 10-24, auto-release on approval. It adds error reporting and the crime redesign: no raw counts; % vs typical district, change vs last year, rate per 1,000. The live build showed Berlin counts like "12.555" and nothing elsewhere. 1.1.0 (26) = the Netherlands (province → municipality picker from `school_city_counts`, OR-matched track filter); it is on TestFlight only, because Apple reviews one version at a time. Details: mobile repo `docs/DEVJOURNAL.md`.
- **Website:** `/app` page and landing strip now show Apple's official badge (toolbox.marketingtools.apple.com; the old tools.applemarketingtools.com host is NXDOMAIN) linking to the App Store; TestFlight wording gone. Lovable d13d05c + 435a8d6 (2.7 credits), published and verified in the live bundle.
- **Data repo:** `feature/nl-pipeline-improvements` (35 commits) merged to main; only this journal conflicted (entries interleaved by date).
- **Website NL navigation:** spec sent to the Lovable agent (RPC-driven country → province → municipality, track multi-select, hide German-only sections for NL, geo_* added to import whitelists). The agent's plan awaits the user's approval; nothing published.

**Found, not fixed (trial/paywall audit).** On the web the "trial ended" block is only on `/menu`: `/dashboard` has no check; expired paid users are never blocked; `FreeTrialBlocker` is not mounted anywhere; Google sign-ups never get a `user_access` row (13 users, Feb–May) and are never blocked. Live settings: test phase on, 200 seats, 30 days; free tier 10 min (unused); €9.99 / 3 months. 46 accounts: 30 expired trials, 2 paid, 14 without a row. iOS enforces all of it on every screen but lacks the web's "trial already used" check.

## 2026-10-03 — Admission data refreshed for all German schools, primary schools included (live)

**What:** a new run of admission criteria, application windows and open days for all 2,824 German schools (1,185 secondary, 1,639 primary). The April data was stale: its open days had all passed. Primary schools had no admission data at all.
- **Extraction:** `scripts_shared/enrichment/refresh_admission_open_days.py` crawls the school's own site (registration and date pages first, max 30 pages, cached), then Gemini 3 Flash extracts in one call per school.
- **No search grounding:** the run isn't limited by the daily grounding quota.
- **Upload:** `upload_admission_refresh.py` stages rows to `_admission_staging` with a token, then SQL promotes them.

**Checks:**
- **Evidence:** every criterion, window and open day must carry a quote that is found verbatim on a crawled page.
- **Dates:** past open days are dropped (the latest becomes `last_open_day_seen`), and so are windows that ended more than 400 days ago.
- **New after a spot check (`implausible()`):**
  - Drop open days whose date contradicts the quote's year or weekday. Flash sometimes moves a past date into next year: "18. August 2026" became 2027-08-18, and "Donnerstag, 8. Januar" became a Friday in 2027.
  - Drop first-day ceremonies (Einschulung) and evenings for already-admitted classes. Info evenings about Einschulung are kept.
  - Together this removed 35 of 2,211 events.
- **Windows:** one window whose "opens" date fell after "closes" lost its invented opening date.

**Results:**
- **Read:** 2,473 schools with something found, 53 with nothing on their site, 228 unreadable sites (JS-only or blocked, still unreadable on retry), 70 without a website. The second pass picked up the 115 websites filled earlier the same day.
- **Promoted:** 2,526 schools (1,058 secondary, 1,468 primary):
  - open days replaced by the new upcoming ones;
  - criteria, window and notes replaced where found, otherwise kept;
  - `admission_fetched_at` set to now.
- **Live now:**
  - secondary: 1,105 with criteria, 659 with a window, 531 with upcoming open days;
  - primary: 1,321 / 692 / 563.
- **Backup:** `_admission_backup_20261003` (2,526 rows); the staging insert policy is dropped.
- **Cost:** about $33 (Gemini 3 Flash, 48M input tokens).

**App (Lovable):**
- **Admission section:** primary schools now show it.
- **Official dates & rules:** a block for the city from `city_admission_rules` is shown under the school's own data, or alone when the school has none.
- **Past dates:** hidden or marked "previous round" (Lovable 5468472, earlier the same day).
- **Crime fixes found in review:** the safety-dot colour now comes from the % value (it used the district rank as a quintile), and the crime filter now applies at 0% and negative limits.

**Next:** run again around January 2027, when secondary schools announce their 2027/28 open days. Update the `city_admission_rules` rows marked "pattern from 2026/27" when the cities publish their dates.

## 2026-10-03 — Held-back descriptions rewritten (247 live)

**What:** the 296 schools that were held back from the 09-28 verify-and-trim got a full rewrite. These were weak placeholder texts plus trims that would have cut more than 25%. The script is `scripts_shared/enrichment/rewrite_held_descriptions.py`. For each school it:
1. researches the school with grounding;
2. generates DE and EN text from the updated template (no student, teacher or class counts);
3. verifies every claim with Gemini 3.1 Pro against the crawled school site;
4. applies the policy-B trim;
5. runs the guards: hold back on any validation problem, if the trim removes >25%, or if the text states a count (regex);
6. re-embeds the text.

The 150 schools that failed in the 09-28 run were retried with Pro first; 63 more were applied, so 1,586 verify-trim edits are live in total.

**Results:**
- **247 / 296 rewritten and promoted:** 84 secondary, 163 primary.
- **49 kept their current text:**
  - 34 new texts failed the trim checks;
  - 9 would lose >25% after the trim;
  - 4 had no research sources (thin Grundschule entries);
  - 2 stated counts.
- **Length:** median DE length went from 1,035 to 1,284 characters. Every remaining claim was confirmed by a source (typically 11–19 sources per school).
- **One manual edit:** igis Köln said "two sites:" and then listed one, because the second site was trimmed as unsourced. The lead-in was removed, and the edit is recorded in the results row.

**Safety:**
- **Backup:** `_desc_backup_rewrite_20261003` (247 rows: description, de, en, grounding, researched_at, embedding).
- **md5 guard:** promotion ran only where md5 of the live DE and EN text still matched the replaced text; 247/247 matched.
- **Provenance:** per-claim records are in `description_verifications` (batch `rewrite-2026-10-03`, `applied = true`).
- **Similar schools:** `school_similarities` was rebuilt for all 10 German cities, since every one had re-embedded schools. The result is 11,850 rows (1,185 × 10), 0 cross-city pairs, NL untouched; the backup is `_sim_backup_20261003`.

**Cost:** about $57 for the Pro checks and about $1 for the Flash trims, both logged. Research and generation were not logged; the estimate is $40–60 more.

## 2026-10-03 — Crime shown as a rate vs a typical district, plus year-on-year change (live)

**What:** parents no longer see raw offence counts. Each school now carries offences per 1,000 residents for its area, the % difference from the city's median district, and the % change from the previous year. Everything comes from official PKS / statistics-office tables. The script is `scripts_shared/processing/compute_crime_rates.py`; its outputs are in `data_shared/crime_rates_2026-10-03/`.

**Why:** a raw count ("4,312 offences") means nothing without population and a reference. Checking the old data also showed that much of it wasn't real:
- **Köln, Düsseldorf, Stuttgart:** district numbers were modelled (city total × a hand-set index).
- **Bremen:** values were hand-typed approximations.
- **Leipzig:** had fallen back to the city average.
- **München and Frankfurt:** repeated the city total for every school.

**New columns** (both tables): `crime_area`, `crime_area_level` (district/city), `crime_rate_per_1000`, `crime_rate_year`, `crime_vs_city_pct`, `crime_change_pct`, `crime_change_years`, `crime_source`.
- `crime_safety_category` is recomputed: safe ≤ −15% vs the median district, elevated ≥ +15%.
- The comparison uses the **median district**, not the city-wide rate. City centres (München Altstadt-Lehel +759%, Bremen Mitte +365%) pull the city rate up so far that almost every other district would look "safe".

**Coverage (2,824 German schools):**

| City | Level | Schools | Areas | Change | Source |
|---|---|---|---|---|---|
| Berlin | district | 752 | 12 Bezirke | 2024→2025 | Kriminalitätsatlas |
| München | district | 275 (+8 suburbs null) | 25 Stadtbezirke | 2024→2025 | Statistisches Amt |
| Stuttgart | district | 176 | 23 Stadtbezirke | 2024→2025 | PP Stuttgart |
| Leipzig | district | 178 (+8 outside Leipzig null) | 58 Ortsteile | 2024→2025 | Stadt Leipzig open data |
| Bremen | district | 208 (+47 Bremerhaven null) | 15 Beiratsbereiche | 2023→2024 | PKS Bremen PDFs |
| Köln / Düsseldorf / Frankfurt | city only | 258 / 145 / 173 | — | 2024→2025 | no district data published |
| Hamburg, Dresden | none | 431 / 165 | — | — | no usable small-area data |

**Safety:**
- **Backup:** `_crime_backup_20261003` (2,228 rows, 8 cities) holds the old values.
- **Old count columns:** Berlin keeps them (they are real official figures); the other cities' old modelled count columns were set to NULL.
- **Data fix:** Eberhard-Ludwigs-Gymnasium (Stuttgart) moved from bezirk West to Nord.

**App:** sent to Lovable. Counts are removed. The details card shows "x% below/above a typical district" plus the year-on-year line, and for city-only data a city-wide line without a label. No label is shown when there is no data (before, `mapSafetyLevel(null)` returned "moderate"). AI Search and the comparison rank on `crime_vs_city_pct`.

## 2026-10-03 — Missing school websites filled (115), official city admission rules

**Websites:**
- **Tool:** `scripts_shared/processing/fill_missing_websites.py` (Gemini Flash + Google Search).
- **Acceptance rule:** a URL is kept only if the fetched page names the school or its street (`_site_ok`).
- **Validation fix:** `_site_ok` now ignores city names and generic school words. Before, any Munich page passed for a Munich school, and re-validation cut the accepted sites from 120 to 115.
- **Results:** 115 applied (München 85 is the bulk), 39 have no site of their own, 31 candidates rejected.
- **Files:** `data_shared/website_fill_2026-10-03/` (deltas, `apply.sql`, `rollback.sql`).

**City admission rules:**
- **Table:** `city_admission_rules` (20 rows, public SELECT), with each city's official registration window and process for primary and secondary schools.
- **Content:** for primary, catchment and cut-off birthdate; for secondary, the steps. Each row has a `source_url`.
- **Status:** `published` where 2027/28 dates are out, otherwise `pattern from 2026/27`.
- **Use:** shown in every school's admission section, and the only content where the school's own site has nothing.

**Primary schools:** they now have the same admission columns as secondary schools (`admission_*`, `open_days*`).

**Test-canary finding:**
- **What:** the 2 München rows with `test-canary.example` (fixed 09-27) trace back to a Claude write test against production on 2026-04-19.
- **Scope:** only those 2 schools were affected.
- **Closed:** the anon write access used then is no longer open.

## 2026-09-28 — Verify-and-trim applied to all German descriptions

**What:** every German school description (2,824 schools, both tables) was checked with Gemini 3.1 Pro against the school's own crawled website plus Google Search. Policy B then deleted contradicted, outdated and unsourced concrete claims (`verify_trim_descriptions.py --german --policies B`). The texts were not regenerated.

**Results:**
- **Claims:** 42,424 checked:
  - confirmed 87.5%;
  - unsourced 7.9%;
  - contradicted 3.9%;
  - outdated 0.7%.
- **Schools:**
  - **1,523 edited:** 2,664 field edits, typically one or two sentences.
  - **942 unchanged:** no change needed.
  - **359 held back unchanged:** listed with reasons in `data_shared/description_verify_full_2026-09-27/held_back_schools.csv`, 145 of them in Berlin. A school is held back when a trim would remove more than 25% of either text, fails the deletion-only check, or still leaves a broken sentence after 4 attempts. Many are thin placeholder texts ("details … are not provided") that need a rewrite.
- **Traceability:** new table `description_verifications` (RLS, no policies). It holds one record per school with each claim's verdict, evidence URL, quote and whether its deletion was `applied`.
- **Backup:** `_desc_backup_verify_20260927` (1,523 schools). Edits were promoted only where md5(live text) still matched the checked text; 2,664 of 2,664 applied.
- **Cost:** about $520 for the Pro checks, plus Flash trims and grammar checks. There were 1,779 search queries, within the free tier.

**Fixes made during the run:**
- **Quota stop:** the Google Search grounding quota ran out after about 1,600 grounded calls in one day. Plain calls kept working, and the quota reset at 00:00 PT; the run was resumed the next morning, and results are written per school, so a crash or quota stop loses nothing.
- **Broken sentences:** spot checks found trims leaving fragments, which led to a per-sentence grammar check with re-trim feedback. On a 60-school sample, 56 passed cleanly and 4 were held back.
- **Lists:** the trim now deletes only the unsourced item of a list.
- **Hold-back rule:** it now works per language, so DE and EN stay in step.
- **Crawler:** it now skips malformed links.

## 2026-09-27 — Register repair: websites and operators (86 fixes, live)

**What:** fixed the register errors that the description audits traced back to our own data, and the scraper bugs behind them. The script is `scripts_shared/processing/repair_register_fields.py`; it only writes deltas, with guarded UPDATEs.

**Changes:**
- **Stuttgart websites (48):**
  - **Cause:** the scraper took the first external link on each stuttgart.de page. For schools without their own link, that was the city's accessibility guide `stuttgart-inklusiv.de`.
  - **Fix:** new values come from the LOBW Schulverzeichnis, or a Google-searched URL whose page must name the school or its street. Branch sites share their main school's site.
  - 4 schools have no website of their own and are now empty.
- **Stuttgart operator (15):**
  - **Cause:** it was guessed from name keywords, and primary rows split off combined schools were hard-coded 'Öffentlich'.
  - **Fix:** the value now comes from LOBW. The named operator decides (gGmbH, e.V., foundation, church → Privat), then the operator type. The type alone is not enough: 'Baden-Württemberg' also covers element-i and Kolping.
  - Newly private: St. Agnes, Albertus-Magnus, Waldschule Degerloch, Betty-Hirsch, Lessing-Schulen, Johannes-Brenz, Galileo, Raiffeisen and the Freie Aktive / Freie Evangelische primaries.
- **Bremen operator (21):**
  - **Cause:** it was guessed from name keywords. 'frei' matched "Freiligrathstraße" and "Freie Hansestadt".
  - **Fix:** the value now comes from the official Schulform ("Private Grundschule", "Private berufsbildende Schule", …).
  - 20 were changed to Privat (Technikerschule, St.-Johannis, Ökumenisches Gymnasium, …) and 1 to Öffentlich (Verwaltungsschule).
- **München websites (2):** `https://test-canary.example/` existed in Supabase only, not in our files. Something wrote test values into production; the finals' values were restored.

**Safety:**
- **Backup:** `_register_backup_20260927` (82 rows).
- **Rollback:** `data_shared/register_repair_2026-09-27/rollback.sql`.
- **Local files:** the final files were patched too (backup in `finals_backup/`), and a cell-level diff confirmed that only `website` and `traegerschaft` changed.
- **Scrapers:** both scrapers were fixed, so a re-scrape doesn't bring the errors back.

## 2026-09-27 — Verify-and-trim tested on 100 schools

**What:** tested a cheaper alternative to regenerating descriptions. It keeps each text and deletes only the claims that a source contradicts, shows as outdated, or can't support.
- **Checker:** `verify_trim_descriptions.py`. It crawls the school's own site, then Gemini checks each claim and Flash deletes the flagged ones (deletion only).
- **Reference:** a second independent audit of 70 new schools (1,000 claims), plus the first audit's 30 schools on their original text.
- **Scoring:** `eval_verify_trim.py`.

**Results:** `docs/audits/DESCRIPTION_VERIFY_TRIM_TEST_2026-09.md`.
- **Current quality:** 30% of the 70 new schools carry at least one materially wrong claim (84% of claims verified, 5% wrong). Across all 100 it is 28%.
- **Best option, Pro + policy B:** removes 59% of material errors and 52% of all wrong claims, but also 4% of correct claims (5% of the text). Schools with a material error fall from 27% to 11%.
  - Cost: $0.20 per school, about $560 for all German schools.
  - Flash + B gets 27% → 14% for about $125.
- **Not applied to Supabase;** this was a test only.

**Lessons:**
- Given only a URL, Gemini answered from memory, so the site is now crawled first (frames, meta refresh, two menu levels, TLS/HTTP fallbacks).
- Deleting "every mention, in both languages" matters.
- Treating unmatched evidence quotes as unsourced (policy C) costs too many correct claims.

**Side findings (data, not descriptions):**
- **Placeholder websites:** 2 Munich rows have `https://test-canary.example/` as their website (Erasmus-Grasser-Gymnasium, GS Bäckerstraße 58).
- **Wrong websites:** St. Agnes Stuttgart and Freie Aktive Schule Stuttgart have unrelated sites stored.
- **Wrong operator:** `traegerschaft` is wrong for St. Agnes, Technikerschule Bremen and Freie Aktive Schule, all stored as public but not public.
- **Borrowed content:** 2 descriptions borrow content from a same-named school in another city.

## 2026-09-27 — Descriptions: audited errors fixed, people counts removed, stricter research prompt

**What:**
- **Research prompt tightened** (Lovable `generate-school-descriptions` 8ce0230, replica f1d34d8): sourced facts only, no student/teacher counts, grounding required (`[RESEARCH_FAILED:NO_SOURCES]` otherwise), and the polish step gets the city name.
- **The 10 material errors from the audit are corrected** (`CORRECTIONS` in `scripts_shared/enrichment/scrub_description_claims.py`).
- **Student and teacher counts removed** from the German descriptions. Each edit was made by Gemini 3 Flash (thinking low) and accepted only if the validator passed: `only_deletions()` requires every new sentence to be a case-sensitive subset of the old one, and no count may remain.
- **Upload path:** `upload_description_scrub.py` puts edits in a staging table behind a token-guarded anon INSERT policy. Promotion runs with an md5 guard on the current text.
- **Second pass:** in the count regex, a number has to end in a digit. That way ordinals like "die 8. Klassen" no longer count as student numbers. Four awkward leftovers ("zählt aktuell die Schülerinnen und Schüler") were fixed by hand.

**Results:**
- **Edits:**
  - pass 1: 2,950 field edits for 1,684 schools;
  - pass 2: 35 edits.
- **Remaining counts:** 33 of 2,824 German schools still mention a count in `description_de`/`_en`. These are rows the validator rejected, mostly "Mit nur etwa 110 Schülern in vier Klassen"-type sentences where the count is the subject.
- **Backup:** the pre-edit text for all 1,698 schools touched is in `_desc_backup_20260927` (RLS, no policies). The staging table has been dropped.
- **Legacy `description` column:** unchanged, and 1,723 German schools still have counts there. It is only the raw research input to the polish step, and neither app displays it.
- **NL** (7,689 schools, not in scope): 5,298 descriptions mention approximate student counts.
- **Pilot regeneration** with the new prompt (10 schools, not applied):
  - 8/10 came back grounded, with 7–17 sources each; 2 returned no sources;
  - no counts;
  - about $0.13 per school plus grounding searches (~$450–500 for all German schools);
  - 2 of the 7 known outdated facts came back even with sources.

  Regeneration alone does not fix stale facts.

## 2026-09-27 — Description audit: 1 in 3 schools has a materially wrong claim

**What:** a stratified 30-school sample (3 per German city) and 485 specific claims, each checked against school websites, register data and official portals. Contradictions were re-confirmed on raw page text; 40 random "verified" verdicts were spot-checked.

**Results:** 82% verified, 14% unsourced, 4% contradicted (2% minor, 2% material). 10 of 30 schools carry at least one materially wrong claim, mostly **outdated** facts (ended programmes, stale counts). Numbers are the weakest category (47% unsourced or wrong).

**Details:** `docs/audits/DESCRIPTION_AUDIT_2026-09.md` (+ claim-level JSON). The 10 material errors are not yet corrected. The side findings on city-scope pollution (Leipzig/Bremerhaven/Munich suburbs) and `plz = '0None'` await a decision.

## 2026-09-27 — Description provenance: grounding sources are now stored

**Why:** a 30-school audit of the AI-researched descriptions found many specific claims with no traceable source. Gemini's Google-Search grounding returns the sources behind each answer, but the research job discarded them.

**What:**
- New nullable `description_grounding jsonb` on `schools` and `primary_schools`: `{model, generated_at, queries[], sources[{title, uri, redirect_uri}], supports[{text, start, end, sources[], confidence[]}]}`. Hand-written texts use `{method: "manual", sources[...]}`.
- `research-school-descriptions` edge function (Lovable commit 82cb91d, 1.8 credits, deployed, not invoked) now stores it on success and clears it on failure/reset. The temporary `vertexaisearch…grounding-api-redirect` links are resolved to their final URLs (best effort, 5 s each), and all response parts are concatenated.
- `scripts_shared/enrichment/replicate_lovable_description_jobs.py`: `research()` returns `(text, grounding)` in the same shape, and the emitted SQL fills `description_grounding`.
- Luisen-Gymnasium (164501) got a manual record (its website pages + the NRW register).

**Observed while testing:**
- `gemini-3.1-pro-preview` returns sources and supported text segments.
- `gemini-3-flash-preview` (the fallback) often returns only `webSearchQueries` and no sources.
- A long description prompt can come back with no grounding at all. That happened for 100255 Gymnasium Heinzelmännchenweg: its description was written without any cited search result and should be treated as unverified.

A NULL grounding, or one without sources, therefore means the text is unsourced.

## 2026-09-26 (late) — Location data recomputed for two relocated NRW schools

**What:** new `scripts_nrw/processing/refresh_school_locations.py`. It runs the real NRW phases for the moved schools in a sandbox, reusing `add_new_schools.run_phases` with the website phase skipped, and overwrites only location columns: traffic, transit, POIs, crime and `bezirk`. Each POI/transit group is replaced as a whole, so slots from the old site cannot survive. The crime rank/category comes from the schools of the new Bezirk (ranks are uniform per Bezirk). All other columns and rows are asserted unchanged. `--reuse` applies a dry run's sandbox output without new API calls, and `--emit-sql` writes the Supabase UPDATEs plus a rollback snapshot.

**Applied (finals + Supabase):**
- **Luisen-Gymnasium Düsseldorf (164501):** Bezirk 1 → **Bezirk 3**; crime rank 11 "Stark belastet" → 8 "Belastet"; accidents nearby 183 → 80; transit stops within 1 km 0 → 74 (the old value was itself broken); nearest rail: Düsseldorf Völklinger Straße, 738 m.
- **Gesamtschule Ossendorf Köln (100212):** stays in Ehrenfeld (rank 9); new stops (39 within 1 km, nearest tram Alter Flughafen Butzweilerhof), accidents 24 → 30, POIs at Fitzmauricestr.

**Notes:**
- Overpass returned 504/429 for parts of the combined Düsseldorf–Köln bounding box. Every skipped tile was ≥1.7 km from both schools, and all reported stops are closer, so no nearer stop was missed. Run one city at a time next time.
- Fixed while doing this: all-NULL columns get a guessed `text` type, and `NULL::text` cannot be written into integer columns, so the emitter now writes bare `NULL`.
- Friedrich-Wilhelm-Gymnasium (moved ~200 m) was not recomputed.
- Luisen-Gymnasium's description was then regenerated (research + DE/EN + 768-dim embedding, with the register address passed as known data). The text now says the school moved to Völklinger Straße in 2026. The Düsseldorf `school_similarities` were rebuilt (same-city; 570 pairs). Rollback: `data_shared/supabase_sql/luisen_description_2026-09/rollback_snapshot.sql`.
- Follow-up (user request): trimmed to verified facts only. Each claim was checked against luisen-gymnasium.de (homepage + /schulprofil) and our register data; the texts were hand-written without generation and re-embedded. Removed:
  - history and numbers: 1837 founding / "girls' school", student and teacher counts, €106.5M cost
  - building details: "Clusterschule", 700-seat sports hall, cafeteria/auditorium, green roofs, laptop classes
  - unconfirmed partners and programmes: Symphoniker, Görres cooperation, "two-week internship in France"
  - "Unterbilk" (the new address may be in Hafen)
  Düsseldorf similarities rebuilt again. Lesson: generated descriptions carry plausible but unsourced specifics, so don't feed the research prompt hints (e.g. a Stadtteil) that we haven't verified ourselves.

## 2026-09-26 (late) — school_similarities rebuilt: same city (DE), country-wide (NL)

**Rule (user decision):** similar schools are matched **within the same city** for German schools, and **across the whole Netherlands** for Dutch schools. Dutch `city` values are municipalities, and 87 of them have a single school, so a same-city rule would leave those schools with no matches.

**What:** rebuilt in SQL transactions via the Lovable MCP, using the same method as the `compute-school-similarities` edge function: cosine over the 768-dim embeddings, top 10 per school, ranks 1–10. Only the candidate filter differs (`o.city = s.city` for DE, `o.city LIKE 'nl-%'` for NL).

**Why:** add the new Düsseldorf school 100255 to other schools' lists and restore the 10th entry lost when the Erkrath rows were deleted.

**Result:** 28,140 pairs. DE: 1,185 schools × 10, 0 cross-city pairs. NL: 1,629 × 10, none outside NL. 100255's top matches are all Düsseldorf schools (Elisabeth-Selbert-Gymnasium, Élise Freinet Gesamtschule, …).

**Edge function updated (Lovable commits 09a7577 + 5a95870, 2.2 credits, deployed, not invoked):** `compute-school-similarities` now applies the same rule (`COUNTRYWIDE_PREFIXES = ["nl"]`, otherwise same city; unknown prefixes default to same city) and returns a per-scope summary. It also had a latent bug: its single unranged select hit PostgREST's 1,000-row cap (`Content-Range: 0-999/2814`), so the admin button would have wiped the table and rebuilt only ~1,000 schools. The fetch now paginates, and a failed page throws before the delete.

## 2026-09-26 (evening) — Per-value vintage stamps + app year labels

**Why:** after the Berlin/Frankfurt advance, an audit of both apps showed the web app labelled teacher counts and the migration share with `data_school_year`. So 67 Berlin schools whose portraits had no 2025/26 teacher figure, and every Berlin migration share (still 2024/25), were shown as '25. Neither app had a "data as of" line, compare views showed no years, the web crime section hardcoded '23/'24, and a school without a student count showed "0".

**Data:** new nullable columns `lehrer_data_year` and `migration_data_year` in `schools` and `primary_schools` (added via the Lovable MCP SQL tool). Backfill:
- first in SQL (newest year column equal to the current value; otherwise NULL = unknown)
- then 606 Berlin 2025/26 teacher counts stamped `2025_26` from the finals, guarded on the live value matching.

Result: every city fully stamped except a handful of Munich rows with unknown vintage; Frankfurt has no teacher counts at all. Pipeline: `stable_fields` emits both stamps, the stable upload group carries them, and `emit_school_year_advance_sql.py` moves `lehrer_data_year` together with `lehrer_current` (branch `feature/vintage-stamps`).

**Web (Lovable agent, 16.9 credits, commit c3d9850):**
- teachers/migration labelled by their own stamps; ratio year only when student and teacher years match (tooltip explains mixed years)
- missing student counts stay undefined (hidden on cards, excluded from averages, filters and sorting)
- "Datenstand / Data as of" line in the detail modal; crime section uses `crime_total_crimes_current` with its real year and a previous-year change only when that exact column exists
- comparison tooltip and new values table show each school's year plus a "Werte aus unterschiedlichen Jahren" note
- MCP tools, /data-coverage, semantic-search and both description generators moved off `schueler_2024_25`/`lehrer_2024_25`

Tests (10), typecheck and preview build pass. `semantic-search` and the two description functions are deployed; the frontend and `mcp` go live on the next Lovable publish.

**iOS:** see the schoolnossa-mobile journal (branch `feature/data-vintage-labels`).

## 2026-09-26 — Wave B: Frankfurt live, NRW SJ 2026/27 deltas applied, Berlin 2025/26 via school portraits

**Approach:** new vintages are applied as deltas only — column-scoped refresh scripts on the finals plus targeted Supabase UPDATEs, with a pre-update snapshot kept as rollback SQL. No pipeline re-runs, no description/embedding regeneration.

**Frankfurt (B1) — Supabase:** applied the prepared SQL through the Lovable MCP. 64 `schools` + 82 `primary_schools` rows now read `data_school_year = 2025_26`. Remaining Frankfurt rows on 2024_25 (1 secondary, 15 primary) are the vocational/unmatched schools that keep their earlier values by design. Rollback: `data_shared/supabase_sql/frankfurt_school_year_2025_26/rollback_snapshot_2026-09-26.sql`.

**NRW (B2):** the ministry published the SJ 26/27 Schulsozialindex list on 2026-09-21, but under a **new filename** (`schulsozialindex_schulliste_sj_26_27.csv`, the old pattern stays 404) and in **cp1252** instead of cp850. The scraper now resolves the newest edition across both naming schemes and chooses the codec that yields real umlauts; the old parser would have mangled every name. Both editions are archived (git-tracked) in `data_nrw/cache/nrw_schulsozialindex_sj_{2025_26,2026_27}.csv`.

New `scripts_nrw/processing/refresh_nrw_master_delta.py` diffs `schuldaten.csv` against the previous snapshot (baseline now `data_nrw/cache/nrw_schuldaten_baseline.csv`), writes only the fields that changed at the source, sets `sozialindexstufe` from the newest list, asserts no other column moves, and emits Supabase UPDATEs. A re-run is a no-op. Delta found:
- Sozialindex: statewide only 16 of 4,151 schools changed; in scope 4 Köln schools founded recently got their first value (100218 → 1, 100231 → 2, 100233 → 6, 100235 → 8). Supabase has no Sozialindex column (it only feeds the Abitur regression), so this change is local only.
- Renames (7): e.g. Hermann Gmeiner → Frida Kahlo Schule (Düsseldorf), Gymnasium Bernburger Straße → Elisabeth-Selbert-Gymnasium, GE Weidenpesch → Ernst-Simons-Gesamtschule, Ursulinenschule; two Köln primaries went from Kath. to Gem. Grundschule.
- Relocations (3): Luisen-Gymnasium Düsseldorf (Bastionstr. → Völklinger Str. 122-124, ~2.4 km), Gesamtschule Ossendorf (Am Wassermann → Fitzmauricestr. 5, ~3.5 km), Friedrich-Wilhelm-Gymnasium Köln (~200 m). Their traffic/transit/crime/POI values still describe the old site.
- 2 website updates. Supabase: 13 rows updated and verified; rollback in `data_shared/supabase_sql/nrw_master_delta_2026-09/rollback_snapshot_2026-09-26.sql`.

**Berlin (B4):** `bildungsstatistik.berlin.de` retired `ListGen/SVZ_Fakt5` (404); its `/next/` successor is an Auth0-gated staff tool (all API routes 401), and a Wayback copy from 2026-07-14 shows it never published 2025/26. daten.berlin.de, Statistik BB and *Blickpunkt Schule* only have aggregates. The one public per-school source is the **bildung.berlin.de school portraits** (students as of 06.10.2025, staff 01.11.2025), which show the current year only — so they are scraped now and the HTML is kept in `data_berlin/cache/schulportrait_2026-09/` as the archive.
- New `scripts_berlin/scrapers/scrape_schulportrait_statistics.py` (resumable, 1 req/s, BSN verified on the tab heading; searches returning several hits are resolved by opening each) → `data_berlin/raw/bildungsstatistik_2025_26.csv` in the SVZ_Fakt5 shape: 785 schools, 707 with teachers. Teacher w/m left empty (portraits give only percentages).
- Plausibility vs 2024/25 (778 matched): students 446,055 → 447,008, teachers 37,805 → 37,201, median teacher ratio 1.04; every >40 % teacher jump is a school still growing (students grew too), so the portrait's "Lehrkräfte" matches the old definition.
- New `scripts_berlin/processing/refresh_bildungsstatistik_counts.py` wrote `schueler_2025_26` / `lehrer_2025_26` and re-derived the stable fields: 238/258 secondary and 451/490 primary rows now on 2025_26; the rest (mostly private schools that publish no 2025/26 figures) keep 2024/25. Re-run is a no-op.
- `emit_school_year_advance_sql.py` now also advances `lehrer_current` (COALESCE keeps the live value where the new year has none). Berlin SQL: `data_shared/supabase_sql/berlin_school_year_2025_26/` (238 schools + 451 primary) — **not applied yet**, pending approval.
- Side finding: for several private schools the finals' `schueler_2024_25` held web-researched values far from the official list (Private Kant-Schule Berlin International 66 vs official 846, Freie Schule Pankow 616 vs 117, Ev. Schule Köpenick 82 vs 611). The 2025/26 advance replaces them with official figures.

**Follow-up the same day (user decisions):**
- **Berlin → Supabase applied:** 238 `schools` + 451 `primary_schools` advanced to 2025_26 (`schueler_current`, `lehrer_current` via COALESCE, `data_school_year`). Live totals on 2025_26: 159,173 students secondary, 197,064 primary. Rollback: `data_shared/supabase_sql/berlin_school_year_2025_26/rollback_snapshot_2026-09-26.sql`.
- **Erkrath rows removed:** Gymnasium Hochdahl (165669) and Realschule Hochdahl (183246) deleted from Supabase (their 25 `school_similarities` pairs cascaded, so a few schools now list 9 similar schools until the next full similarity rebuild) and from the three NRW finals that held them. The full-row backup is in this session's tool-results JSON (copying it into `data_shared` was blocked by the permission classifier). Intermediate files still contain them; a fresh scrape drops them because their Ort is now Erkrath.
- **New school added — 100255 Gymnasium Heinzelmännchenweg** (Düsseldorf, Bezirk 2, operating since 2026-08-01 at an interim site, Schlüterstr. 18-20). New `scripts_nrw/processing/add_new_schools.py` runs the real NRW phases on a one-school input in a sandbox (module directory constants repointed; transit stop cache sandboxed; Anmeldezahlen skipped) and appends the aligned row to the city finals and the combined NRW parquet, asserting existing rows are unchanged. Two fixes found on the way: macOS `._*` files on the exFAT drive broke a glob, and the sandbox ranked `crime_safety_rank` 1-of-1 — district-level ranks are now taken from same-Bezirk peers (5). The register lists no website/phone and no Sozialindex yet; no official student counts exist for a school in its first year. Local `embedding` stays empty (finals use 3072-dim OpenAI; no OpenAI key configured).
  - Supabase: inserted (id `90280fc9-…`), then the Lovable admin jobs replicated for this one row — researched EN description (Google Search grounding), DE/EN polish, 768-dim Gemini embedding — and its top-10 `school_similarities` rows computed in SQL with pgvector (same cosine top-10 as the `compute-school-similarities` edge function). Nearest: Gymnasium Brügelmannstraße and Gymnasium Neustadt-Nord (two other new Köln schools), 0.87/0.85.
- **Not done by choice:** location re-enrichment of the relocated Luisen-Gymnasium and Gesamtschule Ossendorf (user: new schools only).

## 2026-09-26 — NL uploaded to Supabase (dark launch)

**What:** 1,629 secondary and 6,060 primary Dutch schools are live in `schools` / `primary_schools`, with descriptions, 768-d embeddings and similar-school ids. Invisible on the website — its city picker is hardcoded to the 10 German ids — until the NL navigation ships.

**Contract fixes found while mapping onto the live tables** (all at source, all verified before upload):
- 3,662 primary schools were "private". Dutch bijzonder schools are state-funded (Art. 23, no tuition); every DUO school is now public, `traegerschaft` 'Öffentlich'.
- Transit score was 0-10 and 93% saturated; now Berlin's own 0-100 formula (range 0-95, median 31/21 vs Berlin 60).
- Crime rank ranked school rows (to 5,858); now gemeenten (1-338), identical across both tables for all 284 shared gemeenten. Ameland, Schiermonnikoog, Vlieland and Rozendaal stay blank — CBS suppresses their figures.
- 2024/25 counts sat under `schueler_2023_24`; previous-year columns now shift with the vintage.

**Shape of the NL rows:** `schulnummer` 'NL-' + BRIN6; `city` 'nl-<gemeente>' — PostgREST caps responses at 1,000 rows (measured), so one national bucket would have truncated silently; largest gemeente is 196. New `geo_country/geo_region/geo_municipality` on both tables, filled for **all** rows (German backfill included). Adapter: `scripts_international/nl/processing/nl_prepare_supabase_upload.py`, validated against a snapshot of the live `information_schema` (the PostgREST OpenAPI root is service-role only).

**Write path:** no service-role key exists and anon is read-only, and ~115 MB cannot go through SQL tool calls. So: temporary staging tables with an insert-only anon policy restricted to `NL-` ids (no SELECT), `nl_upload_to_supabase.py` POSTs the rows, the policy is dropped (window ~3 min), counts verified exact (proving no stray inserts), one `INSERT...SELECT` per table, staging dropped. The promote call returned HTTP 499 but had committed — checked before retrying. SQL steps + rollback in `data_shared/supabase_sql/nl_upload_2026-09/`.

**App fix (Lovable, 1.5 credits):** MCP `list_cities` fetched raw rows under the 1,000-row cap, so it already miscounted (1,000 of 1,186) and NL would have pushed German cities out of it. Now `school_city_counts(p_category)` groups in the database; optional `country` filter; output shape unchanged. Migration `20260926081205`.

**Still open:** NL navigation (country -> region -> gemeente) and a multi-select track filter in the web app; the Lovable import whitelist lacks `geo_*`, so re-importing a German city through Admin -> Data Import would blank them; NL-only fields (inspectorate rating, TTO, denomination) have no live columns yet.

## 2026-09-25 — NL primary backfill complete; embeddings un-stringified

**What:** The scheduled daily backfill finished on 2026-09-21: all 6,060 primary descriptions grounded, embedded and matched. Its runs also fixed two real bugs (`nl_to_core_schema` read the description cache from a hardcoded `data_nl`, so a primary rebuild matched nothing; and the description pipeline wrote CSV only, so the finalizer dropped descriptions on the next rebuild).

**Why this entry:** the second fix introduced a new one. The pipeline began writing the parquet *from the frame it had read out of CSV*, and CSV cannot hold arrays — every 768-float embedding became a 12k-char string. 100% populated, all distinct, so invisible to the constant-column check, but unloadable into Supabase's `vector(768)`. The task also kept firing after completion (Sept 23, 24), re-stringifying the table each morning.

**Fixes:** the pipeline now merges only the columns it owns (`PIPELINE_OWNED_COLUMNS`) into the existing parquet instead of overwriting it wholesale; primary embeddings restored as `float32[768]` (6,060/6,060 parse cleanly, and `most_similar_school_01` matches a fresh recompute on 100% of rows, so the matches were never wrong); `check_constant_columns.py` now flags vector columns stored as text; the scheduled task is paused.

**Result:** both NL tables pass QA clean. Secondary 1,629 and primary 6,060 schools, all 262 Berlin columns, descriptions + embeddings + similar-schools 100%.

## 2026-09-19 — Wave B1: Frankfurt student counts on school year 2025/26

**What:** Hessen published Verzeichnis 6 edition 2026 on 2026-08-26 at `/sites/statistik.hessen.de/files/2026-08/verz-6_26.xlsx`, not at the expected `/2026-09/verz-6_26_0.xlsx`. It reports the survey of 1 Nov 2025, i.e. **SJ 2025/26**. It is now in the Frankfurt finals: `schueler_2025_26` added, `schueler_2024_25` replaced by the official edition-2025 figures, and `schueler_current` / `data_school_year` advanced to 2025_26 for **169 of 199 schools** (82 primary, 87 secondary). Row counts and embeddings unchanged.

**Why the old data needed more than a URL bump:**
- **The pinned URL rotted.** Hessen deleted `verz-6_25_0.xlsx` once edition 2026 appeared (404). `frankfurt_verz6_enrichment.get_verz6()` now reads the newest edition from `statistik.hessen.de/publikationen/verzeichnisse`, and every edition is archived in git as `data_frankfurt/cache/verz6_{EE}.xlsx`. The old `hessen_verz6.xlsx` is now `verz6_25.xlsx`; older editions exist only in that archive. The legacy master scraper uses the same resolver.
- **Year labels come from the file.** Counts go into `schueler_{year}` taken from the edition's sentence "Erhebung ... vom 01. November 2025". Before, the column name `schueler_2024_25` was hardcoded, so a re-run would have written 2025/26 numbers under the 2024/25 label.
- **The finals never had official counts.** Their `schueler_2024_25` equalled the web-researched `schueler_gesamt` (e.g. Adorno-Gymnasium 1300, Abendgymnasium 180) and matched Verz6 for only 4 schools.
- **Two wrong IDs from the April fuzzy match.** "Klingerschule" carries Kirchnerschule's number (3161) and "Monikahausschule" carries Bonifatiusschule's (3168); both are in different postcodes. `apply_verz6_counts` now requires the same postcode or name similarity ≥ 0.85, and skips vocational schools. Verz6 counts only general-education branches: Wilhelm-Merton-Schule would have gone 1600 → 103. All three keep their previous values.

**New tools:**
- `scripts_frankfurt/processing/refresh_verz6_student_counts.py` updates the finals without a full pipeline run, like `refresh_traffic_columns.py`. It asserts that only `schueler_*`, the stable fields and `data_school_year` change; a re-run changes nothing.
- `scripts_shared/emit_school_year_advance_sql.py`: `upload_to_supabase.py` only fills NULLs, so this emits UPDATEs that overwrite `schueler_current` + `data_school_year` only where Supabase holds an older or NULL year. Re-running it is a no-op. Reusable for NRW and Berlin.

**Supabase (pending, needs the Lovable MCP in an interactive session):** `data_shared/supabase_sql/frankfurt_school_year_2025_26/` has 64 `schools` + 82 `primary_schools` rows. 23 advanced secondary schools are local-only (the known Förderschulen/IGS drift) and are not in the SQL. The legacy `schueler_2024_25` column in Supabase keeps its web values, because the app reads `schueler_current`.

**Also:** one Frankfurt school closed between editions (4390 ASB Erasmus Frankfurter Stadtschule; it was never in our tables) and none opened. The `wave-b-school-data-check` scheduled task no longer probes Hessen and runs Mon + Thu until NRW lands (~Sept 23).

**Still open:** fix the two wrong schulnummern (changing them also changes the Supabase join key), Wave B2 NRW, B3 Leipzig, B4 Berlin.

## 2026-09-14 — Web time tracking counts active time only

**What:** Both web timers measured wall-clock time, so background tabs inflated session analytics (one session ≈23 h) and **consumed free-tier minutes**. Lovable (app commit `d596bb4`, 10.6 credits) added `src/lib/activeClock.ts` — counts only while the page is visible and within 5 min of the last input (5 vitest cases) — and used it in `SessionTrackingContext` (per-visit active `durationMs`, `wallMs` kept for reference, snapshot persists incl. the in-progress visit, no writes while hidden, `ended_at` = last active moment, `pagehide` flush) and `FreeTrialBlocker` (countdown + `record_usage` from active seconds). New column `user_sessions.tracking_version` (legacy rows = 1, new = 2); `src/lib/sessionDurations.ts` makes AdminAnalytics and `usage-report` cap only legacy visits at 30 min, with a note on the admin page that pre-14.09 sessions are estimated.

**Review notes:** diff reviewed; `blur` also pauses the clock (focus in an iframe or another window stops counting — acceptable). Frontend parts go live on the next Lovable publish (together with the 13.09 web error reporting); `usage-report` is already deployed.

**Found, not fixed (pre-existing):** anonymous sessions are never stored — the client inserts with `.select("id").single()`, and anon has no SELECT policy on `user_sessions`, so the RETURNING fails; 0 anonymous rows in May–Sept. Only matters for landing-page analytics since the app is behind sign-in.

## 2026-09-14 — Daily report fixes (tab minutes 1000× too high, pre-tracking counts)

**What:** The first report showed `dashboard/ki 1372641min`. `user_sessions.tab_durations` is in **milliseconds** (SessionTrackingContext `durationMs`) but was divided by 60 as seconds; the tracker also keeps counting while a tab sits open in the background (that session: 82,358,484 ms ≈ 23 h). `usage-report` now reports *engaged time* from `journey` step durations capped at 30 min per visit (fallback: `tab_durations` capped per tab) → `dashboard/ki 30min, dashboard/search 3.4min, …`. Counts now come from source tables (`auth.users`, `user_access.test_started_at` / `subscription_started_at`) instead of `usage_events`, which only exists since 2026-09-13 — "New paid (7d)" went 0 → 2 (subscriptions of 09-09/09-10). Subject says "logins" instead of "active"; a ⚠️ subject now has an explanatory banner at the top of the body. A forced run returns the text body. Lovable app commit `bef92c7`, 3.9 credits (alerting total 37.5).

**Why the earlier email looked broken besides that:** its ⚠️ and "4 failures" were artefacts of the 13.09 forced test (smoke-test events, since deleted; sweep heartbeat not yet written). The real 14.09 08:00 report had neither.

**Open:** the web session tracker itself still counts background time, so the admin analytics page overstates durations the same way — separate fix (pause on `visibilitychange`) if wanted.

## 2026-09-13 — Ops alerting: failure + usage emails (StoryTeller port)

**What:** Ported StoryTeller's `functions_alerts/` design to SchoolNossa across three repos; full reference in `docs/OPS_ALERTING.md`.
- **Backend (Lovable agent, 3 runs, 33.6 credits; app commits `7c808d7`, `8c7935d`, `e973ea1`):** tables `failure_events` / `failure_state` / `usage_events`, `claim_failure_alert()` (atomic 30-min per-fingerprint cooldown), usage triggers on `auth.users` / `user_access` / `app_feedback`, pg_net dispatch triggers, pg_cron (`alert-sweep` */15, `usage-report` 06:00+07:00 UTC gated to 08:00 Berlin, retention 03:30 on `expires_at`). New edge functions `client-errors` (public ingest), `alert-dispatch`, `alert-sweep`, `usage-report`; every existing function wrapped in `withFailureReporting`; explicit reports for OTP email, payments, RevenueCat (+ churn usage events). Web app: `errorReporting.ts`, `AppErrorBoundary`, global handlers.
- **Pipeline (this repo):** `scripts_shared/failure_reporter.py` + all 12 city orchestrators and the international template run `main` through `run_with_failure_reporting`. Crash / non-zero exit / orchestrator "Phase N failed" ERROR line → PERMANENT (instant email); success with stray ERROR lines → DEGRADED (daily report). Needed the log-line rule because not every orchestrator exits non-zero on a phase failure (Stuttgart exits 0). Opt out with `SCHOOLNOSSA_ALERTS=0`.
- **Mobile (`schoolnossa-mobile` branch `feature/error-reporting`, `ad4e1e0`):** `lib/core/error_reporting.dart`.

**Why:** No failure visibility anywhere (no Crashlytics/Sentry, edge functions only console-logged) and no usage signal besides the admin dashboard. Traffic is tiny (45 accounts, ~1 session/week), so each signup/trial/payment gets its own email and the report is daily, not 6-hourly; no zero-traffic canary (it became spam in StoryTeller).

**Review findings fixed before sign-off:** (1) an alerter self-failure would have looped forever (5xx in `alert-dispatch` → PERMANENT event → trigger → `alert-dispatch`…) — guarded in trigger and reporter; (2) browsers throw offline errors as `TypeError: Failed to fetch` / `Load failed`, which matched `code_bug` → every offline visitor would have emailed PERMANENT; (3) non-PERMANENT rows clogged the retry queue; (4) Python line numbers / hashed bundle names destabilised fingerprints; (5) heartbeat meta write race between report and sweep (now `merge_alert_heartbeat()`); (6) first-occurrence counter off by one (Lovable caught it in its own test).

**Results (verified live):** smoke PERMANENT → 202 + one email, 3 repeats → `count_since_notify=3` and no email; forced report delivered; pipeline phase-failure test emailed, DEGRADED not; `Failed to fetch` (web) and `SocketException` (mobile) → TRANSIENT, not emailed; web client can't escalate to PERMANENT; bogus component 400; internal functions 401 without secret; cron sweeps succeeding every 15 min. All 4 test emails landed in the Gmail inbox. Test rows cleaned. **Pending:** web changes go live on the next Lovable publish; mobile reporting ships with the next TestFlight build.

## 2026-09-10 (afternoon) — NL refresh, primary pipeline, and three cached-failure bugs

**What:** Acted on the answers to the open questions: refreshed DUO, brought primary (basisonderwijs) into scope, filled descriptions, added the private-school roster, and drafted the Scholen op de Kaart licence request. Branch `feature/nl-pipeline-improvements`.

**The recurring bug of this session — a failure cached in the shape of a success.** Three instances, all of which reported healthy coverage:
- **NL traffic** (morning): fell back to a coordinate-less source, wrote nothing, logged `[OK] OK`.
- **GB crime**: `data.police.uk` rate-limits hard; 8 unthrottled threads draw HTTP 429 for ~68% of requests. `except Exception: crimes = []` swallowed the 429 and **wrote the empty result to disk**. All 5,144 cached files were byte-identical `{"_total": 0}`, so no re-run could ever repair it, and `crime_data_source`/`_year` were stamped unconditionally — coverage read 100% with every value zero.
- **NL descriptions**: Pass 0/1 guarded on key *presence*, so a generation that failed and was stored as `""` read as cached success. 246 schools had a Dutch description and an empty English one, skipped in milliseconds by every re-run.

The lesson for QA: **check distinct values, not null rates.** All three passed a coverage check. `_cached_ok()` now guards on value, and the traffic and crime enrichers raise rather than write a constant.

**DUO refresh — we were a year *newer* than labelled, not staler.** "Leerlingen 2025" carries peildatum 1 oktober 2025, i.e. school year 2025/26, but the scraper labelled it 2024_25. Labels now derive from each file's own peildatum year and `build_urls()` probes forward for a newer vintage rather than pinning a filename (how Munich sat 19 months stale). NL is now `data_school_year` 2025_26, ahead of Berlin's 2024_25 — exactly what the stable-field stamps exist to express.

**Teacher counts were not teachers.** DUO's staff sheet counts all personnel — directie, support, trainees — at *instelling* level while students are per *vestiging*, so every location of a scholengemeenschap carried the institution's full headcount (Het Stedelijk: 498 at each of 6 locations) and `student_teacher_ratio` had a median of **2.3**. Now filtered to FUNCTIEGROEP "Onderwijsgevend personeel" and apportioned across locations by student share: median **10.7** headcount, 13.8 per FTE at the observed 0.774 average — consistent with the published national figure.

**Chain order preferred a stale paid file.** Demographics and the finalizer selected `nl_schools_with_pois.csv` (by column count — 81 POI columns beat everything), which is only refreshed for new schools and so is routinely older and shorter. That silently reverted each run to the previous row count. The free chain is now linear and POI is merged back by column instead.

**Primary (basisonderwijs) — new pipeline.** 6,060 regular primary schools (TYPE_PO=BO) with enrollment, apportioned teaching staff and contacts; ratio median 12.8 headcount ≈ 16.6 per FTE. SBO/SO/VSO excluded — separate systems with their own admission routes, not primary schools with a flag. Rather than duplicate seven enrichment scripts, `NL_DATA_DIR` selects the data root and `NL_SECTOR` the Inspectorate sector; location-keyed caches (BRON accidents, the 250MB GTFS feed, CBS tables) stay under `data_nl` so both levels share one download.

**Geocoding moved to PDOK Locatieserver** (official BAG-backed, no 1 req/s cap, returns RD New alongside WGS84 — which the traffic enricher works in natively). Nominatim stays as fallback. 1,629 secondary in ~5 minutes at 100%; it is what makes 6k primary schools tractable at all.

**Also:** private (particulier/B3) roster — 35 VO schools that have no BRIN and appear nowhere in DUO, added with stable synthetic ids (`NLPRIV_<slug>`) and city-centroid coordinates, flagged as such. `area_foreign_born_pct` derived from CBS herkomstland columns (the old matcher looked for "niet-westers", which CBS retired) and deliberately **not** mapped into `migration_background_pct`, which carries Berlin's pupil-level figure — an area statistic there would repeat the unscaled-crime category error. A `--limit` run used to write its sampled head back over the full final table; it now writes a sample file.

**POI moved to Overpass/OSM** (decided rather than spending ~$932 on Places for primary at the observed $0.154/school). `nl_poi_overpass_enrichment.py` emits the same 81 Berlin-schema POI columns, so the two enrichers are interchangeable per country. It pulls each category once nationwide (~10 queries, cached and shared between both school levels) and does the distance work locally against a 1km grid index — one query per school would have meant 6,060 calls against a donated service. The main endpoint 504s under national-scale queries, so it falls back to the kumi.systems mirror. The two *school* POI categories deliberately do not come from OSM: we already hold the authoritative DUO registry of every funded school with coordinates, which is more complete and better named than `amenity=school`, and OSM rarely distinguishes primary from secondary in NL — so that half is an improvement on what Places would have returned, not a downgrade.

**Descriptions, embeddings, similar schools (secondary).** description 83% -> 99.6%, description_local 100%; 1,629/1,629 embeddings at 768 dims. Three more German-name assumptions surfaced on the way, each silently dropping international data: `compute_similar()` required a `schulnummer` column, so it built the whole cosine matrix for NL and then discarded it (embeddings present, every `most_similar_school_*` null, nothing reported); the embedding-text fallback knew only `schulname`/`stadt`, so a description-less international row would have embedded an empty string. And once similar-schools worked, **66% of top matches were another campus of the same school** — NL registers per vestiging, so a scholengemeenschap repeats with one shared name and near-identical descriptions. Suppressing same-BRIN candidates takes that to 0/1,629. Note this one was invisible to the constant-column check: the column was 100% populated with 1,629 distinct values, and only domain knowledge exposes it.

**POI self-match.** The constant-column check earned its keep: `poi_primary_school_01_distance_m` was 0.0 for all 6,060 primary schools, because each school sits in its own school-category catalog and is its own nearest neighbour — which also inflated every within-500m school count by one. Registry-derived catalogs now carry the school id and exclude the row being scored.

**Final state:** secondary 1,629 x 245 cols (coords/traffic/POI 100%, descriptions 99.6%, embeddings + similar schools 100%, students 91%, teachers 88%, deprivation 89%, inspectorate 38.6%); primary 6,060 x 245 (coords/students/traffic/POI 100%, teachers 99.8%, deprivation 100%, inspectorate 21.7%). Both produce all 262 Berlin columns and pass the constant-column check clean. POI provenance is stamped per row: 1,614 secondary rows keep their paid Google Places data, 15 + all 6,060 primary carry Overpass.

**Still open for NL:** embeddings + similar-schools (needs the Supabase table decision, not just a pipeline run); CBS achterstandsscore for PO (the VO edition URLs would not join); tuition for the 35 private schools; enrollment pressure (Amsterdam OSVO, PDF-only); Technasium/Cultuurprofiel/Topsport lists (two need a headless browser, ~85 schools between them).

## 2026-09-10 — NL retrospective fixes + international schema alignment

**What:** Audited the April NL pipeline before extending it, fixed what the audit found, aligned the international schema with the August stable-fields refresh, and filled four gaps from free national sources. Branch `feature/nl-pipeline-improvements`. No paid API calls (POI/descriptions/embeddings untouched; verified by the merge-update path).

**Why:** The Sept-2026 expansion re-evaluation put "finish NL" first. Before adding fields it was worth checking what the April run actually produced — three of the four things fixed here were silently wrong, not missing.

**Schema drift (the thing that would have broken the UI):** the Wave A refresh added 9 year-agnostic stable columns (`schueler_current`, `data_school_year`, `crime_*_current`, …) and wired them into all 9 German mappers plus `upload_to_supabase.py`, but not `scripts_international/international_to_berlin_schema.py`. NL and GB shipped **none** of them — 25 Berlin columns missing from both Berlin-schema outputs. Fixed, plus three semantic bugs found on the way:
- **Crime units.** Berlin's `crime_*` columns are Häufigkeitszahlen (per 100k); the core schema carries per-1,000. NL rates landed unscaled in `crime_total_crimes_avg`, so NL read ~200× safer than Berlin on the same axis (median 44.8 vs 9,578). Now scaled ×100 → median 4,480.
- **Crime vocabulary.** NL emitted `high` where the German tertiles emit `elevated`; the UI filters on the literal. Fixed at source (`nl_crime_enrichment`) and defensively in the mapper.
- **`traffic_accidents_1000m` → `plz_observation_count`** was mapping accident counts into the Telraam sensor-observation block (which is also CC-BY-NC). Dropped.
- **Vintage laundering.** `CORE_TO_BERLIN_MAP` pins `students_current` → `schueler_2024_25`. That is right only while the country's data *is* 2024/25 — after a DUO refresh the value would be mislabelled, or dropped entirely if the Berlin reference had no column for the new year. Vintage now travels with the data (`students_data_year`, resolved from the newest `students_<year>` column DUO actually delivered) and the stable fields carry the value regardless.

**Traffic was a silent no-op (0/1626).** `nl_traffic_enrichment` downloaded the BRON flat CSV, which links accidents to NWB road segments and has no coordinates, so it fell back to gemeente aggregation — and the April log shows `No accident data available` while Phase 3 still reported `[OK] OK (0.7s)`. It also never wrote the core `traffic_accidents_*` columns, so even a good run couldn't have reached the final table. Rewritten against the Rijkswaterstaat **WFS geocoded release** (`ongevallen_2022_2024`, EPSG:28992, CC0): 382,421 accidents, all geocoded, per-school 500m/1000m counts via a 1km grid index (Unfallatlas/STATS19 semantics). **0% → 100%**, median 183 accidents within 1km. Now raises instead of reporting success on an empty result.

**New free enrichments:**
- `nl_quality_ses_enrichment` — Onderwijsinspectie oordelen (CC0, peildatum 1-9-2026) → `school_quality_rating` **0% → 38.6%**. Ratings are per *afdeling* (HAVO/VWO/VMBOGT separately), so aggregated worst-case; only 666 of 1,591 VO vestigingen carry a real oordeel nationally (~42% ceiling), the rest are `Geen eindoordeel` and stay NULL rather than reading as a negative. Assessment date shipped alongside — some are up to 10 years old. Plus CBS achterstandsscore (peildatum 1-10-2024) → `deprivation_index_national` **0% → 89.2%**; uses Tabel 2 *zonder drempel* where present because Tabel 1 zeroes 43% of schools at the funding threshold and can't be ranked. Read via `calamine` — openpyxl rejects CBS's `styles.xml` as invalid XML.
- `nl_profiles_enrichment` — Nuffic TTO register + Amsterdam Schoolwijzer JSON:API. Schoolwijzer supplies **school email addresses, which DUO publishes nowhere** (0.1% → 4.3%), plus phone/website gap-fill and municipality-verified profile flags. TTO has no BRIN, so matching is by website domain — and a naive domain join over-tagged badly (184 flags from 126 entries: one scholengemeenschap domain maps to 12 vestigingen including VBO-only and cluster-4 sites that cannot host tvwo/thavo). Constrained to require the vestiging to teach a level the TTO entry names, with city agreement for ambiguous multi-location domains → **129 flags, 0 level violations**.
- `nl_school_board` 0% → 100% (was already in the intermediate, never mapped).

**Tooling:** `nl_merge_enrichment_update.py` merges free-layer refreshes into the final table on `school_id` instead of re-running the pipeline (phase 6 POI is ~$250, phase 8 is LLM). `deprivation_index` coalesces rather than overwrites — the new school-level CBS score is the better analogue of `belastungsstufe` but covers 89% against the area-level 99.8%, so blind replacement would have traded 10 points of coverage for the better definition. New orchestrator phases 7.2 / 7.3 / 10 (CLI now parses float phase ids), so the whole free refresh is `--phases 3,7.2,7.3,10`.

**Result:** NL final 234 → 242 columns, Berlin-schema output has **0 missing Berlin columns (was 25)**. `schueler_current` 92.7%, `lehrer_current` 93.4%, `data_school_year` 2024_25, `crime_total_crimes_current` 99.8% @ 2025 (matching Berlin's crime vintage), traffic 100%, `belastungsstufe` 100%.

**Still open for NL:** DUO vintage is a year stale — enrollment 2025 (peildatum 1-10-2025) and the 1-9-2026 vestigingen file are out, and staff runs to 2025; NL currently sits on 2024/25, which happens to match Berlin's own student vintage, so this pairs naturally with Wave B. Also: `description` 84% (256 schools) and `summary_en` 0%; embeddings + similar-schools (Berlin does this in-DB via pgvector — needs a Supabase decision, not just a pipeline run); tuition for the ~33 B3 particulier VO schools, which are **not in DUO at all** and would need Munich-private-style inserts; enrollment pressure (Amsterdam OSVO publishes per-school capacity vs first-choice, but PDF-only); `migration_background_pct` 0% (the CBS kerncijfers fetch returns no migration column); Technasium list is JS-rendered and needs Playwright; Scholen op de kaart remains licensing-gated. GB has the same 25-column fix applied but its own data holes (students 25%, teachers 3%, Ofsted 0%) and a suspicious `crime_safety_category` that is 'moderate' for all 5,303 rows.


## 2026-08-23 — Stuttgart PLZ fix (plz == schulnummer digits since April)

**What:** Every Stuttgart row (81 secondary / 95 primary, locally and in Supabase) carried the stuttgart.de directory entry id as `plz` (STG-17965 -> plz 17965; 6-digit ids truncated, e.g. STG-310163 -> 31016). Root cause: `scripts_stuttgart/scrapers/stuttgart_school_scraper.py::scrape_detail_page` took the first 5-digit number on the detail page (`re.search(r'(\d{5})\s*(Stuttgart)?', html)`) — that is the id in the `data-content` JSON, not the PLZ. The wrong PLZ was baked into the templated `description` ("Adresse: ..., 19965 Stuttgart") and 36 LLM `description_de` texts.

**Fix:** new `extract_address_from_html()` reads the PLZ from the page's schema.org JSON-LD (`CityovSerializer-<id>` -> `address.postalCode`), falls back to the "Anschrift" box, then to the first `7[01]xxx Stuttgart` before the footer (footer = Rathaus 70173); `is_valid_plz()` rejects anything that is not `^7[01]\d{3}$` or equals the id. `repair_cached_plz()` re-fetches only the PLZ for cached entries with an invalid plz, so the 7-day cache self-heals (258/258 repaired). Side fix in `stuttgart_crime_enrichment.py`: the scraped Stadtbezirk (`ortsteil`) is now preferred over the approximate PLZ->Bezirk table (with real PLZ the table would have moved 14 secondary schools into the wrong Bezirk; crime columns are unchanged vs. before).

**Regeneration (free chain only):** scraper (cache) -> traffic -> transit -> crime -> combiner (merge-back kept all paid columns) -> `scripts_stuttgart/processing/stuttgart_plz_repair.py` (exact per-row `'<old> Stuttgart' -> '<new> Stuttgart'` replacement in description/description_de, writes master_table.*, *_final_with_embeddings.parquet, *_final.csv; no re-embedding, no LLM) -> `stuttgart_to_berlin_schema.py`. Backup of the previous finals/raw/intermediates/cache in `data_stuttgart/final/backup_2026-08-23/`. Result: 176/176 rows corrected, all plz 70xxx (70173–70619), no column lost coverage; the *_final/berlin_schema column set changed only in all-NULL placeholder columns because the Berlin reference parquets were rebuilt after the Wave A Stuttgart run.

**Supabase:** `data_shared/supabase_sql/stuttgart_plz_fix_2026-08-23.sql` — 176 idempotent UPDATEs (schools 81, primary_schools 95), guarded on the old plz (verified read-only: 176/176 guards match), fixing `plz` and `replace()`-ing the old PLZ in description / description_de / description_en. Not executed (Lovable MCP auth). Note `insert_new_schools_2026-08-23.sql` still carries plz '19965' for STG-19965 (already present in Supabase, so the INSERT is a no-op).

## 2026-08-24 (afternoon) — All four follow-ups executed

1. **Lovable app adopts the stable fields** (agent commit `7465884`, 6.8 credits, typecheck+build green): both import whitelists carry the 9 stable columns; `useSchools` prefers `*_current` with vintage-stamp year labels (year-config chain kept as fallback); PinnedSchoolsContext, SchoolCard and SchoolDetailsModal (demand + crime) updated. Follow-up fix sent: Google retired `gemini-3-pro-preview`/`gemini-2.5-pro` — research + tuition edge functions moved to `gemini-3.1-pro-preview` (their admin buttons would have 404'd).
2. **Description/embedding backlog cleared (337 rows, all cities)** via `scripts_shared/enrichment/replicate_lovable_description_jobs.py` — a faithful local replica of the three admin jobs (same prompts/models/params; research via `gemini-3.1-pro-preview` + Google Search for the 51 description-less rows incl. all 48 MUCPRIV, DE/EN via `gemini-3-flash-preview`, embeddings `gemini-embedding-001` 768-d). Writes went through a temporary IS-NULL-gated anon UPDATE policy (April pattern; created and dropped in the same session; 755 guarded column-writes, 0 failures). **Both tables now have 0 rows missing description / description_de / description_en / embedding.** `school_similarities` rebuilt in-database (pgvector, per-city batches): 11,860 pairs, 1,186/1,186 schools, sim 0.73–1.0.
3. **Berlin transit refreshed live** — BVG API returned; both enrichers fetched fresh data (258/258 secondary and 489/490 primary non-zero summaries, incl. 04A44 and 6 schools that never had transit), finals rebuilt, embeddings/descriptions untouched. Supabase NULL gaps filled (8 rows); April values elsewhere left as-is (a full overwrite refresh remains a separate decision).
4. **Wave B**: both source files still 404 (Hessen `verz-6_26_0.xlsx`, NRW `schulliste_sj_26_27_open_data.csv`) — on schedule for ~Sept 9 / ~Sept 23.

Known emitter sharp edge (minor): `--emit-sql` with `assume_missing` also emits columns that legitimately don't exist on a table (e.g. `transit_all_lines_1000m` on primary_schools) — discard those files; the dry-run is authoritative.

## 2026-08-24 — Supabase handoff COMPLETE (applied via Lovable MCP SQL tool)

All pending SQL applied from this session through `mcp__lovable__query_database` (project 01ef7680, DB whzvzoumldeqgyrqlilt), everything idempotent, one transient 499 retried:
- 7 remaining stable-fill files (612 rows; `ALTER TABLE primary_schools ADD COLUMN nachfrage_prozent_current numeric` was missing from the original DDL — added, and `supabase_stable_fields.sql` updated to match).
- Stuttgart PLZ fix (176 rows compressed into 2 set-based UPDATEs): 81+95 rows now all 70xxx, 0 descriptions with the old bogus PLZ.
- 48 Munich private INSERTs + 21 non-Munich DELETEs (both blocks incl. Garching/Kirchheim) + Munich metadata fill (136 rows: leitung, gruendungsjahr, schueler/lehrer, sprachen, traegerschaft, besonderheiten).
**Final state**: `schools` 1,186 rows / `primary_schools` 1,639; Munich 129/154 matching the local finals row-for-row; `upload_to_supabase.py --groups stable --dry-run` reports **0 remaining fills**. Stable coverage: sec schueler_current 901, lehrer_current 640; pri schueler_current 1,303, nachfrage_prozent_current 85.
**Still open (Lovable-side)**: add stable columns to the import whitelist + adopt in useYearConfig/useSchools (doc §3); run description research + embeddings for the 48 new Munich private rows (they have base data only); Berlin BVG re-run when the API returns; Wave B ~Sept 9/23.

## 2026-08-23 (afternoon) — Munich cleanup, Berlin transit restore, Supabase handoff applied

**Evening follow-up — Wave A regression repair (Dresden/Munich/Hamburg):** the code study showed several "Lovable-side" fields were really April pipeline uploads whose local columns the Wave A rebuild lost. All three restored locally, verified against live Supabase, embeddings/descriptions untouched:
- **Dresden**: `dresden_to_berlin_schema._derive_school_types()` — schulart = Saxon Schulart (school_type_name / key 11-14), school_type = Grundschule / cross-level label / Förderschule→Gemeinschaftsschule / else schulart. 165/165 rows match Supabase exactly.
- **Munich**: combiner `_merge_side_layers()` pulls the Places-contact pass, `munich_traegerschaft_recovery.csv` and the website-metadata output (fill-gaps on schulnummer); telefon/website/traegerschaft 0 → 81/78/82 (sec) and 105/104/130 (pri). Contact columns added to the shared merge-back carry list. Bonus: the Apr-28 recovery layer was never uploaded — **136 Munich rows of fill-gaps now available** (`data_shared/supabase_sql/munich_metadata_fill_2026-08-23/`, leitung +75, gruendungsjahr +55, schueler +48, lehrer +46, traegerschaft +48, sprachen +41, besonderheiten +26).
- **Hamburg**: both combiners merge `hamburg_{secondary,primary}_schools_with_metadata.csv` fill-gaps (the mapper's `hamburg_schools_with_website_data.csv` reference was a dead path); mappers guard against renames onto existing columns. besonderheiten/gruendungsjahr/lehrer/description_de restored to the exact Supabase counts (161/124/95/169 sec, 238/125/104/255 pri).
- Remaining stable fill regenerated: now **612 rows** (`stable_2026-08-23/`, 7 files) — grew because Hamburg's restored lehrer_2024_25 feeds lehrer_current (+408).


**Munich:** (1) `munich_school_master_scraper.filter_munich_schools` matched 'München' as a *substring* of city/name → 21 non-Munich rows reached Supabase (Waldmünchen ×3, Schwabmünchen ×4, Ostermünchen ×2, Münchenbernsdorf/Thüringen, Ingolstadt 'Münchener Straße', Grafing ×2, plus the Landkreis suburbs Garching ×4 and Kirchheim ×4). Now exact municipality + PLZ 80/81, never by name; `munich_data_combiner._drop_non_munich_rows` guards the intermediates. Finals re-run (phases 7–9, embeddings skipped): secondary 139→129, primary 165→154, paid columns intact. (2) The 48 researched private schools (`MUCPRIV_*`) were verified **not** present in Supabase under any other id (name / street+PLZ / <120 m match against all 256 Munich rows — all of which are public `BY-SCHUL_*` entries). `phase4_ingest_to_pipeline.school_type_label` now emits Munich's own vocabulary (Gymnasien 15 / Realschulen 8 / Waldorfschule 2 / Internationale Schule 3 / Montessorischule 2 / Grundschulen 17; only Christophorus-Schule stays 'Sekundarschule') instead of the generic placeholder. SQL: `data_shared/supabase_sql/insert_munich_private_2026-08-23.sql` (48) and `delete_non_munich_2026-08-23.sql` (13 clearly non-Munich + 8 Landkreis suburbs as a separate block). Side note: Supabase Munich private rows carry no descriptions/POI/embeddings yet.

**Stuttgart PLZ** (separate entry above): root cause in the scraper's first-5-digit regex; 176 guarded UPDATEs in `stuttgart_plz_fix_2026-08-23.sql`; `insert_new_schools_2026-08-23.sql` regenerated so STG-19965 carries 70173.

**Berlin transit:** BVG API still refusing connections. Both enrichers (`scripts_berlin/enrichment/enrich_schools_with_transit.py`, `scripts_berlin_primary/enrichment/enrich_grundschulen_with_transit.py`) now retry, trip a circuit breaker after 10 consecutive failures, keep previous values per schulnummer (new per-school caches `data_berlin*/cache/bvg_transit_cache.csv`, seeded from the last good April run, git f685437), and never write a 0/0 summary; `rebuild_final_table.blank_failed_transit_summary()` keeps an existing 0/0 from shipping or gap-filling. Real run today: secondary 252/258 and primary 489/490 rows got their April transit summaries back (identical to Supabase), everything else byte-identical. When the API is back, the same three commands refresh live.

**Supabase (applied by the interactive session with the Lovable MCP, observed live):** stable-fields DDL ran (9 + 6 columns), the 8 new schools are inserted, the stable fill was in progress at 16:40 (done: Berlin, Bremen, Dresden, Düsseldorf, Frankfurt, Hamburg, Köln `schools` + Berlin/Bremen/Dresden/Düsseldorf `primary_schools`; `stable_2026-08-23/` regenerated to the 11 remaining files). Still to run: Munich private INSERTs, non-Munich DELETEs, Stuttgart PLZ fix.

**Lovable "data admin" processing — now studied from the code** (Lovable project synced to GitHub as `jvr1980/schoolnossa-2d5310de` on 2026-08-23; full write-up in `docs/LOVABLE_DATA_ADMIN_PROCESSING.md`: import = full replace per city with a column whitelist + post-import SQL, year config in `app_settings.year_config_*`, seven enrichment jobs, origin of every "extra" Supabase field, data contract, and the Lovable-side change set needed to adopt the stable fields). Earlier data-side inference for the record: Columns/values that exist in Supabase but were never produced by any local file, with their run timestamps: English translations `description_en` (Berlin 259, Dresden, Düsseldorf, Frankfurt, Hamburg, Köln, Stuttgart; primaries too), `admission_*_en` (345–890 rows), `open_days_en` (151); German descriptions where the pipeline had none (`description_de` Hamburg 170/257, Frankfurt 73/100) with `description_researched_at` (Feb–Apr 2026); granular tuition research (`tuition_granular_*`, `tuition_income_matrix`, `tuition_sibling_discounts`, `tuition_tier`+`_reasoning`, `income_based_tuition`, `scholarship_available`, fee columns; `tuition_granular_generated_at` Feb–Apr, ~250 rows); environment fetches `env_aqi_*` (142 schools / 726 primary, `env_aqi_fetched_at`) and `env_pollen_*` (491 primary, Feb); metadata/contact research (Munich `telefon` 91, `website` 88, `traegerschaft` 79 — jedeschule has none; Hamburg `besonderheiten` 161/238, `gruendungsjahr` 124/125, `lehrer_2024_25` 95/104; Frankfurt primary `gruendungsjahr`, `lehrer`, `nachfrage_*`); type normalisation `school_type`/`schulart` for Dresden (75/90) and Bremen; and the abitur predictions (`abitur_*_estimated*`, from this repo's `apply_abitur_predictions.sql`). Berlin also keeps `crime_*_2023` and the Telraam `plz_*` traffic columns (200 rows) that the Wave A Berlin rebuild no longer produces locally. Proper study of the code needs the Lovable MCP (file reading) or a GitHub sync of the app.

## 2026-08-23 — Stable-Fields Upload Prep + New-School INSERTs (blocked on Lovable auth)

**What:** Prepared everything for the Supabase side of the Wave A refresh so it is a handful of Lovable MCP SQL-tool calls: the stable-fields DDL, the stable fill as generated SQL, and the INSERTs for the schools the refresh found. Found and fixed three real bugs on the way. The Lovable MCP server was in "Needs authentication" state in this (non-interactive) session and no service-role key exists locally, so **nothing was written to Supabase yet**.

**Why:** Lovable Cloud owns the DB: the anon key is read-only, DDL and writes have to go through Lovable (its MCP SQL tool). The April backfill needed temporary RLS UPDATE policies; emitting the fill as idempotent SQL avoids any policy window entirely.

**Bugs found & fixed:**
- `scripts_shared/schema/stable_fields.py` built the vintage stamps with `pd.NA`; `upload_to_supabase._is_empty()` only recognised float NaN, so `_coerce()` str()'d it to `'<NA>'` — **950 cells would have been written as the literal text `'<NA>'`** (REST and SQL paths alike). Root cause fixed (plain None) + `_is_empty()` now treats any scalar NA as empty. After the fix `abitur_year` (+196) = `abitur_durchschnitt_current`, `crime_data_year` (+2131) = `crime_total_crimes_current`.
- `scripts_bremen/bremen_to_berlin_schema.py` never mapped `schulform` → `schulart`/`school_type`, so `_bucket()` saw NaN, all 254 rows went to the "secondary" split and the primary split file was stale from April 19. Now `schulart = schulform`, `school_type` bucketed like the live Bremen rows (Grundschule / Gymnasium / else Oberschule) → 114 primary + 140 secondary. Also: Berlin's reference schema moved to `crime_*_2024/2025`, so Bremen's 2023-vintage `crime_*_2023` columns were silently dropped from the output (and `crime_total_crimes_current` could not be derived) — now kept as extras (93 primary / 114 secondary rows restored).
- Berlin transit: **every** Berlin row in the local finals has `transit_accessibility_score = 0` and `transit_stop_count_1000m = 0` (258/258 secondary, 489/490 primary) — the BVG API was down during Wave A and the summary was recomputed to zero while the nearest-stop columns were carried. Supabase is untouched (fill-gaps), but do **not** run `--groups transit_summary` for Berlin until the transit re-run; the insert script drops a 0/0 transit summary as a failure marker.

**New tooling:**
- `upload_to_supabase.py --emit-sql DIR` — same planning as `--dry-run`, but writes chunked `UPDATE t AS s SET col = COALESCE(s.col, v.col) FROM (VALUES ...)` statements with explicit casts (types inferred from live rows, DDL types for the still-missing stable columns). Output: `data_shared/supabase_sql/stable_2026-08-23/` — 24 files, 353 KB, 2,694 rows (`schueler_current` +2211, `lehrer_current` +1417, `crime_total_crimes_current` +2131, `migration_current` +244, `nachfrage_prozent_current` +231, `abitur_durchschnitt_current` +196, `data_school_year` +2174, `abitur_year` +196, `crime_data_year` +2131). Structurally validated (tuple/field counts, casts, quoting) and spot-checked against local data + Supabase ids.
- `scripts_shared/insert_new_schools_to_supabase.py` — local rows whose (city, schulnummer) is absent from Supabase; list / `--emit-sql` (idempotent `INSERT ... SELECT ... WHERE NOT EXISTS`) / `--apply` (REST POST). Hamburg reads the Berlin-schema output (the `_final.csv` has no plz/strasse/email). `--assume-stable-cols` for SQL meant to run right after the DDL.

**New schools found by the refresh (8 rows / 7 schools) → `data_shared/supabase_sql/insert_new_schools_2026-08-23.sql`:** Berlin `04A44` Willkommensschule TXL (ISS); Hamburg `5605-0` Schule Leuschnerstraße (Stadtteilschule), `7747-0` Academy of LIFE (schools + primary_schools, like the 30 other Hamburg Grund- und Stadtteilschulen), `5485-0` Grundschule am Schilfufer (opens 2027); Stuttgart `STG-19965` Kolping-Kolleg (adult Gymnasium — comparable Abendgymnasien/Kollegs already exist in Supabase); Bremen `449` Neue Oberschule Osterholz (schools), `138` Schule an der Luxemburger Straße (primary_schools; Bildungsgang `G`).

**Pre-existing local↔Supabase drift surfaced by the same diff (NOT inserted — decide separately):** Munich 31 + 17 `MUCPRIV_*` researched private schools (commit 301fb50; synthetic ids, never uploaded); Frankfurt 34 local-only (Förderschulen/Berufsschulen/Grundschulen in the secondary file — April curation left them out) + 8 Supabase-only ids; Düsseldorf 3 (Waldorf + 2 international schools, ids 187410/990001/990002); Berlin `09S06`, `12A44`, `04P41` and Bremen `513` exist only in Supabase (closed locally). `python3 scripts_shared/insert_new_schools_to_supabase.py` lists all of them.

**Also noticed:** Stuttgart `plz` equals the digits of `schulnummer` for 71/81 secondary rows (e.g. `STG-17965` → plz `17965`; Stuttgart is 70xxx) — already in Supabase and baked into LLM descriptions; needs a separate fix.

**Next session (needs Lovable MCP re-auth: `/mcp` in an interactive `claude`, or `claude mcp add --transport http lovable https://mcp.lovable.dev`):**
1. Run `scripts_shared/schema/supabase_stable_fields.sql` via the Lovable SQL tool.
2. `python3 scripts_shared/upload_to_supabase.py --groups stable --dry-run` (now passes the ghost-column guard; expect the numbers above).
3. Run `data_shared/supabase_sql/insert_new_schools_2026-08-23.sql` (8 statements), then the 24 files in `data_shared/supabase_sql/stable_2026-08-23/` via the SQL tool — or, if a service-role key / RLS policy is available instead, `upload_to_supabase.py --groups stable` and `insert_new_schools_to_supabase.py --ids ... --apply`.
4. Re-run `--groups stable --dry-run` to confirm 0 remaining fills.

## 2026-08-22 — Wave A Free Data Refresh + Stable Fields Migration

**What:** Executed the zero-API-cost half of the August source audit (`docs/DATA_REFRESH_PLAN_2026.md`): refreshed base data for Berlin (SJ 2026/27), Munich, Hamburg, Dresden, Stuttgart, Bremen; Unfallatlas 2025 everywhere; crime 2025 (Kriminalitätsatlas 2016-2025, Hamburg Stadtteilatlas 2025, BKA PKS 2025 T01 for Munich/Stuttgart); introduced stable year-agnostic fields so the Lovable app stops breaking on yearly refreshes. Zero paid API calls (verified by log grep + env-stripped runs).

**Why:** Asset was uniformly on SJ 2024/25; Munich's scraper was pinned to a Jan-2025 jedeschule snapshot (19 months stale); Berlin's portal already serves 2026/27. NRW/Hessen/Leipzig deliberately deferred to Wave B (their annual releases land ~Sept).

**Refresh-safety infrastructure (the bulk of the work):**
- `scripts_shared/processing/merge_enriched_columns.py` — merge-back of paid columns (POI/descriptions/embeddings/tuition/env/admission + LLM-scraped year stats) from the previous final parquet on `schulnummer`; wired into Munich/Stuttgart/Dresden combiners. Handles mixed-dtype normalization (pyarrow write failures).
- Embedding generators no longer null paid vectors on keyless runs (Hamburg restore-from-parquet, Munich parquet-input preference, Stuttgart preserve-carried).
- Berlin secondary path unification (scrapers wrote to CWD, combiner read scripts_berlin/processing/, manifest said data_berlin/raw/ — none existed); both Berlin orchestrators shell-quoted (space in volume name broke every subprocess with exit 2); new `convert_crime_xlsx_to_csv.py` (the missing Kriminalitätsatlas step — semantics reverse-engineered to byte-match the old parquet: HZ sheets, violent = Raub+Straßenraub+KV+gef.KV, rank tertiles safe/moderate/elevated).
- Munich orchestrator phase 7 re-ingests the 48 researched private schools (a base refresh silently dropped 31 secondary rows — caught and restored same-day).
- Berlin scrapers resolve the newest Schuljahr from the live dropdown (was pinned to value 16 = 2025/26; now picks 17 = 2026/27) and ISS scraper relabels year columns from the site's own headers (anti-vintage-laundering).
- `scripts_shared/processing/refresh_traffic_columns.py` — surgical Unfallatlas update for unchanged-base cities (Frankfurt/Leipzig/NRW), overwrites traffic_* in finals on schulnummer.

**Stable fields (additive):** `scripts_shared/schema/stable_fields.py` derives `schueler_current`, `lehrer_current`, `migration_current`, `nachfrage_prozent_current`, `abitur_durchschnitt_current`, `crime_total_crimes_current` + vintage stamps (`data_school_year`, `abitur_year`, `crime_data_year`), newest-first fallback chains. Wired into all 9 schema mappers + `upload_to_supabase.py` (new `stable` field group, ghost-column guard that aborts instead of silently skipping). Supabase DDL prepared in `scripts_shared/schema/supabase_stable_fields.sql` — **pending execution via Lovable** (Cloud-managed), then `--groups stable` upload.

**Results (before → after, POI/embedding preservation verified per table):**
- Berlin primary: 257 → **490 rows** on SJ 2026/27 base; local parquet had drifted to a stale 257-row subset while Supabase served 492 — recovered descriptions/embeddings (490/490) from a Supabase snapshot after the old parquet's numeric-ID keyspace matched 0 rows.
- Berlin secondary: 259 → 258 (one school closed, one opened), crime 2024+2025 (Lichtenberg → safe, Tempelhof → moderate), traffic re-attached, transit partially fresh (BVG API refused connections mid-run; rest carried).
- Munich: fresh jedeschule 2026-08-15 base, PKS 2025 (HZ 7684→6061), 139/165 rows, POI 108/148 + embeddings preserved exactly.
- Hamburg 172/259 (+2/+2 new schools), Stuttgart 81/95 (+1), Dresden 159, Bremen 254 (+1) — all with Unfallatlas 2025 and preserved paid columns; Dresden split finals gained embeddings (0 → 75/90).
- Frankfurt/Leipzig/NRW: traffic 2025 in all finals (100% row match), no base change.

**Still open:** Supabase DDL + stable upload (needs Lovable MCP OAuth — `claude mcp add --transport http lovable "https://mcp.lovable.dev"` from an interactive terminal); ~6 new schools need Supabase INSERTs (uploader is PATCH-only); regression scripts + LLM prompt contracts still read year-suffixed names; Berlin transit re-run when BVG API recovers; Wave B triggers ~Sept 9 (Hessen) / Sept 23 (NRW).

## 2026-04-20 — NL POI Enrichment + GB Pipeline Chain Fix

**What:** Ran NL Phase 6 POI enrichment (0% → 100% POI coverage across 1,626 schools) and repaired the GB pipeline so Phase 3 (traffic) actually produces data and Phases 4/5/9 flow it into the final table.

**Why:** Status audit showed both pipelines well short of Berlin parity. NL had a POI script but had never been run. GB had multiple silent failures that left traffic/transit empty in the final table despite the phases reporting OK.

**NL fixes:**
- Rewrote `nl_poi_enrichment.py` to use `scripts_shared.enrichment.enrich_schools_with_pois.enrich_school` with a 5-worker thread pool, per-school JSON cache (keyed by `vestiging_code`/`brin_code`), and preferred the most-enriched input (`nl_schools_with_demographics.csv`) so POI output carries all prior enrichments.
- `nl_combine_and_finalize.py`: `find_best_input` now picks the intermediate with the highest column count rather than a fixed priority list — ordering-agnostic to handle POI-after-demographics.
- Result: 100% POI coverage (81 POI cols), 1,626 rows, ~7,800 Google Places API calls (~$250).

**GB fixes (four bugs, not one):**
- `gb_traffic_enrichment.py`: STATS19 raw now ships as `collision_severity`; script expected `accident_severity`. Added a rename before the slim-cache write. Without this, Phase 3 errored and silently stayed un-run for weeks.
- `gb_transit_enrichment.py`: NaPTAN `Status` filter was `== "act"` but real values are `active`/`inactive`. The whole 101 MB NaPTAN file was being parsed to 0 stops, so transit enrichment was a no-op. Also deleted the empty `naptan_stops_clean.csv` cache to force a re-parse.
- `gb_to_core_schema.py` — column-name mismatch: the OSM-based base scraper produces DfE-native names (`establishment_type_group`, `la_name`, `attainment8_average`, `progress8_average`) but the finalizer read GIAS names (`establishment_type`, `local_authority`, `ks4_*`). Added an alias rename at load time so the scraper stays idiomatic and the finalizer speaks one vocabulary.
- `gb_to_core_schema.py` — `compute_academic_score` crashed on DfE suppression codes (`'z'`, `'x'`). Wrapped the float coercion in try/except returning NaN.

**GB results (before → after):**
- Traffic: 0% → **100%** (9 cols)
- Transit rail: 0% → 50.7%, tram 0% → 12.7%, bus 0% → 88.9%, stop_count 100%
- `gb_establishment_type`: 0% → 90.2%
- `gb_local_authority`: 0% → 92.6%
- `gb_ks4_attainment8`/`progress8`: populated at 80.1% (via column-alias fix)

**Still-open GB gaps** (not in this change): POI script missing entirely, IMD decile / FSM% join (raw exists at `data_gb/raw/gb_imd.csv` but needs postcodes.io LSOA lookup), Ofsted scraping, remaining KS4 detail (progress8 CI bounds, ebacc, grade5 %), embeddings + similar schools, tuition.

**Still-open NL gaps:** embeddings + similar schools, Inspectorate ratings, school_track/board, buurt/wijk codes, tuition, enrollment pressure.

## 2026-04-19 — Schema Mapper Gaps + Supabase Backfill (1,512 rows)

**What:** Audited a Supabase coverage report that flagged many 0%-populated columns across cities, tracked each gap to either a mapper bug or a genuine source gap, fixed the mappers, extended the Supabase uploader, and filled the resulting cells.

**Why:** The audit showed columns like Dresden `email` and `bezirk` at 0/75, Frankfurt `leitung` at 0/99, Stuttgart `schueler_2024_25` at 0/80, Bremen `description_en` at 0/140, Leipzig `ortsteil` at 0/94. Most were mapping bugs — the data existed locally under a different column name or was being silently dropped by a combiner/schema-mapper save-order issue. Only the truly un-fixable gaps turned out to be external source limits (Munich jedeschule only returns addresses, Leipzig Saxon source lacks Stadtbezirk, Hamburg crime ships categories instead of counts).

**Pipeline fixes (permanent, committed):**
- **Dresden `dresden_to_berlin_schema.py`:** new `_fill_from_saxon_source` helper populates Berlin-canonical columns from raw Saxon names — `mail`→`email`, `phone_code_1+phone_number_1`→`telefon`, `headmaster_firstname+lastname`→`leitung`, `community_part`→`ortsteil`, `crime_stadtbezirk`→`bezirk` (with "StB N " prefix stripped).
- **Leipzig `leipzig_to_berlin_schema.py`:** same Saxon-source helper adapted. Also added primary/secondary split output so the stale April-13 berlin_schema artefacts the Supabase uploader consumed actually get refreshed.
- **Frankfurt `frankfurt_to_berlin_schema.py`:** `schulleitung`→`leitung` rename that was missing. Moved the `scripts_shared.schema.core_schema` import into a try/except since it fails outside the orchestrator and was silently truncating the save.
- **Bremen `bremen_to_berlin_schema.py`:** `summary_en`→`description_en` fill-gaps (Bremen's description pipeline wrote summary_* not description_*). Added primary/secondary split outputs; preserved `description_en` as a Bremen-specific extra since Berlin schema doesn't include it.
- **Stuttgart `processing/stuttgart_data_combiner.py`:** `find_most_enriched_file` now prefers `_with_metadata` over `_with_pois`. The metadata enrichment (leitung, schueler, lehrer, sprachen, gruendungsjahr, besonderheiten) layers on top of pois but was being silently skipped, so the master table carried only the pois file and downstream outputs lost all student/teacher/principal data. Added defensive `school_type`-from-`schulart` normalization so generic `'primary'/'secondary'` buckets never leak past the combiner again (the scraper was fixed in d6819cd but raw CSVs on disk predate that commit).
- **Stuttgart `stuttgart_to_berlin_schema.py`:** reordered save-before-import, same pattern as Frankfurt.

**Supabase uploader rewrite (`scripts_shared/upload_to_supabase.py`):**
- Replaces the old `UPDATE_FIELDS = [6 items]` with organized `FIELD_GROUPS` (contact / location / school_attr / descriptions / crime / transit_summary). `--groups` flag selects which categories upload.
- `COL_ALIASES` maps local `transit_bus_01_name`→Supabase `transit_bus_name` (Supabase uses unprefixed names for nearest stop, _02/_03 for the rest).
- Per-field PATCH with `?<col>=is.null` URL guard so the server-side filter matches the Python NULL-only check — a race between our SELECT and PATCH cannot overwrite a non-NULL value.
- Service-role key from env (`SUPABASE_SERVICE_ROLE_KEY`) with fallback to embedded anon key when Lovable installs a scoped RLS policy.
- Per-table schema probe; columns missing from a target table (e.g. `transit_all_lines_1000m` on `primary_schools`) are logged as skipped instead of crashing.
- Dry-run prints per-city per-field fill counts plus a grand total.

**Lovable coordination:** Anon key was RLS-blocked from UPDATE. Went with temporary per-column `IS NULL`-gated UPDATE policies on `schools` and `primary_schools` (21 columns), covering contact, location, school_attr, descriptions, crime, transit_summary. Lovable dropped them after the fill. Verified drop by PATCH-then-reread on a real Dresden row.

**Supabase results:**
- 1,357 rows filled in the main run (before Stuttgart), 155 more in the Stuttgart re-run, plus 175 Stuttgart `school_type` values corrected via Lovable SQL. 1,512 touched total.
- Top per-field fills: `besonderheiten` +783, `schulart` +483, `leitung` +443, `lehrer_2024_25` +425, `gruendungsjahr` +408, `ortsteil` +351, `schueler_2024_25` +321, `description_en` +203.
- Biggest city-level wins: Dresden `schools` email/telefon/leitung/ortsteil/bezirk 0/75 → 75/75; Frankfurt `primary_schools` leitung 0/100 → 99/100; Hamburg `primary_schools` besonderheiten 0/257 → 238/257; Munich `primary_schools` besonderheiten 0/148 → 146/148, lehrer/schueler 0 → ~138.
- Zero overwrites, zero HTTP errors across ~6,800 PATCH calls.

**Remaining genuine source gaps (require new data source, out of scope):**
- Munich `schools` (secondary) contact info: jedeschule.codefor.de only returns name/address/coords. All 108 rows are 0% for email/telefon/website/leitung.
- Leipzig `bezirk`: Saxon source has `community_part` (→Ortsteil) only; no reliable Stadtbezirk mapping.
- Frankfurt `bezirk` / `lehrer` / `migration_*`: Schulwegweiser (the primary source) doesn't expose them.
- Hamburg `crime_total_crimes_2023`: Hamburg's crime feed ships categories not raw counts.
- `transit_all_lines_1000m` on `primary_schools`: table schema gap Lovable offered to add.

**Commits (all on `main`):** 9c30ebb, c1a06ad, ad8fea4, 54392c0, 0a0f1fc, f16dfac, merged as 9eb8e34.

## 2026-04-17 — Cross-City Admission Criteria + Open Day Enrichment

**What:** Built and ran a new cross-city Gemini enrichment that visits every German school's website to extract structured admission criteria and upcoming open day dates. Two scripts:
1. `scripts_shared/enrichment/enrich_german_schools_with_admission_and_open_days.py` — main enrichment, feeds homepage URL to Gemini 2.5 Flash with URL-context + Google-Search grounding
2. `scripts_shared/enrichment/reenrich_admission_open_days_via_sitemap.py` — fix pass that discovers the site's sitemap.xml, picks admission/events subpage URLs by keyword scoring, and re-calls Gemini with those URLs for schools where the homepage-only pass failed

**Why:** SchoolNossa had rich enrichments (traffic, transit, crime, POIs, descriptions) but no structured fields for the two things parents care about most at decision time: how to apply and when to visit. Both exist on every school website in unstructured form.

**Scope:** 2,578 rows across 15 city-tables (9 German cities). 2,229 schools had a usable website URL; 349 (mostly Munich) had none.

**Results after both passes:**
- 1,932 success (83% of URL-having schools)
- 1,851 schools with admission bullets (83%)
- 729 schools with application windows (33%)
- 420 schools with upcoming open days (19% — expected for mid-April, most are Oct–Feb)
- 1,452 schools with past open day dates (65% — useful for next-cycle predictions)
- 36 remaining parse_error (1.6%), 261 no_admission_info (12%)

**Sitemap re-enrichment impact:** Recovered 107 schools (from 114 parse_error down to 36, a 68% reduction). Discovery methods: sitemap (117), homepage anchors (14), canonical probing (2).

**Key design decisions:**
- Shared URL-keyed cache (`data_shared/cache/admission_open_days/cache.json`, sha1 of normalized URL) so duplicate URLs across primary/secondary tables hit Gemini once. Hamburg Primary got 257 cache hits and 0 API calls.
- 60-day TTL because open-day calendars are seasonal
- German prompt; structured JSON output with ISO dates, bullet lists
- Scripts are standalone (not wired into per-city orchestrators) — run after all city pipelines produce final master tables

## 2026-04-17 — Fix school_type Mapping Bug (Stuttgart + Frankfurt)

**What:** Fixed school_type containing generic placeholders ("secondary", "Weiterführende Schule") instead of specific German school types. Added validation guard to prevent recurrence.

**Why:** Lovable frontend filter UI relies on school_type being a canonical German type (Gymnasium, Realschule, etc.). Stuttgart had `school_type = 'secondary'` for all 80 rows despite having correct `schulart` values. Frankfurt had a fallback path that could produce "Weiterführende Schule" for unmatched categories.

**Fixes:**
- `scripts_stuttgart/scrapers/stuttgart_school_scraper.py` — changed `school_type: classification` → `school_type: schulart` so schools get their actual German type (Gymnasium, Realschule, Gemeinschaftsschule, etc.) instead of generic "secondary"/"primary"
- `scripts_frankfurt/scrapers/frankfurt_schulwegweiser_scraper.py` — added `weiterfuehrende-allgemeinbildende-schulen` → `Gesamtschule` to category fallback mapping, preventing "Weiterführende Schule" placeholder
- `scripts_shared/schema/core_schema.py` — added `CANONICAL_DE_SCHOOL_TYPES`, `GENERIC_SCHOOL_TYPE_PLACEHOLDERS`, and `validate_school_types()` guard function
- `scripts_stuttgart/stuttgart_to_berlin_schema.py` — wired in `validate_school_types(strict=True)` as CI guard
- `scripts_frankfurt/frankfurt_to_berlin_schema.py` — wired in `validate_school_types(strict=True)` as CI guard

**Audit results:** All other cities (Berlin, Hamburg, NRW, Munich, Bremen, Dresden, Leipzig) already use specific German types correctly. The regression script (`run_regression.py:391-398`) already had a band-aid workaround preferring `schulart` when `school_type` is generic — this is now redundant but harmless.

**Convention documented:** `school_type` must always be one of the canonical German types used by the filter UI. Never the generic "secondary"/"primary" placeholders.

## 2026-04-12 — Fix Student/Teacher Data Gaps Across All German Cities

**What:** Investigated and fixed missing student/teacher counts (`schueler_2024_25`, `lehrer_2024_25`) across all German city pipelines. Previously only Berlin had good coverage; most other cities were at 0%.

**Why:** Lovable.Dev dashboard showed that slider filters for student/teacher counts only worked for Berlin. Root cause analysis revealed two issues: (1) website enrichment scripts for most cities only generated descriptions, not student/teacher counts; (2) schema transformers didn't map available source data to the canonical Berlin fields.

**Phase 1 — Schema transformer fixes (zero API cost):**
- `scripts_frankfurt/frankfurt_to_berlin_schema.py` — added `schueler_gesamt` → `schueler_2024_25` mapping (data existed but was only used for ndH ratio calculation)
- `scripts_munich/munich_to_berlin_schema.py` — added student/teacher passthrough mappings
- `scripts_dresden/dresden_to_berlin_schema.py` — added student/teacher passthrough mappings
- `scripts_stuttgart/stuttgart_to_berlin_schema.py` — added student/teacher passthrough mappings
- All mappings use fill-gaps-only guards (`pd.isna()` check before writing)

**Phase 2 — Website enrichment scripts (Gemini 2.5 Flash + URL context + Google Search grounding):**
- `scripts_munich/enrichment/munich_website_metadata_enrichment.py` — upgraded from description-only to full NRW-style metadata extraction. Added Google Search fallback for schools without website URLs (Bayern API doesn't provide them)
- `scripts_dresden/enrichment/dresden_website_metadata_enrichment.py` — fully implemented (was a `NotImplementedError` stub)
- `scripts_hamburg/enrichment/hamburg_website_metadata_enrichment.py` — new script, targeting teacher data gap. Fixed `schul_homepage` column mapping and separate `data_hamburg_primary/` path handling
- `scripts_stuttgart/enrichment/stuttgart_website_metadata_enrichment.py` — new script
- `scripts_frankfurt/enrichment/frankfurt_website_metadata_enrichment.py` — new script, prioritizes final files over intermediate (intermediate lacked website URLs)
- `scripts_shared/verify_coverage.py` — new coverage reporting script

**Phase 3 — Retry logic added to all `_call_gemini` functions (all 7 city scripts + NRW):**
- Empty responses: retry up to 2x with 3s delay
- JSON parse errors: retry up to 2x with 3s delay
- 500 INTERNAL server errors: retry up to 2x with 5s delay
- Improved coverage by ~10-20% on second pass

**Results after 2 passes:**

| City | Schools | Schüler (before → after) | Lehrer (before → after) |
|------|---------|------------------------|----------------------|
| Munich | 148 pri | 0% → **93%** | 0% → **94%** |
| Hamburg sec | 170 | 97% (kept) | 0% → **56%** |
| Hamburg pri | 257 | 98% (kept) | 0% → **40%** |
| Dresden | 75+90 | 0% → **65%** | 0% → **50%** |
| Stuttgart sec | 80 | 0% → **66%** | 0% → **56%** |
| Stuttgart pri | 95 | 0% → **54%** | 0% → **39%** |
| Frankfurt sec | 99 | 0% → **52%** | 0% → **33%** |
| Frankfurt pri | 100 | 97% (kept) | 8% → **32%** |
| Leipzig | 186 | 0% → **45%** | 0% → **32%** |
| Bremen | 253 | 0% → **49%** | 0% → **30%** |
| NRW (D'dorf+Köln) | 407 | 60% (kept) | 31-46% (kept) |

**Bonus coverage gained:** 80-98% `description_de` across all cities (was 0% for most), plus `schulleitung`, `besonderheiten`, `sprachen` filled.

**Key technical decisions:**
- All enrichment follows NRW template: `google.genai` with `UrlContext` + `GoogleSearch` grounding
- Fill-gaps-only semantics everywhere — never overwrite existing source data
- JSON caching per schulnummer avoids repeated API calls on re-runs
- Munich uses Google Search grounding alone (no URL context) since Bayern API provides almost no website URLs — still achieved 93% coverage

**Files changed:** 14 scripts modified/created across 8 city pipelines + shared utils

## 2026-04-12 — NRW Pipeline Retrigger: Köln & Düsseldorf Data Regenerated

**What:** Full NRW pipeline re-execution for Köln (155 primary + 103 secondary) and Düsseldorf (91 primary + 58 secondary). Data had been lost and needed regeneration from scratch. All 11 phases completed for both school types.

**Why:** Previously generated NRW data files were missing from `data_nrw/`. Pipeline scripts and some cache files (transit, website metadata, anmeldezahlen matches) were intact, allowing partial reuse.

**Pipeline phases executed:**
1. School Master Data (NRW Schulministerium Open Data → filter Köln+Düsseldorf)
2. Traffic Accidents (Unfallatlas 2022–2024, 500m radius)
3. Transit Accessibility (Overpass API, fresh query — old cache expired)
4. Crime Statistics (PKS NRW, Bezirk-level)
5. POI Enrichment (Google Places API)
5b. Anmeldezahlen (Düsseldorf only — re-downloaded CSV + 4 PDFs from city open data)
5c. Website Metadata & Descriptions (Gemini 2.5 Flash, ~380 school websites)
6. Data Combination
7. Embeddings (Gemini fallback, 768-dim — no OPENAI_API_KEY)
8. Berlin Schema Enforcement

**Düsseldorf Anmeldezahlen recovery:**
- Primary CSV: `opendata.duesseldorf.de` — 85/91 schools matched (2026/27 data)
- Secondary PDFs: 4 school-type PDFs from `duesseldorf.de/fileadmin/` — 46/58 schools matched
- 13 primary + 20 secondary schools oversubscribed (>100% demand/capacity)

**Firecrawl tuition extraction (new):**
- Built `scripts_nrw/enrichment/nrw_tuition_firecrawl_enrichment.py`
- Deep-crawls ~30 private school websites via Firecrawl map+scrape
- Extracts structured tuition data (11 schema columns) via Gemini
- Result: Most German private schools don't publish fees online — 5/35 had any info (free schools identified, income-based models noted, "auf Anfrage" captured in tuition_notes)
- Added `FIRECRAWL_API_KEY` to `.env`

**Output files** (`data_nrw/final/`):
- `nrw_{primary,secondary}_school_master_table_final_with_embeddings.parquet`
- `nrw_{primary,secondary}_school_master_table_final.csv`
- `{koeln,duesseldorf}_{primary,secondary}_school_master_table_final_with_embeddings.parquet`
- Berlin schema parquets for frontend compatibility

**Key fix:** Berlin primary reference parquet was missing from `data_berlin_primary/final/` — copied from worktree `.claude/worktrees/condescending-haslett/` to unblock Phase 8.

## 2026-04-09 — Abitur Predictions: Frankfurt, Munich, Stuttgart

**What:** Extended the Ridge regression Abitur prediction pipeline to three new cities. Generated rebased predictions for all Abitur-eligible schools and merged results into master table parquets.

**Why:** Frankfurt, Munich, and Stuttgart pipelines were complete but had no Abitur quality estimates. Model was trained on Berlin (128) + Hamburg (73) labeled schools; CV R²=0.417, MAE=0.179.

**Key technical changes (scripts_shared/regression/):**
- Removed Model B (Abitur erfolgsquote) — CV R²=−0.24, no improvement from Y-standardization or binary classification
- Added FRANKFURT_COLUMN_MAP for different column naming in Frankfurt/Munich/Stuttgart
- Expanded ABITUR_ELIGIBLE_TYPES to include Frankfurt types (IGS, KGS, Gemeinschaftsschule, Berufsoberschulen, etc.)
- Added schulart fallback when school_type is generic "secondary" (Stuttgart/Munich pattern)
- Implemented _impute_gisd_from_zensus(): OLS proxy for GISD quintile from Zensus features with R²<0.10 guard
- Downloaded Zensus 100m grid: Frankfurt (16,380 cells), Munich (25,566), Stuttgart (13,546)

**Stuttgart known issue:** PLZ column contains the numeric part of the Schulnummer instead of real postal codes — GISD imputation skipped. Needs fix in Stuttgart scraper.

**Results:**
| City | Predicted | Total | Rebase shift | State avg |
|---|---|---|---|---|
| Frankfurt | 56 | 99 | +0.030 | 2.38 (Hessen) |
| Munich | 30 | 108 | −0.221 | 2.29 (Bayern) |
| Stuttgart | 45 | 80 | −0.051 | 2.32 (BaWü) |

**Output:** Prediction columns merged into {city}_{type}_school_master_table_final_with_embeddings.parquet for all three cities. Standalone prediction parquets also in data_{city}/final/.

## 2026-04-09 — Leipzig Pipeline: 186 Schools Complete

**What:** Built the complete Leipzig school data pipeline (all school types combined: 92 Grundschulen, 37 Oberschulen, 31 Gymnasien, 24 Sonstige, 2 Förderschulen). All 9 phases + tuition + description pipelines.

**Data source:** Sachsen Schuldatenbank API (`schuldatenbank.sachsen.de`) — provides school metadata, coordinates, legal_status_key (1=public, 2=private). Crime data from Leipzig Kriminalstatistik (Ortsteil-level).

**Key technical decisions:**
- `legal_status_key` → `traegerschaft` mapping added to combiner's `clean_data()` — scraper returns "unbekannt" but has the key (1=oeffentlich, 2=frei/privat)
- Berlin reference parquet not available in worktree — schema transformer made resilient with fallback to other cities or partial-only enforcement
- Description and tuition pipelines write to cache files, then merged back into main CSV before embeddings/schema phases
- Symlinked `config.yaml` to `scripts_shared/generation/` for API key access

**Results:**
| Field | Coverage |
|---|---|
| Schools | 186 (156 public, 30 private) |
| Coordinates | 186/186 (100%) |
| Traffic (Unfallatlas) | 186/186 (100%) |
| Transit (Overpass) | 186/186, all scored |
| Crime (Leipzig PKS) | 186/186, Ortsteil-level |
| POI (Google Places) | 186/186 (100%) |
| Descriptions (Perplexity+Gemini) | 178/186 (96%) |
| Sprachen | 113/186 (61%) |
| Besonderheiten | 185/186 (99%) |
| Website | 185/186 (99%) |
| Tuition (30 private) | 30/30 classified (22 medium, 4 low, 2 high, 2 ultra) |
| Embeddings (OpenAI) | 186/186, 3072-dim |
| Berlin Schema | PASS, 265 Berlin + 92 Leipzig extras |

**Notable:** Leipzig International School correctly classified as "ultra" tier (€1,010-1,070/month). 7 of 30 private schools have income-based tuition.
## 2026-04-07 — Dresden Pipeline: 159 Schools from Sächsische Schuldatenbank API

**What:** Added Dresden (Sachsen) as a new city to SchoolNossa. Complete pipeline with 9 phases — from data source research through enrichment implementation.

**Key findings:**
- Sächsische Schuldatenbank has a free CSV API with WGS84 coords (easiest school data source so far)
- 159 schools: 88 Grundschulen, 39 Oberschulen, 30 Gymnasien, 2 Förderschulen (124 public + 35 private)
- 100% coordinate coverage, 97% website coverage
- Dresden Open Data Portal has per-Stadtteil crime data (best granularity after Hamburg)
- Traffic: Unfallatlas with ULAND=14 (same pattern as NRW)
- No per-school Sozialindex in Sachsen — using GISD proxy

**Template:** NRW pipeline (closest match for traffic/transit approach)

**Files:** `scripts_dresden/` (10 scripts), `data_dresden/` (5 dirs), `docs/dresden_data_availability_research.md`

**Status:** Full pipeline executed. All phases complete including descriptions (159/159) and tuition (35/35 private schools). Embeddings skipped (no OPENAI_API_KEY in worktree).

**Tuition results (35 private schools):**
- 33 medium tier (€120-200/month), 1 ultra (Dresden International School: €1,300/month), 1 low (€65/month)
- 16 income-based tuition, 13 confirmed flat-fee
- Income matrices generated for all 35 schools

**Final output (split for frontend):**
- `dresden_primary_school_master_table_final.csv` — 90 schools (88 GS + 6 cross-level)
- `dresden_secondary_school_master_table_final.csv` — 75 schools (69 OS/Gym + 6 cross-level)
- `dresden_school_master_table_final.csv` — 159 combined
- Secondary file: 265/265 Berlin schema columns matched

**Cross-level schools** (Waldorf, DIS, Gemeinschaftsschulen) duplicated into both files with appropriate schultyp

## 2026-04-09 — Bremen Pipeline: Full Run + Descriptions + Tuition

**What:** Ran the complete Bremen pipeline end-to-end, including description generation (Perplexity+OpenAI) for secondary schools and tuition extraction for private schools. Generated QA report and schema drift report.

**Why:** All enrichment phases needed to be executed to produce a production-ready dataset for the frontend.

**Results:**
| Metric | Value |
|--------|-------|
| Total schools | 253 (113 primary, 65 secondary, 75 other) |
| Coordinates | 252/253 (99.6%) |
| Traffic (Unfallatlas) | 250/252 schools, 7,809 accidents across 3 years |
| Transit (Overpass) | 7,194 stops, avg 14.1 within 500m |
| Crime (PKS) | 206/253 matched (81%), 22 Beiratsbereiche |
| POI (Google Places) | 252 schools, 83 columns, 8 categories, 2,751 API calls |
| Descriptions (secondary) | 59/65 with bilingual DE+EN (Perplexity+OpenAI) |
| Descriptions (primary) | In progress via shared pipeline |
| Tuition (secondary) | 3 private: FEBB €150, Mentor €355, FGS €75-450 |
| Tuition (primary) | 3 private: 2 high tier, 1 low tier |
| Embeddings | 253/253 (text-embedding-3-large, 3072-dim) |
| Berlin schema | 265 columns match exactly |
| QA | 9/10 checks OK |

**Key fixes during pipeline run:**
- Scraper column mapping: `Name1`→`schulname`, `Planbezirk`→`stadtteil`, `Region`→`bezirk`, `Internet`→`website`
- Crime enrichment dtype: added `pd.to_numeric()` before `.rank()/.round()` to fix object-type errors
- Website metadata: added config.yaml key loading for Gemini API key

## 2026-04-08 — Bremen Pipeline: Implementation (All 9 Phases)

**What:** Added Bremen as a new city to SchoolNossa. Completed all workflow phases: research, scaffold, and full implementation of all 9 pipeline scripts.

**Why:** Bremen is a city-state (~200 schools) with good open data availability. Expands SchoolNossa coverage to 5 cities (Berlin, Hamburg, NRW, Munich, Bremen).

**Technical approach:**
- **Phase 1 (Research):** Documented 8 data categories. Key sources: Schulwegweiser Excel (bildung.bremen.de) + GeoBremen Shapefile (EPSG:25832) for school master data, Unfallatlas for traffic, Overpass API for transit, parliamentary PDFs for crime (22 Beiratsbereiche).
- **Phase 2 (Scaffold):** Combined pipeline (all school types together, like Hamburg). NRW as template city.
- **Phase 3 (Implementation):** All 9 phases implemented:
  - Scraper: Downloads Excel + Shapefile, converts EPSG:25832→WGS84, joins sources, geocodes missing via Nominatim
  - Traffic: Unfallatlas ULAND=04 (identical to NRW pattern)
  - Transit: Overpass API with bbox splitting (identical to NRW pattern)
  - Crime: Hardcoded 22 Beiratsbereiche data with tabula-py PDF parsing fallback, Stadtteil→Beirat mapping
  - POI: Google Places API (New), 8 categories, threaded
  - Website: Gemini + Google Search grounding for metadata extraction
  - Combiner, Embeddings (OpenAI+Gemini fallback), Schema Transformer

**Key files:**
- `docs/bremen_data_availability_research.md`
- `scripts_bremen/Bremen_school_data_asset_builder_orchestrator.py`
- `scripts_bremen/{scrapers,enrichment,processing}/` (10 Python scripts)
- `scripts_bremen/bremen_to_berlin_schema.py`

## 2026-04-07 — Munich Primary School Pipeline: 148 Grundschulen

**What:** Built the complete Munich primary school (Grundschule) pipeline by refactoring all 11 existing secondary-only scripts to support a `school_type` parameter. Ran all 9 phases producing 148 fully enriched Grundschulen.

**Why:** Munich only had a secondary school pipeline (108 schools). Primary schools were out of scope. Adding them brings Munich to 256 total schools.

**Technical approach:**
- Refactored all scripts (scraper, 5 enrichment scripts, combiner, embeddings, schema transformer, orchestrator) to accept `school_type='primary'|'secondary'`
- jedeschule.codefor.de used as data source — filters for `Grundschulen` type patterns
- Orchestrator now runs both types by default: `--school-types primary,secondary`
- All file paths parameterized: `munich_{school_type}_schools_with_{enrichment}.csv`
- Added OSM Overpass-based private school detection (code written, not yet wired in)

**Results (primary):**
| Field | Coverage |
|---|---|
| Schools | 148 (147 Grundschulen + 1 Grundschule) |
| Coordinates | 148/148 (100%) |
| Traffic (Unfallatlas) | 148/148, avg 61.8 accidents/500m |
| Transit (Overpass) | 148/148, avg score 93.7/100 |
| Crime (PKS city-level) | 148/148, HZ=7684/100k |
| POI (Google Places) | 148/148, 1197 API calls |
| Descriptions (Gemini) | 148/148 bilingual DE+EN |
| Embeddings (OpenAI) | 148/148, 3072-dim |
| Berlin Schema | PASS, 153/265 columns |

**Known gap:** jedeschule.codefor.de has zero private schools for Munich. Private school detection via OSM is implemented but not yet integrated. Tuition pipeline pending traegerschaft tagging.

## 2026-04-07 — Stuttgart Pipeline: Full City Build (95 Primary + 80 Secondary)

**What:** Built the complete Stuttgart school data pipeline from scratch — scraper, 4 enrichments, descriptions, tuition, embeddings, and Berlin schema alignment.

**Data source selection:**
- **Primary source:** `stuttgart.de/organigramm/adressen` — official city directory with 258 school entries. Scraped via RSS feed (790 URLs) → detail page scraping (JSON metadata + HTML fields). Provides: name, coordinates (WKT), phone, email, Schulart, Stadtbezirk, website.
- **Supplementary:** LOBW Dienststellensuche (`lobw.kultus-bw.de/didsuche/`) — scraped via ASMX web service API for student/teacher/class counts. jedeschule.codefor.de for principal names.
- **Rejected:** City PDFs (poster layout, unparseable), jedeschule as primary (inflated BW data with duplicates), Statistisches Landesamt (€101 paywall).

**Pipeline phases (13 total):**
1. School data scrape (stuttgart.de directory) → 95 primary + 80 secondary
2. Traffic (Unfallatlas BW, ULAND=08) → 100%
3. Transit (Overpass API, 2068 stops) → 100%
4. Crime (PKS Stuttgart 2023, bezirk-level estimates) → 100%
5. POI (Google Places API) → 100%
6. Data combiner → 178 columns
7. Embeddings (Gemini gemini-embedding-001, 768d) → 100%
8. Berlin schema enforcement → PASS
10. Descriptions (Perplexity Pass 0 + OpenAI Pass 1+2) → 100% EN/DE
11-13. Tuition (Gemini Pass 1+2, GPT-5.2 Pass 3) → 17/17 private schools

**Final coverage:**
| Field | Primary (95) | Secondary (80) |
|---|---|---|
| Coordinates | 100% | 100% |
| Phone | 99% | 100% |
| Email | 92% | 95% |
| Website | 100% | 100% |
| Schulleitung | 54% | 60% |
| Schülerzahl | 68% | 84% |
| Lehrerzahl | 63% | 69% |
| Description EN/DE | 100% | 100% |
| Besonderheiten | 91% | 96% |
| Transit/Traffic/Crime/POI | 100% | 100% |
| Tuition (private) | 5/5 | 12/12 |
| Embeddings | 100% | 100% |
| Berlin schema | PASS | PASS |

**Key technical decisions:**
- Used LOBW ASMX web service API (`SearchDienststellen` + `GetDienststelle`) — undocumented but stable, returns SCHUELER/KLASSEN/LEHRER per school
- Stuttgart crime: bezirk-level estimates using PKS city totals × district crime indices × district population
- Tuition Pass 3 results: 8/12 secondary private schools confirmed income-based (Waldorf + Evangelische), 4/12 flat-fee

**Files added:**
- `scripts_stuttgart/` — 13 pipeline scripts (scraper, 4 enrichments, combiner, embeddings, schema, orchestrator)
- `data_stuttgart/final/` — 6 parquet + 6 CSV final outputs
- `data_stuttgart/intermediate/` — enrichment chain CSVs
- `data_stuttgart/cache/` — LOBW, jedeschule, Unfallatlas, transit stops, description/tuition caches

---

## 2026-04-07 — Frankfurt POI Gap Fixed: 49% → 99% + Full Clean Pipeline Run

**What:** Fixed a persistent secondary school POI coverage gap (49/99 → 98/99 schools), updated Berlin schema canonical backfills for school stats, and ran the full Frankfurt pipeline to produce clean final output.

**Root cause of POI gap:** `frankfurt_poi_enrichment.py` always read from `with_crime.csv` (no POI data) regardless of whether a `with_pois.csv` already existed. On a checkpoint-based re-run, only newly processed schools were written to the output — losing all previously enriched data. The same 26 alphabetically-first schools (indices 0–25) were consistently skipped because they'd been in a stale checkpoint from a partial prior run, and the next run overwrote the output with only the new batch.

**Fixes:**
1. **POI enrichment input fallback** — added `with_pois.csv` as highest-priority input source (before `with_crime.csv`), so partial results are preserved across re-runs
2. **Already-enriched skip logic** — schools with non-null `poi_supermarket_count_500m` are now excluded from `to_process`, preventing duplicate API calls
3. **No-checkpoint POI-file guard** — don't drop existing POI columns when reading from `with_pois.csv` (only drop when starting truly fresh from `with_crime.csv`)
4. **Better error logging** — exception handler now logs `idx`, `type(e).__name__`, and message for easier diagnosis
5. **POI→final merge** — after POI fix, merged 81 updated POI columns from `master_table.csv` into `_final.csv` and parquet before Phase 9, so the Berlin schema output carries full POI coverage

**Final output (clean):**
| Field | Secondary | Primary |
|---|---|---|
| schulnummer | 100% | 100% |
| website | 100% | 99% |
| email | 96% | 99% |
| schulleitung | 100% | 99% |
| schueler_2024_25 | 100% | 97% |
| poi_supermarket_count_500m | 99% | 99% |
| transit_accessibility_score | 100% | 100% |
| crime data | 100% | 100% |
| description | 100% | 100% |
| tuition_display | 97% | 99% |
| embedding | 100% | 100% |

**Files changed:**
- `scripts_frankfurt/enrichment/frankfurt_poi_enrichment.py` — input fallback + already-enriched skip + no-drop guard + better error logging

<!-- NEW ENTRIES GO ABOVE THIS LINE -->

## 2026-04-06 — Frankfurt Pipeline Rebuilt: Schulwegweiser as Primary Source

**What:** Completely rebuilt the Frankfurt data pipeline to use frankfurt.de/schulwegweiser as the PRIMARY data source, replacing Hessen Verzeichnis 6 as Phase 1.

**Why:** The Schulwegweiser is the city's own school directory — more authoritative, more current, and far richer than the statistical Verzeichnis 6. It covers all 4 school categories (Grundschulen, Weiterführende allgemein, Förderschulen, Weiterführende beruflich) with ~279 schools vs 158 from Verzeichnis 6. Most importantly it provides official website URLs, contact info, Schulleitung, Schulprofile, Fremdsprachen, Ganztagsform, Besondere Angebote, and Auszeichnungen directly — no web research needed.

**Coverage map confirmed by browser inspection:**

| Field | Primary | Sec. allgemein | Beruflich | Förderschulen |
|---|---|---|---|---|
| Schulleitung | ✓ | ✓ | ✓ | ✓ |
| Schülerzahl, Klassenzahl | ✓ | ✓ | ✓ | ✓ |
| Official website | ✓ | ✓ | ✓ | mostly |
| Email + Telefon | ✓ | ✓ | ✓ | ✓ |
| Schulform (typed) | ✓ | ✓ | ✓ | ✓ |
| Profile / Schwerpunkte | ✓ | ✓ | - | ✓ |
| Förderschwerpunkt | - | - | - | ✓ |
| Frühe Fremdsprache | ✓ | - | - | - |
| 1./2./3. Fremdsprache | - | ✓ | ✓ | - |
| Ganztagsform (Einrichtungsart) | ✓ | ✓ | - | - |
| Besondere Angebote | ✓ | ✓ | ✓ | - |
| Auszeichnungen | - | ✓ | - | - |
| Berufsbereiche + Ausbildungsberufe | - | - | ✓ | - |
| Stadtteil | ✓ | ✓ | ✓ | ✓ |

**Technical implementation:**
- Phase 1: `frankfurt_schulwegweiser_scraper.py` (rewritten as full primary scraper)
  - Scrapes all 4 categories via Playwright + page-text line parser
  - Text parser locates content start by finding school name→first known label
  - All multi-value fields (Fremdsprachen, Schulform, Besondere Angebote etc.) handled
  - Nominatim geocoding of addresses → lat/lon
  - JSON cache keyed by category; partial-resumption support
  - Outputs: `raw/frankfurt_primary_schools.csv`, `secondary_schools.csv`, `vocational_schools.csv`
- Phase 2: `frankfurt_verz6_enrichment.py` (new optional phase)
  - Downloads Verzeichnis 6 Excel, joins by fuzzy name (≥0.75) + PLZ
  - Adds `schulnummer` + `ndh_count`; generates SW-{slug} IDs for non-matches
- Data combiner: rewritten with new column order reflecting Schulwegweiser-first schema
- Berlin schema transformer: updated mappings (school_type, sprachen, ganztagsform, Trägerschaft → tuition_display, leistungsprofil, betreuungsangebot); metadata_source updated

**School counts:**
- Grundschulen: 6 pages × 20 = ~120
- Weiterführende allgemein: 5 pages × 20 = ~100
- Förderschulen: 1 page = 19
- Weiterführende beruflich: 2 pages × 20 = ~40
- Total: ~279 (vs 158 from Verzeichnis 6)

**Parser validation:** 14/14 field extraction checks passed against Adorno-Gymnasium sample.

**Files changed:**
- `scripts_frankfurt/scrapers/frankfurt_schulwegweiser_scraper.py` — full rewrite as primary scraper
- `scripts_frankfurt/scrapers/frankfurt_verz6_enrichment.py` — new
- `scripts_frankfurt/processing/frankfurt_data_combiner.py` — new column schema, removed old SW overlay
- `scripts_frankfurt/frankfurt_to_berlin_schema.py` — updated field mappings
- `scripts_frankfurt/Frankfurt_school_data_asset_builder_orchestrator.py` — Phase 1 = SW primary, Phase 2 = Verz6 join

## 2026-04-06 — Frankfurt Schulwegweiser Scraper (Phase 2 — Official Websites & Profiles)

**What:** Built a Playwright-based scraper for the Frankfurt city school portal (frankfurt.de/schulwegweiser) as a new Phase 2 in the Frankfurt pipeline.

**Why:** The Verzeichnis 6 source (Hessen Statistik) provides no official school website URLs, email addresses, Schulprofile, or Ganztagsform data. The Schulwegweiser portal has all of this, and it's the canonical city-maintained school directory. This gives us deterministic, authoritative data rather than relying entirely on Perplexity-based web research.

**Data extracted per school:**
- `website` — official school URL (external link on detail page)
- `sw_email`, `sw_telefon` — contact details
- `sw_schueler` — student count from portal
- `sw_schulleitung` — principal name
- `sw_profile` — Schwerpunkte / school profiles (comma-joined)
- `sw_sprachen` — Frühe Fremdsprache
- `sw_ganztagsform` — Einrichtungsart (all-day school type)
- `sw_besonderheiten` — Besondere Angebote

**Technical details:**
- Playwright headless Chromium with anti-bot headers to bypass Cloudflare
- Crawls list pages: Grundschulen (6 pages × 20) + Weiterführende (5 pages × 20)
- Detail page extraction: 2-child div pattern for label/value pairs + external link detection
- All scraped data cached to `data_frankfurt/cache/schulwegweiser_cache.json`
- Fuzzy name matching (SequenceMatcher, threshold ≥ 0.65) to join portal data to Verzeichnis 6 schools
- Outputs: `data_frankfurt/intermediate/frankfurt_{type}_schools_with_schulwegweiser.csv`
- Data combiner updated: `merge_schulwegweiser()` overlays portal data even when later enrichments (traffic/transit/crime/POI) are the loaded source

**Phase renumbering:**
- Phase 2 is now Schulwegweiser (was Traffic). Traffic/Transit/Crime/POI/Combiner/Embeddings/Schema shifted to 3-9. Description pipeline is now Phase 10. Tuition phases are 11-13.

**Files changed:**
- `scripts_frankfurt/scrapers/frankfurt_schulwegweiser_scraper.py` — new scraper
- `scripts_frankfurt/processing/frankfurt_data_combiner.py` — `merge_schulwegweiser()` + fallback chain update
- `scripts_frankfurt/Frankfurt_school_data_asset_builder_orchestrator.py` — new phase 2, renumbered 3-13

## 2026-04-06 — Description Pipeline: Website Coverage 53% → 99% + Primary Schools

**What:** Improved the shared description pipeline to find school websites for nearly all schools, and ran description + tuition pipelines on Frankfurt primary schools.

**Why:** The original pipeline found websites for only 53% of Frankfurt secondary schools (39/73). Every school has a website — this was a data quality gap. Also fixed a corrupted primary `_final.csv` that had 2 schools instead of 85.

**Root cause of missing websites:** Perplexity's Sonar API returns source citation URLs alongside its text response, but we were only capturing the text — discarding the citation array that often contains the school's official website URL.

**Fixes:**
1. **Capture Perplexity citations** in Pass 0 — stored as `pass0_citations` in cache
2. **Pass citations to Pass 2** — structured extraction prompt now includes source URLs as hints
3. **Citation domain filter** with scoring: school portals (+5), school keywords in domain (+3), city name in domain (+2), name match (+4); blocks directories, FOIA portals, city portals
4. **Targeted website fallback**: if Pass 2 + citation analysis fail, fires targeted Perplexity query *"What is the official website of [school] in [city]?"*
5. **URL normalization**: strips `[1][2]` citation markers, normalizes to base homepage
6. **In-place parquet update**: `save_results` now updates `_final_with_embeddings.parquet` preserving embeddings — prevents schema transformer from overwriting enriched data

**Results:**
- Frankfurt secondary: **39/73 (53%) → 72/73 (99%)** websites found
- Frankfurt primary description pipeline: running (descriptions + structured extraction for 85 schools)
- All POI coverage confirmed 100% (primary CSV was corrupted with 2 rows; regenerated from parquet)

**Files changed:** `scripts_shared/generation/school_description_pipeline.py`

## 2026-04-08 — Hamburg Full Rebuild: Primary Pipeline + Deduplication Fix

**What:** Created a complete Hamburg primary school (Grundschule) pipeline and rebuilt both Hamburg pipelines from scratch with data quality fixes.

**Why:** Hamburg only had secondary schools. Also discovered Zweigstellen (branch campuses) and duplicate WFS rows were inflating school counts in both pipelines.

**Key fixes:**
- Removed Zweigstellen from both scrapers — these are satellite campuses, not separate schools
- Deduplicated on `schulnummer` — WFS returned multiple rows per school from GeoJSON entrance points
- Combined schools (Grund-/Stadtteilschule) now appear in **both** pipelines since they serve both levels
- Secondary went from 286 → **170** schools; primary has **257** schools; 30 overlap

**Results:**
| | Secondary | Primary |
|---|---|---|
| Schools | 170 (84 STS + 80 Gym + 6 combined) | 257 Grundschulen |
| Columns | 187 | 182 |
| Coordinates | 100% | 100% |
| Transit | 100% | 100% |
| Traffic | 68% | 68% |
| Crime | 100% | 100% |
| POI | 100% (81 cols) | 100% (81 cols) |
| Embeddings | 170/170 | 257/257 |
| Berlin Schema | 265 cols | 265 cols |

**Structure:** `scripts_hamburg_primary/` + `data_hamburg_primary/` (separate from secondary)

## 2026-04-01 — Munich Secondary School Pipeline Scaffolded and Implemented

**What:** Built the complete Munich (Bayern) secondary school data pipeline — all 9 phases from research through Berlin schema enforcement. This is the 5th city in the SchoolNossa platform.

**Why:** Expanding SchoolNossa coverage to Munich, Germany's third-largest city.

**Key results:**
- Research document: `docs/munich_data_availability_research.md` (8 data categories assessed)
- Orchestrator + 9 phase scripts in `scripts_munich/`
- Data sources: Schulsuche CSV (ISO-8859-15), Unfallatlas (ULAND=09), Overpass API transit, city-level PKS crime, Google Places POI
- Munich-specific: no coordinates in school CSV (needs geocoding via jedeschule.codefor.de + Nominatim)
- Notable limitation: Bavaria does NOT publish per-school academic performance data (VERA password-protected)
- Crime: Munich is Germany's safest major city for 50 consecutive years (HZ 7,684/100k vs Frankfurt 14,840)
- Template city: NRW pipeline pattern (CSV-based schools, Unfallatlas traffic, district-level crime estimation)
- Pipeline ready to run but requires: geocoding step execution, Google Places API key, Gemini/OpenAI API keys for descriptions and embeddings

**Branch:** `feature/munich-pipeline`

<!-- NEW ENTRIES GO ABOVE THIS LINE -->

## 2026-03-30 — Frankfurt Pipeline Built End-to-End

**What:** Built the complete Frankfurt am Main school data pipeline from scratch — all 8 phases from data source research through Berlin schema enforcement.

**Why:** Expanding SchoolNossa coverage to Frankfurt (Hessen), the fifth largest city in Germany.

**Results:**
- **85 primary schools** + **73 secondary schools** = **158 total**
- Data source: Hessen Statistisches Landesamt Verzeichnis 6 (Excel with multi-level headers)
- Coordinates: jedeschule.codefor.de + Nominatim geocoding fallback → 100% coverage
- All enrichments at 100% coverage: transit (Overpass), traffic (Unfallatlas ULAND=06/UKREIS=12), crime (city-level PKS), POI (Google Places), embeddings (Gemini 3072d)
- Berlin schema alignment: PASS for both primary and secondary
- QA reports generated: both PASS

**Key decisions:**
- Hessen doesn't publish Stadtteil-level crime data — used city-level aggregate (documented limitation)
- Hessen doesn't publish school-level Abitur/MSA data — academic performance columns are NULL
- Used ndH (non-German native language) count from Verzeichnis 6 as belastungsstufe proxy
- 38 secondary schools classified as "Weiterführende Schule" (Förderschulen, Abendschulen, etc.) rather than forcing them into Gymnasium/Realschule/Gesamtschule categories
- Fixed `Schul-nummer` → `schulnummer` column rename in data combiner (Hessen-specific hyphenation)

**Files created:**
- `scripts_frankfurt/` — Full pipeline (scraper, 4 enrichments, combiner, embeddings, schema transformer, orchestrator)
- `data_frankfurt/` — Raw, intermediate, final, cache directories with all output files
- `docs/FRANKFURT_DATA_SOURCES_RESEARCH.md` — Data availability research
- `data_frankfurt/final/QA_REPORT_*.md` — QA validation reports
