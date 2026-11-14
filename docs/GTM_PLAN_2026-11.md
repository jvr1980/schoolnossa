# SchoolNossa GTM plan: automated, no spam, €100/month

**Written:** 2026-11-07 · **Channels:** Instagram, Facebook (organic + Meta ads), YouTube (organic) · **Ads cap:** €100/month · **Not used:** email to existing users, DMs, group posting, comment bots, follower buying.

## 1. Where we start (measured 2026-11-07)

| Metric | Value |
|---|---|
| Website visitors, last 3 months | 109 (Direct 100, Google 3, ChatGPT 1, Claude 1) |
| Accounts | 51 total, 5 in the last 30 days |
| Beta places used (first 500 get 3 months free) | 48 → **452 left** |
| Stripe payments | 0 |
| Data | 10 German cities + all of NL; open days for 647 secondary / 553 primary schools; official 2027/28 registration windows per city |

The problem is reach, not product. Almost nobody knows SchoolNossa exists.

## 2. Why the May 2026 test was expensive and converted badly

The May campaign: €120 Meta + €120 Google Demand Gen, all in one week (3–10 May), 14 cities, age 18+, Advantage+ placements, every click to `schoolnossa.com/`.

| What went wrong | What this plan does instead |
|---|---|
| **May is off-season.** No German city has a secondary deadline then except Munich. | Ads run only in each city's **open-day and registration window** (Nov–Mar). |
| **14 cities at once, €240 in 7 days.** Too thin per city for Meta to learn, and too much at once. | **1–3 cities per flight**, ~€3/day, flights follow the deadlines. |
| **Generic message** ("Schule finden – endlich einfach") with no reason to act now. | **Deadline + local hook:** "Anmeldewoche Hamburg: 1.–5. Februar. 123 Schulen mit Tag der offenen Tür im Überblick." |
| **Homepage landing.** The visitor had to find their city, and anon users see no school data. | **City landing page** with the open days and the official deadline, visible without an account. The sign-up offer is on that page. |
| **Advantage+ placements** (Audience Network, Messenger) bring cheap accidental clicks. | **Instagram + Facebook Feed/Reels/Stories only.** |
| **Two ad platforms for one small budget.** | **Meta only** for paid. YouTube stays organic (search intent is free there). |
| **No measurement** of which click became an account. | **Campaign code in the URL → saved with the new account.** No pixel, no cookies. |

## 3. Who and when

**Who:** parents whose child changes school in 2027 (Year 4 → 5, Berlin Year 6 → 7), plus families moving to a German city or the Netherlands (relocation, EN content).

**When (from `city_admission_rules`, 2027/28):**

| City | Secondary registration | Primary | Ad flight |
|---|---|---|---|
| Hamburg | 1–5 Feb 2027 | 4–22 Jan 2027 | 18 Nov – 31 Jan |
| Bremen | form by 8 Feb | (Nov 2026, passed) | 1 Dec – 7 Feb |
| Düsseldorf | 15–17 Feb | – | 5 Jan – 14 Feb |
| Köln | Feb (2027 dates TBC) | – | 5 Jan – 20 Feb |
| Dresden, Leipzig | by 26 Feb | – | 15 Jan – 25 Feb |
| Berlin | 3–11 Mar (Probeunterricht sign-up 8–9 Feb) | – | 18 Nov – 11 Mar |
| Frankfurt | form by 5 Mar | Mar/Apr (for 2028) | 1 Feb – 5 Mar, Mar–Apr |
| Stuttgart | 8–11 Mar | – | 1 Feb – 11 Mar |
| München | 10–14 May (certificate 3 May) | 3 Mar | 20 Apr – 14 May |
| Netherlands | central registration in March (big cities) | – | 1 Feb – 31 Mar |

## 4. Paid: €100/month on Meta

**Set-up for every flight:**
- Objective **Traffic → link clicks** (no pixel needed). Placements: Instagram + Facebook Feed, Reels, Stories. No Audience Network.
- Geo: city + 15 km radius. Age 28–55. Language DE (or NL / EN). Advantage+ audience off. The creative does the targeting ("Euer Kind kommt 2027 in die 5. Klasse?").
- **One campaign per month with a Meta campaign spending limit of €100.** This is Meta's hard cap, above my scripts.
- 2–3 ads per ad set: deadline card, open-days carousel, 15 s screen recording.
- Link: `schoolnossa.com/<city-page>?c=meta-<city>-<yymm>-<ad>`.

**Budget split (proposal):**

| Month | Split |
|---|---|
| Nov (from 18th) | €50: Berlin 25, Hamburg 25 (test 3 hooks) |
| Dec | Hamburg 40, Berlin 40, Bremen 20 (pause 23–26 Dec) |
| Jan | Hamburg 30, Düsseldorf + Köln 30, Dresden + Leipzig 20, Bremen 20 |
| Feb | Berlin 35, Frankfurt 20, Stuttgart 20, Dresden + Leipzig 15, NL test 10 |
| Mar | NL 50 (Amsterdam, Utrecht, Den Haag, Rotterdam), Berlin 25, Stuttgart 15, München primary 10 |
| Apr–May | München 60, Frankfurt primary 40 |
| Jun–Sep | €30–50: relocation ads in EN ("Moving to Berlin with kids?") |

**Expected (rough, to be checked in week 2):** €0.30–0.80 per click → 125–330 clicks/month → at 5–10% sign-up, **6–33 new accounts/month**. Target: **≤ €5 per account**. That is a learning budget. The parts that compound are the organic posts and YouTube.

**Automatic rules (run daily):**
- Pause an ad after 1,500 impressions with CTR < 0.6 %, or after €8 spend with 0 sign-ups.
- Pause an ad set if 7-day frequency > 2.5 (**the anti-spam rule**: nobody sees us more than ~2–3 times a week).
- Move budget to the ad with the lowest cost per account, within the month's split.
- Stop everything if the month's spend reaches €100, even if Meta's cap fails.

## 5. Organic: Instagram + Facebook Page

**Cadence (hard caps):** 3 feed posts/week, at most 1 story/day, no reposting the same post. Each post must be useful even if nobody clicks.

**Content, generated from our own data each week:**
1. **"Tage der offenen Tür diese Woche – {Stadt}"** carousel: school, date, time, district. For the 2 cities with the nearest deadline.
2. **Deadline cards**, 14 days and 3 days before each city's registration window. Once each.
3. **"So funktioniert die Schulwahl in {Stadt}"**: 5-slide explainer from the official rules, with the source named. One per city, evergreen.
4. **Feature demo** every 2 weeks (commute times, compare, AI connector), from the existing screenshots and the narrated 9:16 beta video.

**Rules for every text** (from earlier decisions): verified facts only, with the source; "early beta, a side project built in free time; the first 500 testers get 3 months free", never "non-commercial"; no personal name or account details in screenshots; support contact support@schoolnossa.com.

## 6. YouTube (organic)

Parents search YouTube for "Anmeldung Gymnasium Berlin 2027", "Übergang 4 nach 5 Hamburg", "aanmelden middelbare school Amsterdam". Few videos answer this per city. That is our free advantage.
- **1 Short per week**: the explainer or deadline card as a 30–45 s vertical video with captions (rendered with the existing `render.js` pipeline; no voice at first).
- **1 longer video per city** (2–3 min): "Schulwahl {Stadt} 2027: Termine, Regeln, Tage der offenen Tür", landscape, captions, chapters, link in the description.
- Search-style titles and descriptions; link to the city page with `?c=yt-<city>`.

## 7. What has to exist before the first ad (all built by me)

| # | Piece | How | Effort |
|---|---|---|---|
| 1 | **Public city pages** `/schulen/<city>/tage-der-offenen-tuer` (+ NL page): open days for the next 4–6 weeks (school name, date, link to the school's site), the official deadline and the beta offer. A definer RPC returns only these public fields; the school tables stay RLS-locked. | SQL tool + Lovable (sent via Chrome), I click Publish | ~20–30 credits |
| 2 | **Campaign capture**: the `c` URL parameter goes into the sign-up link (also through Google/Apple sign-in) and is saved with the new account. No cookies, no pixel, no consent banner. | Lovable | small |
| 3 | **Growth repo** `schoolnossa-growth` (private): templates, generator, publisher, ad rules, reports | me | 2–3 days |
| 4 | **Marketing lines in the existing 08:00 daily email**: new accounts by source, ad spend this month | Lovable (usage-report function) | small |

## 8. Hands-off design: who does what

**I cannot create accounts, type passwords or generate keys for you.** Everything else is mine.

**Your one-time part (~20 minutes, once):**
1. Create **@schoolnossa** on Instagram as a Professional account and link it to the SchoolNossa Facebook Page (5 min, phone).
2. Create the **SchoolNossa** YouTube channel in your Google account (2 min).
3. Meta: I set up the developer app, the System User and its access in Chrome (with your go-ahead). You press **Generate token** and paste it into `secrets/meta.env` (3 min).
4. Say yes to: the €100/month cap, public open-day pages, and the first 6 sample posts (10 min, one message).

**Ongoing: nothing required.**
- Daily: two marketing lines in the 08:00 email you already get.
- Weekly: a one-page report (link). You only hear from me when a decision is yours: a budget change, Meta rejecting an ad twice, a comment that needs a human, or the 31 December checkpoint.
- Optional: say "stop" or "pause" in any Claude chat. Ads stop within minutes, and posting stops.

## 9. How the automation runs

Three parts, so that no single failure stops everything:

| Part | Runs where | Does what |
|---|---|---|
| **Weekly brain** | Claude scheduled task on your Mac, Mondays 07:00 (catches up when the Mac wakes) | Pulls stats and new accounts by code. Picks the cities from the deadline calendar. Writes 2 weeks of posts from the data. **A fact check**: every date and number in a caption must match a database row, or the post is dropped. Renders images and videos (Playwright + ffmpeg; voice from Gemini TTS). Pushes the queue. Sets up next week's ad flights by API. Uploads the YouTube video through YouTube Studio in Chrome. Writes the report. |
| **Daily hands** | GitHub Actions in `schoolnossa-growth`, 07:30 Berlin; runs with the Mac off | Publishes the posts due today to Instagram and the Facebook Page (Graph API). Pulls ad stats. Applies the pause rules. Errors go to the existing `client-errors` → failure email. |
| **Safety net** | Meta itself | The campaign spending limit (€100) per month. A token with **no messaging, comment-reply or group permissions**: the system technically cannot DM, comment or post in groups. |

**Why posts are queued 2 weeks ahead:** if the Mac is off for a week, nothing visible stops. Only the YouTube upload waits.

**YouTube detail:** videos uploaded by a new, unaudited API project are locked as private ([Google docs](https://developers.google.com/youtube/v3/docs/videos/insert)). So I upload through YouTube Studio in Chrome (the file-upload tool now works there). I request the API audit in parallel, and switch once it passes.

**Instagram detail:** the API fetches each image from a public URL (JPEG). The rendered images go to a small public repo `schoolnossa-media`; they are public posts anyway. Publishing limit: 50+ per day; we use 3 per week.

## 10. KPIs (in the weekly report)

- New accounts per week, by campaign code (organic IG, YouTube, each ad)
- Cost per account (target ≤ €5) and cost per click
- Beta places left (452 now). When they run out, the hook changes to the 48 h trial
- First paid conversions (€9.99 / 3 months) by source
- Anti-spam check: 7-day ad frequency ≤ 2.5, posts per week ≤ 3, hides/negative feedback

**Checkpoint, 31 December (automatic default):** if the cost per account is above €10 after €150 spend, **ads stop by themselves** and the posts continue. You get one message and can overrule it.

## 11. Risks and what happens

| Risk | What happens |
|---|---|
| Mac off on Monday | The queue covers 2 weeks; the ad rules run in the cloud. Only the YouTube upload waits. |
| Meta rejects an ad | The next variant goes in automatically. After 2 rejections you get one message. |
| Token stops working (e.g. password change) | Failure email the same morning; ads keep their caps. You generate a new token (3 min). |
| A wrong date or number in a post | The fact check blocks it before the queue. Corrections: I delete the post and post a correction. |
| Low organic reach at first (0 followers) | Expected. Paid reach + YouTube search carry the first months. |
| Meta's look-alike or detailed targeting changes | The creative carries the targeting ("Familien in Hamburg, Anmeldewoche 1.–5. Februar"). |

## 12. Decisions needed

1. **Public open-day pages:** OK to show a few weeks of open days per city without an account? This is a small exception to the "school data needs an account" rule. It is the landing page and the SEO page. (Recommended: yes.)
2. **€100/month cap** with the monthly split in section 4: OK?
3. **The first 6 sample posts:** you look once; after that, posts from the same templates go out without asking.
