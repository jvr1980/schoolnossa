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

## 7. What has to exist before the first ad

| # | Piece | Who | Effort |
|---|---|---|---|
| 1 | **Public city pages** `/schulen/<city>/tage-der-offenen-tuer` (+ NL page): open days for the next 4–6 weeks (school name, date, link to the school's site), the official deadline and the beta offer. Needs a definer RPC that returns only these public fields (school tables stay RLS-locked). | me, via Lovable | ~20–30 credits |
| 2 | **Campaign capture**: the `c` URL parameter goes into the sign-up link and is saved with the new account. No cookies, no pixel, no consent banner needed. | me | small |
| 3 | `scripts_marketing/`: content generator (DB → slides/videos), Meta Graph poster (IG + FB), Meta Marketing API flights + rules, YouTube uploader, weekly metrics report | me | 2–3 days |
| 4 | Scheduling: GitHub Actions cron for posting and rules (works when the Mac is off) + a weekly Claude task for the report and next week's content | me | small |

## 8. One-time set-up only you can do (~45 min)

I can't create accounts or handle passwords, so these steps are yours:
1. **Instagram:** create **@schoolnossa** as a Professional (Business) account and link it to the **SchoolNossa** Facebook Page. (In May the ads ran under the personal account jvr.1980.)
2. **Meta Business:** create a Meta developer app (Business type) and a **System User** with access to the Page, the Instagram account and the ad account. Generate a non-expiring token with `ads_management`, `ads_read`, `pages_manage_posts`, `pages_read_engagement`, `instagram_basic`, `instagram_content_publish`, and put it in the git-ignored `secrets/meta.env`. For our own accounts the app can stay in development mode (no App Review).
3. **YouTube:** create the **SchoolNossa** channel (brand account) and a Google Cloud OAuth client. Approve the OAuth once. Request the free **YouTube API audit**: until it passes, API uploads are private only, so for the first weeks I upload through Chrome.
4. Check that the ad account's payment method and VAT details are SchoolNossa OÜ.

## 9. How the automation runs

```
Mon 07:00  weekly Claude task: pulls Meta/IG/YouTube stats + new accounts by campaign code
           → report in marketing/reports/YYYY-WW.md → applies the budget rules
           → generates next week's posts into marketing/queue/YYYY-WW/
Daily      GitHub Action: publishes the queue items due today, runs the ad rules
48 h       every queued item is visible in the queue before it publishes
```

**Approval model:** you approve the templates, the guardrails and the monthly split once. After that, posts from approved templates publish without asking. **Weeks 1–4: review-ahead.** Each item sits in the queue for 48 h and you can delete it to veto. From week 5: fully automatic, if you're happy. I ask you again only for a new template type, a new country, or a budget change.

## 10. KPIs (in the weekly report)

- New accounts per week, by campaign code (organic IG, YouTube, each ad)
- Cost per account (target ≤ €5) and cost per click
- Beta places left (452 now). When they run out, the hook changes to the 48 h trial
- First paid conversions (€9.99 / 3 months) by source
- Anti-spam check: 7-day ad frequency ≤ 2.5, posts per week ≤ 3, hides/negative feedback

**Review after 6 weeks (end of December):** keep, change or stop. If the cost per account is above €10 after €150 spend, the money moves to YouTube Shorts promotion or stops.

## 11. Decisions needed

1. **Public open-day pages:** OK to show a few weeks of open days per city without an account? This is a small exception to the "school data needs an account" rule. It is the landing page and the SEO page. (Recommended: yes.)
2. **Approval mode:** review-ahead for the first 4 weeks, then automatic? (Recommended: yes.)
3. **Budget split** in section 4: OK, or different cities first?
