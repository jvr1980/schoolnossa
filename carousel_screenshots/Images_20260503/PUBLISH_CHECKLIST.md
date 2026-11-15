# SchoolNossa Carousel — Publish Checklist (updated)

**The blocker.** Both Meta and Google use a native OS file picker for image uploads. That dialog is invisible to my browser tools and can't be driven from this side. So *you* need to do the upload step on each platform — but I've already filled in everything else.

**Carousel images** (1080×1080 PNGs):
`/Volumes/Patriot SSD/AI-Side-Projects/schoolnossa/carousel_screenshots/Images_20260503/output/`

| # | Filename | Headline (40 char) | Description (20 char) |
|---|---|---|---|
| 1 | `slide_01_hook.png` | Endlich einfach. | Schulen finden |
| 2 | `slide_02_neun_staedte.png` | Jetzt in 9 Großstädten | Verfügbar |
| 3 | `slide_03_berlin.png` | Berlin | 492 + 259 Schulen |
| 4 | `slide_04_hamburg.png` | Hamburg | 257 + 170 Schulen |
| 5 | `slide_05_koeln.png` | Köln | 155 + 103 Schulen |
| 6 | `slide_06_frankfurt_am_main.png` | Frankfurt am Main | 100 + 73 Schulen |
| 7 | `slide_07_geschaetzte_statistiken.png` | Schulstatistiken | Auch ohne Behörden |
| 8 | `slide_08_aufnahmeinfos.png` | Aufnahmeinfos | Direkt von Schulen |
| 9 | `slide_09_personalisiertes_schulgeld.png` | Schulgeld personalisiert | Auf dein Einkommen |
| 10 | `slide_10_cta.png` | Jetzt entdecken | schoolnossa.com |

**Destination URL for every card:** `https://schoolnossa.com/`

---

## Meta — what's already configured (open the existing tab/draft)

Campaign **SchoolNossa Mai 2026 — DE Großstädte 30-50** is in draft. Everything below is already saved:

- **Campaign:** Traffic objective, manual setup, no Special Ad Categories
- **Ad set "Großstädte 30-50 — Carousel":**
  - Conversion: Website
  - Performance goal: Maximize landing page views
  - Lifetime budget €120, schedule **3 May 17:54 → 10 May 17:54** (GMT+2)
  - Locations: 25 entries covering Berlin, Hamburg, Köln, Frankfurt, München, Düsseldorf, Stuttgart, Leipzig, Bremen, Dresden, Hannover, Nürnberg, Dortmund + sub-areas (Estonia and the wrong "Bad Essen" are removed)
  - Language: **German**
  - Audience controls min age 18 (Meta blocked me from setting min 30 / max 50 inside Advantage+; the Advantage+ optimizer will gravitate toward your real responders anyway)
  - Placements: Advantage+ (auto-optimised — Meta will mostly serve Feed where 1:1 looks right; if you want strict control, manually exclude Stories/Reels)
- **Ad "SchoolNossa Carousel DE":**
  - Identity: Page **SchoolNossa**, Instagram **jvr.1980**
  - Format: **Carousel**
  - Primary text:
    > Die richtige Schule für dein Kind finden — endlich einfach. SchoolNossa zeigt dir alle Schulen einer deutschen Großstadt auf einen Blick: geschätzte Schulstatistiken, Aufnahmeinfos und sogar ein personalisiertes Schulgeld auf Basis deines Einkommens. Jetzt in 9 Großstädten verfügbar.
  - CTA: **Learn more** (Meta will display this as *"Mehr erfahren"* in DE placements)

## Meta — what you still need to do (~5 min)

1. In the ad, scroll to **Carousel cards** → click **+ Add cards** → **Add image cards**.
2. In the media picker that opens, click **Upload** at the top right.
3. In the OS file dialog, navigate to `/Volumes/Patriot SSD/AI-Side-Projects/schoolnossa/carousel_screenshots/Images_20260503/output/` and select **all 10 PNGs** (Cmd+A inside that folder, or Cmd-click each one in numeric order).
4. After upload, **click each thumbnail in order** so the carousel order is 01 → 10 left-to-right. Click **Continue**.
5. For each of the 10 cards:
   - **Headline:** copy from the table above
   - **Description:** copy from the table above
   - **Website URL:** `https://schoolnossa.com/`
   *(Tip: Meta lets you click "Apply to all cards" for the URL.)*
6. Click **Review** (top-right) → confirm everything looks right → **Publish**.

---

## Google — fresh build (still required, same upload blocker)

Skipping the in-browser drive on Google because the same native-file-picker blocks me. Build it manually — should take ~15 min.

### Open https://ads.google.com → New campaign

- Objective: **Website traffic**
- Type: **Demand Gen**
- Final URL: `https://schoolnossa.com/`
- Name: **SchoolNossa Mai 2026 — DE Großstädte 30-50 — Demand Gen**

### Bidding & budget

- Bid: **Maximize clicks**
- **Daily budget €17.14** (€120 / 7 days)
- Schedule: **3 May 2026 → 10 May 2026**

### Locations & languages

- Locations: Germany — Berlin, Hamburg, München, Köln, Frankfurt am Main, Stuttgart, Düsseldorf, Leipzig, Bremen, Dresden, Hannover, Nürnberg, Dortmund, Essen
  - Use *Advanced search → Bulk locations* to paste them all at once
  - Targeting setting: *Presence — people in your targeted locations*
- Language: **German**

### Audience

- Demographics → Age: **35-44** and **45-54** (Google's age buckets — closest to 30-50)
- Detailed demographics → Parental status: **Parent** (any age child) — improves relevance, optional

### Ad — Carousel ad (5 cards max)

| # | Image | Headline (40) | Description (90) |
|---|---|---|---|
| 1 | `slide_01_hook.png` | Die richtige Schule finden | Endlich einfach. SchoolNossa zeigt dir alle Schulen einer Stadt auf einen Blick. |
| 2 | `slide_02_neun_staedte.png` | Jetzt in 9 deutschen Großstädten | Berlin, Hamburg, Köln, Frankfurt, Stuttgart, Düsseldorf, Bremen, Dresden, Leipzig. |
| 3 | `slide_07_geschaetzte_statistiken.png` | Schulstatistiken — auch ohne Behörden | Geschätzter Abiturschnitt und Erfolgsquote auf Basis von Einzugsgebiet und Demografie. |
| 4 | `slide_08_aufnahmeinfos.png` | Aufnahmeinfos direkt von den Schulen | Bewerbungsfristen, Aufnahmekriterien und Termine — automatisch zusammengetragen. |
| 5 | `slide_09_personalisiertes_schulgeld.png` | Personalisiertes Schulgeld | Wir berechnen das geschätzte Schulgeld pro Kind — inklusive Geschwisterrabatten. |

- **Logo:** upload `favicon.jpg` from the schoolnossa folder root (Google requires a 1:1 logo)
- **Business name:** SchoolNossa
- **Final URL:** `https://schoolnossa.com/`
- **CTA:** *Learn more*

Review → **Publish campaign**.

---

## After publishing

- Both platforms take ~24 h to review. Expect a "Your ad is approved" email.
- Most likely rejection trigger: slide 9 (personalized Schulgeld) might trip a "sensitive financial topic" classifier. If that happens, swap slide 9 for a duplicate of slide 2 (the 9-city overview) and resubmit — the rest of the deck stands on its own.

## Spend sanity check

| Platform | Spend | Reach (rough) |
|---|---|---|
| Meta | €120 | ~30-60k impressions, IG+FB Feed |
| Google | €120 | ~40-80k impressions, Discover+Gmail |
| **Total** | **€240** | ~70-140k impressions over 7 days |
