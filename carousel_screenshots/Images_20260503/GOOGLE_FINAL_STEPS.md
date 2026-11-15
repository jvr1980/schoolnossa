# Google Demand Gen — Final Steps

You're already in the ad editor. Here's what's saved and what's left.

## What I configured (saved, all good)

- **Campaign:** SchoolNossa Mai 2026 — DE Großstädte 30-50 — Demand Gen
- **Goal:** Clicks (Maximise clicks)
- **Daily budget:** €17.14
- **Schedule:** 3 May 2026 → 10 May 2026
- **Ad group "Großstädte 30-50":** 14 German cities (Berlin, Hamburg, Munich, Cologne, Frankfurt, Stuttgart, Düsseldorf, Leipzig, Dortmund, Essen, Bremen, Dresden, Hanover, Nuremberg)
- **Language:** German
- **Ad name:** SchoolNossa Carousel DE
- **Ad type:** Carousel image ad
- **Final URL:** https://schoolnossa.com/
- **Ad-level Headline:** Schule finden — endlich einfach
- **Description:** Geschätzte Schulstatistiken, Aufnahmeinfos und personalisiertes Schulgeld auf einen Blick.
- **Business name:** SchoolNossa

## What you still need to do (~10 min)

### 1. Add the logo (Required)

In the **Logos** section, click **+ Add → Upload** → select `favicon.jpg` from `/Volumes/Patriot SSD/AI-Side-Projects/schoolnossa/favicon.jpg`. Google requires a 1:1 square logo and the favicon is exactly that.

### 2. Add the 5 carousel cards (Required)

In the **Cards** section, click **+ Add** and create 5 cards using the table below. For each card:
- Click **+ Add** in the **Images** section → **Upload** → select the PNG below
- Type the **Headline**
- Type the **Final URL** = `https://schoolnossa.com/`
- (Optional) Set **Call to action text** = *Mehr erfahren*
- Click **Done**

| Card | Image (in `carousel_screenshots/Images_20260503/output/`) | Headline (40 char max) | Description (90 char max) |
|---|---|---|---|
| 1 | `slide_01_hook.png` | Die richtige Schule finden | Endlich einfach. Alle Schulen einer deutschen Großstadt auf einen Blick. |
| 2 | `slide_02_neun_staedte.png` | Jetzt in 9 deutschen Großstädten | Berlin, Hamburg, Köln, Frankfurt, Stuttgart, Düsseldorf, Bremen, Dresden, Leipzig. |
| 3 | `slide_07_geschaetzte_statistiken.png` | Schulstatistiken — auch ohne Behörden | Geschätzter Abiturschnitt und Erfolgsquote auf Basis von Einzugsgebiet und Demografie. |
| 4 | `slide_08_aufnahmeinfos.png` | Aufnahmeinfos direkt von den Schulen | Bewerbungsfristen, Aufnahmekriterien und Termine — automatisch zusammengetragen. |
| 5 | `slide_09_personalisiertes_schulgeld.png` | Personalisiertes Schulgeld | Wir berechnen das geschätzte Schulgeld pro Kind — inklusive Geschwisterrabatten. |

### 3. Review and publish

- Click **Go to review** (bottom-left)
- Verify campaign + ad group + ad summary
- Click **Publish campaign**

Google's review takes ~24 h. You'll get an email when it's approved.

## Notes / things I couldn't set

- **Age 30-50** isn't directly settable in Demand Gen; Google's optimised targeting will gravitate toward your responders. If you want a hard cap, after publishing go into the ad group settings → Demographics → exclude **18-24** and **65+**.
- **Maximise clicks** with no target CPC is the most aggressive way to get traffic; if German Demand Gen CPC turns out painful you can revisit the bid strategy in a few days.
