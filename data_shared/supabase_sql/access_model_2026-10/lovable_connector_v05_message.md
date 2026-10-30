SchoolNossa MCP connector 0.5: full details for the top 3 results, travel times from the user's saved places, richer cards, and hardening of the Google functions. Build on 0.4. Keep the OAuth setup, the paid-only check, the existing tool names and inputs, and the 0.4 wrapper. Rebuild the owned MCP file with `bun scripts/build-mcp-function.ts` and refresh the manifest. Version "0.5.0". Don't publish.

## A. Database (migration; settings row separately, as before)

Extend `public.mcp_consume(p_tool, p_bucket)` with a third limited bucket, `'commute'`, limit `COALESCE((v_limits->>'commute_per_day')::integer, 20)`. Everything else in the function stays as it is.

Settings:
```sql
UPDATE public.app_settings
SET value = value || '{"commute_per_day": 20}'::jsonb
WHERE key = 'mcp_limits';
```

## B. Full details for the top 3 results

In `search_schools`, `find_schools_by_description` and `list_pinned_schools`:
- The first 3 schools get a `details` object with the same fields `get_school` returns: description, address, website, contact, students/teachers with years, Abitur, admission criteria/window, languages, and the Dutch-only fields.
- Each expanded school counts as one `detail` via `rpc('mcp_consume', { p_tool: '<tool>:expanded', p_bucket: 'detail' })`. When the detail allowance is used up, return those schools slim, with `details_limited: true` on the result.
- Schools 4+ stay slim.
- The text content includes the key facts (section H) of the 3 expanded schools, then: "For the other N schools, the map and side-by-side comparisons, open SchoolNossa: https://schoolnossa.com · iPhone app: https://schoolnossa.com/app".

## C. New tool `commute_times`

- **Title:** "Travel times from your saved places".
- **Annotations:** readOnlyHint true, destructiveHint false, idempotentHint true, openWorldHint false.
- **Input:**
  - `schoolIds`: 1–10 uuids from earlier results
  - `category`: primary | secondary, default secondary
  - `place`: optional string, matched case-insensitively against the label or `poi_type` of one of the user's saved places
  - `mode`: transit | bicycling | walking | driving, default transit
- **Never accept a typed address or coordinates.** The only origins are the caller's own rows in `user_pois` (read with the caller's token, so RLS applies).
- **Guard:** paid check → `mcp_consume('commute_times', 'commute')`.
- **No saved places:** return a normal result: "You haven't saved any places in SchoolNossa yet. Add your home, work or other places under 'Meine Orte / My places' in the SchoolNossa web app (https://schoolnossa.com/menu) or the iPhone app, then ask again."
- **Origins:** the matching place, or, without `place`, up to 3 saved places.
- **Destinations:** the schools' coordinates.
- **Call:** `${SUPABASE_URL}/functions/v1/google-directions` with the caller's bearer token and the publishable key as `apikey`; body `{ origins, destinations, mode }`. Origins carry `{ id, label, lat, lng }`.
- **Output:** per school, per place: `{ place_label, mode, duration_text, duration_seconds, distance_text }`, plus the usual school `url` / `source` and the attribution block. Never return a place's address or coordinates; the place label ("Zuhause", "Home", "Arbeit") is enough.
- Attach the card UI to this tool too.

## D. Cards (ui://schoolnossa/schools.html)

- **List view:**
  - the 3 expanded schools as rich cards: the key facts like the app's detail modal, description truncated to ~4 lines with "mehr in SchoolNossa", and buttons "In SchoolNossa öffnen" / "iPhone-App"
  - then a compact line "+N weitere in SchoolNossa", linking to the web app
  - show `details_limited` gracefully, as slim cards plus a hint
- **New travel-times view:** for each school, its name and one row per saved place with an icon for the mode, the duration and the distance. Footer link: "Orte in SchoolNossa verwalten" → https://schoolnossa.com/menu.
- **No map yet.** It needs CARTO tiles with an API key, which is pending. Structure the code so a map block can be added later.
- Links still only to schoolnossa.com (incl. /menu, /school/…, /app, /connectors).

## E. Message for accounts without a subscription

Replace the `subscription_required` text (tool text and card) with:

"Using SchoolNossa in ChatGPT and Claude requires an active SchoolNossa subscription. Subscribers can search with SchoolNossa's AI search, see full details of the top results and get travel times from their saved places. More information: https://schoolnossa.com/connectors"

The card shows the same text, a link to /connectors and the iPhone app. No prices, plans, sign-up or checkout links (ChatGPT directory rules).

## F. Hardening (edge functions)

`google-directions`, `google-places-autocomplete` and `google-places-details` currently don't check the caller, so anyone with the public key can run Google requests on SchoolNossa's bill. Require the caller's JWT with `rpc('has_app_access')`, exactly as `semantic-search` does: 401 without a valid user, 403 without access. Before changing them, confirm that every caller in the web app sends a signed-in session; the iPhone app uses `functions.invoke` with the session. If any public page uses them, tell me instead of breaking it.

## G. Texts

- **/connectors (DE + EN):** add "travel times from your saved places (add them under 'My places' in the app)" and "full details for the top 3 results". Add the new limit: "20 travel-time requests per 24 hours".
- **llms.txt:** add the travel-time tool in both languages.
- **/privacy section 8**, append one sentence:
  - DE: "Für Reisezeiten verwendet der Assistent Ihre in SchoolNossa gespeicherten Orte; dabei werden nur deren Bezeichnungen (z. B. „Zuhause“) und die berechneten Reisezeiten an den Assistenten übermittelt, nicht die Adressen."
  - EN: "For travel times, the assistant uses the places you saved in SchoolNossa; only their labels (e.g. "Home") and the calculated travel times are sent to the assistant, not the addresses."

## H. Keep the long texts away from the model (card-only data)

Goal: ChatGPT and Claude may use conversations for model training on consumer accounts, so the model should only see what it needs. The SchoolNossa card shows the rest.

- **Model-visible** (`content` text and `structuredContent`), per school:
  - name, category, type / tracks, public/private, district / municipality
  - students and teachers with years
  - Abitur average and pass rate (secondary)
  - the admission application window dates
  - `url`, `source`, and the attribution block
  - for `commute_times`: the travel times, as in C
- **Card-only:** description texts (`description_de` / `description_en`), `besonderheiten`, admission criteria bullets and admission notes, full address, phone, email and website. Put them in the tool result's `_meta` (e.g. `_meta["schoolnossa/card"]`), which the MCP Apps host passes to the UI but not to the model.
  - Applies to `get_school` and the 3 expanded schools in the list tools.
- **Verify** in the ext-apps / Apps SDK docs how the App receives `_meta` with the tool result, and that hosts keep it out of the model context. Cite what you found in your reply.
  - If a host can't deliver `_meta` to the card, the card falls back to the model-visible facts plus a "Details in SchoolNossa" button. Never move the long texts back into the model-visible fields.
- Hosts without UI simply get the key facts and the link.
- Tests: no description or admission text appears in `content` or `structuredContent` for any tool.

## I. Terms and /connectors

- **/terms (DE + EN)**, next to the existing bulk-extraction rule:
  - DE: "Inhalte, die über die SchoolNossa-Connectoren in KI-Assistenten abgerufen werden, dürfen nicht zum Training von KI-Modellen verwendet werden."
  - EN: "Content retrieved through the SchoolNossa connectors in AI assistants may not be used to train AI models."
- **/connectors**, privacy block, add a tip:
  - DE: "Tipp: Damit Ihre Gespräche nicht für das Training von KI-Modellen verwendet werden, deaktivieren Sie in ChatGPT „Das Modell für alle verbessern“ (Einstellungen → Datenkontrollen) bzw. in Claude „Claude verbessern helfen“ (Einstellungen → Datenschutz)."
  - EN: "Tip: to keep your conversations out of AI model training, turn off "Improve the model for everyone" in ChatGPT (Settings → Data controls) or "Help improve Claude" in Claude (Settings → Privacy)."
- **The connector's first-time hint:** in the server `instructions`, add one sentence for the assistant: "If the user asks about privacy or AI training, point them to https://schoolnossa.com/connectors."

## Checks

- Tests for: top-3 expansion and its quota fallback, `commute_times` (no places → friendly message; place matching; no address or coordinates in the output), and the new subscription text.
- Typecheck, tests and build pass. Without a token, the MCP endpoint still returns the same 401 challenge.
- Report: migration name, the final tool list with annotations, the changed edge functions, and anything that blocks part F.
