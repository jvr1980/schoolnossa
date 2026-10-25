# SchoolNossa connector: directory submissions (Claude + ChatGPT)

Prepared 2026-10-25. Server: `https://whzvzoumldeqgyrqlilt.supabase.co/functions/v1/mcp` (Lovable `@lovable.dev/mcp-js`, Supabase OAuth 2.1 with dynamic client registration). Access: paid subscribers only; tokens from OAuth clients need `has_paid_access()` (enforced in the MCP code and in RLS).

## Status

- **Claude (custom connector), tested 2026-10-25** with the admin (paid) account. Auth detected automatically ("Sign in now" + "Register automatically"). Consent page on schoolnossa.com, then all six tools listed. Query: covered cities + Gymnasien in Berlin-Pankow. Correct live data (2025/26 student and teacher numbers).
- **ChatGPT (Plugins → Add → Add custom MCP server), tested 2026-10-25.** Same OAuth flow. Query: VWO schools in Utrecht. Correct. ChatGPT ran the read-only tools without asking (default "Allow low-risk tools").
- **Server side:** `auth.oauth_clients` has "Claude" (15:47 UTC) and "ChatGPT" (15:52 UTC), both registered dynamically.

## Blockers before submitting (decisions for José)

1. **Reviewer login.** Web sign-in needs email + password + a 6-digit code emailed by `send-signin-otp`; there is no bypass on web or iOS. OpenAI requires "a login and password for a fully featured demo account" with no 2FA barriers. Anthropic needs credentials and steps a reviewer can follow alone.
   - Recommended: a dedicated reviewer account (e.g. `review@schoolnossa.com`):
     - paid
     - a few pinned schools, so the account is "fully populated"
     - exempt from the emailed code through a server-side allowlist checked by `send-signin-otp` / `verify-signin-otp`
     - nothing personal on it
     - password rotated after each review
   - Keep it separate from Apple's demo account (john.doe@absolutnow.com).
2. **Privacy policy section** for AI connectors. Both directories require it; a draft is at the end of this file. It needs José's approval before it goes live.
3. **OpenAI organization verification:** platform.openai.com → Settings → Organization → General (SchoolNossa OÜ).
4. **Connector 0.3 is LIVE (2026-10-25):**
   - results default 5, max 10
   - `search_schools` requires a city
   - slim list fields
   - daily limits of 60 searches + 30 details per user (rolling 24 h; admins exempt; `mcp_usage` table, `mcp_consume()`)
   - limit events go to the daily report, not instant mail
   - new `find_schools_by_description` (the app's AI search)
   - every result has `url` (`/school/<id>?cat=`) + `source`, plus an attribution block
   - `/school/:id` deep link (signed out → sign-in → back to the school)
   - /terms bans bulk extraction

   Verified live: limit 50 rejected; 5 slim results with url/source; call logged; deep link opens the detail modal; the AI search works in Claude after "Refresh tools list". After a server update, existing connections need **⋯ → Refresh tools list** in Claude.

   Follow-ups for the AI search (these also affect the app's KI-Suche):
   - scores are flat (~0.75 for the top 8), so the ranking barely discriminates
   - there's no school-type filter (an ISS came up for a "Gymnasium" query)
   - it takes 30–60 s
5. **Connector 0.4 (MCP App cards) is in progress.** Once it's live, the Claude listing needs 3–5 carousel screenshots (PNG ≥1000 px wide, cropped to the app response, with a paired prompt each), and the allowed link URIs: https://schoolnossa.com.

## Assets

- Icon: `schoolnossa_icon_1024.png`, `schoolnossa_icon_512.png` (from the iOS app icon)
- Documentation URL: https://schoolnossa.com/connectors (built 2026-10-25)
- Privacy policy URL: https://schoolnossa.com/privacy
- Terms: https://schoolnossa.com/terms
- Support contact: info@schoolnossa.com
- Company: SchoolNossa OÜ, Tornimäe tn 5, Tallinn, Estonia, https://schoolnossa.com

## Listing text

**Name:** SchoolNossa

**One-liner** (Claude, ≤200 chars):
Find and compare schools in Germany and the Netherlands: search by city, district, type and size, see school details, Abitur results and admission info, and keep a shortlist.

**Description** (≤2,000 chars, both directories):
SchoolNossa helps parents choose a school. Connect your SchoolNossa account to search over 10,000 primary and secondary schools in ten German cities (Berlin, Bremen, Dresden, Düsseldorf, Frankfurt am Main, Hamburg, Köln, Leipzig, München, Stuttgart) and across the Netherlands, in plain language.

What you can do:
- See which cities and municipalities are covered, with school counts.
- Search schools by city, district, school type, public or private ownership, and size.
- Open one school's details: address and contact, students and teachers by school year, Abitur averages and pass rates (secondary schools), and admission criteria and application windows where available.
- Keep a shortlist: list, pin and unpin schools; the same list appears in the SchoolNossa web and iPhone apps.

SchoolNossa combines official school statistics with location and neighbourhood information. Analyses are directional estimates, not guarantees.

Requires an active SchoolNossa subscription. The free 48-hour trial covers the website and the iPhone app, not the connector.

**Categories:** Education (primary); also Research / Family & parenting if the portal offers them.

**Use cases** (Claude step 4):
- Shortlisting secondary schools in a district.
- Comparing two schools' Abitur results and size.
- Checking admission criteria and application windows.
- Managing a shortlist from chat.

Prerequisite: a SchoolNossa account with an active subscription. Data access: reads school data, and reads and writes the user's own shortlist.

**Data handling** (Claude step 7): own API (SchoolNossa's database); no health data; no sponsored content; no financial transactions.

**Example / starter prompts:**
1. Which cities does SchoolNossa cover?
2. Find public Gymnasien in Berlin-Pankow with more than 800 students.
3. Show the details and Abitur results of the Carl-von-Ossietzky-Gymnasium in Berlin.
4. Which secondary schools in Utrecht offer VWO?
5. Pin the Heinrich-Schliemann-Gymnasium to my shortlist and show my shortlist.

## Reviewer instructions (draft; fill in the reviewer credentials)

1. **Claude:** Settings → Connectors → Add → Add custom connector. URL: `https://whzvzoumldeqgyrqlilt.supabase.co/functions/v1/mcp`.
   **ChatGPT:** the connector from the directory listing, or Plugins → Add → Add custom MCP server with the same URL and OAuth.
2. Connect. On the SchoolNossa sign-in page, use the reviewer credentials below, then press "Approve" on the consent page.
3. Run the five example prompts above. Together they exercise every tool:
   - `list_cities`
   - `search_schools`
   - `get_school`
   - `pin_school` + `list_pinned_schools`
   - `unpin_school` (e.g. "remove it again")
4. Reviewer account: `<email>` / `<password>`. It's a paid subscription and the sign-in code is waived for this account.

## Compliance notes

- **ChatGPT commerce rules:** the connector sells nothing and shows no plans, prices or checkout links.
  - Non-subscribers get "Using SchoolNossa in ChatGPT and Claude requires an active SchoolNossa subscription … More information: https://schoolnossa.com".
  - That is an informational link, which the guidelines allow.
- **Tool annotations:** title + readOnlyHint + destructiveHint + openWorldHint, explicit on all six tools (destructiveHint added to the read-only tools 2026-10-25).
- **Response data:** school records and the user's shortlist only. School ids are returned because the follow-up tools need them; there are no session or trace ids.
- **ChatGPT model training:** ChatGPT's connect screen says data from an app may be used to train OpenAI models if the user has "Improve the model for everyone" enabled. That is the user's ChatGPT setting, but the privacy section should mention it.

## Draft privacy-policy section (needs José's approval)

**DE: Nutzung über KI-Assistenten (ChatGPT, Claude)**

Sie können Ihr SchoolNossa-Konto mit KI-Assistenten wie ChatGPT (OpenAI) oder Claude (Anthropic) verbinden. Die Verbindung erfolgt über OAuth: Sie melden sich bei SchoolNossa an und bestätigen den Zugriff. Ihr Passwort wird dabei nicht an den Anbieter des Assistenten weitergegeben.

Der Assistent erhält nur die Daten, die er über die SchoolNossa-Werkzeuge abfragt: Schuldaten und Ihre Merkliste. Diese Inhalte werden an den Anbieter des Assistenten übermittelt und dort nach dessen Datenschutzbestimmungen verarbeitet. Je nach Ihren Einstellungen beim Anbieter können sie auch zum Training von KI-Modellen verwendet werden.

Zur Bereitstellung des Zugriffs speichern wir die Registrierung des Assistenten und die Zugriffstoken der Verbindung, bis Sie die Verbindung trennen oder die Token ablaufen. *[Falls Werkzeugaufrufe protokolliert werden, z. B. zur Missbrauchsabwehr: Art der Aufrufe, Zeitpunkt und Speicherdauer ergänzen.]*

Sie können die Verbindung jederzeit in den Einstellungen von ChatGPT bzw. Claude trennen. Fragen: info@schoolnossa.com.

**EN: Use through AI assistants (ChatGPT, Claude)**

You can connect your SchoolNossa account to AI assistants such as ChatGPT (OpenAI) or Claude (Anthropic). The connection uses OAuth: you sign in to SchoolNossa and approve access. Your password is never shared with the assistant's provider.

The assistant only receives the data it requests through SchoolNossa's tools: school data and your shortlist. This content is sent to the assistant's provider and processed under that provider's privacy policy. Depending on your settings with the provider, it may also be used to train AI models.

To provide access, we store the assistant's client registration and the connection's access tokens until you disconnect or the tokens expire. *[If tool calls are logged, e.g. for abuse prevention: add what is logged, when, and for how long.]*

You can disconnect at any time in your ChatGPT or Claude settings. Questions: info@schoolnossa.com.
