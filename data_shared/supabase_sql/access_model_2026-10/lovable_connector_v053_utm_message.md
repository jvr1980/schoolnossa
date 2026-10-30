Connector 0.5.3: tag every SchoolNossa link from the connector with UTM parameters, and count those visits on the website. Don't change tools, inputs, limits, OAuth, or anything the model sees apart from the link URLs. Don't publish.

## A. Tag the links

**Helper:** `trackedUrl(url, source, content)`
- Appends `utm_source=<source>&utm_medium=ai_connector&utm_campaign=mcp&utm_content=<content>` and keeps existing parameters (e.g. `?cat=secondary`).
- Only for `https://schoolnossa.com` URLs. Leave every other URL untouched (OpenStreetMap and CARTO attribution).

**Source** comes from the caller's OAuth client:
- `auth.jwt()->>'client_id'` → `auth.oauth_clients.client_name`. Today there are two clients, "ChatGPT" and "Claude".
- Mapping: ChatGPT → `chatgpt`; Claude → `claude`; any other client, or none → `ai_assistant`.
- Use a small SECURITY DEFINER function, e.g. `public.mcp_client_source()`, executable by `authenticated` only. Resolve it once per tool call.

**Content values:**
| Value | Where |
|---|---|
| `answer` | the model-visible `url` of each school (structuredContent and text) |
| `card_open` | "In SchoolNossa öffnen / Open in SchoolNossa" and "Details in SchoolNossa" buttons |
| `card_more` | "+N weitere in SchoolNossa" and "mehr in SchoolNossa" |
| `card_map` | "Gesamte Karte in SchoolNossa öffnen / Open the full map in SchoolNossa" |
| `card_places` | "Orte in SchoolNossa verwalten / Manage places in SchoolNossa" |
| `card_footer` | the footer schoolnossa.com link |
| `app` | every iPhone-app link (`/app`), wherever it appears |
| `connectors` | `/connectors` links (subscription notice) |
| `message` | links inside text messages (top-3 trailer, limit reached, no saved places, place errors) |
| `attribution` | the attribution block (`website`, `iphone_app`) and the "Source: SchoolNossa (…)" line |

**Card side:**
- Pass the resolved source to the card, e.g. as `attribution.link_source`.
- The card builds its own links with `trackedUrl`, swapping `utm_content` on the school URL for its buttons.
- `isAllowedLink` must still accept the tagged URLs, and still only schoolnossa.com.

**Version:** bump to "0.5.3", so the card URI becomes `ui://schoolnossa/schools.v0.5.3.html`. Keep answering the older URIs with the new HTML.

## B. Count the visits on the website (no personal data)

**Table** `public.connector_visits`:
- Columns: `id` bigserial, `created_at` timestamptz default now(), `source` text, `content` text, `path` text.
- RLS on. The only policy is SELECT for admins (`public.is_admin(auth.uid())`).

**RPC** `public.log_connector_visit(p_source text, p_content text, p_path text)`:
- SECURITY DEFINER; execute granted to `anon` and `authenticated`.
- Accepts only the source and content values listed in A; anything else is a no-op.
- Stores the path without its query string, at most 200 characters.
- No user id, IP, user agent or cookie.

**Web app:** once at startup, at the router level:
- If the URL has `utm_medium=ai_connector`, call the RPC with `utm_source`, `utm_content` and `location.pathname`.
- Then remove all `utm_*` parameters from the address bar with `history.replaceState`. Keep `cat` and any other parameters. This stops reloads and shared links from being counted twice.
- Don't use cookies, localStorage or sessionStorage for this: storing on the device would need consent under §25 TTDDDG.
- It must never block rendering, and errors are ignored. Signed in or not doesn't matter.
- Count the visit before any redirect (e.g. to /signin), and keep the existing return-to-page behaviour.

## C. Checks

- **Tests for:**
  - the helper keeps existing parameters and only tags schoolnossa.com
  - each content value appears where listed
  - the source mapping (ChatGPT, Claude, another client, no client)
  - external links stay untouched
  - the RPC ignores unknown values
  - the URL cleanup keeps `cat`
- Typecheck, tests and build pass. Without a token, the MCP endpoint still returns the same 401 challenge.
- **In your reply:** the migration name, the function names, and one SQL query that lists visits per day, source and content for the last 30 days.
