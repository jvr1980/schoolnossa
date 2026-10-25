SchoolNossa MCP connector 0.4: an MCP App, so that ChatGPT and Claude show SchoolNossa results as branded SchoolNossa cards that link back to the web and iPhone app. Build on 0.3. Don't change the tools' names, inputs, limits, the paid-only check, the quota guard or the OAuth setup. Don't publish; I'll test first.

## Why a change of approach is needed

`@lovable.dev/mcp-js` (0.26.3 in this project; 3.3.0 on npm) can't attach `_meta.ui.resourceUri` to a tool definition or register custom resources. MCP Apps need both. So take ownership of the MCP server:

- **Preferred:** implement `supabase/functions/mcp` with the official `@modelcontextprotocol/sdk` (Streamable HTTP, stateless) plus `@modelcontextprotocol/ext-apps` server helpers (`registerAppTool`, `registerAppResource`). Port the 0.3 tools 1:1.
- **Fallback, if the official transport doesn't run on Supabase Edge (Deno):** keep the mcp-js handler and wrap it. Serve `resources/list` and `resources/read` yourself, add `_meta.ui` to the matching entries of the `tools/list` response, and advertise `resources` in the `initialize` capabilities.

Either way, these must stay exactly as they are today:
- URL `https://whzvzoumldeqgyrqlilt.supabase.co/functions/v1/mcp`
- Supabase OAuth issuer `https://whzvzoumldeqgyrqlilt.supabase.co/auth/v1` with accepted audience `authenticated`
- the protected-resource metadata document at `/functions/v1/mcp/.well-known/oauth-protected-resource` (same JSON)
- the 401 `WWW-Authenticate: Bearer realm="mcp", resource_metadata="…/functions/v1/mcp/.well-known/oauth-protected-resource"` challenge for missing or invalid tokens
- bearer tokens verified as Supabase JWTs, and the caller's token forwarded to Supabase so RLS applies
- CORS headers as now

Existing Claude and ChatGPT connections must keep working without reconnecting. If you take ownership of the generated file, delete its AUTO-GENERATED banner as its header says, and keep `.lovable/mcp/manifest.json` accurate. Don't let the Vite plugin overwrite the file.

## The UI resource

One resource, `ui://schoolnossa/schools.html`, mimeType `text/html;profile=mcp-app`, served with `registerAppResource`. It renders from the latest tool result (`structuredContent`), received through the MCP Apps bridge (`App` from `@modelcontextprotocol/ext-apps`, `app.connect()` with no transport argument, tool-result notifications).

It must be self-contained: inline CSS and JS, the logo inlined as SVG or a data URI, no external fetches or fonts. So `_meta.ui.csp` declares no `connectDomains`, `resourceDomains` or `frameDomains`. Don't set `_meta.ui.domain` unless a host rejects the resource without it. For Claude the value would be `d0dc1e727c1765970e10154cfc0d3b16.claudemcpcontent.com` (SHA-256 of the server URL, first 32 hex chars).

It renders four views, chosen from the shape of `structuredContent`:

1. **List** (`search_schools`, `find_schools_by_description`, `list_pinned_schools`). One card per school, up to 10:
   - name
   - type chip(s); for Dutch schools, track chips as in the app
   - public/private
   - district or municipality
   - students and school year
   - for AI search: a small "match" indicator in rank order
   - an "Open in SchoolNossa" button that opens the school's `url` via `app.openLink`
2. **Detail** (`get_school`). A header card with name, type, address and website, then key facts like the app's detail modal:
   - students and teachers with years
   - Abitur average and pass rate when present
   - admission criteria and application window when present
   - Dutch-only fields as in the app
   - buttons "Open in SchoolNossa" (school url) and "Get the iPhone app" (https://schoolnossa.com/app)
3. **Limit / subscription message** (`structuredContent.error` = `daily_limit_reached` or `subscription_required`). A friendly card with the same text the tool returns, and buttons to https://schoolnossa.com and https://schoolnossa.com/app. No prices, plans or checkout links (ChatGPT rule).
4. **Empty result.** "No schools found", plus a hint to widen the search.

Every view has a slim footer: SchoolNossa logo + "Data from SchoolNossa · schoolnossa.com · iPhone app".

**Design:**
- Use the web app's look: primary colour, rounded cards, the logo, the same German/English wording.
- Pick the language from the host's locale: German if it starts with "de", otherwise English.
- Support light and dark mode through the host's theme/style variables.
- Lay out cleanly at phone width and in inline display mode. No horizontal scrolling. Keep it compact: about 3 cards visible before the host's own height limit, with a "+N more in SchoolNossa" link.
- Report its height to the host as the ext-apps SDK expects.

## Tool metadata

- Attach the UI (via `registerAppTool` / `_meta.ui.resourceUri`, plus the `openai/outputTemplate` alias if the helper doesn't add it) to: `search_schools`, `find_schools_by_description`, `get_school`, `list_pinned_schools`.
- `list_cities`, `pin_school` and `unpin_school` stay text-only.
- Keep `structuredContent` as defined in 0.3, so hosts without UI support keep working unchanged, and keep the text content with the "Source: SchoolNossa" line.
- Annotations unchanged.
- Bump the version to "0.4.0".

## Links

The only links the UI opens are https://schoolnossa.com (incl. `/school/...` and `/app`). Note this list in your reply; the Claude directory listing declares it as "allowed link URIs".

## Checks (report each)

- Typecheck, tests and build pass. Add tests for the view selection and for HTML escaping: all school fields must be escaped, never injected as raw HTML.
- Without a token, the endpoint returns 401 with exactly the same `WWW-Authenticate` header as before. The protected-resource metadata JSON is byte-identical to before.
- With a valid token, `initialize` → `tools/list` shows the 7 tools, with `_meta.ui.resourceUri` on the four listed tools. `resources/read` of `ui://schoolnossa/schools.html` returns the HTML with mimeType `text/html;profile=mcp-app`.
- Render the HTML locally with a sample list result and a sample detail result, and check both at 375 px and 768 px width in light and dark mode.
