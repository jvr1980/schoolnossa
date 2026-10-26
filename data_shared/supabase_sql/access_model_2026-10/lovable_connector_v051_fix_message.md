Connector 0.5.1: two problems seen in live tests after publishing 0.5. Fix both without changing OAuth, tool names, inputs, limits or the model-visible fields. Don't publish.

## 1. ChatGPT still shows the 0.4 card

After "Refresh tools", ChatGPT renders the old 0.4 layout: slim cards, no expanded facts. It caches the card HTML by its resource URI. Version the URI with each release (e.g. `ui://schoolnossa/schools.v0.5.1.html`, derived from the server version constant), and use the same value in `_meta.ui.resourceUri`, `ui/resourceUri`, `openai/outputTemplate` and `resources/list` / `resources/read`. Keep answering `resources/read` for the old URI too (serving the new HTML), so already-open conversations don't break.

## 2. Card-only texts don't reach the card (Claude)

In Claude, the new layout renders (key facts, "Details in SchoolNossa" fallback), but no description or admission texts appear. So `_meta["schoolnossa/card"]` isn't arriving in `ontoolresult`. Two likely causes; check both and fix:

- **(a) Server side.** In `wrap.ts`, `captureCardMetadata(async () => inner(req))` returns as soon as the inner handler returns its `Response`. If mcp-js streams (SSE) or runs the tool after returning the Response, the AsyncLocalStorage scope is read before the tool has set the card data, so `meta` is undefined and nothing gets injected.
  - Fix: read the inner response body inside the captured scope (`await readMessage(response.clone())` within the `run` callback), then inject `_meta`.
  - Prove it with a test against the real built handler path (createSupabaseHandler + wrapHandler, SSE and JSON response modes, a stubbed authenticated tool context), asserting that the `tools/call` response contains `result._meta["schoolnossa/card"]`.
- **(b) Host side.** If a host doesn't pass result `_meta` to the app, don't rely on it. Use the MCP Apps app-only tool pattern instead:
  - Add a tool `card_details` (input: `schoolIds` 1–3 + `category`) whose `_meta.ui.visibility` is app-only (`["app"]`), so the model can't call it and never sees its result.
  - The card calls it with `app.callServerTool(...)` for the expanded schools when `_meta` is missing, and renders description / admission / contact texts from its result.
  - Same guards as `get_school`: paid check, and each expanded school counts as one `detail`, deduplicated so a school already counted by the list call isn't counted twice in the same call chain. If an id-based dedupe is complicated, count it once in the list call only and make `card_details` count 0.
  - Check that both Claude and ChatGPT honour app-only visibility (MCP Apps spec / ext-apps docs). If a host would show `card_details` to the model, say so before shipping it.

Prefer (a) if it fixes Claude; add (b) as the fallback path in the card either way.

## Checks

- Tests for the URI versioning, the server-side injection (real handler path) and the `card_details` visibility.
- Typecheck, tests and build pass. The MCP endpoint without a token still returns the same 401 challenge.
- In your reply, say which of (a) / (b) was the actual cause, and cite the docs on app-only visibility.
