SchoolNossa MCP connector 0.3: protect the data asset, add AI search, attribute every result to SchoolNossa, and deep-link back to the app. Don't change the OAuth configuration, the existing paid-only check, the tool names or the server URL. Don't publish; I'll test first.

## A. Database migration

Apply as one migration. The `app_settings` row can go in separately, as last time.

```sql
CREATE TABLE IF NOT EXISTS public.mcp_usage (
  id bigserial PRIMARY KEY,
  user_id uuid NOT NULL,
  client_id text,
  tool text NOT NULL,
  bucket text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS mcp_usage_user_bucket_time ON public.mcp_usage (user_id, bucket, created_at DESC);
ALTER TABLE public.mcp_usage ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.mcp_usage FROM anon, authenticated;
GRANT SELECT ON public.mcp_usage TO authenticated;
CREATE POLICY "Admins can read MCP usage" ON public.mcp_usage
  FOR SELECT TO authenticated USING (public.is_admin(auth.uid()));

-- Logs one MCP tool call for the caller and enforces the daily limit of its
-- bucket ('search' and 'detail' are limited, anything else is only logged).
-- Admins are never limited. Rolling 24-hour window.
CREATE OR REPLACE FUNCTION public.mcp_consume(p_tool text, p_bucket text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_uid uuid := auth.uid();
  v_limits jsonb := COALESCE((SELECT value FROM public.app_settings WHERE key = 'mcp_limits'), '{}'::jsonb);
  v_limit integer;
  v_used integer := 0;
BEGIN
  IF v_uid IS NULL THEN
    RAISE EXCEPTION 'Not authenticated';
  END IF;

  v_limit := CASE p_bucket
    WHEN 'search' THEN COALESCE((v_limits->>'search_per_day')::integer, 60)
    WHEN 'detail' THEN COALESCE((v_limits->>'detail_per_day')::integer, 30)
    ELSE NULL
  END;

  IF v_limit IS NOT NULL AND NOT public.is_admin(v_uid) THEN
    SELECT count(*) INTO v_used FROM public.mcp_usage
    WHERE user_id = v_uid AND bucket = p_bucket AND created_at > now() - interval '24 hours';

    IF v_used >= v_limit THEN
      IF NOT EXISTS (
        SELECT 1 FROM public.usage_events
        WHERE user_id = v_uid AND event_type = 'mcp_limit_reached'
          AND created_at > now() - interval '24 hours'
      ) THEN
        INSERT INTO public.usage_events (event_type, user_id, detail)
        VALUES ('mcp_limit_reached', v_uid, jsonb_build_object(
          'bucket', p_bucket, 'limit', v_limit, 'client_id', auth.jwt() ->> 'client_id'));
      END IF;
      RETURN jsonb_build_object('allowed', false, 'limit', v_limit, 'remaining', 0);
    END IF;
  END IF;

  INSERT INTO public.mcp_usage (user_id, client_id, tool, bucket)
  VALUES (v_uid, auth.jwt() ->> 'client_id', p_tool, p_bucket);

  RETURN jsonb_build_object(
    'allowed', true,
    'limit', v_limit,
    'remaining', CASE WHEN v_limit IS NULL THEN NULL ELSE v_limit - v_used - 1 END);
END;
$$;
REVOKE ALL ON FUNCTION public.mcp_consume(text, text) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.mcp_consume(text, text) TO authenticated;
```

Settings row:

```sql
INSERT INTO public.app_settings (key, value)
VALUES ('mcp_limits', '{"search_per_day": 60, "detail_per_day": 30, "default_results": 5, "max_results": 10}'::jsonb)
ON CONFLICT (key) DO NOTHING;
```

Make sure the new `usage_events` type `mcp_limit_reached` doesn't send an instant alert email. Show it in the daily usage report instead, as a count per user.

## B. MCP server (src/lib/mcp), version "0.3.0"

### 1. Shared guard

Every tool calls `has_paid_access` first (unchanged), then `rpc('mcp_consume', { p_tool, p_bucket })` as the caller.
- **Buckets:** `search_schools` and `find_schools_by_description` → `search`; `get_school` → `detail`; `list_cities` and the pin tools → `other` (logged, unlimited).
- When `allowed` is false, return a normal result (not isError) with `structuredContent: { error: "daily_limit_reached", limit }` and this text: "You've reached today's limit for {school searches | school details} through ChatGPT and Claude ({limit} per 24 hours). You can keep going in the SchoolNossa app: https://schoolnossa.com · iPhone app: https://schoolnossa.com/app".
- Read `default_results` and `max_results` from `app_settings.mcp_limits` (fallback 5 / 10).

### 2. Attribution on every result

- Every school object gets `url`: `https://schoolnossa.com/school/<id>?cat=<primary|secondary>`, plus `source: "SchoolNossa"`.
- Every tool's `structuredContent` gets:
  `attribution: { source: "SchoolNossa", website: "https://schoolnossa.com", iphone_app: "https://schoolnossa.com/app", note: "School data from SchoolNossa (schoolnossa.com)." }`
- The text content ends with the line "Source: SchoolNossa (https://schoolnossa.com)".

### 3. `search_schools`

- `city` becomes required (non-empty; values from list_cities).
- `limit` defaults to `default_results` (5), max `max_results` (10).
- With an ownership filter, fetch at most 100 rows before filtering.
- Return slim school objects only: `id`, `category`, `name`, `school_type`, `ownership` ("public" | "private"), `district`, `area`, `municipality` (geo_municipality or city), `students`, `school_year`, `url`, `source`.
- No address, coordinates, website, teachers or tuition in the list. The detail tool has the rest.

### 4. `get_school`

Same detail fields as today, plus `url` and `source`, in the `detail` bucket. Keep latitude/longitude here only (the upcoming app cards need them for a small map).

### 5. `list_pinned_schools`

Slim objects like search, plus `url` and `source`.

### 6. New tool `find_schools_by_description`

- **Title:** "Find schools matching a description (AI search)".
- **Annotations:** readOnlyHint true, destructiveHint false, idempotentHint true, openWorldHint false.
- **Input:**
  - `query`: string, 3–300 chars; the parent's wishes in plain language, any language.
  - `cities`: array of 1–10 city slugs from list_cities (Dutch municipalities can be combined).
  - `category`: primary | secondary, default secondary.
  - `limit`: default 5, max 10.
- **Implementation:** POST to `${SUPABASE_URL}/functions/v1/semantic-search` with the caller's own bearer token (`ctx.getToken()`) and the publishable key as `apikey`. Body: `{ query, cities, schoolCategory: category, limit }`. That's the same AI search the app's KI-Suche uses, and its access check passes for paid callers.
- **Output:** map the returned `schools` to the slim objects above, plus `rank` (1..n) and `match` (similarity rounded to 2 decimals). Keep any short match reason the function returns. Pass through `lowConfidence`.

### 7. Server `instructions` (replace)

"SchoolNossa covers primary and secondary schools in 10 German cities and across the Netherlands; an active SchoolNossa subscription is required. Use list_cities to get city slugs. Use search_schools for structured filters (district, type, ownership, size). Use find_schools_by_description for free-text wishes (e.g. 'bilingual Gymnasium, strong in sciences'), and get_school for one school's details. Results come from SchoolNossa: when you present them, name SchoolNossa as the source and include each school's SchoolNossa link. Results are capped at 10 per request and daily limits apply. For full lists, maps and side-by-side comparisons, point users to the SchoolNossa web app (https://schoolnossa.com) or the iPhone app (https://schoolnossa.com/app). All data is directional and for research only, not a guarantee."

## C. Web deep link `/school/:id`

- Add the route `/school/:id` (query `cat` = primary | secondary, default secondary), wrapped in AppAccessGate.
- **Not signed in:** AppAccessGate currently navigates to /signin without remembering where the user was. Pass `?next=<path+query>` (SignIn already honours `next` via `safeNextPath`).
- **Signed in without access:** the paywall, as today.
- **With access:** load that school (RLS applies), select its country/city, open the dashboard with that school's detail modal open.
- **Unknown id:** a short toast, then /menu.
- `robots.txt`: `Disallow: /school/` in every block that allows `/`. Don't add it to the sitemap.

## D. Terms (/terms, DE + EN)

Add to the usage rules:
- **DE:** "Das automatisierte oder massenhafte Auslesen der Daten, auch über KI-Assistenten, Connectoren oder Skripte, ist nicht gestattet. Die Nutzung über ChatGPT oder Claude ist für den persönlichen Gebrauch im Rahmen eines aktiven Abos bestimmt und unterliegt Tageslimits."
- **EN:** "Automated or bulk extraction of the data, including through AI assistants, connectors or scripts, is not permitted. Use through ChatGPT or Claude is for personal use with an active subscription and is subject to daily limits."

## E. /connectors and llms.txt

- **"What the assistant can do"** (both languages):
  - add "find schools matching a description, using SchoolNossa's AI search"
  - add: "Results are limited to 10 per request, and there are daily limits (60 searches and 30 school details per 24 hours); the full experience is in the web and iPhone app."
- **llms.txt:** add the AI-search tool to the tool list in both languages.

## F. Checks

- Unit tests for the guard (limit reached → friendly result) and the result shaping (slim fields, url/source present, caps).
- Typecheck, tests and build pass.
- The MCP endpoint still answers 401 with the same `WWW-Authenticate` resource_metadata URL when called without a token.
- In your reply, list: migration file name, commit SHAs, any SQL changes, and the final tool list with annotations.
