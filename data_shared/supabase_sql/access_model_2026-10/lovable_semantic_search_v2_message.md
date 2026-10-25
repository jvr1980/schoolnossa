Improve the AI search (`semantic-search`), used by the web app's KI-Suche, the iPhone app and the MCP tool find_schools_by_description. Keep the request and response contract (fields, `returnAll` mode, `city` / `cities`, `schoolCategory`, `limit`) so the published iPhone app keeps working. Don't publish.

## Problem (measured on live data, 2026-10-25)

All school embeddings (gemini-embedding-001, 768 d) are very similar to each other: two Berlin schools average 0.84 cosine similarity, min 0.68. So every query scores almost the same against every school. For "bilingual Gymnasium with a strong science profile" in Berlin:
- the top 8 raw scores are 0.741–0.753
- the p10–p90 spread over all 260 schools is only 0.065
- the ranking is close to arbitrary

Subtracting the mean school vector (centering) before the cosine:
- raises the spread to 0.219 (3.4×)
- brings science-profile schools to the top: Manfred-von-Ardenne, Marie-Curie, Willi-Graf, Humboldt, Archenhold

The function is also slow (30–60 s via the connector), because it downloads every school's embedding through PostgREST and scores them in Deno.

## 1. Rank inside the database (migration)

Add `public.semantic_rank_schools(p_query vector(768), p_category text, p_cities text[], p_school_type text DEFAULT NULL)`:
- SECURITY DEFINER, `SET search_path = public`
- EXECUTE for service_role only (REVOKE from PUBLIC, anon, authenticated)
- Scope: the rows of `schools` (secondary) or `primary_schools` (primary) whose `city` is in `p_cities`, with a non-null embedding, optionally `school_type ILIKE '%' || p_school_type || '%'`. Always exclude non-operating schools (`schulname ILIKE '%kein schulbetrieb%'`).
- Centroid: `avg(embedding)` over all rows of the same table and the same `geo_country` as the scope. Use the whole country, not just the scope, so a single small Dutch municipality still gets a stable centroid. Exclude non-operating schools here too.
- Return per school: `id`, `centered_score = 1 - ((embedding - centroid) <=> (p_query - centroid))`, `raw_similarity = 1 - (embedding <=> p_query)`, ordered by centered_score DESC.
- If the database lacks `avg(vector)` or vector subtraction, say so and propose the closest alternative before writing anything.

## 2. Use it in the edge function

- Keep the translation step and the query embedding (RETRIEVAL_QUERY, 768 d).
- Replace the "fetch all embeddings + cosine in Deno" step with one `rpc('semantic_rank_schools', …)` call through the existing service-role client. Then fetch the display fields for the top `limit` ids (or only the ids in `returnAll` mode), as today.
- **similarity** (kept 0..1 for clients): `similarity = (centered_score + 1) / 2`. It's monotonic, so clients that sort by it keep working.
- **Language boost:** keep it, sized for the new scale: add 0.15 to centered_score (not 0.5) for a matching language, then clamp to 1.
- **lowConfidence:** true when the top centered_score < 0.10. Make that a constant, and log the top centered and raw scores per request.
- New optional request field `schoolType` (string, partial match) → `p_school_type`. In the MCP tool `find_schools_by_description`, add an optional `schoolType` input, passed through, and return `match` = centered_score rounded to 2 decimals.

## 3. Verify (report the numbers)

For Berlin, secondary, query "bilingualer Gymnasium mit starkem naturwissenschaftlichem Profil", report:
- the top 8 with centered scores, before and after
- the p10–p90 spread of centered scores
- the response time of the function, before and after

Also check:
- `returnAll` mode returns every school in scope with a similarity, sorted.
- The web KI-Suche and `find_schools_by_description` still work.
- A single small Dutch municipality returns sensible results.
