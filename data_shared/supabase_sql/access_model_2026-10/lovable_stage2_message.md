Stage 2 of the access model: lock the school tables. Stage 1 is published, and existing accounts got their launch trial. Apply this SQL as one migration, verbatim, and change nothing else (no frontend or edge-function changes). If anything fails, tell me exactly what and stop.

```sql
-- Stage 2 (apply only AFTER the stage-1 frontend is published: the landing
-- page and /data-coverage must already read public_school_stats()).
-- School data becomes readable only with an active trial or paid
-- subscription; OAuth-client tokens (ChatGPT/Claude) need paid
-- (has_app_access() handles both).

DROP POLICY IF EXISTS "Schools are publicly readable" ON public.schools;
DROP POLICY IF EXISTS "Primary schools are publicly readable" ON public.primary_schools;
DROP POLICY IF EXISTS "School similarities are publicly readable" ON public.school_similarities;

CREATE POLICY "Schools readable with active access" ON public.schools
  FOR SELECT TO authenticated USING ((SELECT public.has_app_access()));
CREATE POLICY "Primary schools readable with active access" ON public.primary_schools
  FOR SELECT TO authenticated USING ((SELECT public.has_app_access()));
CREATE POLICY "School similarities readable with active access" ON public.school_similarities
  FOR SELECT TO authenticated USING ((SELECT public.has_app_access()));

-- trial_claims got Supabase's default full table privileges (incl. TRUNCATE,
-- which RLS does not cover). Clients only need what the admin policies use.
REVOKE ALL ON public.trial_claims FROM anon, authenticated;
GRANT SELECT, DELETE ON public.trial_claims TO authenticated;

-- school_city_counts() returns counts only and feeds the city/municipality
-- pickers; keep it working for everyone by running it as definer.
ALTER FUNCTION public.school_city_counts(text) SECURITY DEFINER;
ALTER FUNCTION public.school_city_counts(text) SET search_path = public;
```

Afterwards, please confirm:
(1) the migration file name;
(2) in the preview, signed in as the admin, the dashboard still loads schools and the city/municipality picker shows counts;
(3) signed out, the landing page and /data-coverage still show their numbers.
Don't publish; no publish is needed, since this is database-only.