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

-- school_city_counts() returns counts only and feeds the city/municipality
-- pickers; keep it working for everyone by running it as definer.
ALTER FUNCTION public.school_city_counts(text) SECURITY DEFINER;
ALTER FUNCTION public.school_city_counts(text) SET search_path = public;

-- Rollback:
-- DROP POLICY "Schools readable with active access" ON public.schools;
-- DROP POLICY "Primary schools readable with active access" ON public.primary_schools;
-- DROP POLICY "School similarities readable with active access" ON public.school_similarities;
-- CREATE POLICY "Schools are publicly readable" ON public.schools FOR SELECT USING (true);
-- CREATE POLICY "Primary schools are publicly readable" ON public.primary_schools FOR SELECT USING (true);
-- CREATE POLICY "School similarities are publicly readable" ON public.school_similarities FOR SELECT USING (true);
-- ALTER FUNCTION public.school_city_counts(text) SECURITY INVOKER;
