-- Temporary, insert-only staging. Created after 01 so it inherits the geo_*
-- columns. anon may INSERT rows whose ids are NL-prefixed and nothing else:
-- no SELECT/UPDATE/DELETE policy exists, so staged rows are unreadable and
-- the live tables are untouched until 04. Dropped in 05.
CREATE TABLE public.nl_stage_schools (LIKE public.schools INCLUDING DEFAULTS);
CREATE TABLE public.nl_stage_primary_schools (LIKE public.primary_schools INCLUDING DEFAULTS);
ALTER TABLE public.nl_stage_schools ADD CONSTRAINT nl_stage_schools_schulnummer_key UNIQUE (schulnummer);
ALTER TABLE public.nl_stage_primary_schools ADD CONSTRAINT nl_stage_primary_schulnummer_key UNIQUE (schulnummer);
ALTER TABLE public.nl_stage_schools ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.nl_stage_primary_schools ENABLE ROW LEVEL SECURITY;
CREATE POLICY nl_stage_insert_only ON public.nl_stage_schools
  FOR INSERT TO anon WITH CHECK (schulnummer LIKE 'NL-%' AND city LIKE 'nl-%');
CREATE POLICY nl_stage_insert_only ON public.nl_stage_primary_schools
  FOR INSERT TO anon WITH CHECK (schulnummer LIKE 'NL-%' AND city LIKE 'nl-%');
REVOKE ALL ON public.nl_stage_schools, public.nl_stage_primary_schools FROM anon, authenticated;
GRANT INSERT ON public.nl_stage_schools, public.nl_stage_primary_schools TO anon;
NOTIFY pgrst, 'reload schema';
