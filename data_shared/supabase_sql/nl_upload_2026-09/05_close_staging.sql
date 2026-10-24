DROP TABLE IF EXISTS public.nl_stage_schools;          -- drops its policy too
DROP TABLE IF EXISTS public.nl_stage_primary_schools;
NOTIFY pgrst, 'reload schema';
