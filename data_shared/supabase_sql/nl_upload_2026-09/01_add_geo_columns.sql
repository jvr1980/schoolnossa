-- Country -> region -> municipality hierarchy (additive; nothing reads these yet).
ALTER TABLE public.schools
  ADD COLUMN IF NOT EXISTS geo_country text,
  ADD COLUMN IF NOT EXISTS geo_region text,
  ADD COLUMN IF NOT EXISTS geo_municipality text;
ALTER TABLE public.primary_schools
  ADD COLUMN IF NOT EXISTS geo_country text,
  ADD COLUMN IF NOT EXISTS geo_region text,
  ADD COLUMN IF NOT EXISTS geo_municipality text;
