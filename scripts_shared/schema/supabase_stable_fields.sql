-- Stable (year-agnostic) columns for the SchoolNossa Supabase (Lovable Cloud).
-- Additive only: no existing column is renamed or dropped, so the Lovable app
-- keeps working unchanged and can migrate to these names at its own pace.
--
-- Run via the Lovable MCP SQL tool or paste into the Lovable agent chat.
-- After this DDL, populate with:
--   python3 scripts_shared/upload_to_supabase.py --groups stable --dry-run
--   python3 scripts_shared/upload_to_supabase.py --groups stable
-- (fill-gaps semantics: new columns start NULL everywhere, so the existing
--  uploader fills them without any overwrite risk; RLS: same coordination as
--  the April 2026 backfill, or service-role key)

ALTER TABLE schools
  ADD COLUMN IF NOT EXISTS schueler_current integer,
  ADD COLUMN IF NOT EXISTS lehrer_current integer,
  ADD COLUMN IF NOT EXISTS migration_current numeric,
  ADD COLUMN IF NOT EXISTS nachfrage_prozent_current numeric,
  ADD COLUMN IF NOT EXISTS abitur_durchschnitt_current numeric,
  ADD COLUMN IF NOT EXISTS crime_total_crimes_current numeric,
  ADD COLUMN IF NOT EXISTS data_school_year text,
  ADD COLUMN IF NOT EXISTS abitur_year text,
  ADD COLUMN IF NOT EXISTS crime_data_year text;

ALTER TABLE primary_schools
  ADD COLUMN IF NOT EXISTS schueler_current integer,
  ADD COLUMN IF NOT EXISTS lehrer_current integer,
  ADD COLUMN IF NOT EXISTS migration_current numeric,
  ADD COLUMN IF NOT EXISTS crime_total_crimes_current numeric,
  ADD COLUMN IF NOT EXISTS data_school_year text,
  ADD COLUMN IF NOT EXISTS crime_data_year text;

-- Semantics (documented in scripts_shared/schema/stable_fields.py):
--   *_current        = newest available value for that metric, whatever its year
--   data_school_year = school year the schueler/lehrer values come from ('2024_25' style;
--                      NULL when the value came from an aggregate of unknown vintage)
--   abitur_year      = calendar year of abitur_durchschnitt_current
--   crime_data_year  = calendar year of crime_total_crimes_current
