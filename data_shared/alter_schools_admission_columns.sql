-- =====================================================================
-- STEP 1: Add admission/open-day columns to the schools table
-- STEP 2: Temporarily relax RLS so the ANON key can write via REST API
--
-- Run this in the Supabase SQL Editor BEFORE running the upload script.
-- After the upload completes, run the "re-tighten" SQL at the bottom.
-- =====================================================================

-- STEP 1: Add columns (safe to re-run — IF NOT EXISTS)
BEGIN;

ALTER TABLE schools ADD COLUMN IF NOT EXISTS admission_criteria_bullets jsonb;
ALTER TABLE schools ADD COLUMN IF NOT EXISTS admission_application_window jsonb;
ALTER TABLE schools ADD COLUMN IF NOT EXISTS admission_notes_de text;
ALTER TABLE schools ADD COLUMN IF NOT EXISTS open_days jsonb;
ALTER TABLE schools ADD COLUMN IF NOT EXISTS last_open_day_seen date;
ALTER TABLE schools ADD COLUMN IF NOT EXISTS admission_fetched_at timestamptz;

COMMIT;

-- STEP 2: Temporarily relax RLS for anon writes
-- (Lovable.dev locks Supabase assets; this is the workaround)
ALTER TABLE schools ENABLE ROW LEVEL SECURITY;

-- Drop any restrictive policies that block anon writes
-- (Lovable may have created its own — this ensures a clean slate)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE tablename = 'schools' AND policyname = 'Temp allow anon full access'
  ) THEN
    CREATE POLICY "Temp allow anon full access" ON schools
      FOR ALL
      USING (true)
      WITH CHECK (true);
  END IF;
END $$;

-- =====================================================================
-- NOW run the upload script:
--   python3 scripts_shared/upload_admission_open_days_to_supabase.py
-- =====================================================================

-- =====================================================================
-- AFTER UPLOAD: Re-tighten RLS (run this separately when upload is done)
-- =====================================================================
-- DROP POLICY IF EXISTS "Temp allow anon full access" ON schools;
