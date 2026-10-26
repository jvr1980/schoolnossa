-- 2026-10-26: remove the two Berlin secondary schools the source marks as
-- "KEIN Schulbetrieb" (not operating). User-approved. school_similarities rows
-- cascade. Guarded on id + name; rollback: 01_rollback_kein_schulbetrieb.sql.
DELETE FROM public.schools
WHERE id IN ('babd11e6-f96f-48df-b96b-708ea430f6e2', '7d97602e-ed81-4d77-9159-7d37784edb91')
  AND schulname ILIKE '%kein schulbetrieb%'
  AND city = 'berlin';
