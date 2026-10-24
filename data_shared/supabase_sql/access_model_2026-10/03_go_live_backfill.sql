-- Go-live (run right after the stage-1 frontend is published, via the
-- Lovable SQL tool): every existing account without an active paid
-- subscription gets ONE fresh trial window starting now. Their emails are
-- already in trial_claims (seeded by the stage-1 migration), so deleting the
-- account and signing up again will not give another one.
-- Only accounts that existed when the stage-1 migration ran (their claim has
-- source 'backfill') are touched, so a re-signup in between stays closed.
-- Active paid accounts (incl. the App Review demo account) and test rows an
-- admin ended manually are not touched. Usage events are suppressed so the
-- alerting does not send one "trial started" email per account.

BEGIN;
SET LOCAL app.suppress_usage_events = 'on';

-- a) accounts that never got an access row (Google/Apple sign-ins)
INSERT INTO public.user_access
  (user_id, access_type, test_started_at, test_ended_at, test_manually_ended, login_count)
SELECT u.id, 'test', now(),
       now() + make_interval(hours => COALESCE(
         (SELECT (value->>'hours')::int FROM public.app_settings WHERE key = 'trial_hours'), 48)),
       false, 0
FROM auth.users u
WHERE NOT EXISTS (SELECT 1 FROM public.user_access a WHERE a.user_id = u.id)
  AND u.id IN (SELECT first_user_id FROM public.trial_claims WHERE source = 'backfill')
ON CONFLICT (user_id) DO NOTHING;

-- b) expired test accounts: move the end of their window (start kept as history)
UPDATE public.user_access
SET test_ended_at = now() + make_interval(hours => COALESCE(
      (SELECT (value->>'hours')::int FROM public.app_settings WHERE key = 'trial_hours'), 48)),
    updated_at = now()
WHERE access_type = 'test'
  AND COALESCE(test_manually_ended, false) = false
  AND (test_ended_at IS NULL OR test_ended_at < now())
  AND user_id IN (SELECT first_user_id FROM public.trial_claims WHERE source = 'backfill');

-- verify before COMMIT: every account has a row; no active-paid row changed
SELECT access_type,
       count(*) AS n,
       count(*) FILTER (WHERE access_type = 'test' AND test_ended_at > now()) AS active_trial
FROM public.user_access GROUP BY 1;

COMMIT;
