\set ON_ERROR_STOP on
\pset footer off
CREATE FUNCTION pg_temp.act(p_sub text, p_client text DEFAULT NULL) RETURNS void LANGUAGE sql AS $$
  SELECT set_config('request.jwt.claims',
    CASE WHEN p_sub IS NULL THEN '{"role":"anon"}'
         ELSE jsonb_build_object('sub', p_sub, 'role', 'authenticated')
              || CASE WHEN p_client IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('client_id', p_client) END
    END::text, false) $$;

\echo '== T9 backfill: expired tester extended, manual end + paid untouched, no trial_started events'
SELECT u.email, a.access_type, a.test_manually_ended,
       round(extract(epoch FROM a.test_ended_at - now())/3600) AS hours_left, a.subscription_ends_at::date
FROM public.user_access a JOIN auth.users u ON u.id = a.user_id ORDER BY u.email;
DO $$ BEGIN
  ASSERT (SELECT round(extract(epoch FROM test_ended_at - now())/3600) FROM public.user_access WHERE user_id='00000000-0000-0000-0000-000000000001') = 48;
  ASSERT (SELECT test_ended_at < now() FROM public.user_access WHERE user_id='00000000-0000-0000-0000-000000000005');
  ASSERT (SELECT subscription_ends_at = '2027-12-31' AND test_ended_at IS NULL FROM public.user_access WHERE user_id='00000000-0000-0000-0000-000000000003');
  ASSERT (SELECT count(*) FROM auth.users u WHERE NOT EXISTS (SELECT 1 FROM public.user_access a WHERE a.user_id=u.id)) = 0;
  ASSERT (SELECT count(*) FROM public.usage_events WHERE user_id IN ('00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000004')) = 0;  -- extended by the backfill: no event
  ASSERT (SELECT test_ended_at <= test_started_at FROM public.user_access WHERE user_id='00000000-0000-0000-0000-0000000000a2');
END $$;
\echo '   re-signups after the migration stay closed'

\echo '== T10 stage-2 RLS on schools: rows visible per caller'
CREATE TEMP TABLE seen (who text, first_party bigint, oauth bigint, city_counts bigint);
GRANT ALL ON seen TO authenticated, anon;
SET ROLE authenticated;
DO $$
DECLARE r record; a bigint; b bigint; c bigint;
BEGIN
  FOR r IN SELECT * FROM (VALUES
      ('trial',   '00000000-0000-0000-0000-000000000001'),
      ('ended',   '00000000-0000-0000-0000-000000000005'),
      ('paid',    '00000000-0000-0000-0000-000000000003'),
      ('admin',   '00000000-0000-0000-0000-000000000004')) v(who, sub)
  LOOP
    PERFORM pg_temp.act(r.sub);                   SELECT count(*) INTO a FROM public.schools;
    SELECT sum(school_count) INTO c FROM public.school_city_counts('secondary');
    PERFORM pg_temp.act(r.sub, 'claude-client');  SELECT count(*) INTO b FROM public.schools;
    INSERT INTO seen VALUES (r.who, a, b, c);
  END LOOP;
END $$;
SELECT pg_temp.act(NULL);
SET ROLE anon;
INSERT INTO seen SELECT 'anon', (SELECT count(*) FROM public.schools), NULL, (SELECT sum(school_count) FROM public.school_city_counts('secondary'));
RESET ROLE;
SELECT * FROM seen;
DO $$ BEGIN
  ASSERT (SELECT first_party = 2 AND oauth = 0 FROM seen WHERE who='trial');
  ASSERT (SELECT first_party = 0 AND oauth = 0 FROM seen WHERE who='ended');
  ASSERT (SELECT first_party = 2 AND oauth = 2 FROM seen WHERE who='paid');
  ASSERT (SELECT first_party = 2 AND oauth = 2 FROM seen WHERE who='admin');
  ASSERT (SELECT first_party = 0 FROM seen WHERE who='anon');
  ASSERT (SELECT bool_and(city_counts = 2) FROM seen);  -- counts keep working for everyone
END $$;
\echo 'ALL ASSERTIONS PASSED'
