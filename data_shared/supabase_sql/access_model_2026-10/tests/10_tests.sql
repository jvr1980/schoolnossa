\set ON_ERROR_STOP on
\pset footer off
-- helper: act as a user (optionally an OAuth client token)
CREATE FUNCTION pg_temp.act(p_sub text, p_client text DEFAULT NULL) RETURNS void LANGUAGE sql AS $$
  SELECT set_config('request.jwt.claims',
    CASE WHEN p_sub IS NULL THEN '{"role":"anon"}'
         ELSE jsonb_build_object('sub', p_sub, 'role', 'authenticated')
              || CASE WHEN p_client IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('client_id', p_client) END
    END::text, false) $$;

\echo '== T1 normalize_email'
SELECT e, public.normalize_email(e) FROM (VALUES ('Jose.Von+x@GoogleMail.com'), ('A.B+c@Example.COM'), ('plain'), (''), (NULL)) v(e);
DO $$ BEGIN
  ASSERT public.normalize_email('Jose.Von+x@GoogleMail.com') = 'josevon@gmail.com';
  ASSERT public.normalize_email('A.B+c@Example.COM') = 'a.b@example.com';
  ASSERT public.normalize_email(NULL) IS NULL AND public.normalize_email('') IS NULL;
END $$;

\echo '== T2 claims seeded for all 5 existing accounts'
DO $$ BEGIN ASSERT (SELECT count(*) FROM public.trial_claims) = 5; END $$;

\echo '== T3 brand-new signup gets a 48h trial + one trial_started event'
INSERT INTO auth.users (id, email) VALUES ('00000000-0000-0000-0000-0000000000a1', 'new.parent@gmail.com');
SELECT access_type, round(extract(epoch FROM test_ended_at - test_started_at)/3600) AS hours
FROM public.user_access WHERE user_id = '00000000-0000-0000-0000-0000000000a1';
DO $$ BEGIN
  ASSERT (SELECT round(extract(epoch FROM test_ended_at - test_started_at)/3600) FROM public.user_access WHERE user_id='00000000-0000-0000-0000-0000000000a1') = 48;
  ASSERT (SELECT count(*) FROM public.usage_events WHERE event_type='trial_started' AND user_id='00000000-0000-0000-0000-0000000000a1') = 1;
END $$;

\echo '== T4 delete account (edge-function order: rows, then auth user) and re-sign-up with a Gmail variant -> closed window, no event'
DELETE FROM public.user_access WHERE user_id = '00000000-0000-0000-0000-0000000000a1';
DELETE FROM auth.users WHERE id = '00000000-0000-0000-0000-0000000000a1';
INSERT INTO auth.users (id, email) VALUES ('00000000-0000-0000-0000-0000000000a2', 'New.Parent+again@googlemail.com');
SELECT access_type, test_ended_at <= test_started_at AS window_closed FROM public.user_access WHERE user_id = '00000000-0000-0000-0000-0000000000a2';
DO $$ BEGIN
  ASSERT (SELECT test_ended_at <= test_started_at FROM public.user_access WHERE user_id='00000000-0000-0000-0000-0000000000a2');
  ASSERT (SELECT count(*) FROM public.usage_events WHERE user_id='00000000-0000-0000-0000-0000000000a2') = 0;
END $$;

\echo '== T5 admin deletes the auth user directly (cascade path) -> claim still blocks re-trial'
INSERT INTO auth.users (id, email) VALUES ('00000000-0000-0000-0000-0000000000b1', 'cascade@example.com');
DELETE FROM auth.users WHERE id = '00000000-0000-0000-0000-0000000000b1';
INSERT INTO auth.users (id, email) VALUES ('00000000-0000-0000-0000-0000000000b2', 'cascade@example.com');
DO $$ BEGIN ASSERT (SELECT test_ended_at <= test_started_at FROM public.user_access WHERE user_id='00000000-0000-0000-0000-0000000000b2'); END $$;

\echo '== T6 clients cannot insert their own row, cannot call grant_initial_access or check_trial_used'
SET ROLE authenticated;
SELECT pg_temp.act('00000000-0000-0000-0000-000000000002');
DO $$ BEGIN
  BEGIN INSERT INTO public.user_access (user_id, access_type, test_ended_at) VALUES ('00000000-0000-0000-0000-000000000002', 'test', '2099-01-01');
        RAISE EXCEPTION 'insert should have failed';
  EXCEPTION WHEN insufficient_privilege THEN NULL; END;
  BEGIN PERFORM public.grant_initial_access('00000000-0000-0000-0000-000000000002', 'x@y.z');
        RAISE EXCEPTION 'grant_initial_access should not be executable';
  EXCEPTION WHEN insufficient_privilege THEN NULL; END;
  BEGIN PERFORM public.check_trial_used('old.tester@gmail.com');
        RAISE EXCEPTION 'check_trial_used should not be executable';
  EXCEPTION WHEN insufficient_privilege THEN NULL; END;
END $$;

\echo '== T7 ensure_user_access for a pre-existing no-row account (claim is its own) -> 48h trial'
SELECT access_type, round(extract(epoch FROM test_ended_at - test_started_at)/3600) AS hours FROM public.ensure_user_access();
DO $$ BEGIN ASSERT (SELECT round(extract(epoch FROM test_ended_at - test_started_at)/3600) FROM public.ensure_user_access()) = 48; END $$;
\echo '   second call is a no-op'
DO $$ BEGIN ASSERT (SELECT count(*) FROM public.ensure_user_access()) = 1; END $$;
RESET ROLE;

\echo '== T8 access matrix (first-party token vs OAuth client token)'
CREATE TEMP TABLE matrix (who text, sub text, app_first_party boolean, app_oauth boolean, paid boolean);
GRANT ALL ON matrix TO authenticated, anon;
SET ROLE authenticated;
DO $$
DECLARE r record; a1 boolean; a2 boolean; p boolean;
BEGIN
  FOR r IN SELECT * FROM (VALUES
      ('expired tester', '00000000-0000-0000-0000-000000000001'),
      ('trial (T7)',     '00000000-0000-0000-0000-000000000002'),
      ('paid demo',      '00000000-0000-0000-0000-000000000003'),
      ('admin',          '00000000-0000-0000-0000-000000000004'),
      ('re-signup',      '00000000-0000-0000-0000-0000000000a2')) v(who, sub)
  LOOP
    PERFORM pg_temp.act(r.sub);                 a1 := public.has_app_access(); p := public.has_paid_access();
    PERFORM pg_temp.act(r.sub, 'chatgpt-client'); a2 := public.has_app_access();
    INSERT INTO matrix VALUES (r.who, r.sub, a1, a2, p);
  END LOOP;
END $$;
SELECT pg_temp.act(NULL);
SET ROLE anon;
INSERT INTO matrix SELECT 'anon', NULL, public.has_app_access(), NULL, public.has_paid_access();
RESET ROLE;
SELECT who, app_first_party, app_oauth, paid FROM matrix;
DO $$ BEGIN
  ASSERT (SELECT NOT app_first_party AND NOT app_oauth AND NOT paid FROM matrix WHERE who='expired tester');
  ASSERT (SELECT app_first_party AND NOT app_oauth AND NOT paid FROM matrix WHERE who='trial (T7)');
  ASSERT (SELECT app_first_party AND app_oauth AND paid FROM matrix WHERE who='paid demo');
  ASSERT (SELECT app_first_party AND app_oauth AND paid FROM matrix WHERE who='admin');
  ASSERT (SELECT NOT app_first_party AND NOT app_oauth FROM matrix WHERE who='re-signup');
  ASSERT (SELECT NOT app_first_party AND NOT paid FROM matrix WHERE who='anon');
END $$;
