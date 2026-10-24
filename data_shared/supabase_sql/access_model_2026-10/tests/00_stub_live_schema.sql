-- Minimal stand-in for the live Lovable Cloud schema (only what the access
-- migration touches), copied from the live definitions queried 2026-10-24.
CREATE ROLE anon NOLOGIN;
CREATE ROLE authenticated NOLOGIN;
CREATE ROLE service_role NOLOGIN BYPASSRLS;
GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;

CREATE SCHEMA auth;
GRANT USAGE ON SCHEMA auth TO anon, authenticated, service_role;
CREATE TABLE auth.users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  email text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE FUNCTION auth.jwt() RETURNS jsonb LANGUAGE sql STABLE AS $$
  SELECT COALESCE(NULLIF(current_setting('request.jwt.claims', true), ''), '{}')::jsonb $$;
CREATE FUNCTION auth.uid() RETURNS uuid LANGUAGE sql STABLE AS $$
  SELECT NULLIF(auth.jwt() ->> 'sub', '')::uuid $$;
GRANT EXECUTE ON FUNCTION auth.jwt(), auth.uid() TO anon, authenticated, service_role;

CREATE TABLE public.app_settings (key text PRIMARY KEY, value jsonb NOT NULL DEFAULT '{}'::jsonb, updated_at timestamptz NOT NULL DEFAULT now());
INSERT INTO public.app_settings (key, value) VALUES
  ('test_phase_enabled', '{"enabled": true}'), ('free_trial_minutes', '{"minutes": 10}'),
  ('test_phase_max_users', '{"count": 200}'), ('yearly_subscription_price', '{"price": 9.99}');

CREATE TABLE public.user_access (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL UNIQUE REFERENCES auth.users(id) ON DELETE CASCADE,
  access_type text NOT NULL DEFAULT 'free' CHECK (access_type = ANY (ARRAY['test','paid','free','disabled'])),
  test_started_at timestamptz, test_ended_at timestamptz, test_manually_ended boolean DEFAULT false,
  subscription_started_at timestamptz, subscription_ends_at timestamptz,
  login_count integer DEFAULT 0, total_usage_seconds integer DEFAULT 0, free_usage_seconds integer DEFAULT 0,
  last_login_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.user_access ENABLE ROW LEVEL SECURITY;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.user_access TO anon, authenticated, service_role;

CREATE TABLE public.trial_used_emails (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), email text NOT NULL UNIQUE,
  original_access_type text NOT NULL, trial_used_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE public.usage_events (id bigserial PRIMARY KEY, event_type text, user_id uuid, detail jsonb, created_at timestamptz DEFAULT now());
CREATE TABLE public.admin_allowlist (email text PRIMARY KEY);

CREATE FUNCTION public.is_admin(_user_id uuid) RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path TO 'public' AS $$
  SELECT EXISTS (SELECT 1 FROM public.admin_allowlist WHERE admin_allowlist.email = (SELECT email FROM auth.users WHERE id = _user_id)) $$;

CREATE FUNCTION public.check_trial_used(check_email text)
RETURNS TABLE(has_used_trial boolean, original_access_type text, trial_used_at timestamptz)
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public' AS $$
BEGIN
  RETURN QUERY SELECT true, tue.original_access_type, tue.trial_used_at FROM public.trial_used_emails tue WHERE LOWER(tue.email) = LOWER(check_email) LIMIT 1;
  IF NOT FOUND THEN RETURN QUERY SELECT false, NULL::TEXT, NULL::timestamptz; END IF;
END; $$;
GRANT EXECUTE ON FUNCTION public.check_trial_used(text) TO anon, authenticated;

CREATE POLICY "Users can view their own access" ON public.user_access FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users can create own free/test access on signup" ON public.user_access FOR INSERT TO authenticated
  WITH CHECK ((auth.uid() = user_id) AND (access_type = ANY (ARRAY['free','test'])) AND subscription_started_at IS NULL AND subscription_ends_at IS NULL);

-- live pre-migration version of the delete trigger
CREATE FUNCTION public.record_trial_on_account_delete() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'public' AS $$
BEGIN RETURN OLD; END; $$;
CREATE TRIGGER trigger_record_trial_on_delete BEFORE DELETE ON public.user_access FOR EACH ROW EXECUTE FUNCTION public.record_trial_on_account_delete();
CREATE FUNCTION public.usage_event_on_user_access() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER usage_event_user_access AFTER INSERT OR UPDATE ON public.user_access FOR EACH ROW EXECUTE FUNCTION public.usage_event_on_user_access();

-- school tables as live (public read)
CREATE TABLE public.schools (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), schulname text, city text, geo_country text, geo_region text, geo_municipality text);
CREATE TABLE public.primary_schools (LIKE public.schools INCLUDING ALL);
CREATE TABLE public.school_similarities (school_id uuid, similar_id uuid);
ALTER TABLE public.schools ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.primary_schools ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.school_similarities ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Schools are publicly readable" ON public.schools FOR SELECT USING (true);
CREATE POLICY "Primary schools are publicly readable" ON public.primary_schools FOR SELECT USING (true);
CREATE POLICY "School similarities are publicly readable" ON public.school_similarities FOR SELECT USING (true);
GRANT SELECT ON public.schools, public.primary_schools, public.school_similarities TO anon, authenticated;
INSERT INTO public.schools (schulname, city) VALUES ('Gymnasium A', 'berlin'), ('Gymnasium B', 'berlin');
CREATE FUNCTION public.school_city_counts(p_category text)
RETURNS TABLE(city text, geo_country text, geo_region text, geo_municipality text, school_count bigint)
LANGUAGE sql STABLE AS $$ SELECT city, geo_country, geo_region, geo_municipality, count(*) FROM public.schools GROUP BY 1,2,3,4 $$;
GRANT EXECUTE ON FUNCTION public.school_city_counts(text) TO anon, authenticated;

-- pre-existing accounts: an expired tester, a Google user with no row, a paid user, an admin, a manually-ended tester
INSERT INTO auth.users (id, email, created_at) VALUES
  ('00000000-0000-0000-0000-000000000001', 'Old.Tester@gmail.com', now() - interval '60 days'),
  ('00000000-0000-0000-0000-000000000002', 'google.user@gmail.com', now() - interval '90 days'),
  ('00000000-0000-0000-0000-000000000003', 'john.doe@absolutnow.com', now() - interval '30 days'),
  ('00000000-0000-0000-0000-000000000004', 'admin@schoolnossa.com', now() - interval '200 days'),
  ('00000000-0000-0000-0000-000000000005', 'ended@example.com', now() - interval '40 days');
INSERT INTO public.user_access (user_id, access_type, test_started_at, test_ended_at, test_manually_ended, subscription_ends_at) VALUES
  ('00000000-0000-0000-0000-000000000001', 'test', now() - interval '60 days', now() - interval '30 days', false, NULL),
  ('00000000-0000-0000-0000-000000000003', 'paid', NULL, NULL, false, '2027-12-31'),
  ('00000000-0000-0000-0000-000000000004', 'test', now() - interval '200 days', now() - interval '170 days', false, NULL),
  ('00000000-0000-0000-0000-000000000005', 'test', now() - interval '40 days', now() - interval '10 days', true, NULL);
INSERT INTO public.admin_allowlist VALUES ('admin@schoolnossa.com');
