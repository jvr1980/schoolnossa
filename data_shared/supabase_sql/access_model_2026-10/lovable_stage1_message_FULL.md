Move access control to the server: a 48-hour trial for new users, one trial per email, and paid-only access for the ChatGPT/Claude connector. Today the browser decides access. `/dashboard` only checks sign-in, a missing `user_access` row counts as allowed, the client chooses its own trial row, and the web "Delete account" doesn't delete the account. After this change the database is the single source of truth, and the web app, the iOS app and the MCP server all follow it.

Please do NOT change the RLS policies on `schools`, `primary_schools` or `school_similarities` in this step. Do NOT backfill or modify existing users' `user_access` rows; I'll do both separately right after publishing. Don't publish; I'll review first.

## A. Database migration

Apply this SQL as one migration, verbatim. If anything fails or you need to change it, tell me exactly what you changed.

```sql
-- Access model 2026-10: 48-hour trial granted by the server, one trial per
-- normalized email, has_app_access()/has_paid_access() as the single source
-- of truth. Applied through the Lovable agent as a migration (stage 1).
-- Stage 2 (separate, after the frontend is published) locks the school tables.

-- 1) Settings: a 48h trial replaces the seat-limited 30-day test phase
INSERT INTO public.app_settings (key, value)
VALUES ('trial_hours', '{"hours": 48}'::jsonb)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();

UPDATE public.app_settings
SET value = '{"enabled": false}'::jsonb, updated_at = now()
WHERE key = 'test_phase_enabled';

-- 2) Email normalization: lowercase, strip +tag, Gmail dots, googlemail -> gmail
CREATE OR REPLACE FUNCTION public.normalize_email(p_email text)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = public
AS $$
  WITH e AS (SELECT lower(trim(p_email)) AS v),
  p AS (
    SELECT v,
           split_part(split_part(v, '@', 1), '+', 1) AS local,
           CASE WHEN split_part(v, '@', 2) = 'googlemail.com' THEN 'gmail.com'
                ELSE split_part(v, '@', 2) END AS domain
    FROM e
  )
  SELECT CASE
    WHEN v IS NULL OR v = '' THEN NULL
    WHEN position('@' IN v) = 0 THEN v
    WHEN domain = 'gmail.com' THEN replace(local, '.', '') || '@' || domain
    ELSE local || '@' || domain
  END
  FROM p
$$;

-- 3) One trial per normalized email. Rows survive account deletion.
CREATE TABLE IF NOT EXISTS public.trial_claims (
  email_normalized text PRIMARY KEY,
  first_user_id uuid,
  claimed_at timestamptz NOT NULL DEFAULT now(),
  source text NOT NULL DEFAULT 'signup'
);
ALTER TABLE public.trial_claims ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Admins can read trial claims" ON public.trial_claims
  FOR SELECT TO authenticated USING (public.is_admin(auth.uid()));
CREATE POLICY "Admins can delete trial claims" ON public.trial_claims
  FOR DELETE TO authenticated USING (public.is_admin(auth.uid()));

-- Every existing account counts as having claimed its trial. Their one
-- launch window is applied separately at go-live (03_go_live_backfill.sql).
INSERT INTO public.trial_claims (email_normalized, first_user_id, claimed_at, source)
SELECT DISTINCT ON (public.normalize_email(u.email))
       public.normalize_email(u.email), u.id, u.created_at, 'backfill'
FROM auth.users u
WHERE u.email IS NOT NULL AND u.email <> ''
ORDER BY public.normalize_email(u.email), u.created_at
ON CONFLICT (email_normalized) DO NOTHING;

INSERT INTO public.trial_claims (email_normalized, claimed_at, source)
SELECT public.normalize_email(email), min(trial_used_at), 'trial_used_emails'
FROM public.trial_used_emails
GROUP BY 1
ON CONFLICT (email_normalized) DO NOTHING;

-- 4) Grant initial access. Server-side only; never callable by clients.
--    New email -> 'test' with a trial_hours window.
--    Email that already had a trial (a different, earlier account) -> 'test'
--    whose window is already closed, so the apps show the paywall.
CREATE OR REPLACE FUNCTION public.grant_initial_access(p_user_id uuid, p_email text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_norm text := public.normalize_email(p_email);
  v_hours integer := COALESCE(
    (SELECT (value->>'hours')::integer FROM public.app_settings WHERE key = 'trial_hours'), 48);
  v_rows integer := 1;
BEGIN
  IF EXISTS (SELECT 1 FROM public.user_access WHERE user_id = p_user_id) THEN
    RETURN;
  END IF;

  IF v_norm IS NOT NULL THEN
    INSERT INTO public.trial_claims (email_normalized, first_user_id)
    VALUES (v_norm, p_user_id)
    ON CONFLICT (email_normalized) DO NOTHING;
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    -- The claim may already belong to this same account (backfilled claims
    -- for accounts that never got an access row): that is not a re-signup.
    IF v_rows = 0 AND EXISTS (
      SELECT 1 FROM public.trial_claims
      WHERE email_normalized = v_norm AND first_user_id = p_user_id
    ) THEN
      v_rows := 1;
    END IF;
  END IF;

  INSERT INTO public.user_access
    (user_id, access_type, test_started_at, test_ended_at, test_manually_ended, login_count, last_login_at)
  VALUES
    (p_user_id, 'test', now(),
     CASE WHEN v_rows > 0 THEN now() + make_interval(hours => v_hours) ELSE now() END,
     false, 0, now())
  ON CONFLICT (user_id) DO NOTHING;
END;
$$;
REVOKE ALL ON FUNCTION public.grant_initial_access(uuid, text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.grant_initial_access(uuid, text) TO service_role;

-- 5) Signup trigger: every provider (email, Google, Apple) creates auth.users.
--    A failure must never block signup; ensure_user_access() heals it later.
CREATE OR REPLACE FUNCTION public.grant_access_on_signup()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  BEGIN
    PERFORM public.grant_initial_access(NEW.id, NEW.email);
  EXCEPTION WHEN others THEN
    RAISE WARNING 'grant_access_on_signup failed for %: %', NEW.id, SQLERRM;
  END;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS on_auth_user_created_access ON auth.users;
CREATE TRIGGER on_auth_user_created_access
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE FUNCTION public.grant_access_on_signup();

-- 6) Self-heal for clients: creates the row with the same rules if missing
CREATE OR REPLACE FUNCTION public.ensure_user_access()
RETURNS SETOF public.user_access
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  IF auth.uid() IS NULL THEN
    RAISE EXCEPTION 'Not authenticated';
  END IF;
  PERFORM public.grant_initial_access(
    auth.uid(), (SELECT email FROM auth.users WHERE id = auth.uid()));
  RETURN QUERY SELECT * FROM public.user_access WHERE user_id = auth.uid();
END;
$$;
REVOKE ALL ON FUNCTION public.ensure_user_access() FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.ensure_user_access() TO authenticated;

-- 7) Access checks for RLS, edge functions and the MCP server
CREATE OR REPLACE FUNCTION public.has_paid_access()
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT auth.uid() IS NOT NULL AND (
    public.is_admin(auth.uid())
    OR EXISTS (
      SELECT 1 FROM public.user_access a
      WHERE a.user_id = auth.uid()
        AND a.access_type = 'paid'
        AND (a.subscription_ends_at IS NULL OR a.subscription_ends_at > now())
    )
  )
$$;

CREATE OR REPLACE FUNCTION public.has_app_access()
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
AS $$
  SELECT CASE
    WHEN auth.uid() IS NULL THEN false
    -- Tokens issued to OAuth clients (ChatGPT, Claude, any MCP client)
    -- carry a client_id claim: those need a paid subscription.
    WHEN (auth.jwt() ->> 'client_id') IS NOT NULL THEN public.has_paid_access()
    ELSE public.has_paid_access() OR EXISTS (
      SELECT 1 FROM public.user_access a
      WHERE a.user_id = auth.uid()
        AND a.access_type = 'test'
        AND COALESCE(a.test_manually_ended, false) = false
        AND a.test_ended_at > now()
    )
  END
$$;
REVOKE ALL ON FUNCTION public.has_paid_access() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.has_app_access() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.has_paid_access() TO anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.has_app_access() TO anon, authenticated, service_role;

-- 8) Clients can no longer create or choose their own access rows, and the
--    email-probe RPC is closed (it told anyone whether an email had an account)
DROP POLICY IF EXISTS "Users can create own free/test access on signup" ON public.user_access;
REVOKE EXECUTE ON FUNCTION public.check_trial_used(text) FROM PUBLIC, anon, authenticated;

-- 9) Deletion backstop: remember the email for every access type
CREATE OR REPLACE FUNCTION public.record_trial_on_account_delete()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  user_email text;
BEGIN
  SELECT email INTO user_email FROM auth.users WHERE id = OLD.user_id;
  IF user_email IS NOT NULL THEN
    INSERT INTO public.trial_claims (email_normalized, first_user_id, source)
    VALUES (public.normalize_email(user_email), OLD.user_id, 'account_delete')
    ON CONFLICT (email_normalized) DO NOTHING;
  END IF;
  RETURN OLD;
END;
$$;

-- 10) Usage events: no "trial_started" for a re-signup whose window is already
--     closed; bulk maintenance can suppress events (SET LOCAL app.suppress_usage_events = 'on')
CREATE OR REPLACE FUNCTION public.usage_event_on_user_access()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
declare
  v_email text;
  v_old text := case when tg_op = 'UPDATE' then old.access_type else null end;
begin
  if coalesce(current_setting('app.suppress_usage_events', true), '') = 'on' then
    return new;
  end if;

  begin
    select email into v_email from auth.users where id = new.user_id;

    if new.access_type = 'test' and (tg_op = 'INSERT' or v_old is distinct from 'test')
       and new.test_ended_at > coalesce(new.test_started_at, now()) then
      insert into public.usage_events (event_type, user_id, detail)
      values ('trial_started', new.user_id, jsonb_build_object(
        'email', v_email,
        'test_started_at', new.test_started_at,
        'test_ended_at', new.test_ended_at
      ));
    end if;

    if new.access_type = 'paid' and (tg_op = 'INSERT' or v_old is distinct from 'paid') then
      insert into public.usage_events (event_type, user_id, detail)
      values ('paid_started', new.user_id, jsonb_build_object(
        'email', v_email,
        'subscription_started_at', new.subscription_started_at,
        'subscription_ends_at', new.subscription_ends_at
      ));
    elsif tg_op = 'UPDATE' and new.access_type = 'paid' and v_old = 'paid'
      and new.subscription_started_at is distinct from old.subscription_started_at then
      insert into public.usage_events (event_type, user_id, detail)
      values ('paid_renewed', new.user_id, jsonb_build_object(
        'email', v_email,
        'subscription_started_at', new.subscription_started_at,
        'subscription_ends_at', new.subscription_ends_at
      ));
    end if;
  exception when others then
    raise warning 'usage_event_on_user_access failed: %', sqlerrm;
  end;
  return new;
end;
$function$;
```

In the same migration, add a public stats function for the landing page and /data-coverage, so they keep working once the school tables stop being public in step 2:
- `public.public_school_stats()`: SECURITY DEFINER, STABLE, `SET search_path = public`, EXECUTE granted to anon and authenticated.
- It returns ONLY aggregates: counts, non-null counts, sums and averages, per city / category (secondary, primary) / school_type where the pages need them. Never per-school rows, names, ids, addresses, coordinates or any single school's value.
- Shape it to exactly what `src/components/landing/DataSnapshot.tsx` and `src/pages/DataCoverage.tsx` compute today, then switch both to it. They must render the same numbers as now.

## B. Frontend

1. `src/hooks/useUserAccess.ts`
   - If the user's `user_access` row is missing, call `supabase.rpc('ensure_user_access')` and use the returned row. `initializeUserAccess` must no longer insert into `user_access`; make it call `ensure_user_access` instead.
   - Fetch the server decision with `supabase.rpc('has_app_access')` together with the row and expose it, e.g. `hasAccess`. This is the gate; it also covers admins. Keep it fresh on `refetch()`, including after payment.
   - Add `hasPaidAccess` from `supabase.rpc('has_paid_access')`.
   - Read `trial_hours` from `app_settings`, and add `getTrialHoursRemaining()`. Only 'test' rows are trials: hours until `test_ended_at`, minimum 0.
   - `canAccessApp()` returns the server decision. While loading, don't decide; don't flash content or the paywall.
2. Gate every page that shows school data, at least `/dashboard` and `/menu`. When the user is signed in, loading has finished and `hasAccess` is false, don't render or fetch school data. Show the existing AccountDisabledModal with the PaymentModal instead, with reason "trial expired" for 'test' rows and "subscription expired" for 'paid' rows. Today `/dashboard` only checks sign-in, and MainMenu doesn't gate expired paid accounts.
3. `src/pages/SignUp.tsx`: remove the `check_trial_used` call (that RPC is now closed) and the client-side `user_access` insert. After signup, call `ensure_user_access`; the database trigger normally already created the row. A returning email gets an already-expired trial and lands on the paywall. Don't show any special "you already had a trial" error at signup.
4. Trial UI: TestPhaseInfo (and anywhere showing days left) shows the hours remaining in the trial, e.g. "Testzugang: noch 31 Std." / "Trial: 31 hours left", and no seat counter. The test phase is now off (`test_phase_enabled` = false), which already makes the "unlock/pay" button visible for non-paid users. Keep that.
5. Copy, DE + EN in `src/lib/translations.ts` and anywhere else (landing, pricing, signup, FAQ, terms): replace the "30 Tage / 30 days" beta-trial and "5 Minuten / 5 minutes" free-trial wording with the 48-hour trial, e.g. "48 Stunden kostenlos testen" / "Try free for 48 hours". Keys I found: signup.benefit1, signup.freeTrial, signup.accountCreatedTest, signup.startFreeTrial, hero.freeMinutes, pricing.tryFree. Check for others. Also localize the hero's hard-coded English "Sign Up" button (DE: "Registrieren").
6. `src/pages/AccountSettings.tsx` "Delete account": call the existing `delete-account` edge function (`supabase.functions.invoke('delete-account')`). Only when it succeeds, sign out, show the success toast and navigate home. On failure, show an error and keep the user signed in. Today it deletes a few rows client-side and reports success while the account remains. The edge function is what the iOS app already uses.
7. WebMCP (`src/lib/webmcp`, the `useWebMCP` call in Dashboard.tsx): only register tools when `hasPaidAccess` is true, the same rule as the MCP server.

## C. MCP server (`src/lib/mcp`)

- Add a shared helper that calls `rpc('has_paid_access')` with the user's client (`supabaseForUser(ctx)`). Every tool (list_cities, search_schools, get_school, list_pinned_schools, pin_school, unpin_school) calls it right after the `isAuthenticated()` check.
- If it returns false, return a normal result (not isError) with `structuredContent: { error: "subscription_required" }` and this text: "Using SchoolNossa in ChatGPT and Claude requires an active SchoolNossa subscription, and this account doesn't have one. Trial accounts can use SchoolNossa on schoolnossa.com and in the iOS app. More information: https://schoolnossa.com". Include no links to checkout or payment pages; the ChatGPT app directory forbids linking to purchase flows.
- Update the server `instructions`: SchoolNossa covers schools in Germany and the Netherlands, and an active SchoolNossa subscription is required. Bump `version` to "0.2.0".

## D. Edge functions

1. `semantic-search`: it currently uses the service role and never checks the caller. Require the caller's JWT. Create a client with the anon key and the request's Authorization header, then call `rpc('has_app_access')`. Return 401 when there is no valid user and 403 when access is false; only then run the search. The iOS app calls it through `functions.invoke` with the user's session, so it keeps working.
2. `verify-payment`: when the user has no `user_access` row (Google/Apple sign-ups), the current `.update()` matches 0 rows and the payment is reported as successful without granting access. Use an upsert on `user_id` instead, or make sure the row exists first.
   - Extend from the later of now and the current `subscription_ends_at`, so an early renewal doesn't lose remaining time.
   - Keep `subscription_started_at` = now.
   - Verify that a row was written, and fail loudly otherwise.

## E. Checks

- Unit tests for the gating and hours-remaining logic. Typecheck, tests and preview build must pass.
- In your reply, list: the migration file name, the commit SHAs, any change you made to the SQL, and the exact return shape of `public_school_stats()`.
