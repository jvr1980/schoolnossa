Move access control to the server: a 48-hour trial for new users, one trial per email, and paid-only access for the ChatGPT/Claude connector. Today the browser decides access. `/dashboard` only checks sign-in, a missing `user_access` row counts as allowed, the client chooses its own trial row, and the web "Delete account" doesn't delete the account. After this change the database is the single source of truth, and the web app, the iOS app and the MCP server all follow it.

Please do NOT change the RLS policies on `schools`, `primary_schools` or `school_similarities` in this step. Do NOT backfill or modify existing users' `user_access` rows; I'll do both separately right after publishing. Don't publish; I'll review first.

## A. Database migration

Apply this SQL as one migration, verbatim. If anything fails or you need to change it, tell me exactly what you changed.

```sql
__MIGRATION_SQL__
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
