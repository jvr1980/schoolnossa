# Your one-time setup (about 20 minutes)

Only the steps I'm not allowed to do: creating accounts and generating access keys. Everything else is mine.

## 1. Instagram account (phone, ~5 min)

1. Instagram app → your profile → tap your username at the top → **Add account** → **Create new account**.
   Username: `schoolnossa` (if taken: `schoolnossa.de`). Use **developer@schoolnossa.com** or **support@schoolnossa.com** as its email, not a personal address.
2. Settings → **Account type and tools** → **Switch to professional account** → **Business** → category **Education** (*Bildung*).
3. In the same settings, **link it to the SchoolNossa Facebook Page** (Accounts Center → Accounts → Add accounts, or "Connect a Facebook Page" during the switch).
4. Edit profile:
   - **Name:** SchoolNossa
   - **Profile picture:** `samples/profile_picture.jpg` (in this folder)
   - **Bio:** `Tage der offenen Tür & Anmeldetermine für 10 Städte + NL. Frühe Beta, Projekt aus der Freizeit.`
   - **Link:** `https://schoolnossa.com/schulen?c=ig-bio`

## 2. YouTube channel (~2 min)

On studio.youtube.com, signed in with the Google account that should own it (developer@schoolnossa.com recommended): **Create channel** → name **SchoolNossa**, handle `@schoolnossa`. Nothing else; I upload in YouTube Studio in Chrome.

## 3. Meta access key (~5 min, after I've prepared it)

I set up the developer app and a "system user" in Meta Business Settings in Chrome, and give it access to the Page, the Instagram account and the ad account. Then you:

1. Business Settings → **Users → System users → schoolnossa-growth** → **Generate token**.
2. App: **SchoolNossa Growth**. Expiry: **Never**. Tick exactly these permissions:
   `pages_show_list`, `pages_read_engagement`, `pages_manage_posts`, `instagram_basic`, `instagram_content_publish`, `ads_management`, `ads_read`, `business_management`.
   (No messaging, comment or group permissions: the system cannot DM, comment or post in groups.)
3. Copy the token into the file `schoolnossa-growth/secrets/meta.env` as `META_TOKEN=…` (I create the file with everything else filled in), and tell me "done".

## 4. Check the ad account (~3 min)

In Meta Ads Manager → Billing: a payment method is on file, and the account currency is EUR. I set the monthly spending limits myself.

## 5. Switch the Meta app to Live (~2 min, needs your OK)

developers.facebook.com → **SchoolNossa Growth** → **App settings → Basic**: privacy policy URL `https://schoolnossa.com/privacy`, category **Education**. Then **Publish** (Development → Live).
Without this, Meta refuses every ad creative (error subcode 1885183, "app is in development mode"), even with a valid token. Posting and reads work in Development mode, so this only shows at the first ad sync.
