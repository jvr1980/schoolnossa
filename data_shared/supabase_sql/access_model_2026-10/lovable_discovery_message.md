Small discovery update, so ChatGPT and Claude can find, read and correctly describe SchoolNossa. Only touch `public/robots.txt`, `public/llms.txt` and the static meta tags / JSON-LD in `index.html`. No other code, no database changes, and don't publish.

## 1. public/robots.txt

- Set "Last updated" to 2026-10-25.
- Replace the header notice with this meaning, in English: the data on schoolnossa.com is proprietary; automated scraping and any use for AI or machine-learning training are prohibited. AI search and answer assistants may read the public pages allowed below, so that they can cite and link to SchoolNossa.
- Add a new section "# AI search and assistant fetchers (public pages only)" with these four user agents: `OAI-SearchBot`, `ChatGPT-User`, `Claude-SearchBot`, `Claude-User`. Give each exactly the same Allow/Disallow lines as the Googlebot block.
- Remove `ChatGPT-User` from the "Block all AI training and scraping bots" list, since it is now allowed above.
- Keep every other block exactly as it is: GPTBot, ClaudeBot, anthropic-ai, Claude-Web, CCBot, Google-Extended, PerplexityBot, Applebot-Extended, the scrapers, and the `User-agent: *` default.
- Add `Allow: /app` (the public iPhone-app page) to the Googlebot, Bingbot and new AI-fetcher blocks.

## 2. public/llms.txt

Rewrite it in llms.txt format (H1, a blockquote summary, then sections with markdown links). Write it in German first, followed by an English section with the same content. Facts to use (current, from the database):

- **What it is:** a school finder for parents in Germany and the Netherlands. Over 10,000 schools: about 2,800 secondary schools and 7,700 primary schools, with 220+ data points each, comparison, filters, commute times and AI-assisted recommendations.
- **Coverage:** 10 German cities (Berlin, Bremen, Dresden, Düsseldorf, Frankfurt am Main, Hamburg, Köln, Leipzig, München, Stuttgart) and the whole Netherlands (all 342 municipalities for primary schools; secondary schools in 288 municipalities). Users can combine up to 10 Dutch municipalities in one search.
- **Access:** public pages are free. School details need an account: a free 48-hour trial, then a subscription (€9.99 for 3 months). An iPhone app is available (/app).
- **ChatGPT and Claude:** SchoolNossa can be added as a connector (MCP server) in Claude and ChatGPT at `https://whzvzoumldeqgyrqlilt.supabase.co/functions/v1/mcp`. It requires an active SchoolNossa subscription. Tools: list covered cities, search schools, get one school's details, and manage the user's pinned shortlist.
- Analyses and forecasts are directional estimates, not guarantees (keep this note).
- **Pages:** / (overview, features, pricing), /about, /how-to-use, /data-coverage (which data exists per city and school type), /app (iPhone app), /contact. Under "Optional": /privacy, /terms.

## 3. index.html

- Update the meta description, og:description, twitter:description and the JSON-LD description from "Mehr als 600 Schulen …" to the current scope, for example: "Über 10.000 Schulen in Deutschland und den Niederlanden mit 220+ Datenpunkten: vergleichen, filtern, Wegzeiten und KI-Empfehlungen. 48 Stunden kostenlos testen."
- Keep the title.
- If the JSON-LD names a country or service area, add the Netherlands next to Germany.

## Reply with

The final contents of robots.txt and llms.txt, and the changed lines of index.html.
