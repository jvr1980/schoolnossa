Two small changes to prepare the ChatGPT and Claude directory submissions. Nothing else, no database changes, and don't publish.

## 1. MCP tool annotations (src/lib/mcp/tools)

The ChatGPT directory requires explicit booleans for readOnlyHint, destructiveHint and openWorldHint on every tool. Add `destructiveHint: false` to the four read-only tools: list_cities, search_schools, get_school and list_pinned_schools. pin_school and unpin_school already have it. Change nothing else in the MCP server.

## 2. New public page /connectors: "SchoolNossa in ChatGPT & Claude"

Add it in German and English (translations.ts), linked from the footer next to "Anleitung / How to use". The route must not be behind AppAccessGate. Add `Allow: /connectors` to the Googlebot, Bingbot and AI-fetcher blocks in robots.txt, add it to sitemap.xml, and add it to llms.txt under the pages in both languages.

Content, in this order:

- **What it is:** with a SchoolNossa subscription, you can use SchoolNossa inside ChatGPT and Claude. Ask in plain language, e.g. "Find Gymnasien in Berlin-Pankow" or "Which secondary schools in Utrecht offer VWO?", and the assistant searches SchoolNossa's data for you.
- **Requirements:**
  - an active SchoolNossa subscription (€9.99 for 3 months)
  - the 48-hour trial is not enough for the connectors; trial accounts use the website and the iPhone app
  - on the assistant side, a plan that supports custom connectors or plugins
- **Connect in Claude:** Settings → Connectors → Add → Add custom connector. Name: SchoolNossa. URL: `https://whzvzoumldeqgyrqlilt.supabase.co/functions/v1/mcp`. Add → Connect, sign in to SchoolNossa, then tap "Approve".
- **Connect in ChatGPT:** Plugins → Add → Add custom MCP server. Name: SchoolNossa. Server URL as above. Authentication: OAuth. Tick the confirmation box → "Create as a plugin" → Connect, sign in to SchoolNossa, then tap "Approve".
- **What the assistant can do:**
  - list covered cities and municipalities
  - search schools by city, district, school type, ownership and size
  - show one school's details (contact, students and teachers, Abitur results, admission information where available)
  - list, add and remove schools on your pinned shortlist
- **Privacy:**
  - SchoolNossa only sends the assistant the school data it asks for, plus your pinned shortlist; never your password or payment details.
  - What the assistant does with that data is covered by the provider's own privacy policy (OpenAI or Anthropic).
  - Link to /privacy.
- **Disconnect:** remove the connector or plugin in your Claude or ChatGPT settings at any time. That stops the assistant's access.
- **Help:** info@schoolnossa.com.

Keep the visual style of /how-to-use. In your reply, list the changed files and the final page text in both languages.
