Privacy policy: add a section on use through AI assistants (ChatGPT, Claude), and make the logging match what it says. Approved text below; use it verbatim. Don't publish.

## 1. /privacy (src/pages/Privacy.tsx), both languages

- Insert a new section **8** before "Änderungen / Changes", and renumber "Änderungen / Changes" to **9**.
- Update the date line to "Zuletzt aktualisiert: Oktober 2026" / "Last updated: October 2026".

**DE — 8. Nutzung über KI-Assistenten (ChatGPT, Claude)**

Sie können Ihr SchoolNossa-Konto mit KI-Assistenten wie ChatGPT (OpenAI) oder Claude (Anthropic) verbinden. Die Verbindung erfolgt über OAuth: Sie melden sich bei SchoolNossa an und bestätigen den Zugriff. Ihr Passwort wird dabei nicht an den Anbieter des Assistenten weitergegeben.

Der Assistent erhält nur die Daten, die er über die SchoolNossa-Werkzeuge abfragt: Schuldaten und Ihre Merkliste. Diese Inhalte werden an den Anbieter des Assistenten übermittelt und dort nach dessen Datenschutzbestimmungen verarbeitet. Je nach Ihren Einstellungen beim Anbieter können sie auch zum Training von KI-Modellen verwendet werden.

Zur Bereitstellung des Zugriffs speichern wir die Registrierung des Assistenten und die Zugriffstoken der Verbindung, bis Sie die Verbindung trennen oder die Token ablaufen. Zur Einhaltung der Tageslimits und zur Missbrauchsabwehr protokollieren wir jeden Werkzeugaufruf: Ihr Konto, den verbundenen Assistenten, das aufgerufene Werkzeug und den Zeitpunkt. Suchinhalte werden dabei nicht gespeichert. Diese Protokolle werden nach 90 Tagen automatisch gelöscht.

Sie können die Verbindung jederzeit in den Einstellungen von ChatGPT bzw. Claude trennen. Fragen: info@schoolnossa.com.

**EN — 8. Use through AI assistants (ChatGPT, Claude)**

You can connect your SchoolNossa account to AI assistants such as ChatGPT (OpenAI) or Claude (Anthropic). The connection uses OAuth: you sign in to SchoolNossa and approve access. Your password is never shared with the assistant's provider.

The assistant only receives the data it requests through SchoolNossa's tools: school data and your shortlist. This content is sent to the assistant's provider and processed under that provider's privacy policy. Depending on your settings with the provider, it may also be used to train AI models.

To provide access, we store the assistant's client registration and the connection's access tokens until you disconnect or the tokens expire. To enforce daily limits and prevent abuse, we log each tool call: your account, the connected assistant, the tool used and the time. Search contents are not stored. These logs are deleted automatically after 90 days.

You can disconnect at any time in your ChatGPT or Claude settings. Questions: info@schoolnossa.com.

## 2. Make the logging match the text

1. **90-day retention for `mcp_usage`.** Add a daily job (pg_cron, like the existing alert jobs) that runs `DELETE FROM public.mcp_usage WHERE created_at < now() - interval '90 days'`. If migrations can't schedule cron jobs, apply the schedule separately, as you did for the settings rows, and tell me.
2. **No search text in the server logs.** `supabase/functions/semantic-search/index.ts` logs the raw and translated query text (the "Translated …" and "Generating embedding for query: …" lines). Log only the query length and the scope (category, number of cities) instead. Check the MCP server and the other edge functions for any log line that prints a user's search text, and remove those too.

## Checks

- Typecheck and build pass. The /privacy page renders sections 1–9 in both languages.
- In your reply, list: the migration or cron job name, the log lines you changed (file:line), and anything you couldn't do.
