# Pass 2 – Structured Data Extraction Prompt

**Used in:** `school_description_pipeline.py`
**Model:** GPT-4o (structured JSON output)
**Input:** Raw research text from Pass 0
**Purpose:** Extract structured fields from web research to fill empty columns in the school master table

---

## System prompt

You are a precise data extraction assistant. Your task is to extract specific structured data points about a German school from web research text.

Rules:
- Extract ONLY data explicitly mentioned in the research — do NOT guess, infer, or estimate
- Use `null` for any field where the data is not found or uncertain
- Numeric fields must be integers or floats, never strings
- If conflicting values appear in the research, use the most recent or most credible one
- For `website`: only return a URL if you are confident it is the correct school's official website

---

## User prompt

Based on the following web research about **{schulname}** ({school_type}) in {city}, Germany, extract the structured data below.

Research text:
---
{raw_research}
---

Return a JSON object with exactly these fields:

```json
{
  "website": "<official school website URL, e.g. https://minna-specht-schule.de, or null>",
  "gruendungsjahr": <founding year as integer, e.g. 1965, or null>,
  "lehrer_2024_25": <total number of teaching staff in the 2024/25 school year, integer, or null>,
  "lehrer_2023_24": <total number of teaching staff in the 2023/24 school year, integer, or null>,
  "sprachen": "<language offerings as a string, e.g. 'Englisch ab Klasse 1, Französisch-AG', or null>",
  "besonderheiten": "<notable special features, pedagogical concepts, awards, programs — comma-separated string, e.g. 'Ganztagsschule, Inklusion, Montessori-Pädagogik, MINT-Schwerpunkt', or null>",
  "ganztagsform": "<one of: 'offen', 'gebunden', 'teilgebunden', or null if not determinable>",
  "schueler_gesamt_web": <total student count found on website or in research, integer, or null>,
  "nachfrage_plaetze_2025_26": <number of available enrollment spots for 2025/26, integer, or null>,
  "nachfrage_wuensche_2025_26": <number of enrollment applications or requests for 2025/26, integer, or null>,
  "nachfrage_plaetze_2024_25": <number of available enrollment spots for 2024/25, integer, or null>,
  "nachfrage_wuensche_2024_25": <number of enrollment applications or requests for 2024/25, integer, or null>,
  "migration_2024_25": <percentage of students with migration background in 2024/25 as a float, e.g. 42.5, or null>,
  "migration_2023_24": <percentage of students with migration background in 2023/24 as a float, or null>
}
```

Return ONLY the JSON object — no explanations, no markdown fences, no extra text.

---

## Column mapping (script reference)

| JSON field | CSV column | Notes |
|---|---|---|
| `website` | `website` | Only write if currently null |
| `gruendungsjahr` | `gruendungsjahr` | Only write if currently null |
| `lehrer_2024_25` | `lehrer_2024_25` | Primary target of this extraction |
| `lehrer_2023_24` | `lehrer_2023_24` | Secondary |
| `sprachen` | `sprachen` | Only write if currently null |
| `besonderheiten` | `besonderheiten` | Only write if currently null |
| `ganztagsform` | `ganztagsform` | Write to column if present in schema |
| `schueler_gesamt_web` | — | Cross-check only, do not overwrite `schueler_gesamt` |
| `nachfrage_plaetze_2025_26` | `nachfrage_plaetze_2025_26` | Write if null |
| `nachfrage_wuensche_2025_26` | `nachfrage_wuensche_2025_26` | Write if null |
| `nachfrage_plaetze_2024_25` | `nachfrage_plaetze_2024_25` | Write if null |
| `nachfrage_wuensche_2024_25` | `nachfrage_wuensche_2024_25` | Write if null |
| `migration_2024_25` | `migration_2024_25` | Write if null |
| `migration_2023_24` | `migration_2023_24` | Write if null |
