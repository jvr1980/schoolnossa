# Verify-and-trim test on 100 schools — September 2026

**Question:** can an automated checker find and delete the wrong claims in the existing school descriptions, without regenerating them? And what does it cost?

## Method
- **Sample:** 100 German schools, 10 per city.
  - The 30 schools of the first audit were tested on their original (pre-correction) text, so their 10 known serious errors could be caught.
  - 70 new schools were tested on their live text, after counts were removed. They are 4 secondary + 3 primary per city, drawn with the same reproducible ordering (`md5(id || 'audit-2026-09-27')`).
- **Reference answer:** an independent claim-by-claim audit of the 70 new schools, by 10 Claude agents using the method of the first audit (`DESCRIPTION_AUDIT_2026-09.md`). Combined with the first audit, this gives 1,485 claims:
  - verified / not_found / contradicted;
  - each contradicted claim graded *material* (a parent would be misled) or *minor*.

  The agents never saw the checker's output. Claim data: `description_audit_2026-09_batch2_claims.json`.
- **Checker:** `scripts_shared/enrichment/verify_trim_descriptions.py`.
  1. It crawls the school's own website: homepage plus up to 24 menu pages. It follows frames and meta refresh, and falls back on broken TLS and HTTP-only sites.
  2. Gemini gets the page texts and Google Search. It lists every claim with its verdict (confirmed / contradicted / outdated / unsourced) and a verbatim evidence quote.
  3. Gemini Flash deletes the flagged claims. Deletion only: a validator rejects any added word or number.
- **Deletion policies:**
  - **A** — contradicted + outdated.
  - **B** — A + unsourced concrete claims.
  - **C** — B + "confirmed" claims whose quote is attributed to the school's site but isn't on it.
- **Scoring:** `scripts_shared/enrichment/eval_verify_trim.py`. For each school, a judge (Gemini Flash) marks which reference claims each text version still states.
  - The untouched original works as a control: 2 of 1,397 claims were judged absent from it (0.1%).
  - 3 of the 100 schools could not be judged in the combined run, so the tables below cover 97.

## Current quality (70 new schools, live text)
- **Claims:** 835 verified (84%), 113 unsourced (11%), 52 wrong (5%: 24 material, 28 minor).
- **Schools:** 21 of 70 (30%) have at least one materially wrong claim. Together with the first 30 (counts excluded), it is 28 of 100 (95% CI ≈ 20–37%).
- **Kinds of error:**
  - outdated facts (ended programmes, a closed retreat house, the previous head, G8 → G9);
  - wrong district (4 schools);
  - wrong public/private status (2; our own register field is also wrong);
  - languages or labels the school doesn't offer (Chinese, Hebrew, MINT-EC);
  - content taken from a same-named school in another city (2).

## Results (97 schools, excluding student/teacher counts)
| checker | material errors removed | all wrong claims removed | unsourced removed | correct claims removed | text removed | schools with ≥1 material error |
|---|---|---|---|---|---|---|
| original | — | — | — | — | — | 26 (27%) |
| Pro, A | 48% | 43% | 9% | 2% | 2% | 14 (14%) |
| **Pro, B** | **59%** | **52%** | **34%** | **4%** | **5%** | **11 (11%)** |
| Pro, C | 62% | 62% | 46% | 19% | 14% | 10 (10%) |
| Flash, A | 41% | 35% | 4% | 1% | 2% | 16 (16%) |
| Flash, B | 48% | 40% | 23% | 3% | 3% | 14 (14%) |
| Flash, C | 55% | 50% | 28% | 14% | 9% | 12 (12%) |

**Cost per school:**
- Pro (gemini-3.1-pro-preview): $0.19 to check plus $0.004 to trim; 0.7 searches.
- Flash (gemini-3-flash-preview): $0.03 in total; 2.7 searches, which exceed the 5,000 free searches a month at scale, at $14 per 1,000.

For all 2,824 German schools that means about **$560 with Pro** (searches stay in the free tier) or **about $125 with Flash**.

## What it still misses (Pro, B)
- **Outdated statements still on the school's own site:** TAFF, the Great Britain exchange, the retreat house.
- **Misread pages:**
  - "Generationen Werkstatt" exists, but is not a craft workshop;
  - "MINT" is not "MINT-EC".
- **Sites the crawler cannot read** (JavaScript-only, blocked, dead domain).
- **Errors from our own register data**, which the checker counts as a source (public vs private).

## Lessons
- Given only a URL and a long task, Gemini answered from memory (0 searches, 0 pages opened) and "confirmed" a trip that had ended. Handing it the crawled page text fixed this.
- Gemini's evidence quotes often don't match the page. Treating an unmatched quote as "no source" (policy C) catches a few more errors, but removes 1 in 5 correct claims.
- The deletion prompt must say "every mention, in both languages". Otherwise the English copy or a second mention survives.
