#!/usr/bin/env python3
"""
NRW Anmeldezahlen (Application Numbers) Enrichment
====================================================

Enriches NRW school data with application vs. capacity data from
Düsseldorf Open Data.

Data Sources:
- Düsseldorf primary schools (CSV): Einschulungsverfahren 2026/2027
  https://opendata.duesseldorf.de/sites/default/files/Anmeldezahlen_Einschulungsverfahren_2026_2027.csv
- Düsseldorf secondary schools (PDFs): Anmeldezahlen for Gymnasien,
  Gesamtschulen, Realschulen, Hauptschulen for Schuljahr 2025/26
  https://www.duesseldorf.de/medienportal/pressedienst-einzelansicht/pld/anmeldungen-an-den-weiterfuehrenden-schulen-erste-anmeldephase-fuer-das-schuljahr-2025-2026-beendet

Matching Strategy:
  Uses an LLM (GPT-4o-mini) to match school names from the Anmeldezahlen
  sources to schools in our NRW database. The LLM receives both the
  Anmeldezahlen record (name + street) and all candidate DB schools
  (schulnummer, kurzbezeichnung, schulname, strasse) and returns
  the matching schulnummer for each record. Results are cached to JSON
  so the LLM is only called once per source.

Output columns (Berlin-compatible naming):
- nachfrage_wuensche_2025_26: Number of applications (Anmeldungen)
- nachfrage_plaetze_2025_26: Capacity / available places (Aufnahmekapazität)
- nachfrage_prozent_2025_26: Application-to-capacity ratio as %
- nachfrage_wuensche_2024_25: Previous year applications (for comparison)
- nachfrage_aufnahmen_2024_25: Actual admissions previous year (secondary only)
- nachfrage_data_source: Source indicator

Note: Köln does not publish per-school Anmeldezahlen, so only Düsseldorf
schools will have this data.

Author: NRW School Data Pipeline
Created: 2026-02-21
"""

import pandas as pd
import numpy as np
import logging
import os
import json
from pathlib import Path
from typing import Dict, List

try:
    import pdfplumber

    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Project paths
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data_nrw"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
CACHE_DIR = DATA_DIR / "cache"
ENV_FILE = PROJECT_ROOT / ".env"

# Load .env file if available
try:
    from dotenv import load_dotenv

    load_dotenv(ENV_FILE)
except ImportError:
    if ENV_FILE.exists():
        with open(ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip()
                    if key and key not in os.environ:
                        os.environ[key] = value

# Raw data files
PRIMARY_CSV = RAW_DIR / "duesseldorf_anmeldezahlen_2026_2027.csv"
GYMNASIEN_PDF = RAW_DIR / "duesseldorf_anmeldezahlen_gymnasien_2025_26.pdf"
GESAMTSCHULEN_PDF = RAW_DIR / "duesseldorf_anmeldezahlen_gesamtschulen_2025_26.pdf"
REALSCHULEN_PDF = RAW_DIR / "duesseldorf_anmeldezahlen_realschulen_2025_26.pdf"
HAUPTSCHULEN_PDF = RAW_DIR / "duesseldorf_anmeldezahlen_hauptschulen_2025_26.pdf"

# LLM matching cache
LLM_MATCH_CACHE = CACHE_DIR / "anmeldezahlen_llm_matches.json"


# =============================================================================
# PDF & CSV PARSING
# =============================================================================


def parse_secondary_pdf(pdf_path: Path) -> List[Dict]:
    """Parse a Düsseldorf secondary school Anmeldezahlen PDF."""
    if not PDFPLUMBER_AVAILABLE:
        logger.warning("pdfplumber not available — cannot parse PDFs")
        return []

    if not pdf_path.exists():
        logger.warning(f"PDF not found: {pdf_path}")
        return []

    records = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue

            table = tables[0]  # Main table
            for row in table[1:]:  # Skip header
                if not row or not row[0]:
                    continue

                name_cell = str(row[0]).strip()

                # Skip summary rows — use 'summe' not 'gesamt'
                # ('gesamt' would filter out Gesamtschule names!)
                if "Gesamtsumme" in name_cell or "summe" in name_cell.lower():
                    continue

                # Parse the school name (may span two lines with street)
                lines = name_cell.split("\n")
                school_name = lines[0].strip()
                street = lines[1].strip() if len(lines) > 1 else None

                # Parse numeric columns
                try:
                    anmeldungen_prev = (
                        int(
                            str(row[1]).replace(".", "").replace(",", "").strip()
                        )
                        if row[1]
                        else None
                    )
                except (ValueError, IndexError):
                    anmeldungen_prev = None

                try:
                    aufnahmen_prev = (
                        int(
                            str(row[2]).replace(".", "").replace(",", "").strip()
                        )
                        if row[2]
                        else None
                    )
                except (ValueError, IndexError):
                    aufnahmen_prev = None

                try:
                    anmeldungen_curr = (
                        int(
                            str(row[3]).replace(".", "").replace(",", "").strip()
                        )
                        if row[3]
                        else None
                    )
                except (ValueError, IndexError):
                    anmeldungen_curr = None

                try:
                    bezirk = (
                        int(str(row[4]).strip()) if row[4] else None
                    )
                except (ValueError, IndexError):
                    bezirk = None

                if school_name and (
                    anmeldungen_curr is not None
                    or anmeldungen_prev is not None
                ):
                    records.append(
                        {
                            "school_name": school_name,
                            "street": street,
                            "anmeldungen_2025_26": anmeldungen_curr,
                            "anmeldungen_2024_25": anmeldungen_prev,
                            "aufnahmen_2024_25": aufnahmen_prev,
                            "stadtbezirk": bezirk,
                        }
                    )

    return records


def parse_primary_csv(csv_path: Path) -> List[Dict]:
    """Parse the Düsseldorf primary school Anmeldezahlen CSV."""
    if not csv_path.exists():
        logger.warning(f"CSV not found: {csv_path}")
        return []

    df = pd.read_csv(csv_path, sep=";")
    records = []

    for _, row in df.iterrows():
        school_name = str(row.get("Schulname", "")).strip()
        if not school_name:
            continue

        kapazitaet = row.get("Aufnahmekapazitaet_2026_2027_Kapazitaet")
        anmeldungen = row.get(
            "Schuljahr_2026_2027_Summe_Anmeldungen_Stand_09_10_2025"
        )
        anmeldungen_prev = row.get(
            "Schuljahr_2025_2024_Summe_Anmeldungen_Stand_07_11_2024"
        )
        street = str(row.get("Lage", "")).replace("_", " ").strip()
        schulart = str(row.get("Schulart", "")).strip()

        records.append(
            {
                "school_name": school_name,
                "street": street,
                "schulart": schulart,
                "anmeldungen_2025_26": (
                    int(anmeldungen) if pd.notna(anmeldungen) else None
                ),
                "kapazitaet_2025_26": (
                    int(kapazitaet) if pd.notna(kapazitaet) else None
                ),
                "anmeldungen_2024_25": (
                    int(anmeldungen_prev) if pd.notna(anmeldungen_prev) else None
                ),
                "stadtbezirk": row.get("Stadtbezirk"),
            }
        )

    return records


# =============================================================================
# LLM-BASED SCHOOL MATCHING
# =============================================================================


def build_db_school_list(ddf: pd.DataFrame) -> List[Dict]:
    """Build a list of DB schools for the LLM prompt."""
    db_schools = []
    for idx, row in ddf.iterrows():
        db_schools.append(
            {
                "df_index": int(idx),
                "schulnummer": int(row["schulnummer"]),
                "kurzbezeichnung": str(row.get("kurzbezeichnung", "")),
                "schulname": str(row.get("schulname", "")),
                "strasse": str(row.get("strasse", "")),
                "Schulbezeichnung_1": str(
                    row.get("Schulbezeichnung_1", "")
                ),
                "Schulbezeichnung_2": str(
                    row.get("Schulbezeichnung_2", "")
                ),
            }
        )
    return db_schools


def llm_match_schools(
    anmeldezahlen_records: List[Dict],
    db_schools: List[Dict],
    school_type: str,
    cache_key: str,
) -> Dict[int, int]:
    """
    Use Gemini to match Anmeldezahlen records to DB schools.

    Returns a dict mapping anmeldezahlen record index -> DB df_index.
    Results are cached to avoid repeated LLM calls.
    """
    # Check cache first
    cache = _load_cache()
    if cache_key in cache:
        cached = cache[cache_key]
        logger.info(
            f"  Using cached LLM matches for '{cache_key}' "
            f"({len(cached)} matches)"
        )
        return {int(k): v for k, v in cached.items()}

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        logger.error(
            "GEMINI_API_KEY not set — cannot perform LLM matching"
        )
        return {}

    try:
        import google.generativeai as genai

        genai.configure(api_key=api_key)
    except ImportError:
        logger.error("google-generativeai package not installed")
        return {}

    # Build the prompt
    source_lines = []
    for i, rec in enumerate(anmeldezahlen_records):
        parts = [f"[{i}] Name: {rec['school_name']}"]
        if rec.get("street"):
            parts.append(f"Street: {rec['street']}")
        if rec.get("schulart"):
            parts.append(f"Type: {rec['schulart']}")
        source_lines.append(", ".join(parts))

    db_lines = []
    for s in db_schools:
        parts = [
            f"[{s['schulnummer']}] kurz: {s['kurzbezeichnung']}",
            f"name: {s['schulname']}",
            f"street: {s['strasse']}",
        ]
        if s["Schulbezeichnung_1"] and s["Schulbezeichnung_1"] != "nan":
            parts.append(f"SB1: {s['Schulbezeichnung_1']}")
        if s["Schulbezeichnung_2"] and s["Schulbezeichnung_2"] != "nan":
            parts.append(f"SB2: {s['Schulbezeichnung_2']}")
        db_lines.append(", ".join(parts))

    prompt = f"""You are matching {school_type} schools from Düsseldorf's Anmeldezahlen (application numbers) data to our school database.

For each SOURCE school, find the matching DATABASE school. Match based on school name AND street address. Note:
- Source names use short forms (e.g., "GGS Rolandstrasse", "KGS Essener Strasse", "Max-Schule")
- Database names are long/official (e.g., "Städt. Kath. Grundschule - Primarstufe -")
- The "kurzbezeichnung" field often has the closest match (e.g., "Düsseldorf, KG Kartause-Hain-Schule")
- Street formats differ: Source has "Strasse" spelled out, DB uses "Str." abbreviated
- Umlaut transliteration: Source may use "ue"/"oe"/"ae" where DB has "ü"/"ö"/"ä"
- KG=Kath. Grundschule, GG=Gem. Grundschule, EG=Evang. Grundschule, MG=Montessori-Grundschule
- KGS/GGS/EGS/MGS in source correspond to KG/GG/EG/MG in DB kurzbezeichnung
- Some schools share the same street address — use the school name to disambiguate

SOURCE SCHOOLS (from Anmeldezahlen):
{chr(10).join(source_lines)}

DATABASE SCHOOLS:
{chr(10).join(db_lines)}

Return a JSON object mapping each source index to the matching database schulnummer.
If a source school has no match in the database, omit it.
Return ONLY the JSON object, no explanation.
Example: {{"0": 100020, "1": 100061, "3": 100067}}"""

    logger.info(
        f"  Calling Gemini for {school_type} matching "
        f"({len(anmeldezahlen_records)} source -> {len(db_schools)} DB)..."
    )

    try:
        model = genai.GenerativeModel("gemini-2.5-flash")
        response = model.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                temperature=0,
                response_mime_type="application/json",
            ),
        )
        result_text = response.text.strip()
        raw_matches = json.loads(result_text)

        # Build schulnummer -> df_index lookup
        nr_to_idx = {s["schulnummer"]: s["df_index"] for s in db_schools}

        # Convert: source_idx -> schulnummer -> df_index
        matches = {}
        for src_idx_str, schulnummer in raw_matches.items():
            src_idx = int(src_idx_str)
            schulnummer = int(schulnummer)
            if schulnummer in nr_to_idx:
                matches[src_idx] = nr_to_idx[schulnummer]
            else:
                logger.warning(
                    f"  LLM returned unknown schulnummer {schulnummer} "
                    f"for source [{src_idx}]"
                )

        logger.info(
            f"  LLM matched {len(matches)}/{len(anmeldezahlen_records)} records"
        )

        # Cache the results
        cache[cache_key] = {str(k): v for k, v in matches.items()}
        _save_cache(cache)

        return matches

    except Exception as e:
        logger.error(f"  LLM matching failed: {e}")
        import traceback

        traceback.print_exc()
        return {}


def _load_cache() -> dict:
    """Load the LLM match cache."""
    if LLM_MATCH_CACHE.exists():
        with open(LLM_MATCH_CACHE, "r") as f:
            return json.load(f)
    return {}


def _save_cache(cache: dict):
    """Save the LLM match cache."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(LLM_MATCH_CACHE, "w") as f:
        json.dump(cache, f, indent=2)
    logger.info(f"  Cached matches to {LLM_MATCH_CACHE}")


# =============================================================================
# MAIN ENRICHMENT
# =============================================================================


def enrich_schools(school_type: str = "secondary") -> pd.DataFrame:
    """
    Enrich NRW schools with Anmeldezahlen data.

    Reads from the most recent intermediate file (with_pois or with_crime),
    adds Anmeldezahlen columns, saves to intermediate.
    """
    logger.info(f"Enriching {school_type} schools with Anmeldezahlen...")

    # Find input file
    input_file = (
        INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_pois.csv"
    )
    if not input_file.exists():
        input_file = (
            INTERMEDIATE_DIR / f"nrw_{school_type}_schools_with_crime.csv"
        )
    if not input_file.exists():
        raise FileNotFoundError(
            f"No intermediate file found for {school_type}"
        )

    df = pd.read_csv(input_file)
    logger.info(
        f"  Loaded {len(df)} {school_type} schools from {input_file.name}"
    )

    # Filter to Düsseldorf schools only (Köln doesn't publish this data)
    stadt_col = "stadt" if "stadt" in df.columns else "ort"
    is_ddf = df[stadt_col].str.contains(
        "Düsseldorf|Duesseldorf", case=False, na=False
    )
    ddf_indices = df[is_ddf].index
    logger.info(f"  Düsseldorf schools: {len(ddf_indices)}")

    # Initialize output columns
    nachfrage_cols = [
        "nachfrage_wuensche_2025_26",
        "nachfrage_plaetze_2025_26",
        "nachfrage_prozent_2025_26",
        "nachfrage_wuensche_2024_25",
        "nachfrage_aufnahmen_2024_25",
        "nachfrage_data_source",
    ]
    for col in nachfrage_cols:
        df[col] = None

    # Parse source data
    if school_type == "primary":
        records = parse_primary_csv(PRIMARY_CSV)
        logger.info(f"  Parsed {len(records)} primary schools from CSV")
        cache_key = "primary_2026_27"
    else:
        # Parse all secondary school PDFs
        records = []
        for pdf_path, schulform in [
            (GYMNASIEN_PDF, "Gymnasium"),
            (GESAMTSCHULEN_PDF, "Gesamtschule"),
            (REALSCHULEN_PDF, "Realschule"),
            (HAUPTSCHULEN_PDF, "Hauptschule"),
        ]:
            parsed = parse_secondary_pdf(pdf_path)
            for r in parsed:
                r["schulform"] = schulform
            records.extend(parsed)
            logger.info(
                f"  Parsed {len(parsed)} {schulform} schools from PDF"
            )
        cache_key = "secondary_2025_26"

    if not records:
        logger.warning("  No Anmeldezahlen records found!")
        return df

    # Build DB school list and run LLM matching
    ddf_df = df.loc[ddf_indices]
    db_schools = build_db_school_list(ddf_df)
    matches = llm_match_schools(records, db_schools, school_type, cache_key)

    if not matches:
        logger.warning("  No matches from LLM — enrichment skipped")
        return df

    # Apply matched data
    matched = 0
    unmatched_records = []

    for i, record in enumerate(records):
        if i not in matches:
            unmatched_records.append(record["school_name"])
            continue

        idx = matches[i]

        # Applications
        if record.get("anmeldungen_2025_26") is not None:
            df.at[idx, "nachfrage_wuensche_2025_26"] = record[
                "anmeldungen_2025_26"
            ]

        # Capacity (primary has it directly, secondary uses aufnahmen as proxy)
        if record.get("kapazitaet_2025_26") is not None:
            df.at[idx, "nachfrage_plaetze_2025_26"] = record[
                "kapazitaet_2025_26"
            ]
        elif record.get("aufnahmen_2024_25") is not None:
            # Use previous year actual admissions as capacity proxy
            df.at[idx, "nachfrage_plaetze_2025_26"] = record[
                "aufnahmen_2024_25"
            ]

        # Previous year
        if record.get("anmeldungen_2024_25") is not None:
            df.at[idx, "nachfrage_wuensche_2024_25"] = record[
                "anmeldungen_2024_25"
            ]

        if record.get("aufnahmen_2024_25") is not None:
            df.at[idx, "nachfrage_aufnahmen_2024_25"] = record[
                "aufnahmen_2024_25"
            ]

        # Compute oversubscription percentage
        wuensche = df.at[idx, "nachfrage_wuensche_2025_26"]
        plaetze = df.at[idx, "nachfrage_plaetze_2025_26"]
        if pd.notna(wuensche) and pd.notna(plaetze) and plaetze > 0:
            df.at[idx, "nachfrage_prozent_2025_26"] = round(
                wuensche / plaetze * 100, 0
            )

        df.at[idx, "nachfrage_data_source"] = "Düsseldorf Open Data"
        matched += 1

    logger.info(f"  Matched: {matched}/{len(records)} records")
    if unmatched_records:
        logger.info(
            f"  Unmatched ({len(unmatched_records)}): "
            f"{', '.join(unmatched_records[:10])}"
        )

    # Save
    output_file = (
        INTERMEDIATE_DIR
        / f"nrw_{school_type}_schools_with_anmeldezahlen.csv"
    )
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    logger.info(f"  Saved: {output_file}")

    # Summary
    filled = df["nachfrage_wuensche_2025_26"].notna().sum()
    print(f"\n{'=' * 70}")
    print(f"NRW ANMELDEZAHLEN ENRICHMENT ({school_type.upper()}) - COMPLETE")
    print(f"{'=' * 70}")
    print(f"  Total schools: {len(df)}")
    print(f"  Düsseldorf schools: {len(ddf_indices)}")
    print(f"  With Anmeldezahlen: {filled} ({filled / len(df) * 100:.1f}%)")
    print(f"  Matched records: {matched}/{len(records)}")

    if filled > 0:
        with_data = df[df["nachfrage_wuensche_2025_26"].notna()]
        print(f"\n  Application statistics:")
        print(
            f"    Applications (Wünsche) — "
            f"min: {with_data['nachfrage_wuensche_2025_26'].min():.0f}, "
            f"max: {with_data['nachfrage_wuensche_2025_26'].max():.0f}, "
            f"mean: {with_data['nachfrage_wuensche_2025_26'].mean():.0f}"
        )
        if with_data["nachfrage_plaetze_2025_26"].notna().any():
            has_pct = with_data[
                with_data["nachfrage_prozent_2025_26"].notna()
            ]
            if len(has_pct) > 0:
                print(
                    f"    Oversubscription % — "
                    f"min: {has_pct['nachfrage_prozent_2025_26'].min():.0f}%, "
                    f"max: {has_pct['nachfrage_prozent_2025_26'].max():.0f}%, "
                    f"mean: {has_pct['nachfrage_prozent_2025_26'].mean():.0f}%"
                )
                oversubscribed = (
                    has_pct["nachfrage_prozent_2025_26"] > 100
                ).sum()
                print(
                    f"    Oversubscribed (>100%): "
                    f"{oversubscribed}/{len(has_pct)} schools"
                )

    if unmatched_records:
        print(f"\n  Unmatched Anmeldezahlen records:")
        for name in unmatched_records:
            print(f"    - {name}")

    print(f"{'=' * 70}")

    return df


def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Starting NRW Anmeldezahlen Enrichment")
    logger.info("=" * 60)

    for school_type in ["secondary", "primary"]:
        try:
            enrich_schools(school_type)
        except FileNotFoundError as e:
            logger.warning(f"  Skipping {school_type}: {e}")
        except Exception as e:
            logger.error(f"  Error enriching {school_type}: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    main()
