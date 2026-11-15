#!/usr/bin/env python3
"""
Build `schools` (secondary) rows for Berlin Gemeinschaftsschulen, plus the SQL
that inserts them.

Why: until 2026-11 the directory scrape only searched Schulart 41 (ISS) and 14
(Gymnasien). Gemeinschaftsschulen (Schulart 42, Klasse 1-10/13) never entered
the secondary table, although their Sekundarstufe is an ISS in all but name and
families compare them with ISS. Like the all-through private schools, they
belong in both tables (their Grundstufe is in primary_schools).

bildung_berlin_iss_scraper.py now scrapes Schulart 42 too, so a future full
pipeline run carries them. This script builds the *delta* rows without
re-running the whole pipeline (which would re-pay every API call for the ~260
existing schools):

  identity/contact/stats   data_berlin/intermediate/combined_schools_with_metadata_msa.csv
  2025/26 pupils/teachers  data_berlin/raw/bildungsstatistik_2025_26.csv (whole school,
                           as for the other all-through schools)
  stable *_current fields  scripts_shared.schema.stable_fields.add_stable_fields
  school_type              ISS, or ISS-Gymnasium when Abitur data exists (mirrors
                           the app's reclassify_iss_with_abitur RPC)
  lat/lon                  the school's primary_schools coordinates when the street
                           matches (same site; snapshot file below), else Google
                           geocoding
  transit_*                BVG API (enrich_schools_with_transit.enrich_school)
  poi_*                    Google Places (enrich_schools_with_pois.enrich_school)

Filled inside the database by the emitted SQL (no local copy needed):
  crime_*                  from an existing secondary row of the same Bezirk (Berlin
                           crime is district-level: one variant per Bezirk,
                           verified 2026-11-15)
  env_*                    from the same-site primary_schools row

The anon key can no longer read `schools`/`primary_schools` (RLS), so primary
coordinates come from a snapshot taken with the Lovable SQL tool:
data_berlin/cache/gms_primary_sites_2026-11-15.json.

Descriptions, embeddings, the remaining environment data and private-school
fees are app-owned jobs (Admin -> Data Import, "Fehlende"); they fill the gaps
after insert. Abitur predictions and admission/open days run afterwards.

API results are cached in data_berlin/cache/gms_secondary_enrichment_cache.json,
so a rerun is free.

Outputs:
  data_berlin/intermediate/berlin_gemeinschaftsschulen_secondary_rows.csv
  data_shared/supabase_sql/insert_berlin_gms_secondary_<date>.sql   (idempotent:
      skips any schulnummer already in `schools`; run with the Lovable SQL tool)

Usage:
    python3 scripts_berlin/processing/build_gemeinschaftsschulen_secondary_rows.py [--ids 01K04,...] [--no-api]
"""

import argparse
import csv
import json
import math
import os
import re
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'scripts_berlin' / 'enrichment'))
sys.path.insert(0, str(PROJECT_ROOT / 'scripts_shared' / 'enrichment'))
load_dotenv(PROJECT_ROOT / '.env')

from scripts_shared.schema.stable_fields import add_stable_fields  # noqa: E402

COMBINED = PROJECT_ROOT / 'data_berlin' / 'intermediate' / 'combined_schools_with_metadata_msa.csv'
STATS_2025_26 = PROJECT_ROOT / 'data_berlin' / 'raw' / 'bildungsstatistik_2025_26.csv'
PRIMARY_SITES = PROJECT_ROOT / 'data_berlin' / 'cache' / 'gms_primary_sites_2026-11-15.json'
CACHE = PROJECT_ROOT / 'data_berlin' / 'cache' / 'gms_secondary_enrichment_cache.json'
OUTPUT = PROJECT_ROOT / 'data_berlin' / 'intermediate' / 'berlin_gemeinschaftsschulen_secondary_rows.csv'
SQL_DIR = PROJECT_ROOT / 'data_shared' / 'supabase_sql'

# Berlin Schulnummer prefix -> Bezirk
BEZIRK_BY_PREFIX = {
    '01': 'Mitte', '02': 'Friedrichshain-Kreuzberg', '03': 'Pankow',
    '04': 'Charlottenburg-Wilmersdorf', '05': 'Spandau', '06': 'Steglitz-Zehlendorf',
    '07': 'Tempelhof-Schöneberg', '08': 'Neukölln', '09': 'Treptow-Köpenick',
    '10': 'Marzahn-Hellersdorf', '11': 'Lichtenberg', '12': 'Reinickendorf',
}
# New schools taught at an interim site until their building is finished
# (directory "Zusätzliche Angebote", 2026-11-15; 05K10 confirmed on its own site:
# Schuldorf Hakenfelde until 2029/30). Families applying now attend the interim
# site for years, so address, coordinates, transit and POIs use it; the
# permanent address goes into besonderheiten.
INTERIM_SITES = {
    '09K10': {'strasse': 'Alfred-Randt-Str. 56', 'plz': '12559', 'ortsteil': 'Köpenick',
              'besonderheiten': 'Bis zur baulichen Fertigstellung des Schulstandorts Eisenhutweg 129 '
                                '(12487 Johannisthal) findet der Unterricht am Standort Alfred-Randt-Str. 56, '
                                '12559 Berlin statt.'},
    '05K10': {'strasse': 'Am Forstacker 11', 'plz': '13587', 'ortsteil': 'Hakenfelde',
              'besonderheiten': 'Neue Gemeinschaftsschule, seit Sommer 2026 im Aufbau der Sekundarstufe I. '
                                'Bis zur Fertigstellung des Neubaus Gartenfelder Str. 28 (Insel Gartenfeld, '
                                'geplant zum Schuljahr 2029/30, dann auch mit Oberstufe und Grundstufe) findet '
                                'der Unterricht im Schuldorf Hakenfelde, Am Forstacker 11, 13587 Berlin statt.'},
}
EXTRA_BESONDERHEITEN = {
    '02K11': 'Inklusive Schwerpunktschule und Auftragsschule für den Förderschwerpunkt Autismus und Lernen.',
}

ABITUR_COLS = ['abitur_durchschnitt_2023', 'abitur_durchschnitt_2024', 'abitur_durchschnitt_2025',
               'abitur_erfolgsquote_2024', 'abitur_erfolgsquote_2025']

# Supabase `schools` columns outside the poi_/transit_ families (snapshot
# 2026-11-15). Anything else the local frame carries (msa_*, *_2025_26 counts,
# notendurchschnitt_unified_*) has no column and is dropped.
DB_COLUMNS = set("""
schulnummer schulname school_type schulart traegerschaft gruendungsjahr strasse plz ortsteil
bezirk latitude longitude telefon email website leitung schueler_2024_25 lehrer_2024_25
schueler_2023_24 lehrer_2023_24 sprachen abitur_durchschnitt_2024 abitur_durchschnitt_2023
abitur_durchschnitt_2025 abitur_erfolgsquote_2024 abitur_erfolgsquote_2025
nachfrage_plaetze_2025_26 nachfrage_wuensche_2025_26 nachfrage_prozent_2025_26 belastungsstufe
migration_2024_25 besonderheiten schueler_2022_23 lehrer_2022_23 nachfrage_plaetze_2024_25
nachfrage_wuensche_2024_25 migration_2023_24 metadata_source city tuition_display
schueler_current lehrer_current migration_current nachfrage_prozent_current
abitur_durchschnitt_current data_school_year abitur_year geo_country geo_region
geo_municipality lehrer_data_year migration_data_year
""".split())
DB_PREFIXES = ('poi_', 'transit_')

ENV_COLS = ['env_aqi_annual_avg', 'env_aqi_category', 'env_aqi_dominant_pollutant', 'env_aqi_fetched_at',
            'env_pollen_tree_avg', 'env_pollen_grass_avg', 'env_pollen_weed_avg', 'env_pollen_overall_avg',
            'env_pollen_fetched_at']


def norm_street(s):
    s = str(s or '').lower().replace('straße', 'str').replace('strasse', 'str').replace('.', '')
    return re.sub(r'[\s-]+', '', s)


def load_json(path, default):
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else default


def save_cache(cache):
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=1), encoding='utf-8')


def load_stats_2025_26():
    out = {}
    with open(STATS_2025_26, encoding='utf-8-sig') as f:
        for r in csv.DictReader(f, delimiter=';'):
            out[r['BSN']] = (r.get('Schüler (m/w/d)'), r.get('Lehrkräfte (m,w,d)'))
    return out


def geocode(row, cache, use_api):
    key = row['schulnummer']
    address = f"{row['strasse']}, {row['plz']} Berlin, Germany"
    hit = cache.setdefault('geocode', {}).get(key)
    if hit and hit.get('address') == address:
        return hit['lat'], hit['lon']
    if not use_api:
        return None, None
    from enrich_schools_with_traffic import geocode_address_google
    lat, lon = geocode_address_google(address, os.environ['GOOGLE_PLACES_API_KEY'])
    if lat is not None:
        cache['geocode'][key] = {'lat': lat, 'lon': lon, 'address': address, 'source': 'google'}
    time.sleep(0.1)
    return lat, lon


def transit(key, lat, lon, cache, use_api):
    hit = cache.setdefault('transit', {}).get(key)
    if hit is None and use_api:
        from enrich_schools_with_transit import enrich_school
        hit = enrich_school(lat, lon)
        if hit:
            cache['transit'][key] = hit
        time.sleep(0.3)
    if not hit:
        return {}
    out = dict(hit)
    # Supabase keeps the nearest stop un-numbered (transit_rail_name, ...) and
    # only its coordinates as _01_latitude/_01_longitude.
    for t in ('rail', 'tram', 'bus'):
        for f in ('name', 'distance_m', 'lines'):
            out[f'transit_{t}_{f}'] = out.pop(f'transit_{t}_01_{f}', None)
    return out


def pois(key, lat, lon, name, cache, use_api):
    hit = cache.setdefault('poi', {}).get(key)
    if hit is None and use_api:
        from enrich_schools_with_pois import enrich_school
        _, hit, _ = enrich_school(0, lat, lon, name)
        cache['poi'][key] = hit
    return hit or {}


def clean(v):
    """JSON-safe scalar: NaN -> None, numpy -> python, integral floats -> int."""
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v):
            return None
        return int(v) if v.is_integer() else v
    if hasattr(v, 'item'):
        return clean(v.item())
    return v


CHUNK_ROWS = 25  # ~65 KB per statement


def emit_sql(out, stem):
    """Write idempotent SQL for the Lovable SQL tool, one file per call:
    <stem>_insert_partN.sql  column names once + one value array per row; each row
                             is zipped back into a JSON object and cast to the
                             table's column types by jsonb_populate_record; rows
                             whose schulnummer already exists are skipped
    <stem>_updates.sql       in-database crime_* (same Bezirk) and env_* (same-site
                             primary entry) copies; only fills NULL cells
    Returns the written paths."""
    cols = list(out.columns)
    rows = [[clean(v) for v in r] for r in out.itertuples(index=False)]
    col_list = ', '.join(cols)
    sel_list = ', '.join(f'x.{c}' for c in cols)
    head = (f"-- Berlin Gemeinschaftsschulen -> schools (secondary), {date.today().isoformat()}\n"
            f"-- Built by scripts_berlin/processing/build_gemeinschaftsschulen_secondary_rows.py\n")
    stem.parent.mkdir(parents=True, exist_ok=True)
    paths = []
    sn = cols.index('schulnummer')
    for n, start in enumerate(range(0, len(rows), CHUNK_ROWS), 1):
        chunk = rows[start:start + CHUNK_ROWS]
        payload = json.dumps({'c': cols, 'r': chunk}, ensure_ascii=False, separators=(',', ':'))
        assert '$gms$' not in payload
        sql = (head + f"-- Insert part {n}: {', '.join(r[sn] for r in chunk)}\n"
               f"with d as (select $gms${payload}$gms$::jsonb j),\n"
               f"recs as (\n"
               f"  select (select jsonb_object_agg(c.k #>> '{{}}', v.val)\n"
               f"          from jsonb_array_elements(d.j->'c') with ordinality c(k, i)\n"
               f"          join jsonb_array_elements(r.rv) with ordinality v(val, i) using (i)) o\n"
               f"  from d, jsonb_array_elements(d.j->'r') r(rv))\n"
               f"insert into public.schools ({col_list})\n"
               f"select {sel_list}\n"
               f"from recs, jsonb_populate_record(null::public.schools, recs.o) x\n"
               f"where not exists (select 1 from public.schools s where s.schulnummer = x.schulnummer)\n"
               f"returning schulnummer;\n")
        p = stem.with_name(f'{stem.name}_insert_part{n}.sql')
        p.write_text(sql, encoding='utf-8')
        paths.append(p)

    ids = ', '.join("'" + r[sn] + "'" for r in rows)
    crime_set = ',\n'.join(f'  {c} = src.{c}' for c in CRIME_COLS)
    env_set = ',\n'.join(f'  {c} = p.{c}' for c in ENV_COLS)
    sql = head + f"""-- Run after all insert parts.
-- 1) District-level crime block, copied from an existing secondary row of the same Bezirk
update public.schools n set
{crime_set}
from (select distinct on (bezirk) * from public.schools
      where city = 'berlin' and crime_rate_per_1000 is not null and schulnummer not in ({ids})
      order by bezirk, schulnummer) src
where n.schulnummer in ({ids}) and n.bezirk = src.bezirk and n.crime_rate_per_1000 is null;

-- 2) Environment data from the same-site primary entry (same coordinates)
update public.schools n set
{env_set}
from public.primary_schools p
where n.schulnummer in ({ids}) and p.schulnummer = n.schulnummer
  and p.latitude = n.latitude and p.longitude = n.longitude and n.env_aqi_fetched_at is null;
"""
    p = stem.with_name(f'{stem.name}_updates.sql')
    p.write_text(sql, encoding='utf-8')
    paths.append(p)
    return paths


CRIME_COLS = """crime_safety_rank crime_safety_category crime_violent_crime_avg crime_total_crimes_2023
crime_total_crimes_2024 crime_robbery_2023 crime_street_robbery_2023 crime_assault_2023
crime_aggravated_assault_2023 crime_threats_coercion_2023 crime_bike_theft_2023 crime_drug_offenses_2023
crime_neighborhood_crimes_2023 crime_robbery_2024 crime_street_robbery_2024 crime_assault_2024
crime_aggravated_assault_2024 crime_threats_coercion_2024 crime_bike_theft_2024 crime_drug_offenses_2024
crime_neighborhood_crimes_2024 crime_total_crimes_avg crime_robbery_avg crime_street_robbery_avg
crime_assault_avg crime_aggravated_assault_avg crime_threats_coercion_avg crime_bike_theft_avg
crime_drug_offenses_avg crime_neighborhood_crimes_avg crime_total_crimes_yoy_pct crime_robbery_yoy_pct
crime_street_robbery_yoy_pct crime_assault_yoy_pct crime_aggravated_assault_yoy_pct
crime_threats_coercion_yoy_pct crime_bike_theft_yoy_pct crime_drug_offenses_yoy_pct
crime_neighborhood_crimes_yoy_pct crime_total_crimes_current crime_data_year crime_area crime_area_level
crime_rate_per_1000 crime_rate_year crime_vs_city_pct crime_change_pct crime_change_years
crime_source""".split()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--ids', help='Comma-separated Schulnummern (default: all Gemeinschaftsschulen)')
    ap.add_argument('--no-api', action='store_true', help='Use cached API results only')
    args = ap.parse_args()
    use_api = not args.no_api

    df = pd.read_csv(COMBINED, encoding='utf-8-sig', dtype=str)
    df = df[df['schulart'] == 'Gemeinschaftsschule'].copy()
    if args.ids:
        wanted = [i.strip() for i in args.ids.split(',') if i.strip()]
        df = df[df['schulnummer'].isin(wanted)].copy()
    df = df.reset_index(drop=True)
    print(f'{len(df)} Gemeinschaftsschulen to build')

    df['bezirk'] = df['bezirk'].fillna(df['schulnummer'].str[:2].map(BEZIRK_BY_PREFIX))
    # The directory's mailto links can carry URL-encoded whitespace ("%09foo@...");
    # 61 existing Berlin rows have it too (reported separately, 2026-11-15).
    df['email'] = df['email'].str.replace(r'^(?:%[0-9A-Fa-f]{2}|\s)+', '', regex=True).str.strip()
    # belastungsstufe is text in Supabase ("3", not "3.0")
    df['belastungsstufe'] = pd.to_numeric(df['belastungsstufe'], errors='coerce').map(
        lambda v: None if pd.isna(v) else str(int(v)))
    if 'besonderheiten' not in df.columns:
        df['besonderheiten'] = None
    for key, site in INTERIM_SITES.items():
        m = df['schulnummer'] == key
        for col, val in site.items():
            df.loc[m, col] = val
    for key, text in EXTRA_BESONDERHEITEN.items():
        df.loc[(df['schulnummer'] == key) & df['besonderheiten'].isna(), 'besonderheiten'] = text

    stats = load_stats_2025_26()
    df['schueler_2025_26'] = df['schulnummer'].map(lambda b: (stats.get(b) or (None, None))[0] or None)
    df['lehrer_2025_26'] = df['schulnummer'].map(lambda b: (stats.get(b) or (None, None))[1] or None)
    num_cols = ['schueler_2025_26', 'lehrer_2025_26', 'schueler_2024_25', 'lehrer_2024_25',
                'schueler_2023_24', 'lehrer_2023_24', 'nachfrage_plaetze_2025_26',
                'nachfrage_wuensche_2025_26', 'nachfrage_prozent_2025_26', 'migration_2024_25',
                'migration_2023_24'] + [c for c in ABITUR_COLS if c in df.columns]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    df = add_stable_fields(df)

    has_abitur = df[[c for c in ABITUR_COLS if c in df.columns]].notna().any(axis=1)
    df['school_type'] = has_abitur.map({True: 'ISS-Gymnasium', False: 'ISS'})

    df['city'] = 'berlin'
    df['geo_country'], df['geo_region'], df['geo_municipality'] = 'DE', 'Berlin', 'Berlin'
    df.loc[df['traegerschaft'] == 'Öffentlich', 'tuition_display'] = 'Free (public)'

    sites = load_json(PRIMARY_SITES, {})
    cache = load_json(CACHE, {})
    rows = []
    try:
        for i, row in df.iterrows():
            key = row['schulnummer']
            rec = row.to_dict()
            site = sites.get(key)
            if site and norm_street(site['strasse']) == norm_street(row['strasse']):
                lat, lon, src = site['latitude'], site['longitude'], 'primary site'
            else:
                (lat, lon), src = geocode(row, cache, use_api), 'geocoded'
            rec['latitude'], rec['longitude'] = lat, lon
            if lat is not None:
                site_key = f'{key}@{float(lat):.6f},{float(lon):.6f}'  # re-fetch if the site moves
                rec.update(transit(site_key, lat, lon, cache, use_api))
                rec.update(pois(site_key, lat, lon, row['schulname'], cache, use_api))
            rows.append(rec)
            print(f"[{i + 1}/{len(df)}] {key} {row['schulname'][:48]:48} {src:12} "
                  f"transit={'y' if rec.get('transit_accessibility_score') is not None else 'n'} "
                  f"poi={'y' if rec.get('poi_kita_count_500m') is not None else 'n'}")
            if (i + 1) % 10 == 0:
                save_cache(cache)
    finally:
        save_cache(cache)

    out = pd.DataFrame(rows)
    keep = [c for c in out.columns if c in DB_COLUMNS or c.startswith(DB_PREFIXES)]
    dropped = sorted(set(out.columns) - set(keep))
    out = out[keep]
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT, index=False, encoding='utf-8-sig')
    print(f'\nWrote {len(out)} rows x {len(keep)} columns to {OUTPUT.relative_to(PROJECT_ROOT)}')
    print(f'Dropped (no Supabase column): {dropped}')
    check = [c for c in ('latitude', 'transit_accessibility_score', 'poi_kita_count_500m') if c in out.columns]
    missing = out[check].isna().sum()
    print('Rows still missing:', missing[missing > 0].to_dict() or 'none')

    paths = emit_sql(out, SQL_DIR / f'insert_berlin_gms_secondary_{date.today().isoformat()}')
    print('SQL for the Lovable SQL tool (run in order):')
    for p in paths:
        print(f'  {p.relative_to(PROJECT_ROOT)}  ({p.stat().st_size // 1024} KB)')


if __name__ == '__main__':
    main()
