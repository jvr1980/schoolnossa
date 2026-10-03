#!/usr/bin/env python3
"""
Replace per-district crime counts with comparable, sourced figures:
  crime_rate_per_1000  recorded offences per 1,000 residents in the school's area (latest year)
  crime_vs_city_pct    that rate vs the typical (median) area of the same city, in %  (district-level
                       cities only). Not the city-wide rate: city centres have very high per-resident
                       rates (Stuttgart-Mitte 665 per 1,000), which made almost every other district
                       look 'below average'.
  crime_change_pct     change of recorded offences vs the previous year, in %
plus crime_area, crime_area_level ('district' | 'city'), crime_rate_year, crime_change_years,
crime_source, and crime_safety_category / crime_safety_rank recomputed from the rate.

Why: the September 2026 review found that outside Berlin the district crime numbers were
not data — Köln, Düsseldorf and Stuttgart spread the city total over districts with
hand-set "crime index" multipliers, Bremen's were hand-typed approximations, and Leipzig's
matching had fallen back to a city average for most schools. Official sources:
  Bremen     Senat answers on crime per Beiratsbereich (PKS), 2022–2024
  Stuttgart  PKS Polizeipräsidium Stuttgart per Stadtbezirk, 2023–2025
  Leipzig    Stadt Leipzig open data per Ortsteil (PKS), 2022–2025
  München    Statistisches Amt München per Stadtbezirk, 2022–2025
  Berlin     Kriminalitätsatlas Berlin per Bezirk
  Köln, Düsseldorf, Frankfurt  no official district figures: city-wide rate only
Populations: official residents (Hauptwohnsitz) per unit from the city statistics offices.
Research files (with every source URL): data_shared/crime_rates_<date>/{crime_years,populations,extra}.json

Labels: 'safe' if the area rate is >= 15 % below the typical area, 'elevated' if >= 15 % above,
otherwise 'moderate'; NULL where only a city-wide figure exists. Rank: 1 = lowest area rate.

The fabricated legacy columns (crime_*_2023/2024/avg/yoy_pct, crime_total_crimes_current, …)
are set to NULL for the cities whose values were modelled. Berlin keeps them.

Output: <out>/apply.sql (area-level UPDATEs), backup.sql (copies all crime_* columns of the
affected rows into _crime_backup_<date>), summary.csv.

Usage:
    venv/bin/python scripts_shared/processing/compute_crime_rates.py --research <dir> --out <dir> [--cities ...]
"""
import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'scripts_shared' / 'enrichment'))
import replicate_lovable_description_jobs as job  # noqa: E402

SAFE, ELEVATED = -15.0, 15.0
PRIMARY_LEGACY = ['crime_total_crimes_2023', 'crime_total_crimes_2024', 'crime_total_crimes_avg',
                  'crime_total_crimes_current', 'crime_violent_crime_avg']
NEW_COLS = ['crime_area', 'crime_area_level', 'crime_rate_per_1000', 'crime_rate_year', 'crime_vs_city_pct',
            'crime_change_pct', 'crime_change_years', 'crime_source', 'crime_safety_category', 'crime_safety_rank',
            'crime_data_year']
BREMEN_GROUPS = {  # our ortsteil value → (crime units, population units)
    'Horn / Borgf. / Oberneuland': (['Horn-Lehe', 'Borgfeld', 'Oberneuland'], ['Horn-Lehe', 'Borgfeld', 'Oberneuland']),
    'Mitte / Östl. Vorstadt': (['Mitte', 'Östliche Vorstadt'], ['Mitte/Östliche Vorstadt']),
    'Findorff / Walle': (['Findorff', 'Walle'], ['Findorff', 'Walle']),
    'Gröpelingen': (['Gröpelingen und Industriehäfen'], ['Gröpelingen']),
    **{n: ([n], [n]) for n in ('Burglesum', 'Hemelingen', 'Vahr', 'Neustadt', 'Obervieland', 'Huchting', 'Osterholz',
                               'Schwachhausen', 'Vegesack', 'Blumenthal', 'Woltmershausen')},
}
SOURCES = {
    'bremen': 'PKS Bremen per Beiratsbereich (Senat der Freien Hansestadt Bremen)',
    'stuttgart': 'PKS Polizeipräsidium Stuttgart per Stadtbezirk',
    'leipzig': 'PKS per Ortsteil (Stadt Leipzig open data)',
    'muenchen': 'PKS per Stadtbezirk (Statistisches Amt München)',
    'berlin': 'Kriminalitätsatlas Berlin per Bezirk',
    'koeln': 'PKS Köln, city-wide', 'duesseldorf': 'PKS Düsseldorf, city-wide', 'frankfurt': 'PKS Frankfurt am Main, city-wide',
}


def pct(a, b):
    return round((a / b - 1) * 100, 1) if a is not None and b else None


def district_units(city, crime, pop):
    """{our area value: (label, {year: crimes}, {year: residents})}"""
    if city == 'bremen':
        out = {}
        for ours, (cu, pu) in BREMEN_GROUPS.items():
            years = set.intersection(*[set(crime['units'][u]) for u in cu])
            cr = {y: sum(crime['units'][u][y] for u in cu) for y in years}
            po = {}
            for y in ('2023', '2024', '2025'):
                vals = [pop['units'].get(u, {}).get(f'pop_{y}') for u in pu]
                if all(vals):
                    po[y] = sum(vals)
            out[ours] = (ours, cr, po)
        return out
    return {u: (u, {y: v for y, v in crime['units'][u].items()},
                {y: pop['units'][u].get(f'pop_{y}') for y in ('2023', '2024', '2025') if pop['units'].get(u, {}).get(f'pop_{y}')})
            for u in crime['units'] if u in pop['units']}


def latest_year(crimes, pops):
    common = sorted(set(crimes) & set(pops))
    return common[-1] if common else None


def compute_city(city, cfg, research):
    crime, pop = cfg['crime'](research), cfg['pop'](research)
    city_crime, city_pop = crime['city_total'], pop['city_total']
    y = latest_year({k: v for k, v in city_crime.items() if v}, {k: v for k, v in city_pop.items() if v})
    city_rate = city_crime[y] / city_pop[y] * 1000
    prev = str(int(y) - 1)
    if cfg['level'] == 'city':
        return {None: {'crime_area': cfg['label'], 'crime_area_level': 'city', 'crime_rate_per_1000': round(city_rate, 1),
                       'crime_rate_year': int(y), 'crime_vs_city_pct': None,
                       'crime_change_pct': pct(city_crime[y], city_crime.get(prev)),
                       'crime_change_years': f'{prev}→{y}' if city_crime.get(prev) else None,
                       'crime_source': SOURCES[city], 'crime_safety_category': None, 'crime_safety_rank': None,
                       'crime_data_year': y}}
    units = {k: v for k, v in district_units(city, crime, pop).items() if y in v[1] and y in v[2]}
    rates = sorted(cr[y] / po[y] * 1000 for _, cr, po in units.values())
    typical = rates[len(rates) // 2] if len(rates) % 2 else (rates[len(rates) // 2 - 1] + rates[len(rates) // 2]) / 2
    rows = {}
    for ours, (label, cr, po) in units.items():
        rate = cr[y] / po[y] * 1000
        vs = pct(rate, typical)
        rows[ours] = {'crime_area': label, 'crime_area_level': 'district', 'crime_rate_per_1000': round(rate, 1),
                      'crime_rate_year': int(y), 'crime_vs_city_pct': vs,
                      'crime_change_pct': pct(cr[y], cr.get(prev)), 'crime_change_years': f'{prev}→{y}' if cr.get(prev) else None,
                      'crime_source': SOURCES[city],
                      'crime_safety_category': 'safe' if vs <= SAFE else 'elevated' if vs >= ELEVATED else 'moderate',
                      'crime_data_year': y}
    for rank, ours in enumerate(sorted(rows, key=lambda k: rows[k]['crime_rate_per_1000']), 1):
        rows[ours]['crime_safety_rank'] = rank
    return rows


def lit(v):
    if v is None:
        return 'NULL'
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--research', type=Path, required=True)
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--cities', default='bremen,stuttgart,leipzig,koeln,duesseldorf')
    args = ap.parse_args()
    research = {f: json.loads((args.research / f'{f}.json').read_text()) for f in ('crime_years', 'populations')}
    if (args.research / 'extra.json').exists():
        research['extra'] = json.loads((args.research / 'extra.json').read_text())
    configs = {
        'bremen': {'level': 'district', 'field': 'ortsteil', 'crime': lambda r: r['crime_years']['Bremen'], 'pop': lambda r: r['populations']['Bremen']},
        'stuttgart': {'level': 'district', 'field': 'bezirk', 'crime': lambda r: r['crime_years']['Stuttgart'], 'pop': lambda r: r['populations']['Stuttgart']},
        'leipzig': {'level': 'district', 'field': 'ortsteil', 'crime': lambda r: r['crime_years']['Leipzig'], 'pop': lambda r: r['populations']['Leipzig']},
        'koeln': {'level': 'city', 'label': 'Köln', 'crime': lambda r: r['crime_years']['Köln'], 'pop': lambda r: r['populations']['Köln']},
        'duesseldorf': {'level': 'city', 'label': 'Düsseldorf', 'crime': lambda r: r['crime_years']['Düsseldorf'], 'pop': lambda r: r['populations']['Düsseldorf']},
        'frankfurt': {'level': 'city', 'label': 'Frankfurt am Main', 'crime': lambda r: r['crime_years']['Frankfurt'],
                      'pop': lambda r: {'city_total': r['extra']['Frankfurt']['city_total']}},
    }
    crime_cols = [c for c in ('crime_aggravated_assault_2023', 'crime_aggravated_assault_2024', 'crime_aggravated_assault_avg',
                              'crime_aggravated_assault_yoy_pct', 'crime_assault_2023', 'crime_assault_2024', 'crime_assault_avg',
                              'crime_assault_yoy_pct', 'crime_bike_theft_2023', 'crime_bike_theft_2024', 'crime_bike_theft_avg',
                              'crime_bike_theft_yoy_pct', 'crime_drug_offenses_2023', 'crime_drug_offenses_2024',
                              'crime_drug_offenses_avg', 'crime_drug_offenses_yoy_pct', 'crime_neighborhood_crimes_2023',
                              'crime_neighborhood_crimes_2024', 'crime_neighborhood_crimes_avg', 'crime_neighborhood_crimes_yoy_pct',
                              'crime_robbery_2023', 'crime_robbery_2024', 'crime_robbery_avg', 'crime_robbery_yoy_pct',
                              'crime_street_robbery_2023', 'crime_street_robbery_2024', 'crime_street_robbery_avg',
                              'crime_street_robbery_yoy_pct', 'crime_threats_coercion_2023', 'crime_threats_coercion_2024',
                              'crime_threats_coercion_avg', 'crime_threats_coercion_yoy_pct', 'crime_total_crimes_2023',
                              'crime_total_crimes_2024', 'crime_total_crimes_avg', 'crime_total_crimes_current',
                              'crime_total_crimes_yoy_pct', 'crime_violent_crime_avg')]
    args.out.mkdir(parents=True, exist_ok=True)
    stmts, summary = [], []
    cities = args.cities.split(',')
    for city in cities:
        rows = compute_city(city, configs[city], research)
        for tbl in ('schools', 'primary_schools'):
            legacy_null = ', '.join(f'{c} = NULL' for c in (crime_cols if tbl == 'schools' else PRIMARY_LEGACY))
            # reset everything first, so schools in unmatched areas (e.g. non-Leipzig rows) are left without figures
            stmts.append(f"UPDATE public.{tbl} SET {legacy_null}, " + ', '.join(f'{c} = NULL' for c in NEW_COLS) +
                         f" WHERE city = '{city}';")
            for ours, vals in rows.items():
                sets = ', '.join(f'{k} = {lit(v)}' for k, v in vals.items())
                where = f"city = '{city}'" + (f" AND {configs[city]['field']} = {lit(ours)}" if ours is not None else '')
                stmts.append(f"UPDATE public.{tbl} SET {sets} WHERE {where};")
        for ours, vals in rows.items():
            summary.append({'city': city, 'area': ours or vals['crime_area'], **vals})
    if 'stuttgart' in cities:  # register fix found by the crime research: the school is in Stuttgart-Nord
        stmts.insert(0, "UPDATE public.schools SET bezirk = 'Nord', ortsteil = 'Nord' WHERE city = 'stuttgart' "
                        "AND schulname = 'Eberhard-Ludwigs-Gymnasium' AND bezirk = 'West';")
    (args.out / 'apply.sql').write_text('\n'.join(stmts) + '\n', encoding='utf-8')
    # one backup row per school; primary_schools lack most legacy columns, so they are NULL there
    keep = crime_cols + ['crime_safety_category', 'crime_safety_rank', 'crime_data_year']
    sel_s = ', '.join(keep)
    sel_p = ', '.join(c if c in PRIMARY_LEGACY + ['crime_safety_category', 'crime_safety_rank', 'crime_data_year']
                      else f'NULL::{"numeric" if c.endswith(("_pct", "_avg", "_current")) else "integer"} AS {c}' for c in keep)
    in_cities = ', '.join(f"'{c}'" for c in cities)
    table = f"_crime_backup_{args.out.name[-10:].replace('-', '')}"
    (args.out / 'backup.sql').write_text(
        f"CREATE TABLE IF NOT EXISTS public.{table} AS SELECT 'schools'::text AS tbl, id, city, {sel_s} "
        f"FROM public.schools WHERE false;\n"
        f"INSERT INTO public.{table} SELECT 'schools', id, city, {sel_s} FROM public.schools WHERE city IN ({in_cities}) "
        f"AND id NOT IN (SELECT id FROM public.{table});\n"
        f"INSERT INTO public.{table} SELECT 'primary_schools', id, city, {sel_p} FROM public.primary_schools "
        f"WHERE city IN ({in_cities}) AND id NOT IN (SELECT id FROM public.{table});\n"
        f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY;\n", encoding='utf-8')
    pd.DataFrame(summary).to_csv(args.out / 'summary.csv', index=False, encoding='utf-8-sig')
    print(pd.DataFrame(summary)[['city', 'area', 'crime_rate_per_1000', 'crime_rate_year', 'crime_vs_city_pct',
                                 'crime_change_pct', 'crime_safety_category']].to_string())
    print(f"{len(stmts)} statements → {args.out}/apply.sql")


if __name__ == '__main__':
    main()
