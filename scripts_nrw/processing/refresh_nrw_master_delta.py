#!/usr/bin/env python3
"""
Apply the NRW source deltas to the Düsseldorf/Köln final tables, without
re-running the pipeline.

Two sources change between pipeline runs:
- the Schulsozialindex list, republished each September for the new school
  year → sozialindexstufe (the list is authoritative, so finals are compared
  with it directly);
- schuldaten.csv, the ministry's rolling master list → schulname,
  kurzbezeichnung, strasse, plz, website, and latitude/longitude for schools
  that moved. Only fields that changed at the source since the baseline
  snapshot are written, so values curated after the scrape are left alone.

Rows are matched on schulnummer; no rows are added or removed. Schools that
opened in scope, or finals rows that left it, are reported for a manual
decision (adding one needs the full enrichment chain for that row).

--emit-sql DIR writes UPDATEs for the master fields Supabase stores (it has no
Sozialindex column). Run them via the Lovable MCP SQL tool.

Usage:
    venv/bin/python scripts_nrw/processing/refresh_nrw_master_delta.py --dry-run
    venv/bin/python scripts_nrw/processing/refresh_nrw_master_delta.py \
        --emit-sql data_shared/supabase_sql/nrw_master_delta_2026-09
"""

import argparse
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
for p in (PROJECT_ROOT, PROJECT_ROOT / "scripts_nrw" / "scrapers"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import nrw_school_master_scraper as scraper  # noqa: E402
from scripts_shared.processing.refresh_traffic_columns import CITY_CONFIG, _load, _save  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

FINAL_FILES = [rel for _, finals in CITY_CONFIG['nrw'] for rel in finals]
CACHE_DIR = PROJECT_ROOT / "data_nrw" / "cache"
BASELINE = CACHE_DIR / "nrw_schuldaten_baseline.csv"
LEGACY_BASELINE = PROJECT_ROOT / "data_nrw" / "raw" / "nrw_schuldaten_raw.csv"  # the April 2026 scrape

MASTER_FIELDS = ['schulname', 'kurzbezeichnung', 'strasse', 'plz', 'website']
COORD_FIELDS = ['latitude', 'longitude']
ALLOWED = set(MASTER_FIELDS + COORD_FIELDS + ['sozialindexstufe'])
# Supabase keeps these (no kurzbezeichnung, no Sozialindex)
SQL_FIELDS = {'schulname': 'text', 'strasse': 'text', 'plz': 'text', 'website': 'text',
              'latitude': 'num', 'longitude': 'num'}


def _norm(v):
    """Comparable text for a cell: None/NaN/'' → ''."""
    if v is None or (isinstance(v, float) and pd.isna(v)) or v is pd.NA:
        return ''
    return ' '.join(str(v).split())


def load_master(content: bytes) -> pd.DataFrame:
    """schuldaten.csv → normalized active schools indexed by schulnummer (all of NRW)."""
    df = scraper.parse_schuldaten_csv(content)
    df = scraper.filter_active_schools(df)
    df = scraper.convert_utm_to_wgs84(df)
    df = scraper.normalize_columns(df)
    df['schulnummer'] = df['schulnummer'].astype(str).str.strip()
    return df.set_index('schulnummer')


def source_changes(base: pd.DataFrame, cur: pd.DataFrame) -> dict:
    """{schulnummer: {field: new_value}} for fields that changed at the source.

    Schools whose Ort is no longer Düsseldorf/Köln are skipped: scope_report
    lists them, and dropping them is a manual decision.
    """
    changes = {}
    in_cities = cur['ort'].str.strip().isin(scraper.TARGET_CITIES)
    for snr in base.index.intersection(cur.index[in_cities]):
        b, c = base.loc[snr], cur.loc[snr]
        diff = {f: c[f] for f in MASTER_FIELDS if _norm(b.get(f)) != _norm(c.get(f))}
        if (_norm(b.get('UTMRechtswert')), _norm(b.get('UTMHochwert'))) != \
                (_norm(c.get('UTMRechtswert')), _norm(c.get('UTMHochwert'))):
            diff.update({f: c[f] for f in COORD_FIELDS})
        if diff:
            changes[snr] = diff
    return changes


def load_sozialindex():
    url, school_year = scraper.resolve_schulsozialindex_url()
    path = CACHE_DIR / f"nrw_schulsozialindex_sj_{school_year}.csv"
    if not path.exists():
        path.write_bytes(scraper.download_file(url, "Schulsozialindex"))
    ssi = scraper.parse_schulsozialindex_csv(path.read_bytes())
    ssi['Schulnummer'] = ssi['Schulnummer'].astype(str).str.strip()
    stufe = pd.to_numeric(ssi['Sozialindexstufe'].replace('ohne', None), errors='coerce')
    logger.info(f"Schulsozialindex SJ {school_year}: {len(ssi)} schools ({path.name})")
    return dict(zip(ssi['Schulnummer'], stufe)), school_year


def refresh_file(path: Path, changes: dict, ssi: dict, dry_run: bool):
    df = _load(path)
    before = df.copy()
    snr = df['schulnummer'].astype(str).str.strip()
    edits = []  # (schulnummer, schulname, {field: (old, new)})
    for i in df.index:
        row_edit = {}
        for field, new in changes.get(snr[i], {}).items():
            if field in df.columns and _norm(df.at[i, field]) != _norm(new):
                if pd.api.types.is_integer_dtype(df[field]):  # plz is int in some finals
                    new = int(new)
                row_edit[field] = (df.at[i, field], new)
                df.at[i, field] = new
        if snr[i] in ssi:
            old, new = df.at[i, 'sozialindexstufe'], ssi[snr[i]]
            if not (pd.isna(old) and pd.isna(new)) and old != new:
                row_edit['sozialindexstufe'] = (old, new)
                df.at[i, 'sozialindexstufe'] = new
        if row_edit:
            edits.append((snr[i], df.at[i, 'schulname'], row_edit))

    assert len(df) == len(before), "row count changed"
    changed = [c for c in df.columns
               if not df[c].astype(str).equals(before[c].astype(str))]
    unexpected = [c for c in changed if c not in ALLOWED]
    assert not unexpected, f"unexpected columns changed: {unexpected}"

    for s, name, e in edits:
        logger.info(f"    {s} {name}: " + "; ".join(f"{f} {o!r} → {n!r}" for f, (o, n) in e.items()))
    if edits and not dry_run:
        _save(df, path)
    return edits


def scope_report(cur: pd.DataFrame, final_snrs: set):
    in_scope = cur[cur['ort'].str.strip().isin(scraper.TARGET_CITIES)
                   & cur['Schulform'].isin(scraper.PRIMARY_SCHULFORM_CODES + scraper.SECONDARY_SCHULFORM_CODES)]
    for s in sorted(set(in_scope.index) - final_snrs):
        r = in_scope.loc[s]
        logger.warning(f"  NEW in scope, not in finals: {s} {r['kurzbezeichnung']} "
                       f"({r['strasse']}, {r['plz']}; Schulbetrieb seit {r.get('Schulbetriebsdatum')})")
    for s in sorted(final_snrs - set(in_scope.index)):
        if s.startswith('99'):
            continue  # local additions (scripts_nrw/processing/nrw_school_additions.py)
        why = f"now in {cur.loc[s, 'ort']}" if s in cur.index else "inactive or absent from schuldaten.csv"
        logger.warning(f"  LEFT scope, still in finals: {s} — {why}")


def _sql(v, kind):
    if _norm(v) == '':
        return 'NULL'
    if kind == 'num':
        return repr(round(float(v), 6))
    return "'" + str(v).replace("'", "''") + "'"


def write_sql(out_dir: Path, edits_by_file: dict):
    out_dir.mkdir(parents=True, exist_ok=True)
    by_table = {}
    for rel, edits in edits_by_file.items():
        name = Path(rel).name
        if not name.endswith('.csv'):
            continue  # parquet twins carry the same edits
        city = name.split('_')[0]
        table = 'primary_schools' if '_primary_' in name else 'schools'
        for snr, schulname, e in edits:
            sets = [f"{f} = {_sql(new, SQL_FIELDS[f])}" for f, (_, new) in e.items() if f in SQL_FIELDS]
            if sets:
                by_table.setdefault(table, []).append(
                    f"UPDATE {table} SET {', '.join(sets)}\n"
                    f"  WHERE city = '{city}' AND schulnummer = '{snr}';  -- {' '.join(str(schulname).split())}")
    for table, stmts in by_table.items():
        path = out_dir / f"{table}__nrw_master_delta.sql"
        path.write_text(f"-- {table}: NRW master-list deltas ({len(stmts)} rows), "
                        f"generated {datetime.now():%Y-%m-%d} by refresh_nrw_master_delta.py.\n"
                        f"-- Plain SETs keyed on city + schulnummer: re-running is a no-op.\n\n"
                        + "\n".join(stmts) + "\n")
        logger.info(f"Wrote {path} ({len(stmts)} UPDATEs)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='Report changes without writing')
    ap.add_argument('--baseline', type=Path,
                    help=f'Previous schuldaten.csv snapshot (default {BASELINE.name}, else the April raw scrape)')
    ap.add_argument('--emit-sql', type=Path, metavar='DIR', help='Write Supabase UPDATEs for the master fields')
    args = ap.parse_args()

    baseline = args.baseline or (BASELINE if BASELINE.exists() else LEGACY_BASELINE)
    current_path = CACHE_DIR / f"nrw_schuldaten_{datetime.now():%Y-%m-%d}.csv"
    if not current_path.exists():
        current_path.write_bytes(scraper.download_file(scraper.SCHULDATEN_CSV_URL, "NRW school data"))
    logger.info(f"Diffing {baseline.name} → {current_path.name}")
    base, cur = load_master(baseline.read_bytes()), load_master(current_path.read_bytes())
    changes = source_changes(base, cur)
    logger.info(f"{len(changes)} schools changed at the source (all NRW)")
    ssi, school_year = load_sozialindex()

    edits_by_file, final_snrs = {}, set()
    for rel in FINAL_FILES:
        path = PROJECT_ROOT / rel
        if not path.exists():
            logger.warning(f"  final missing, skipping: {rel}")
            continue
        logger.info(f"  {rel}")
        final_snrs |= set(_load(path)['schulnummer'].astype(str).str.strip())
        edits_by_file[rel] = refresh_file(path, changes, ssi, args.dry_run)

    scope_report(cur, final_snrs)
    if args.emit_sql:
        write_sql(args.emit_sql, edits_by_file)

    if args.dry_run:
        logger.info("DRY RUN — nothing written.")
    else:
        shutil.copyfile(current_path, BASELINE)
        logger.info(f"Baseline advanced to {current_path.name} (Sozialindex SJ {school_year})")


if __name__ == '__main__':
    sys.exit(main())
