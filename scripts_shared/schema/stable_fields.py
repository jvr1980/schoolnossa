#!/usr/bin/env python3
"""
Stable (year-agnostic) field derivation.

Problem: the German tables carry year-suffixed columns (schueler_2024_25,
crime_total_crimes_2023, abitur_durchschnitt_2024, ...). Every yearly refresh
renames the newest column, which ripples through the schema mappers, the
Supabase schema, and the Lovable app. This module derives *stable* columns
that always hold the newest available value plus an explicit vintage stamp,
so downstream consumers (above all the Lovable app) can bind to fixed names.

Additive by design: year-suffixed columns are kept; stable columns are
derived beside them. The Lovable app migrates at its own pace.

Usage (wired into every *_to_berlin_schema.py and the Berlin combiners):

    from scripts_shared.schema.stable_fields import add_stable_fields
    df = add_stable_fields(df)

Fallback-chain semantics mirror what scripts_shared/regression/run_regression.py
hand-rolled (newest year first, then aggregate columns of unknown vintage).
"""

import re

import pandas as pd

# Single authoritative "current school year" constant. Bump once per cycle.
CURRENT_SCHOOL_YEAR = "2026_27"

# Stable column -> (regex matching year-suffixed candidates, aggregate fallbacks)
# The regex must capture the sortable year part in group 1.
_SCHOOL_YEAR_FAMILIES = {
    'schueler_current': (r'^schueler_(20\d\d_\d\d)$', ['schueler_gesamt', 'anzahl_schueler_gesamt', 'schueler_gesamt_web']),
    'lehrer_current': (r'^lehrer_(20\d\d_\d\d)$', ['lehrer_gesamt']),
    'migration_current': (r'^migration_(20\d\d_\d\d)$', []),
    'nachfrage_prozent_current': (r'^nachfrage_prozent_(20\d\d_\d\d)$', []),
}
_CALENDAR_YEAR_FAMILIES = {
    'abitur_durchschnitt_current': (r'^abitur_durchschnitt_(20\d\d)$', []),
    'crime_total_crimes_current': (r'^crime_total_crimes_(20\d\d)$', ['crime_straftaten_2025', 'crime_haeufigkeitszahl_2025']),
}
# Vintage stamps: stable column -> which stable field's source year it records
_VINTAGE_OF = {
    'data_school_year': 'schueler_current',
    'abitur_year': 'abitur_durchschnitt_current',
    'crime_data_year': 'crime_total_crimes_current',
}


def _year_columns(df: pd.DataFrame, pattern: str):
    """Return [(sort_key, column_name)] newest first."""
    rx = re.compile(pattern)
    hits = []
    for col in df.columns:
        m = rx.match(col)
        if m:
            hits.append((m.group(1), col))
    return sorted(hits, reverse=True)


def add_stable_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Derive stable *_current columns + vintage stamps. Idempotent, additive."""
    df = df.copy()
    source_year = {}  # stable column -> year string of the newest populated source

    for stable, (pattern, fallbacks) in {**_SCHOOL_YEAR_FAMILIES,
                                         **_CALENDAR_YEAR_FAMILIES}.items():
        candidates = _year_columns(df, pattern)
        # Plain None (not pd.NA): pd.NA in object columns leaks through
        # float-based isna checks downstream and str()s to '<NA>'.
        values = pd.Series([None] * len(df), index=df.index, dtype='object')
        vintage = pd.Series([None] * len(df), index=df.index, dtype='object')
        for year, col in candidates:  # newest first
            fill = values.isna() & df[col].notna()
            values[fill] = df.loc[fill, col]
            vintage[fill] = year
        for col in fallbacks:  # aggregates of unknown/older vintage last
            if col in df.columns:
                fill = values.isna() & df[col].notna()
                values[fill] = df.loc[fill, col]
                # no vintage claim for aggregate fallbacks
        if values.notna().any() or stable in df.columns:
            df[stable] = pd.to_numeric(values, errors='coerce')
            source_year[stable] = vintage

    for stamp, stable in _VINTAGE_OF.items():
        if stable in source_year:
            df[stamp] = source_year[stable]

    return df


def stable_coverage_report(df: pd.DataFrame) -> dict:
    """{stable column: non-null count} for QA output."""
    cols = list(_SCHOOL_YEAR_FAMILIES) + list(_CALENDAR_YEAR_FAMILIES) + list(_VINTAGE_OF)
    return {c: int(df[c].notna().sum()) for c in cols if c in df.columns}
