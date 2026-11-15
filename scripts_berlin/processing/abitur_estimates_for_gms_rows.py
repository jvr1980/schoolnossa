#!/usr/bin/env python3
"""
Abitur estimates for the Berlin Gemeinschaftsschule secondary rows (2026-11 delta).

apply_predictions_to_parquets.py trains the Ridge model on all cities and writes
estimates into every city final. The 50 Gemeinschaftsschule rows are not in the
Berlin final, so this script runs the same model with those rows appended to a
temporary copy of the Berlin input and keeps only their predictions:

  - the new rows' own Abitur results are NOT used as training labels, so the
    model is the one behind the existing estimates;
  - district-level crime fields are copied from an existing Berlin row of the
    same Bezirk (identical by construction); transit_rail_01_distance_m is the
    nearest rail stop (stored as transit_rail_distance_m in Supabase);
  - Berlin is a training city, so no state rebasing applies (the app reads
    abitur_durchschnitt_estimated*, abitur_prediction_*).

Output: data_berlin/intermediate/berlin_gms_abitur_estimates.json
        (list of {schulnummer, abitur_durchschnitt_estimated, _lower, _upper,
         abitur_prediction_confidence, abitur_prediction_drivers})
"""
import json
import sys
import tempfile
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import scripts_shared.regression.run_regression as rr  # noqa: E402
import scripts_shared.regression.apply_predictions_to_parquets as ap  # noqa: E402
from scripts_shared.regression.scorer import compute_prediction_intervals  # noqa: E402

BERLIN_FINAL = PROJECT_ROOT / 'data_berlin' / 'final' / 'school_master_table_final.csv'
GMS_ROWS = PROJECT_ROOT / 'data_berlin' / 'intermediate' / 'berlin_gemeinschaftsschulen_secondary_rows.csv'
OUTPUT = PROJECT_ROOT / 'data_berlin' / 'intermediate' / 'berlin_gms_abitur_estimates.json'
CRIME_COLS = ['crime_total_crimes_avg', 'crime_violent_crime_avg', 'crime_drug_offenses_avg', 'crime_safety_rank']


def main():
    fin = pd.read_csv(BERLIN_FINAL, encoding='utf-8-sig', dtype=str)
    new = pd.read_csv(GMS_ROWS, encoding='utf-8-sig', dtype=str)
    new_ids = set(new['schulnummer'])
    assert not new_ids & set(fin['schulnummer']), 'Gemeinschaftsschulen already in the Berlin final'

    new = new.rename(columns={'transit_rail_distance_m': 'transit_rail_01_distance_m'})
    crime = fin.dropna(subset=['crime_total_crimes_avg']).drop_duplicates('bezirk').set_index('bezirk')[CRIME_COLS]
    for c in CRIME_COLS:
        new[c] = new['bezirk'].map(crime[c])
    combined = pd.concat([fin, new[[c for c in new.columns if c in fin.columns]]], ignore_index=True)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_csv = Path(tmp) / 'berlin_with_gms.csv'
        combined.to_csv(tmp_csv, index=False, encoding='utf-8-sig')

        orig = rr.load_city_data

        def load_city_data(csv_path, column_map, city_label, **kw):
            if city_label == 'berlin':
                csv_path = str(tmp_csv)
            return orig(csv_path, column_map, city_label, **kw)

        rr.load_city_data = load_city_data
        profiles, labeled, info = rr.load_all_data('all')
        rr.load_city_data = orig

    new_sids = {f'berlin_{s}' for s in new_ids}
    for sid in new_sids:
        labeled.pop(sid, None)  # keep the model identical to the one behind existing estimates
    for p in profiles:
        for key in ap.CITY_SPECIFIC_DIMS | ap.NON_PORTABLE_DIMS:
            p.values.pop(key, None)

    model = ap._train_ridge_model(profiles, labeled, fit_on_all=False)
    print(f"Grade model: R²={model['r2']:.4f}, RMSE={model['rmse']:.4f}, n={model['n_labeled']}, "
          f"features={len(model['feature_keys'])}")
    lower, upper = compute_prediction_intervals(model['X_train'], model['y_train'], model['X_all'],
                                                model['y_pred_all'], model['alpha'], ci_level=0.80)
    out = []
    for i, sid in enumerate(model['school_ids']):
        if sid not in new_sids:
            continue
        out.append({
            'schulnummer': sid.split('_', 1)[1],
            'abitur_durchschnitt_estimated': round(float(model['y_pred_all'][i]), 3),
            'abitur_durchschnitt_estimated_lower': round(float(lower[i]), 3),
            'abitur_durchschnitt_estimated_upper': round(float(upper[i]), 3),
            'abitur_prediction_confidence': round(model['confidences'][i], 3),
            'abitur_prediction_drivers': json.loads(ap._build_drivers_json(model['contributions'][i])),
        })
    OUTPUT.write_text(json.dumps(sorted(out, key=lambda r: r['schulnummer']), ensure_ascii=False, indent=1),
                      encoding='utf-8')
    print(f'{len(out)} of {len(new_ids)} Gemeinschaftsschulen got an estimate -> {OUTPUT.relative_to(PROJECT_ROOT)}')


if __name__ == '__main__':
    main()
