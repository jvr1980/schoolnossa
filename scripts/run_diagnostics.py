#!/usr/bin/env python3
"""
School Performance Estimation — Model Diagnostics

Trains a regression model on Berlin school catchment profiles + Abitur data,
then outputs full diagnostics:

  1. Model fit (R², adjusted R², RMSE, MAE)
  2. Cross-validation results (k-fold or LOO)
  3. Feature selection path (which features were added and why)
  4. Feature importance (standardized coefficients, partial R², p-values)
  5. Per-school predictions with feature contribution breakdown

Usage:

    # With real data from database:
    python scripts/run_diagnostics.py

    # With synthetic demo data (no database needed):
    python scripts/run_diagnostics.py --demo

    # Export predictions to CSV:
    python scripts/run_diagnostics.py --demo --export predictions.csv

    # Custom catchment radius:
    python scripts/run_diagnostics.py --demo --radius 1500
"""

import argparse
import csv
import json
import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
from src.pipeline.types import CatchmentProfile, ModelDiagnostics
from src.pipeline.scorer import train_and_diagnose
from src.pipeline.dimensions import DIMENSIONS


# ---------------------------------------------------------------------------
# Demo data: synthetic Berlin schools with realistic catchment profiles
# ---------------------------------------------------------------------------

def generate_demo_data():
    """Generate synthetic schools that mimic real Berlin patterns.

    The key relationship baked in:
    - Lower crime, higher adult education, more transit → better Abitur grades
    - Some noise to make it realistic
    """
    np.random.seed(42)

    schools = [
        # (id, name, district, lat, lng)
        ("S001", "Gymnasium Steglitz", "Steglitz-Zehlendorf", 52.4560, 13.3280),
        ("S002", "Gymnasium Zehlendorf", "Steglitz-Zehlendorf", 52.4310, 13.2590),
        ("S003", "Leibniz-Gymnasium", "Charlottenburg-Wilmersdorf", 52.4880, 13.3100),
        ("S004", "Schiller-Gymnasium", "Charlottenburg-Wilmersdorf", 52.5040, 13.2880),
        ("S005", "Kant-Gymnasium", "Spandau", 52.5350, 13.2050),
        ("S006", "Humboldt-Gymnasium", "Reinickendorf", 52.5770, 13.3400),
        ("S007", "Pankow-Gymnasium", "Pankow", 52.5700, 13.4020),
        ("S008", "Prenzlauer-Berg-Gym", "Pankow", 52.5400, 13.4200),
        ("S009", "Mitte-Gymnasium", "Mitte", 52.5250, 13.4000),
        ("S010", "Wedding-Sekundarschule", "Mitte", 52.5500, 13.3600),
        ("S011", "Kreuzberg-ISS", "Friedrichshain-Kreuzberg", 52.4950, 13.4100),
        ("S012", "Friedrichshain-ISS", "Friedrichshain-Kreuzberg", 52.5150, 13.4500),
        ("S013", "Neukölln-ISS", "Neukölln", 52.4800, 13.4350),
        ("S014", "Britz-Sekundarschule", "Neukölln", 52.4500, 13.4400),
        ("S015", "Tempelhof-Gymnasium", "Tempelhof-Schöneberg", 52.4700, 13.3850),
        ("S016", "Schöneberg-Gymnasium", "Tempelhof-Schöneberg", 52.4900, 13.3550),
        ("S017", "Treptow-Gymnasium", "Treptow-Köpenick", 52.4900, 13.4700),
        ("S018", "Köpenick-Gymnasium", "Treptow-Köpenick", 52.4400, 13.5800),
        ("S019", "Lichtenberg-ISS", "Lichtenberg", 52.5200, 13.5000),
        ("S020", "Marzahn-ISS", "Marzahn-Hellersdorf", 52.5400, 13.5700),
        # 5 unlabeled schools (to demonstrate prediction)
        ("S021", "Hellersdorf-Gymnasium", "Marzahn-Hellersdorf", 52.5300, 13.6000),
        ("S022", "Spandau-ISS", "Spandau", 52.5400, 13.1900),
        ("S023", "Wilmersdorf-Gymnasium", "Charlottenburg-Wilmersdorf", 52.4850, 13.3200),
        ("S024", "Reinickendorf-ISS", "Reinickendorf", 52.5900, 13.3300),
        ("S025", "Steglitz-ISS", "Steglitz-Zehlendorf", 52.4500, 13.3100),
    ]

    # District characteristics (realistic Berlin data)
    district_profiles = {
        "Steglitz-Zehlendorf":       {"crime": 62,  "adult_ed": 55, "rent": 11.5, "migration": 22},
        "Charlottenburg-Wilmersdorf":{"crime": 125, "adult_ed": 48, "rent": 12.0, "migration": 30},
        "Spandau":                   {"crime": 95,  "adult_ed": 28, "rent": 8.5,  "migration": 35},
        "Reinickendorf":             {"crime": 98,  "adult_ed": 30, "rent": 8.8,  "migration": 33},
        "Pankow":                    {"crime": 78,  "adult_ed": 50, "rent": 11.0, "migration": 18},
        "Mitte":                     {"crime": 186, "adult_ed": 42, "rent": 13.0, "migration": 40},
        "Friedrichshain-Kreuzberg":  {"crime": 165, "adult_ed": 45, "rent": 12.5, "migration": 38},
        "Neukölln":                  {"crime": 148, "adult_ed": 25, "rent": 9.0,  "migration": 45},
        "Tempelhof-Schöneberg":      {"crime": 110, "adult_ed": 38, "rent": 10.5, "migration": 32},
        "Treptow-Köpenick":          {"crime": 72,  "adult_ed": 35, "rent": 9.5,  "migration": 15},
        "Lichtenberg":               {"crime": 92,  "adult_ed": 32, "rent": 9.0,  "migration": 25},
        "Marzahn-Hellersdorf":       {"crime": 88,  "adult_ed": 26, "rent": 7.5,  "migration": 20},
    }

    profiles = []
    for sid, name, district, lat, lng in schools:
        dp = district_profiles[district]
        noise = np.random.normal(0, 1)

        p = CatchmentProfile(school_id=sid, latitude=lat, longitude=lng, radius_m=1000)
        p.set("crime_index", dp["crime"] + np.random.normal(0, 8))
        p.set("adult_abitur_pct", dp["adult_ed"] + np.random.normal(0, 5))
        p.set("avg_rent", dp["rent"] + np.random.normal(0, 1))
        p.set("migration_pct", dp["migration"] + np.random.normal(0, 4))
        p.set("transit_count", max(1, int(12 + np.random.normal(0, 4))))
        p.set("transit_nearest_m", max(50, 250 + np.random.normal(0, 80)))
        p.set("library_count", max(0, int(3 + np.random.normal(0, 1.5))))
        p.set("park_count", max(0, int(5 + np.random.normal(0, 2))))
        p.set("population_density", dp["rent"] * 1500 + np.random.normal(0, 1000))
        p.set("catchment_population", 20000 + np.random.normal(0, 5000))
        p.set("other_schools_count", max(0, int(4 + np.random.normal(0, 2))))
        p.set("supermarket_count", max(0, int(6 + np.random.normal(0, 2))))
        profiles.append(p)

    # Generate Abitur grades: realistic Berlin range 1.8 - 3.2
    # Grade = f(crime, adult_education, rent, migration) + noise
    # Note: lower Abitur grade = better (German grading: 1.0 best, 4.0 worst)
    labeled = {}
    school_info = {}
    for i, (sid, name, district, lat, lng) in enumerate(schools[:20]):  # Only first 20 labeled
        p = profiles[i]
        grade = (
            1.5
            + 0.004 * p.get("crime_index")         # More crime → worse grade
            - 0.012 * p.get("adult_abitur_pct")     # More educated area → better grade
            - 0.02 * p.get("avg_rent")              # Higher rent (affluent) → better grade
            + 0.005 * p.get("migration_pct")         # Correlation (not causation)
            - 0.01 * p.get("transit_count")          # Better connected → slightly better
            + np.random.normal(0, 0.08)              # Noise
        )
        grade = max(1.3, min(3.5, grade))
        labeled[sid] = round(grade, 2)
        school_info[sid] = {"name": name, "district": district}

    # Also store info for unlabeled schools
    for i in range(20, len(schools)):
        sid, name, district, lat, lng = schools[i]
        school_info[sid] = {"name": name, "district": district}

    return profiles, labeled, school_info


# ---------------------------------------------------------------------------
# Formatted output
# ---------------------------------------------------------------------------

def print_header(title: str):
    width = 72
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def print_section(title: str):
    print()
    print(f"── {title} " + "─" * max(0, 68 - len(title)))


def print_diagnostics(diag: ModelDiagnostics, school_info: dict):
    """Print full model diagnostics in human-readable format."""

    print_header("MODEL PERFORMANCE DIAGNOSTICS")

    # 1. Overall fit
    print_section("1. Overall Model Fit")
    print(f"  R²              = {diag.r_squared:.4f}")
    print(f"  Adjusted R²     = {diag.adjusted_r_squared:.4f}")
    print(f"  RMSE            = {diag.rmse:.4f}")
    print(f"  MAE             = {diag.mae:.4f}")
    print(f"  N (labeled)     = {diag.n_samples}")
    print(f"  Features used   = {diag.n_features}")
    print(f"  Intercept       = {diag.intercept:.4f}")
    print()

    reliability = "RELIABLE" if diag.is_reliable else "USE WITH CAUTION"
    print(f"  Model status: {reliability}")
    if not diag.is_reliable:
        print(f"    (CV R² mean = {diag.cv_r_squared_mean:.3f}, "
              f"std = {diag.cv_r_squared_std:.3f})")

    # 2. Cross-validation
    print_section("2. Cross-Validation")
    print(f"  CV R² mean      = {diag.cv_r_squared_mean:.4f}")
    print(f"  CV R² std       = {diag.cv_r_squared_std:.4f}")
    print(f"  CV RMSE mean    = {diag.cv_rmse_mean:.4f}")
    print()
    print(f"  {'Fold':>4}  {'Train':>5}  {'Test':>4}  {'R²':>8}  {'RMSE':>8}  {'MAE':>8}")
    print(f"  {'----':>4}  {'-----':>5}  {'----':>4}  {'--------':>8}  {'--------':>8}  {'--------':>8}")
    for f in diag.cv_folds:
        print(f"  {f.fold:4d}  {f.train_size:5d}  {f.test_size:4d}  {f.r_squared:8.4f}  {f.rmse:8.4f}  {f.mae:8.4f}")

    # 3. Feature selection path
    print_section("3. Forward Stepwise Feature Selection")
    print()
    print("  Additive procedure: at each step, every remaining candidate is tested.")
    print("  The one producing the largest R² improvement is added to the model.")
    print()
    for step in diag.feature_selection_path:
        tested = step.get("candidates_tested", [])

        if step["action"] == "STOP":
            print(f"  Step {step['step']}: STOP")
            print(f"    {step['reason']}")
            print(f"    Model R² at stop: {step['r_squared']:.4f}")
            if tested:
                print(f"    Candidates evaluated ({len(tested)}):")
                for c in tested[:5]:
                    print(f"      {c['feature_label']:<25} R²={c['r_squared']:.4f}  "
                          f"delta={c['delta_r_squared']:+.4f}")
                if len(tested) > 5:
                    print(f"      ... and {len(tested) - 5} more")

        elif step["action"] == "FORCED":
            print(f"  Step {step['step']}: FORCED  '{step['feature']}'")

        else:
            n_tested = len(tested)
            print(f"  Step {step['step']}: ADD  '{step['feature']}' "
                  f"({step.get('feature_label', '')})")
            print(f"    Model R² = {step['r_squared']:.4f}  "
                  f"(+{step['delta_r_squared']:.4f})")
            if n_tested > 1:
                print(f"    All {n_tested} candidates evaluated at this step:")
                for c in tested:
                    marker = " <-- selected" if c["feature"] == step["feature"] else ""
                    print(f"      {c['feature_label']:<25} R²={c['r_squared']:.4f}  "
                          f"delta={c['delta_r_squared']:+.4f}{marker}")
            print()

    # 4. Feature importance
    print_section("4. Feature Importance (Key Predictors)")
    print()
    print(f"  {'Feature':<25} {'Std Coef':>9} {'Direction':>10} {'Partial R²':>11} {'p-value':>9}")
    print(f"  {'-'*25} {'-'*9} {'-'*10} {'-'*11} {'-'*9}")
    for f in diag.features:
        pval_str = f"{f.p_value:.4f}" if f.p_value is not None else "    n/a"
        sig = ""
        if f.p_value is not None:
            if f.p_value < 0.001:
                sig = " ***"
            elif f.p_value < 0.01:
                sig = " **"
            elif f.p_value < 0.05:
                sig = " *"
        print(f"  {f.label:<25} {f.standardized_coef:>+9.4f} {f.direction:>10} "
              f"{f.partial_r_squared:>11.4f} {pval_str}{sig}")

    print()
    print("  Significance: *** p<0.001  ** p<0.01  * p<0.05")

    # 5. Interpretation
    print_section("5. Interpretation")
    print()
    for f in diag.features:
        dim = DIMENSIONS.get(f.key)
        direction_text = "increases" if f.standardized_coef > 0 else "decreases"
        target_effect = "worse" if f.standardized_coef > 0 else "better"
        print(f"  {f.label}: 1 SD increase in this feature {direction_text} "
              f"predicted Abitur grade by {abs(f.standardized_coef):.3f} SD "
              f"({target_effect} performance)")

    # 6. Per-school predictions (labeled schools: actual vs predicted)
    print_section("6. Labeled Schools: Actual vs Predicted")
    print()
    labeled_preds = [p for p in diag.predictions if p.actual is not None]
    labeled_preds.sort(key=lambda p: p.actual)

    print(f"  {'School':<30} {'Actual':>7} {'Predicted':>10} {'Residual':>9} {'Conf':>6}")
    print(f"  {'-'*30} {'-'*7} {'-'*10} {'-'*9} {'-'*6}")
    for p in labeled_preds:
        info = school_info.get(p.school_id, {})
        name = info.get("name", p.school_id)[:30]
        print(f"  {name:<30} {p.actual:>7.2f} {p.predicted:>10.3f} "
              f"{p.residual:>+9.3f} {p.confidence:>6.2f}")

    # 7. Per-school contribution breakdown (top 5 labeled)
    print_section("7. Feature Contributions (Top 5 Schools)")
    print()
    print(f"  Shows how each feature moves the prediction from the intercept ({diag.intercept:.3f})")

    top_labeled = sorted(labeled_preds, key=lambda p: p.actual)[:5]
    for p in top_labeled:
        info = school_info.get(p.school_id, {})
        name = info.get("name", p.school_id)
        print(f"\n  {name} (actual={p.actual:.2f}, predicted={p.predicted:.3f}):")

        # Sort contributions by absolute value
        contribs = sorted(p.feature_contributions.items(), key=lambda x: abs(x[1]), reverse=True)
        for key, val in contribs:
            dim = DIMENSIONS.get(key)
            label = dim.label if dim else key
            bar_len = int(abs(val) * 3)
            bar = ("+" if val > 0 else "-") * max(1, bar_len)
            print(f"    {label:<25} {val:>+8.3f}  {bar}")

    # 8. Unlabeled school predictions
    unlabeled_preds = [p for p in diag.predictions if p.actual is None]
    if unlabeled_preds:
        print_section("8. Unlabeled Schools: Estimated Performance")
        print()
        unlabeled_preds.sort(key=lambda p: p.predicted)

        print(f"  {'School':<30} {'Predicted':>10} {'Confidence':>11}")
        print(f"  {'-'*30} {'-'*10} {'-'*11}")
        for p in unlabeled_preds:
            info = school_info.get(p.school_id, {})
            name = info.get("name", p.school_id)[:30]
            conf_bar = "*" * int(p.confidence * 10)
            print(f"  {name:<30} {p.predicted:>10.3f} {p.confidence:>6.2f}  {conf_bar}")

        print()
        print("  Confidence: distance from training data distribution (1.0 = within, 0.0 = far outside)")

    # 9. Model equation
    print_section("9. Model Equation")
    print()
    terms = [f"{diag.intercept:.4f}"]
    for f in diag.features:
        sign = "+" if diag.coefficients[f.key] >= 0 else "-"
        terms.append(f"{sign} {abs(diag.coefficients[f.key]):.6f} * {f.key}")
    equation = f"  predicted_grade = {terms[0]}"
    for t in terms[1:]:
        equation += f"\n                    {t}"
    print(equation)


def export_predictions(diag: ModelDiagnostics, school_info: dict, path: str):
    """Export predictions + contributions to CSV."""
    if not diag.predictions:
        return

    feature_keys = [f.key for f in diag.features]
    fieldnames = [
        "school_id", "name", "district", "actual", "predicted",
        "residual", "confidence",
    ] + [f"contrib_{k}" for k in feature_keys]

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for p in sorted(diag.predictions, key=lambda x: x.predicted):
            info = school_info.get(p.school_id, {})
            row = {
                "school_id": p.school_id,
                "name": info.get("name", ""),
                "district": info.get("district", ""),
                "actual": p.actual if p.actual is not None else "",
                "predicted": p.predicted,
                "residual": p.residual if p.residual is not None else "",
                "confidence": p.confidence,
            }
            for k in feature_keys:
                row[f"contrib_{k}"] = round(p.feature_contributions.get(k, 0), 4)
            writer.writerow(row)

    print(f"\n  Predictions exported to: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Train school performance model and show diagnostics"
    )
    parser.add_argument(
        "--demo", action="store_true",
        help="Use synthetic Berlin data (no database needed)"
    )
    parser.add_argument(
        "--export", type=str, default=None,
        help="Export predictions to CSV file"
    )
    parser.add_argument(
        "--radius", type=float, default=1000.0,
        help="Catchment radius in meters (default: 1000)"
    )
    parser.add_argument(
        "--cv-folds", type=int, default=5,
        help="Number of cross-validation folds (default: 5)"
    )
    parser.add_argument(
        "--features", type=str, nargs="*", default=None,
        help="Force specific features (default: auto-select)"
    )
    args = parser.parse_args()

    if args.demo:
        print("Using synthetic Berlin demo data...")
        profiles, labeled, school_info = generate_demo_data()
    else:
        # Load from database
        try:
            from src.models.database import SessionLocal
            from src.models.school import School, SchoolMetricsAnnual
            from src.pipeline.profiler import SchoolProfiler
            import asyncio

            db = SessionLocal()
            schools = db.query(School).filter(
                School.latitude.isnot(None),
                School.longitude.isnot(None),
            ).all()

            if not schools:
                print("No schools in database. Use --demo flag for synthetic data.")
                print("  python scripts/run_diagnostics.py --demo")
                sys.exit(1)

            print(f"Found {len(schools)} schools in database.")

            # Build school data for profiler
            schools_data = [
                {
                    "school_id": s.school_id,
                    "name": s.name,
                    "latitude": float(s.latitude),
                    "longitude": float(s.longitude),
                    "address": s.address or "",
                    "school_type": s.school_type or "",
                    "district": s.district or "",
                }
                for s in schools
            ]

            school_info = {s["school_id"]: s for s in schools_data}

            # Profile
            print(f"Profiling {len(schools_data)} schools (radius={args.radius}m)...")
            profiler = SchoolProfiler(radius_m=args.radius)
            profiles = asyncio.run(profiler.profile_schools(schools_data))

            # Load Abitur labels from metrics table
            metrics = db.query(SchoolMetricsAnnual).filter(
                SchoolMetricsAnnual.abitur_average_grade.isnot(None)
            ).order_by(SchoolMetricsAnnual.year.desc()).all()

            labeled = {}
            for m in metrics:
                if m.school_id not in labeled:  # Take most recent year
                    labeled[m.school_id] = float(m.abitur_average_grade)

            db.close()

            if len(labeled) < 5:
                print(f"\nOnly {len(labeled)} schools with Abitur data — need ≥ 5.")
                print("Import Abitur data first, or use --demo flag.")
                sys.exit(1)

            print(f"Found {len(labeled)} schools with Abitur grades.")

        except Exception as e:
            print(f"Database error: {e}")
            print("Use --demo flag for synthetic data.")
            sys.exit(1)

    # Train and diagnose
    print(f"\nTraining regression model...")
    print(f"  Labeled schools: {len(labeled)}")
    print(f"  Total schools:   {len(profiles)}")
    print(f"  CV folds:        {args.cv_folds}")
    if args.features:
        print(f"  Forced features: {args.features}")

    diag = train_and_diagnose(
        profiles=profiles,
        labeled=labeled,
        feature_keys=args.features,
        cv_folds=args.cv_folds,
    )

    if diag is None:
        print("\nModel training failed. Check data quality and feature availability.")
        sys.exit(1)

    print_diagnostics(diag, school_info)

    if args.export:
        export_predictions(diag, school_info, args.export)


if __name__ == "__main__":
    main()
