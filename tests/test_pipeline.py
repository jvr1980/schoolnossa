"""Tests for the catchment profiling and regression scoring pipeline."""

import pytest
import numpy as np

from src.utils.geo import haversine, bounding_box
from src.services.crime import CrimeService
from src.services.population import PopulationService
from src.pipeline.dimensions import (
    DIMENSIONS,
    Direction,
    get_scorable_dimensions,
    get_numeric_dimensions,
    default_weights,
)
from src.pipeline.types import CatchmentProfile, ModelDiagnostics
from src.pipeline.scorer import normalize_profiles, train_and_diagnose


# --- Geo utils ---


class TestHaversine:
    def test_same_point(self):
        assert haversine(52.52, 13.405, 52.52, 13.405) == 0.0

    def test_known_distance(self):
        # Berlin Mitte to Alexanderplatz: ~1.3 km
        d = haversine(52.5200, 13.4050, 52.5219, 13.4133)
        assert 500 < d < 2000

    def test_symmetric(self):
        d1 = haversine(52.52, 13.40, 52.53, 13.41)
        d2 = haversine(52.53, 13.41, 52.52, 13.40)
        assert abs(d1 - d2) < 0.01

    def test_bounding_box(self):
        s, w, n, e = bounding_box(52.52, 13.40, 1000)
        assert s < 52.52 < n
        assert w < 13.40 < e
        assert abs((n - s) / 2 - 0.009) < 0.002


# --- Crime service ---


class TestCrimeService:
    def setup_method(self):
        self.svc = CrimeService()

    def test_lookup_by_address_with_district(self):
        idx = self.svc.get_crime_index(52.52, 13.40, "Some school in Mitte, Berlin")
        assert idx == 186

    def test_lookup_by_postcode(self):
        idx = self.svc.get_crime_index(52.52, 13.40, "Teststr. 1, 12043 Berlin")
        assert idx == 148  # 12043 → Neukölln

    def test_fallback_to_nearest_centroid(self):
        idx = self.svc.get_crime_index(52.434, 13.241, "")
        assert idx == 62  # Steglitz-Zehlendorf

    def test_returns_numeric(self):
        idx = self.svc.get_crime_index(52.52, 13.40, "10115 Berlin")
        assert isinstance(idx, float)


# --- Population service ---


class TestPopulationService:
    def setup_method(self):
        self.svc = PopulationService()

    def test_density_with_known_postcode(self):
        density = self.svc.get_density("Teststr. 1, 10115 Berlin")
        assert density > 10000

    def test_density_fallback(self):
        density = self.svc.get_density("Unknown address, 99999 Nowhere")
        assert density > 0

    def test_catchment_population(self):
        pop, density = self.svc.estimate_catchment_population(
            52.52, 13.40, 1000.0, "Teststr. 1, 10115 Berlin"
        )
        assert pop > 0
        assert density > 0

    def test_small_radius_less_population(self):
        pop_small, _ = self.svc.estimate_catchment_population(52.52, 13.40, 200, "10115")
        pop_large, _ = self.svc.estimate_catchment_population(52.52, 13.40, 2000, "10115")
        assert pop_small < pop_large


# --- Dimensions ---


class TestDimensions:
    def test_dimensions_registered(self):
        assert len(DIMENSIONS) > 10

    def test_has_expected_keys(self):
        expected = {"transit_count", "crime_index", "population_density", "library_count"}
        for key in expected:
            assert key in DIMENSIONS, f"Missing dimension: {key}"

    def test_scorable_excludes_informational(self):
        scorable = get_scorable_dimensions()
        for d in scorable:
            assert d.direction != Direction.INFORMATIONAL

    def test_default_weights_sum_to_one(self):
        weights = default_weights()
        assert abs(sum(weights.values()) - 1.0) < 0.01


# --- Helpers ---


def _make_profiles(n: int = 6) -> list:
    """Create test profiles with a clear linear pattern."""
    np.random.seed(42)
    profiles = []
    for i in range(n):
        p = CatchmentProfile(
            school_id=f"s{i}",
            latitude=52.52 + i * 0.01,
            longitude=13.40,
            radius_m=1000,
        )
        p.set("crime_index", 50.0 + i * 25.0 + np.random.normal(0, 3))
        p.set("transit_count", 15.0 - i * 2.0 + np.random.normal(0, 1))
        p.set("adult_abitur_pct", 55.0 - i * 5.0 + np.random.normal(0, 2))
        p.set("avg_rent", 12.0 - i * 0.8 + np.random.normal(0, 0.3))
        p.set("population_density", 18000.0 - i * 1000 + np.random.normal(0, 500))
        profiles.append(p)
    return profiles


def _make_labeled(n: int = 6) -> dict:
    """Abitur grades that correlate with catchment features."""
    np.random.seed(42)
    return {f"s{i}": round(2.0 + i * 0.2 + np.random.normal(0, 0.05), 2) for i in range(n)}


# --- Normalization ---


class TestNormalization:
    def test_normalizes_to_0_100(self):
        profiles = _make_profiles()
        X_dict, ranges, school_ids = normalize_profiles(profiles)
        assert len(school_ids) == 6

        for key, arr in X_dict.items():
            assert arr.min() >= 0
            assert arr.max() <= 100

    def test_lower_better_inverted(self):
        profiles = _make_profiles()
        X_dict, _, school_ids = normalize_profiles(profiles)

        # s0 has lowest crime → should have highest normalized value (inverted)
        assert X_dict["crime_index"][0] > X_dict["crime_index"][-1]

    def test_empty_input(self):
        X_dict, ranges, ids = normalize_profiles([])
        assert X_dict == {}
        assert ids == []


# --- Regression ---


class TestTrainAndDiagnose:
    def test_returns_none_with_few_labels(self):
        profiles = _make_profiles()
        labeled = {"s0": 2.0, "s1": 2.2}
        result = train_and_diagnose(profiles, labeled)
        assert result is None

    def test_produces_diagnostics(self):
        profiles = _make_profiles()
        labeled = _make_labeled()
        diag = train_and_diagnose(profiles, labeled)

        assert diag is not None
        assert isinstance(diag, ModelDiagnostics)

        # Model fit
        assert 0 < diag.r_squared <= 1.0
        assert diag.adjusted_r_squared <= diag.r_squared
        assert diag.rmse > 0
        assert diag.mae > 0
        assert diag.n_samples == 6
        assert diag.n_features >= 1

    def test_feature_diagnostics(self):
        profiles = _make_profiles()
        labeled = _make_labeled()
        diag = train_and_diagnose(profiles, labeled)

        assert diag is not None
        assert len(diag.features) >= 1

        for f in diag.features:
            assert f.key in DIMENSIONS or f.key in [p.school_id for p in profiles]
            assert f.direction in ("positive", "negative")
            assert isinstance(f.standardized_coef, float)
            assert isinstance(f.partial_r_squared, float)

    def test_feature_selection_path(self):
        profiles = _make_profiles()
        labeled = _make_labeled()
        diag = train_and_diagnose(profiles, labeled)

        assert diag is not None
        assert len(diag.feature_selection_path) >= 1
        assert diag.feature_selection_path[0]["action"] in ("ADD", "FORCED", "STOP")

    def test_cross_validation(self):
        profiles = _make_profiles()
        labeled = _make_labeled()
        diag = train_and_diagnose(profiles, labeled, cv_folds=3)

        assert diag is not None
        assert len(diag.cv_folds) >= 1
        assert isinstance(diag.cv_r_squared_mean, float)
        assert isinstance(diag.cv_rmse_mean, float)

    def test_predictions_for_all_schools(self):
        profiles = _make_profiles()
        labeled = _make_labeled()
        diag = train_and_diagnose(profiles, labeled)

        assert diag is not None
        assert len(diag.predictions) == len(profiles)

        for pred in diag.predictions:
            assert isinstance(pred.predicted, float)
            assert 0 <= pred.confidence <= 1

    def test_labeled_schools_have_actual_and_residual(self):
        profiles = _make_profiles()
        labeled = _make_labeled()
        diag = train_and_diagnose(profiles, labeled)

        assert diag is not None
        labeled_preds = [p for p in diag.predictions if p.actual is not None]
        assert len(labeled_preds) == len(labeled)

        for p in labeled_preds:
            assert p.residual is not None
            assert abs(p.residual) < 1.0  # Should be small for a good fit

    def test_feature_contributions_sum_to_prediction_minus_intercept(self):
        profiles = _make_profiles()
        labeled = _make_labeled()
        diag = train_and_diagnose(profiles, labeled)

        assert diag is not None
        for pred in diag.predictions:
            contrib_sum = sum(pred.feature_contributions.values())
            expected = pred.predicted - diag.intercept
            assert abs(contrib_sum - expected) < 0.01, \
                f"Contributions {contrib_sum:.4f} != predicted-intercept {expected:.4f}"

    def test_forced_features(self):
        profiles = _make_profiles()
        labeled = _make_labeled()
        diag = train_and_diagnose(
            profiles, labeled,
            feature_keys=["crime_index", "transit_count"],
        )

        assert diag is not None
        assert diag.n_features == 2
        feature_keys = [f.key for f in diag.features]
        assert "crime_index" in feature_keys
        assert "transit_count" in feature_keys

    def test_unlabeled_schools_predicted(self):
        """Schools without labels should still get predictions."""
        profiles = _make_profiles(8)  # 8 profiles
        labeled = _make_labeled(6)     # Only 6 labeled

        diag = train_and_diagnose(profiles, labeled)
        assert diag is not None
        assert len(diag.predictions) == 8

        unlabeled = [p for p in diag.predictions if p.actual is None]
        assert len(unlabeled) == 2
        for p in unlabeled:
            assert p.predicted is not None
            assert p.residual is None

    def test_coefficients_match_equation(self):
        profiles = _make_profiles()
        labeled = _make_labeled()
        diag = train_and_diagnose(profiles, labeled)

        assert diag is not None
        assert len(diag.coefficients) == diag.n_features
        for f in diag.features:
            assert f.key in diag.coefficients

    def test_model_reliability_flag(self):
        profiles = _make_profiles()
        labeled = _make_labeled()
        diag = train_and_diagnose(profiles, labeled)

        assert diag is not None
        assert isinstance(diag.is_reliable, bool)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
