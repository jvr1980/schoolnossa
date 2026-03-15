"""Tests for the catchment profiling and scoring pipeline."""

import pytest
import math

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
from src.pipeline.types import CatchmentProfile, NormalizedProfile, ScoreMode
from src.pipeline.scorer import (
    normalize_profiles,
    rules_score,
    regression_score,
    score_schools,
)


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
        # 1km radius should give ~0.009 degrees latitude
        assert abs((n - s) / 2 - 0.009) < 0.002


# --- Crime service ---


class TestCrimeService:
    def setup_method(self):
        self.svc = CrimeService()

    def test_lookup_by_address_with_district(self):
        idx = self.svc.get_crime_index(52.52, 13.40, "Some school in Mitte, Berlin")
        assert idx == 186  # Mitte crime index

    def test_lookup_by_postcode(self):
        idx = self.svc.get_crime_index(52.52, 13.40, "Teststr. 1, 12043 Berlin")
        assert idx == 148  # 12043 → Neukölln

    def test_fallback_to_nearest_centroid(self):
        # Coordinates near Steglitz-Zehlendorf centroid, no address
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
        assert density > 10000  # 10115 is dense inner city

    def test_density_fallback(self):
        density = self.svc.get_density("Unknown address, 99999 Nowhere")
        assert density > 0  # Should return city average

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


# --- Scoring ---


def _make_profiles() -> list:
    """Create test profiles for scoring."""
    profiles = []
    for i, (crime, transit, pop) in enumerate([
        (50, 10, 20000),   # School A: high crime, some transit
        (180, 2, 30000),   # School B: very high crime, poor transit
        (30, 15, 15000),   # School C: low crime, good transit
    ]):
        p = CatchmentProfile(
            school_id=f"school_{i}",
            latitude=52.52 + i * 0.01,
            longitude=13.40 + i * 0.01,
            radius_m=1000,
        )
        p.set("crime_index", crime)
        p.set("transit_count", transit)
        p.set("catchment_population", pop)
        p.set("population_density", pop / 3.14)
        profiles.append(p)
    return profiles


class TestNormalization:
    def test_normalizes_to_0_100(self):
        profiles = _make_profiles()
        normalized, ranges = normalize_profiles(profiles)
        assert len(normalized) == 3

        for p in normalized:
            for key, val in p.normalized.items():
                assert 0 <= val <= 100, f"{key}={val} out of range"

    def test_lower_better_inverted(self):
        profiles = _make_profiles()
        normalized, _ = normalize_profiles(profiles)

        # School C has lowest crime (30) → should get highest normalized crime_index
        # because crime is lower_better (inverted)
        school_c = [p for p in normalized if p.school_id == "school_2"][0]
        school_b = [p for p in normalized if p.school_id == "school_1"][0]
        assert school_c.normalized["crime_index"] > school_b.normalized["crime_index"]

    def test_empty_input(self):
        normalized, ranges = normalize_profiles([])
        assert normalized == []
        assert ranges == {}


class TestRulesScore:
    def test_scores_assigned(self):
        profiles = _make_profiles()
        normalized, _ = normalize_profiles(profiles)
        rules_score(normalized)

        for p in normalized:
            assert p.rules_score >= 0

    def test_custom_weights_affect_ranking(self):
        profiles = _make_profiles()
        normalized, _ = normalize_profiles(profiles)

        # Weight only crime: school C (lowest crime) should rank first
        rules_score(normalized, weights={"crime_index": 1.0})
        ranked = sorted(normalized, key=lambda p: p.rules_score, reverse=True)
        assert ranked[0].school_id == "school_2"  # School C

    def test_equal_weights(self):
        profiles = _make_profiles()
        normalized, _ = normalize_profiles(profiles)
        rules_score(normalized)

        # With equal weights, scores should differ
        scores = [p.rules_score for p in normalized]
        assert len(set(scores)) > 1


class TestRegressionScore:
    def test_needs_min_5_labels(self):
        profiles = _make_profiles()
        normalized, _ = normalize_profiles(profiles)
        labeled = {"school_0": 2.3, "school_1": 3.1}

        r2, imp, sel = regression_score(normalized, labeled)
        assert r2 is None

    def test_works_with_enough_labels(self):
        # Create 6 profiles with clear pattern
        profiles = []
        for i in range(6):
            p = CatchmentProfile(
                school_id=f"s{i}",
                latitude=52.52 + i * 0.01,
                longitude=13.40,
                radius_m=1000,
            )
            p.set("crime_index", 50.0 + i * 20.0)
            p.set("transit_count", 15.0 - i * 2.0)
            profiles.append(p)

        normalized, _ = normalize_profiles(profiles)

        # Label: higher crime → worse Abitur grade (higher number = worse)
        labeled = {f"s{i}": 2.0 + i * 0.3 for i in range(6)}

        r2, imp, sel = regression_score(normalized, labeled)
        assert r2 is not None
        assert r2 > 0.5  # Should fit well given linear relationship
        assert sel is not None
        assert len(sel) >= 1

        # All profiles should now have model_score
        for p in normalized:
            assert p.model_score is not None


class TestScoreSchools:
    def test_full_pipeline(self):
        profiles = _make_profiles()
        result = score_schools(profiles)

        assert result.mode == ScoreMode.RULES
        assert len(result.profiles) == 3
        assert result.r_squared is None

        ranked = result.ranked()
        assert ranked[0].active_score >= ranked[-1].active_score

    def test_with_regression(self):
        profiles = []
        for i in range(6):
            p = CatchmentProfile(
                school_id=f"s{i}",
                latitude=52.52,
                longitude=13.40,
                radius_m=1000,
            )
            p.set("crime_index", 50 + i * 25)
            p.set("transit_count", 15 - i * 2)
            profiles.append(p)

        labeled = {f"s{i}": 2.0 + i * 0.3 for i in range(6)}
        result = score_schools(profiles, labeled=labeled)

        assert result.mode == ScoreMode.REGRESSION
        assert result.r_squared is not None
        assert result.feature_importance is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
