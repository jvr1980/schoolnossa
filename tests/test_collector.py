"""Tests for Berlin Open Data collector"""

import pytest
from src.collectors.berlin_open_data import BerlinOpenDataCollector


def test_parse_school_feature():
    """Test parsing of WFS feature"""
    collector = BerlinOpenDataCollector()

    # Sample feature (simplified)
    feature = {
        "type": "Feature",
        "properties": {
            "id": "12345",
            "bezeichnung": "Test Gymnasium",
            "schulart": "Gymnasium",
            "bezirk": "Mitte",
            "strasse": "Teststraße",
            "hausnummer": "42",
            "plz": "10115",
            "telefon": "030-123456",
            "email": "test@school.berlin",
        },
        "geometry": {
            "type": "Point",
            "coordinates": [13.404954, 52.520008]
        }
    }

    result = collector.parse_school_feature(feature)

    assert result["school_id"] == "12345"
    assert result["name"] == "Test Gymnasium"
    assert result["school_type"] == "Gymnasium"
    assert result["district"] == "Mitte"
    assert result["latitude"] == 52.520008
    assert result["longitude"] == 13.404954
    assert "Teststraße" in result["address"]


def test_build_address():
    """Test address building"""
    collector = BerlinOpenDataCollector()

    properties = {
        "strasse": "Friedrichstraße",
        "hausnummer": "123",
        "plz": "10117",
        "ort": "Berlin"
    }

    address = collector._build_address(properties)

    assert "Friedrichstraße" in address
    assert "123" in address
    assert "10117" in address
    assert "Berlin" in address


if __name__ == "__main__":
    pytest.main([__file__])
