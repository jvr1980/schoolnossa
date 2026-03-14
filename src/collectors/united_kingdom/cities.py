"""UK city definitions and Local Authority code mappings.

Local Authority codes (LA codes) are 3-digit identifiers used by the
Department for Education to identify administrative areas in England.
"""

# London boroughs — all 32 boroughs + City of London
LONDON_LA_CODES = {
    "201": "City of London",
    "202": "Camden",
    "203": "Greenwich",
    "204": "Hackney",
    "205": "Hammersmith and Fulham",
    "206": "Islington",
    "207": "Kensington and Chelsea",
    "208": "Lambeth",
    "209": "Lewisham",
    "210": "Southwark",
    "211": "Tower Hamlets",
    "212": "Wandsworth",
    "213": "Westminster",
    "301": "Barking and Dagenham",
    "302": "Barnet",
    "303": "Bexley",
    "304": "Brent",
    "305": "Bromley",
    "306": "Croydon",
    "307": "Ealing",
    "308": "Enfield",
    "309": "Haringey",
    "310": "Harrow",
    "311": "Havering",
    "312": "Hillingdon",
    "313": "Hounslow",
    "314": "Kingston upon Thames",
    "315": "Merton",
    "316": "Newham",
    "317": "Redbridge",
    "318": "Richmond upon Thames",
    "319": "Sutton",
    "320": "Waltham Forest",
}

# Target cities with their LA codes
TARGET_CITIES = {
    "London": LONDON_LA_CODES,
    "Manchester": {"352": "Manchester"},
    "Birmingham": {"330": "Birmingham"},
    "Leeds": {"383": "Leeds"},
    "Liverpool": {"341": "Liverpool"},
    "Bristol": {"801": "Bristol, City of"},
    "Sheffield": {"373": "Sheffield"},
    "Newcastle": {"391": "Newcastle upon Tyne"},
}


def get_la_codes_for_city(city: str) -> dict:
    """Get LA code to name mapping for a city.

    Args:
        city: City name (e.g., 'London', 'Manchester')

    Returns:
        Dict mapping LA code to LA name

    Raises:
        ValueError: If city is not in the target list
    """
    if city not in TARGET_CITIES:
        raise ValueError(
            f"Unknown city: {city}. Available: {', '.join(TARGET_CITIES.keys())}"
        )
    return TARGET_CITIES[city]


def get_all_target_la_codes() -> dict:
    """Get all LA codes across all target cities.

    Returns:
        Dict mapping LA code to (city, LA name) tuple
    """
    result = {}
    for city, la_map in TARGET_CITIES.items():
        for code, name in la_map.items():
            result[code] = (city, name)
    return result
