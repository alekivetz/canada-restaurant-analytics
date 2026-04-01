# =============================================================================
# Utility: FSA Helper
# =============================================================================
# Script Purpose:
#     Provides cached FSA (Forward Sortation Area) lookup using the Google
#     Geocoding API. Given a latitude and longitude, returns the first 3
#     characters of the Canadian postal code, which represent the FSA.
#
#     A local in-memory cache keyed on rounded coordinates (2 decimal places,
#     ~1km precision) minimizes redundant API calls for nearby locations.
#
# Notes:
#     - Cache is reset on each script run (not persisted to disk)
#     - Coordinates rounded to 2 decimal places for cache grouping
#     - Returns None if coordinates are missing or no postal code is found
# =============================================================================

import requests
from config.config import CONFIG

# In-memory cache to avoid duplicate API calls for nearby coordinates
fsa_cache = {}

def get_fsa_cached(lat, lon, api_key):
    """Return cached FSA for a coordinate pair, fetching from API if not cached."""
    if lat is None or lon is None:
        return None

    # Round to 2 decimal places (~1km) to group nearby locations
    key = (round(lat, 2), round(lon, 2))

    if key in fsa_cache:
        return fsa_cache[key]

    fsa = get_fsa(lat, lon, api_key)
    fsa_cache[key] = fsa
    return fsa


def get_fsa(lat, lon, api_key):
    """Call Google Geocoding API and extract FSA from postal code."""
    url = CONFIG["google_api"]["geocoding_endpoint"].format(lat=lat, lon=lon, api_key=api_key)

    response = requests.get(url)
    data = response.json()

    for result in data.get("results", []):
        for comp in result.get("address_components", []):
            if "postal_code" in comp.get("types", []):
                postal_code = comp.get("long_name")
                return postal_code[:3].upper()

    return None