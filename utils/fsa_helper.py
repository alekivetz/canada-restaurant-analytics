import requests
from config.config import CONFIG

fsa_cache = {}

def get_fsa_cached(lat, lon, api_key):
    if lat is None or lon is None:
        print(f"Skipping FSA lookup for {lat}, {lon}")
        return None
    
    key = (round(lat, 2), round(lon, 2))  # group nearby locations
    
    if key in fsa_cache:
        return fsa_cache[key]

    fsa = get_fsa(lat, lon, api_key)
    fsa_cache[key] = fsa
    return fsa

def get_fsa(lat, lon, api_key):
    url = CONFIG["google_api"]["geocoding_endpoint"].format(lat=lat, lon=lon, api_key=api_key)
    
    response = requests.get(url)
    data = response.json()

    for result in data.get("results", []):
        for comp in result.get("address_components", []):
            if "postal_code" in comp.get("types", []):
                postal_code = comp.get("long_name")
                return postal_code[:3].upper()   # FSA
    
    return None