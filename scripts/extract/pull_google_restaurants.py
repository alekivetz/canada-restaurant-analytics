# =============================================================================
# Extract: Pull Google Restaurants
# =============================================================================
# Script Purpose:
#     This script extracts restaurant data from the Google Places Nearby 
#     Search API for all cities defined in the config. It iterates over each 
#     city's coordinate grid, fetches all nearby restaurants within a 3km 
#     radius, and handles API pagination via next_page_token.
#
#     Each restaurant record is enriched with its city before being saved to 
#     a single JSON file in the raw data folder.
#
# Output:
#     data/raw/google/google_restaurants.json
#
# Notes:
#     - Sleeps 2 seconds between paginated requests (API requirement)
#     - Sleep between coordinate calls is configurable via CONFIG
# =============================================================================

import os
import json
import time
import requests
from datetime import datetime

from config.config import CONFIG

def fetch_restaurants(lat, lon):
    url = CONFIG["api"]["nearby_endpoint"]

    params = {
        "location": f"{lat},{lon}",
        "radius": 3000,
        "type": "restaurant",
        "key": CONFIG["api"]["key"]
    }

    all_results = []

    while True:
        response = requests.get(url, params=params)

        if response.status_code != 200:
            break

        data = response.json()
        results = data.get("results", [])

        all_results.extend(results)

        next_page = data.get("next_page_token")

        if not next_page:
            break

        time.sleep(2)  
        params["pagetoken"] = next_page

    return all_results


def main():
    print("\n====================================================")
    print("Extracting Restaurant Data from Google Places API")
    print("====================================================")

    all_results = []

    for city, data in CONFIG["cities"].items():
        print(f"\n--- Getting restaurants: {city} ---")

        for lat, lon in data["coords"]:
            restaurants = fetch_restaurants(lat, lon)

            for r in restaurants:
                r["city"] = city             
                all_results.append(r)

            print(f"{city} | ({lat},{lon}) complete")
            time.sleep(CONFIG["pipeline"]["sleep_seconds"])

    os.makedirs(CONFIG["pipeline"]["raw_path"], exist_ok=True)

    filepath = os.path.join(CONFIG["pipeline"]["raw_path"], CONFIG["pipeline"]["google_restaurants_path"])

    with open(filepath, "w") as f:
        json.dump(all_results, f, indent=4)

    print("\n====================================================")
    print("Extraction Complete.")
    print(f"Saved {len(all_results)} restaurants to {filepath}")
    print("====================================================")


if __name__ == "__main__":
    main()