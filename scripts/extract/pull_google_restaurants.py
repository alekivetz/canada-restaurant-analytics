# =============================================================================
# Extract: Pull Google Restaurants
# =============================================================================
# Script Purpose:
#     This script extracts restaurant data from the Google Places Nearby 
#     Search API for all cities defined in the config. It iterates over each 
#     city's coordinate grid, fetches all nearby restaurants within a 3km 
#     radius, and handles API pagination via next_page_token.
#
#     Each restaurant record is enriched with its city and FSA code
#     before being saved to a single JSON file in the raw data folder.
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
from config.config import CONFIG
import utils.fsa_helper as fsa_helper


def fetch_restaurants(lat, lon):
    """Fetch all restaurants near a coordinate point, handling API pagination."""
    url = CONFIG["google_api"]["nearby_endpoint"]
    params = {
        "location": f"{lat},{lon}",
        "radius": 3000,
        "type": "restaurant",
        "key": CONFIG["google_api"]["key"]
    }

    all_results = []

    while True:
        response = requests.get(url, params=params)

        if response.status_code != 200:
            break

        data = response.json()
        all_results.extend(data.get("results", []))

        # Google returns up to 3 pages of 20 results via next_page_token
        next_page = data.get("next_page_token")
        if not next_page:
            break

        time.sleep(2)  # Required delay before requesting next page
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
                # Enrich each record with city and FSA
                r["city"] = city
                location = r.get("geometry", {}).get("location", {})
                r["fsa"] = fsa_helper.get_fsa_cached(
                    location.get("lat"),
                    location.get("lng"),
                    CONFIG["google_api"]["key"]
                )
                all_results.append(r)

            print(f"{city} | ({lat},{lon}) complete")
            time.sleep(CONFIG["pipeline"]["sleep_seconds"])

    # Save all results to a single JSON file
    filepath = os.path.join(
        CONFIG["pipeline"]["base_path"],
        CONFIG["pipeline"]["raw_folder"],
        CONFIG["pipeline"]["google_folder"],
        CONFIG["pipeline"]["google_restaurants_file"]
    )

    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as f:
        json.dump(all_results, f, indent=4)

    print("\n====================================================")
    print("Extraction Complete")
    print(f"Saved {len(all_results)} restaurants to {filepath}")
    print("====================================================")


if __name__ == "__main__":
    main()