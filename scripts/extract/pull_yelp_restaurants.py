# =============================================================================
# Extract: Pull Yelp Restaurants
# =============================================================================
# Script Purpose:
#     This script extracts restaurant data from the Yelp Fusion Business 
#     Search API for all cities defined in the config. It iterates over each 
#     city's coordinate grid and fetches up to 50 records per request within 
#     a 4km radius.
#
#     Each restaurant record is enriched with its FSA and formatted 
#     categories before being saved to a single JSON file.
#
# Output:
#     data/raw/yelp/yelp_restaurants.json
#
# Notes:
#     - Returns up to 50 results per coordinate point
#     - Failed requests are caught and logged without stopping the pipeline
#     - Sleep between requests is configurable via CONFIG
# =============================================================================

import os
import json
import time
import requests
from config.config import CONFIG
import utils.fsa_helper as fsa_helper


def fetch_city_data(city):
    """Fetch all restaurants for a city across its coordinate grid."""
    url = CONFIG["yelp_api"]["restaurant_endpoint"]
    headers = CONFIG["yelp_api"]["headers"]
    base_params = {
        "categories": "restaurant",
        "limit": 50,
        "radius": 4000
    }

    all_results = []

    for lat, lon in CONFIG["cities"][city]["coords"]:
        params = base_params.copy()
        params.update({
            "latitude": lat,
            "longitude": lon,
        })

        try:
            response = requests.get(url, params=params, headers=headers)
            data = response.json()

            for record in data.get("businesses", []):
                # Format categories from list of dicts to comma-separated string
                raw_categories = record.get("categories", [])
                record["categories_formatted"] = ", ".join([c.get("title", "") for c in raw_categories])

                # Enrich with FSA via Google Geocoding API
                coords = record.get("coordinates", {})
                record["fsa"] = fsa_helper.get_fsa_cached(
                    coords.get("latitude"),
                    coords.get("longitude"),
                    CONFIG["google_api"]["key"]
                )

                all_results.append(record)

            print(f"{city} | ({lat}, {lon}) complete")
            time.sleep(CONFIG["pipeline"]["sleep_seconds"])

        except Exception as e:
            print(f"{city} | ({lat}, {lon}) failed: {e}")

    return all_results


def main():
    print("\n====================================================")
    print("Extracting Restaurant Data from Yelp Fusion API")
    print("====================================================")

    all_results = []

    for city in CONFIG["cities"]:
        print(f"\n--- Getting restaurants: {city} ---")
        data = fetch_city_data(city)

        if data:
            all_results.extend(data)
        else:
            print(f"No restaurants collected for {city}")

    # Save all results to a single JSON file
    filepath = os.path.join(
        CONFIG["pipeline"]["base_path"],
        CONFIG["pipeline"]["raw_folder"],
        CONFIG["pipeline"]["yelp_folder"],
        CONFIG["pipeline"]["yelp_restaurants_file"]
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