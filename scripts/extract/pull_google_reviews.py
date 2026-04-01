# =============================================================================
# Extract: Pull Google Reviews
# =============================================================================
# Script Purpose:
#     This script reads the extracted restaurant data and fetches up to 5 
#     reviews per restaurant from the Google Places Details API. Each review 
#     is enriched with the restaurant ID before being saved to a single
#     JSON file in the raw data folder.
#
#     This script is intended to run after pull_google_restaurants.py,
#     as it depends on the output of that script.
#
# Output:
#     data/raw/google/google_reviews.json
#
# Notes:
#     - Google Places API returns a maximum of 5 reviews per place
#     - Sleep between requests is configurable via CONFIG
# =============================================================================

import os
import json
import time
import requests
from datetime import datetime

from config.config import CONFIG


def fetch_reviews(restaurant_id):
    url = CONFIG["google_api"]["details_endpoint"]

    params = {
        "place_id": restaurant_id,
        "fields": "name,reviews,rating",
        "key": CONFIG["google_api"]["key"]
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        return []
    data = response.json()
    return data.get("result", {}).get("reviews", [])


def main():
    print("\n====================================================")
    print("Extracting Review Data from Google Places API")
    print("====================================================")

    restaurant_filepath = os.path.join(
        CONFIG["pipeline"]["base_path"],
        CONFIG["pipeline"]["raw_folder"],
        CONFIG["pipeline"]["google_folder"],
        CONFIG["pipeline"]["google_restaurants_file"]
    )

    with open(restaurant_filepath, "r") as f:
        restaurants = json.load(f)

    all_reviews = []

    for i, r in enumerate(restaurants):
        if (i + 1) % 50 == 0:
            print(f"{i + 1} / {len(restaurants)} restaurants processed")
        r_id = r.get("place_id")

        if not r_id:
            continue

        reviews = fetch_reviews(r_id)

        for review in reviews:
            review["restaurant_id"] = r_id
            all_reviews.append(review)

        time.sleep(CONFIG["pipeline"]["sleep_seconds"])

    filepath = os.path.join(
        CONFIG["pipeline"]["base_path"],
        CONFIG["pipeline"]["raw_folder"],
        CONFIG["pipeline"]["google_folder"],
        CONFIG["pipeline"]["google_reviews_file"]
    )
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as f:
        json.dump(all_reviews, f, indent=4)

    print("\n====================================================")
    print("Extraction Complete")
    print(f"Processed {len(restaurants)} restaurants")
    print(f"Saved {len(all_reviews)} reviews to {filepath}")
    print("====================================================")

if __name__ == "__main__":
    main()