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
    url = CONFIG["api"]["details_endpoint"]

    params = {
        "place_id": restaurant_id,
        "fields": "name,reviews,rating",
        "key": CONFIG["api"]["key"]
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        return []
    data = response.json()
    return data.get("result", {}).get("reviews", [])


def main():
    raw_folder = CONFIG["pipeline"]["raw_path"]
    filepath = os.path.join(raw_folder, CONFIG["pipeline"]["google_restaurants_path"])

    with open(filepath, "r") as f:
        restaurants = json.load(f)

    all_reviews = []

    for i, r in enumerate(restaurants):
        r_id = r.get("place_id")

        if not r_id:
            continue

        reviews = fetch_reviews(r_id)

        for review in reviews:
            review["restaurant_id"] = r_id
            all_reviews.append(review)

        print(f"{i+1} / {len(restaurants)} restaurants processed")

        time.sleep(CONFIG["pipeline"]["sleep_seconds"])

    filepath = os.path.join(raw_folder, CONFIG["pipeline"]["google_reviews_path"])

    with open(filepath, "w") as f:
        json.dump(all_reviews, f, indent=4)

    print(f"\nSaved {len(all_reviews)} reviews → {filepath}")


if __name__ == "__main__":
    main()