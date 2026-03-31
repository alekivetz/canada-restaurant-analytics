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
    all_results = []

    for city, data in CONFIG["cities"].items():
        print(f"\n--- Pulling restaurants for {city} ---")

        for lat, lon in data["coords"]:
            restaurants = fetch_restaurants(lat, lon)

            for r in restaurants:
                r["city"] = city
                r["lat"] = lat
                r["lon"] = lon
                r["timestamp"] = datetime.now().isoformat()
                
                all_results.append(r)

            print(f"{city} | ({lat},{lon}) complete")
            time.sleep(CONFIG["pipeline"]["sleep_seconds"])

    os.makedirs(CONFIG["pipeline"]["raw_path"], exist_ok=True)

    filepath = os.path.join(CONFIG["pipeline"]["raw_path"], CONFIG["pipeline"]["google_restaurants_path"])

    with open(filepath, "w") as f:
        json.dump(all_results, f, indent=4)

    print(f"\nSaved {len(all_results)} restaurants → {filepath}")


if __name__ == "__main__":
    main()