import os
from dotenv import load_dotenv
load_dotenv() 

CONFIG = {

    "api": {
        "nearby_endpoint": "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
        "details_endpoint": "https://maps.googleapis.com/maps/api/place/details/json",
        "key": os.getenv("GOOGLE_API_KEY")
    },

    "db": {
        "server": os.getenv("DB_SERVER"),
        "name": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD")
    },

    "cities": {
        "Toronto": {
            "coords": [(43.6532, -79.3832), (43.7, -79.4), (43.75, -79.3)]
        },
        "Vancouver": {
            "coords": [(49.2827, -123.1207), (49.25, -123.1), (49.27, -123.05)]
        },
        "Montreal": {
            "coords": [(45.5017, -73.5673), (45.52, -73.58), (45.55, -73.65)]
        },
        "Calgary": {
            "coords": [(51.0447, -114.0719), (50.99, -114.1), (51.08, -113.95)]
        },
        "Edmonton": {
            "coords": [(53.5461, -113.4938), (53.51, -113.55), (53.57, -113.4)]
        },
        "Ottawa": {
            "coords": [(45.4215, -75.6972), (45.43, -75.65), (45.4, -75.75)]
        }
    },

    "pipeline": {
        "sleep_seconds": 3,
        "raw_path": "data/raw",
        "google_restaurants_path": "google/google_restaurants.json",
        "google_reviews_path": "google/google_reviews.json",
        "census_path_raw": "census/statcan_census_2021_raw.csv",
        "census_path_prepared": "data/prepared/statcan_census_2021_prepared.csv"
    }
}