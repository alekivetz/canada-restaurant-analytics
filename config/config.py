import os
from dotenv import load_dotenv
load_dotenv() 

CONFIG = {

    "google_api": {
        "key": os.getenv("GOOGLE_API_KEY"),
        "nearby_endpoint": "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
        "details_endpoint": "https://maps.googleapis.com/maps/api/place/details/json",
        "geocoding_endpoint": "https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={api_key}"
    },  

    "yelp_api": {
         "headers": {
            "Authorization": f"Bearer {os.getenv('YELP_API_KEY')}"
        },
        "restaurant_endpoint": "https://api.yelp.com/v3/businesses/search",
        "reviews_endpoint": "https://api.yelp.com/v3/businesses/{business_id}/reviews",

    },

    "db": {
        "server": os.getenv("DB_SERVER"),
        "name": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD")
    },

    "cities": {
        "Edmonton": {
            "coords": [
                (53.5461, -113.4938),  # Downtown
                (53.5100, -113.5500),  # South
                (53.5700, -113.4000),  # East
                (53.6000, -113.5000),  # North
                (53.5400, -113.6500),  # West
            ]
        },

        "Calgary": {
            "coords": [
                (51.0447, -114.0719),
                (50.9900, -114.1000),
                (51.0800, -113.9500),
                (51.1000, -114.2000),
                (51.1500, -114.0500),
            ]
        },

        "Toronto": {
            "coords": [
                (43.6532, -79.3832),
                (43.7000, -79.4000),
                (43.7500, -79.3000),
                (43.6500, -79.5500),
                (43.8000, -79.4500),
            ]
        },

        "Vancouver": {
            "coords": [
                (49.2827, -123.1207),
                (49.2500, -123.1000),
                (49.2700, -123.0500),
                (49.3000, -123.1500),
                (49.2000, -123.1800),
            ]
        },

        "Montreal": {
            "coords": [
                (45.5017, -73.5673),
                (45.5200, -73.5800),
                (45.5500, -73.6500),
                (45.6000, -73.5500),
                (45.4800, -73.6000),
            ]
        }
    },

    "pipeline": {
        "sleep_seconds": 3,
        "yelp_pagination_loops": 5,
        "yelp_max_businesses": 100,

        "base_path": "data",

        "raw_folder": "raw",
        "prepared_folder": "prepared",

        "google_folder": "google",
        "yelp_folder": "yelp",
        "census_folder": "census",

        "google_restaurants_file": "google_restaurants.json",
        "google_reviews_file": "google_reviews.json",

        "yelp_restaurants_file": "yelp_restaurants.json",

        "census_raw_file": "statcan_census_2021_raw.csv",
        "census_file": "statcan_census_2021_prepared.csv"
    }
}