# =============================================================================
# Load Bronze Layer
# =============================================================================
# Script Purpose:
#     This script loads raw data into the bronze layer of the DataWarehouse. 
#     It reads JSON files containing restaurant data from Google Maps API,
#     and census data from local JSON and CSV files into their respective 
#     tables in the bronze schema.
#
#     The script also enriches restaurant records with FSA 
#     (Forward Sortation Area) information from the Google Maps API, 
#     using a local cache to minimize API calls.
#
# WARNING:
#     Running this script will truncate all bronze tables before
#     loading. All existing data in the bronze layer will be
#     permanently deleted.
# =============================================================================

import json
import os
import pyodbc
import requests
import pandas as pd
from datetime import datetime

from config.config import CONFIG

fsa_cache = {}

def get_fsa_cached(lat, lon, api_key):
    key = (round(lat, 4), round(lon, 4))  # group nearby locations
    
    if key in fsa_cache:
        return fsa_cache[key]

    fsa = get_fsa(lat, lon, api_key)
    fsa_cache[key] = fsa
    return fsa


def get_fsa(lat, lon, api_key):
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={api_key}"

    response = requests.get(url)
    data = response.json()

    for result in data.get("results", []):
        for comp in result.get("address_components", []):
            if "postal_code" in comp.get("types", []):
                postal_code = comp.get("long_name")
                return postal_code[:3].upper()   # FSA
    
    return None


# Connect to SQL Server
def get_connection():
    db = CONFIG["db"]
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER=127.0.0.1,1433;"
        f"DATABASE={db['name']};"
        f"UID={db['user']};"
        f"PWD={db['password']};"
        "TrustServerCertificate=yes;"
        "LoginTimeout=30;"
    )


# Truncate tables in bronze schema
def truncate_tables(cursor):
    print(">> Truncating bronze tables")

    cursor.execute("TRUNCATE TABLE bronze.google_restaurants")
    cursor.execute("TRUNCATE TABLE bronze.google_reviews")
    cursor.execute("TRUNCATE TABLE bronze.yelp_restaurants")
    cursor.execute("TRUNCATE TABLE bronze.census_2021")


# Load google restaurants
def load_google_restaurants(cursor, filepath):
    if not os.path.exists(filepath):
        print(">> No Google restaurants file found, skipping")
        return
    
    print("\n------------------------------------------")
    print(">> Loading Google restaurants")

    with open(filepath, "r") as f:
        data = json.load(f)
 
    for row in data:
        location = row.get("geometry", {}).get("location", {})
        cursor.execute("""
            INSERT INTO bronze.google_restaurants (
                restaurant_id,
                name,
                rating,
                user_ratings_total,
                price_level,
                city,
                lat,
                lon,
                fsa
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row.get("place_id"),
        row.get("name"),
        row.get("rating"),
        row.get("user_ratings_total"),
        row.get("price_level"),
        row.get("city"),
        location.get("lat"),
        location.get("lon"),
        get_fsa_cached(location.get("lat"), location.get("lon"), CONFIG["google_api"]["key"])
        )
    print(f">> Inserted {len(data)} restaurants into bronze.google_restaurants")


# Load google reviews
def load_google_reviews(cursor, filepath):
    if not os.path.exists(filepath):
        print(">> No Google reviews file found, skipping")
        return
    
    print("\n------------------------------------------")
    print(">> Loading Google reviews")

    with open(filepath, "r") as f:
        data = json.load(f)

    for row in data:
        cursor.execute("""
            INSERT INTO bronze.google_reviews (
                restaurant_id,
                author_name,
                rating,
                text,
                review_time
            )
            VALUES (?, ?, ?, ?, ?)
        """,
        row.get("place_id"),
        row.get("author_name"),
        row.get("rating"),
        row.get("text"),
        row.get("time")
        )
    print(f">> Inserted {len(data)} reviews into bronze.google_reviews")


# Load yelp restaurants
def load_yelp_restaurants(cursor, filepath):
    if not os.path.exists(filepath):
        print(">> No Yelp restaurants file found, skipping")
        return

    print("\n------------------------------------------")
    print(">> Loading Yelp restaurants")

    with open(filepath, "r") as f:
        data = json.load(f)

    for row in data:
        location = row.get("coordinates", {})
        categories_raw = row.get("categories", [])
        cursor.execute("""
            INSERT INTO bronze.yelp_restaurants (
                restaurant_id,
                name,
                rating,
                categories,
                price_level,
                city,
                lat,
                lon,
                fsa
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row.get("id"),
        row.get("name"),
        row.get("rating"),
        row.get("categories"),
        row.get("price"),
        ", ".join([c.get("title", "") for c in categories_raw]),
        location.get("latitude"),
        location.get("longitude"),
        get_fsa_cached(location.get("latitude"), location.get("longitude"), CONFIG["google_api"]["key"])
        )
    print(f">> Inserted {len(data)} restaurants into bronze.yelp_restaurants")



# Load census data
def load_census(cursor, filepath):
    
    print("\n------------------------------------------")
    print(">> Loading census data")

    if not os.path.exists(filepath):
        print(">> No census file found, skipping")
        return

    df = pd.read_csv(filepath)

    for _, row in df.iterrows():
        clean_value = None if pd.isna(row["value"]) else float(row["value"])
        cursor.execute("""
            INSERT INTO bronze.census_2021 (
                fsa,
                variable,
                value
            )
            VALUES (?, ?, ?)
        """,
        
        row["geo_code"],
        row["variable"],
        clean_value
        )

    print(f">> Inserted {len(df)} census rows into bronze.census_2021")

# MAIN 
def main():
    print("\n==========================================")
    print("Loading Bronze Layer")
    print("==========================================")

    start_time = datetime.now()

    conn = get_connection()
    cursor = conn.cursor()

    try:
        truncate_tables(cursor)

        google_restaurants_file = os.path.join(
            CONFIG["pipeline"]["base_path"],
            CONFIG["pipeline"]["raw_folder"],
            CONFIG["pipeline"]["google_folder"],
            CONFIG["pipeline"]["google_restaurants_file"]
        )
        google_reviews_file = os.path.join(
            CONFIG["pipeline"]["base_path"],
            CONFIG["pipeline"]["raw_folder"],
            CONFIG["pipeline"]["google_folder"],
            CONFIG["pipeline"]["google_reviews_file"]
        )
        yelp_restaurants_file = os.path.join(
            CONFIG["pipeline"]["base_path"],
            CONFIG["pipeline"]["raw_folder"],
            CONFIG["pipeline"]["yelp_folder"],
            CONFIG["pipeline"]["yelp_restaurants_file"]
        )
        census_path = os.path.join(
            CONFIG["pipeline"]["base_path"],
            CONFIG["pipeline"]["prepared_folder"],
            CONFIG["pipeline"]["census_file"]
        )

        load_google_restaurants(cursor, google_restaurants_file)
        load_google_reviews(cursor, google_reviews_file)
        load_yelp_restaurants(cursor, yelp_restaurants_file)
        load_census(cursor, census_path)

        conn.commit()

        end_time = datetime.now()

        print("==========================================")
        print("Bronze Load Completed")
        print(f"Duration: {(end_time - start_time).seconds} seconds")
        print("==========================================")

    except Exception as e:
        print("ERROR during bronze load")
        print(e)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()