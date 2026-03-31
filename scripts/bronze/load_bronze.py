import json
import os
import pyodbc
import requests
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


# DB connection
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


# TRUNCATE TABLES 
def truncate_tables(cursor):
    print(">> Truncating bronze tables")

    cursor.execute("TRUNCATE TABLE bronze.google_restaurants")
    cursor.execute("TRUNCATE TABLE bronze.google_reviews")
    cursor.execute("TRUNCATE TABLE bronze.google_categories")


# LOAD RESTAURANTS
def load_restaurants(cursor, filepath):
    print("\n------------------------------------------")
    print(">> Loading restaurants")

    with open(filepath, "r") as f:
        data = json.load(f)
 
    for row in data:
        cursor.execute("""
            INSERT INTO bronze.google_restaurants (
                restaurant_id,
                name,
                rating,
                user_ratings_total,
                price_level,
                lat,
                lon,
                city,
                fsa
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row.get("place_id"),
        row.get("name"),
        row.get("rating"),
        row.get("user_ratings_total"),
        row.get("price_level"),
        row.get("lat"),
        row.get("lon"),
        row.get("city"),
        get_fsa_cached(row.get("lat"), row.get("lon"), CONFIG["api"]["key"])
        )
    print(f">> Inserted {len(data)} restaurants into bronze.google_restaurants")


# LOAD REVIEWS
def load_reviews(cursor, filepath):
    if not os.path.exists(filepath):
        print(">> No reviews file found, skipping")
        return
    
    print("\n------------------------------------------")
    print(">> Loading reviews")

    with open(filepath, "r") as f:
        data = json.load(f)

    for row in data:
        cursor.execute("""
            INSERT INTO bronze.google_reviews (
                restaurant_id,
                author_name,
                rating,
                text,
                review_time,
                city
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """,
        row.get("place_id"),
        row.get("author_name"),
        row.get("rating"),
        row.get("text"),
        row.get("time"),
        row.get("city")
        )
    print(f">> Inserted {len(data)} reviews into bronze.google_reviews")


# LOAD CATEGORIES
def load_categories(cursor, filepath):
    
    print("\n------------------------------------------")
    print(">> Loading categories")
    with open(filepath, "r") as f:
        data = json.load(f)
    for row in data:
        restaurant_id = row.get("place_id")
        for category in row.get("types", []):
            cursor.execute("""
                INSERT INTO bronze.google_categories (
                    restaurant_id,
                    category
                )
                VALUES (?, ?)
            """,
            restaurant_id,
            category
            )
    print(">> Inserted categories into bronze.google_categories")

# LOAD CENSUS
def load_census(cursor, filepath):
    
    print("\n------------------------------------------")
    print(">> Loading census data")

    if not os.path.exists(filepath):
        print(">> No census file found, skipping")
        return

    import pandas as pd

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

        raw_folder = CONFIG["pipeline"]["raw_path"]

        restaurants_path = os.path.join(raw_folder, CONFIG["pipeline"]["google_restaurants_path"])
        reviews_path = os.path.join(raw_folder, CONFIG["pipeline"]["google_reviews_path"])
        census_path = CONFIG["pipeline"]["census_path_prepared"]

        load_restaurants(cursor, restaurants_path)
        load_reviews(cursor, reviews_path)
        load_categories(cursor, restaurants_path)
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