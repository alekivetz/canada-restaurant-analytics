# Data Catalog for Gold Layer

## Overview
The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. It consists of **dimension tables** and **fact tables** built from the unified restaurant, review, category, and census data collected across five Canadian cities.

---

### 1. **gold.dim_restaurant**
- **Purpose:** Stores descriptive attributes for each unique restaurant, serving as the central dimension linking fact tables and category data.
- **Columns:**

| Column Name    | Data Type     | Description                                                                                   |
|----------------|---------------|-----------------------------------------------------------------------------------------------|
| restaurant_key | INT           | Surrogate key uniquely identifying each restaurant record in the dimension table.             |
| google_id      | NVARCHAR(50)  | Google Places API identifier for the restaurant. NULL for Yelp-only records.                 |
| yelp_id        | NVARCHAR(50)  | Yelp Fusion API identifier for the restaurant. NULL for Google-only records.                 |
| name           | NVARCHAR(255) | The name of the restaurant as returned by the source API.                                    |
| lat            | DECIMAL(9,6)  | Latitude coordinate of the restaurant location.                                              |
| lon            | DECIMAL(9,6)  | Longitude coordinate of the restaurant location.                                             |
| source         | NVARCHAR(10)  | Indicates the data source for the restaurant ('google', 'yelp', or 'both').                  |

---

### 2. **gold.dim_location**
- **Purpose:** Stores location and demographic data at the FSA (Forward Sortation Area) level, enriched with Statistics Canada 2021 census data.
- **Columns:**

| Column Name    | Data Type     | Description                                                                                   |
|----------------|---------------|-----------------------------------------------------------------------------------------------|
| location_key   | INT           | Surrogate key uniquely identifying each location record in the dimension table.               |
| city           | NVARCHAR(50)  | The target city the restaurant belongs to (e.g., 'Toronto', 'Vancouver').                    |
| fsa            | NVARCHAR(3)   | Forward Sortation Area code — the first 3 characters of the Canadian postal code.            |
| population     | DECIMAL(18,2) | Total population of the FSA as reported in the 2021 Statistics Canada census.                |
| average_age    | DECIMAL(18,2) | Average age of the population in the FSA as reported in the 2021 census.                     |
| median_income  | DECIMAL(18,2) | Median total household income in the FSA in 2020, as reported in the 2021 census.            |

---

### 3. **gold.dim_category**
- **Purpose:** Stores restaurant category tags sourced from the Yelp Fusion API, normalized to one row per restaurant per category.
- **Columns:**

| Column Name    | Data Type     | Description                                                                                   |
|----------------|---------------|-----------------------------------------------------------------------------------------------|
| category_key   | INT           | Surrogate key uniquely identifying each category record in the dimension table.               |
| restaurant_key | INT           | Surrogate key linking the category to the restaurant dimension table.                        |
| category       | NVARCHAR(100) | The category tag assigned to the restaurant by Yelp (e.g., 'Italian', 'Sushi', 'Fast Food'). |

---

### 4. **gold.fact_restaurants**
- **Purpose:** Stores analytical metrics for each restaurant, including ratings and price levels from both Google and Yelp, with foreign keys linking to dimension tables.
- **Columns:**

| Column Name         | Data Type     | Description                                                                                   |
|---------------------|---------------|-----------------------------------------------------------------------------------------------|
| restaurant_key      | INT           | Surrogate key linking the record to the restaurant dimension table.                          |
| location_key        | INT           | Surrogate key linking the record to the location dimension table. NULL if FSA is unavailable.|
| google_rating       | DECIMAL(3,2)  | Average customer rating from Google Places API (0.0 - 5.0). NULL for Yelp-only records.     |
| yelp_rating         | DECIMAL(3,2)  | Average customer rating from Yelp Fusion API (0.0 - 5.0). NULL for Google-only records.     |
| google_price_level  | NVARCHAR(5)   | Price level from Google Places API ('$', '$$', '$$$', '$$$$'). NULL for Yelp-only records or restaurants with no price level set.     |
| yelp_price_level    | NVARCHAR(5)   | Price level from Yelp Fusion API ('$', '$$', '$$$', '$$$$'). NULL for Google-only records or restaurants with no price level set.          |
| price_level    | NVARCHAR(5)   | Coalesced price level from Google and Yelp. NULL for restaurants with no price level set. |

---

### 5. **gold.fact_reviews**
- **Purpose:** Stores individual customer reviews from Google Places API, linked back to restaurants via the restaurant dimension table.
- **Columns:**

| Column Name    | Data Type     | Description                                                                                   |
|----------------|---------------|-----------------------------------------------------------------------------------------------|
| review_key     | INT           | Surrogate key uniquely identifying each review record in the fact table.                     |
| restaurant_key | INT           | Surrogate key linking the review to the restaurant dimension table.                          |
| author_name    | NVARCHAR(255) | The display name of the review author as returned by the Google Places API.                  |
| rating         | DECIMAL(3,2)  | The rating given by the reviewer (0.0 - 5.0).                                               |
| text           | NVARCHAR(MAX) | The full text content of the review.                                                         |
| review_time    | DATETIME      | The date and time the review was posted, converted from Unix timestamp.                      |