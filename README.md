# Canada Restaurant Analytics

A data warehouse and analytics project built around Canadian restaurant data from the Google Places API and Statistics Canada census data. Designed as a portfolio project demonstrating end-to-end data engineering — from API extraction to a structured warehouse.

---

## Data Architecture

The project follows the Medallion Architecture with Bronze, Silver, and Gold layers:

1. **Bronze Layer**: Raw data ingested from the Google Places API (restaurants, reviews, categories) and Statistics Canada census CSV files, loaded into SQL Server as-is.
2. **Silver Layer**: Data cleansing, standardization, and enrichment — including FSA (Forward Sortation Area) lookup via the Google Geocoding API.
3. **Gold Layer**: Business-ready data modeled into a star schema for analytical queries and reporting.

---

## Project Overview

This project involves:

1. **Data Extraction**: Pulling restaurant and review data from the Google Places API across multiple Canadian cities, enriched with postal code (FSA) data from the Google Geocoding API.
2. **Census Integration**: Incorporating Statistics Canada 2021 census data (population, median age, median income) at the FSA level.
3. **Data Warehousing**: Loading raw data into a SQL Server warehouse using a Python-based ETL pipeline.
4. **Data Modeling**: Building fact and dimension tables in a star schema optimized for analytical queries.
5. **Analytics & Reporting**: SQL-based analysis connecting restaurant performance to neighbourhood demographics.

Skills demonstrated:
- Python ETL pipeline development
- REST API data extraction and pagination
- SQL Server data warehousing
- Medallion architecture (Bronze / Silver / Gold)
- Data modeling (star schema)
- Census data integration
- Docker-based SQL Server setup

---

## Tools & Technologies

- **Python** — ETL scripts, API calls, data loading
- **SQL Server** — Data warehouse (running in Docker)
- **Google Places API** — Restaurant and review data
- **Google Geocoding API** — FSA enrichment
- **Statistics Canada** — 2021 census data
- **Docker** — Local SQL Server instance
- **pyodbc** — Python to SQL Server connectivity
- **pandas** — Census CSV processing

---

## Getting Started

### Prerequisites

- Docker
- Python 3.x
- Google Places API key

### Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/canada-restaurant-analytics.git
cd canada-restaurant-analytics
```

2. Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

3. Create a `.env` file in the project root:
```
GOOGLE_API_KEY=your_api_key_here
DB_SERVER=127.0.0.1,1433
DB_NAME=DataWarehouse
DB_USER=sa
DB_PASSWORD=your_password
```

4. Start SQL Server in Docker:
```bash
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=your_password" \
  -p 1433:1433 --name sql_server \
  -d mcr.microsoft.com/mssql/server:2022-latest
```

5. Initialize the warehouse:
```bash
# Run in SQL Server
init_warehouse.sql
```

---

## Pipeline

```bash
# 1. Extract source data
python -m scripts.extract.prepare_census
python -m scripts.extract.pull_google_restaurants
python -m scripts.extract.pull_google_reviews

# 2. Bronze layer — create tables and load raw data
-- Run in SQL Server: scripts/bronze/ddl_bronze.sql
python -m scripts.bronze.load_bronze

# 3. Silver layer — create tables, load, and validate
-- Run in SQL Server: scripts/silver/ddl_silver.sql
-- Run in SQL Server: scripts/silver/proc_load_silver.sql
-- Run in SQL Server: tests/quality_checks_silver.sql

# 4. Gold layer — build analytical models and validate
-- Run in SQL Server: scripts/gold/ddl_gold.sql
-- Run in SQL Server: tests/quality_checks_gold.sql
```

---

## Repository Structure

```
canada-restaurant-analytics/
│
├── config/                             # Configuration and environment setup
│
├── data/
│   ├── prepared/                       # Cleaned source files ready for loading
│   └── raw/                            # Raw API and census outputs
│
├── docs/                               # Architecture diagrams and documentation
│
├── scripts/
│   ├── extract/                        # API extraction and data preparation
│   ├── bronze/                         # Raw data loading into Bronze layer
│   ├── silver/                         # Cleaning and transformation
│   ├── gold/                           # Business-ready data modeled into a star schema
│   └── init_warehouse.sql              # Warehouse initialization script
│
├── tests/                              # Data quality checks
│
├── init_warehouse.sql                  # Warehouse initialization script
├── .env                                # Environment variables (not committed)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Data Sources

| Source | Data | Coverage |
|--------|------|----------|
| Google Places API | Restaurants, ratings, price level, reviews | Multiple Canadian cities |
| Google Geocoding API | FSA (postal area) lookup | Per restaurant coordinate |
| Statistics Canada 2021 Census | Population, median age, median income | FSA level |

> **Note:** The raw Statistics Canada census file exceeds GitHub's file size limit and is not included in this repository.
> It can be downloaded directly from [Statistics Canada](https://www12.statcan.gc.ca/census-recensement/2021/dp-pd/prof/details/download-telecharger.cfm?Lang=E).

---

## License

This project is licensed under the [MIT License](LICENSE).