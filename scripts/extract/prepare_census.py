# =============================================================================
# Extract: Prepare Census Data
# =============================================================================
# Script Purpose:
#     This script reads the raw Statistics Canada 2021 census CSV, filters
#     to three key demographic variables (population, average age, median
#     income), renames them to clean column names, and saves a prepared CSV
#     for loading into the bronze layer.
#
#     This script is intended to run before load_bronze.py, as it produces
#     the census file that the bronze loader depends on.
#
# Input:
#     data/raw/census/statcan_census_2021_raw.csv
#
# Output:
#     data/prepared/census/statcan_census_2021_prepared.csv
#
# Notes:
#     - Raw file uses latin-1 encoding
#     - Non-numeric values are coerced to NaN
# =============================================================================

import pandas as pd
import os
from config.config import CONFIG


# File paths from config
raw_path = os.path.join(CONFIG["pipeline"]["raw_path"], CONFIG["pipeline"]["census_path_raw"])
census_path = CONFIG["pipeline"]["census_path_prepared"]


def filter_variables(df):
    """Filter to relevant demographic variables and rename to clean column names."""
    df["variable"] = df["variable"].astype(str).str.strip()

    df = df[
        (df["variable"] == "Population, 2021") |
        (df["variable"] == "Average age of the population") |
        (df["variable"] == "Median total income of household in 2020 ($)")
    ].copy()

    # Rename to clean, standardized variable names
    df.loc[df["variable"] == "Population, 2021", "variable"] = "population"
    df.loc[df["variable"] == "Average age of the population", "variable"] = "average_age"
    df.loc[df["variable"] == "Median total income of household in 2020 ($)", "variable"] = "median_income"

    return df


def main():
    print("\n====================================================")
    print("Preparing Statistics Canada Census Data")
    print("====================================================")

    print(">> Loading raw StatsCan CSV")
    df = pd.read_csv(raw_path, encoding="latin-1")

    # Keep only relevant columns and rename
    df = df[[
        "ALT_GEO_CODE",
        "CHARACTERISTIC_NAME",
        "C1_COUNT_TOTAL"
    ]]

    df = df.rename(columns={
        "ALT_GEO_CODE": "geo_code",
        "CHARACTERISTIC_NAME": "variable",
        "C1_COUNT_TOTAL": "value"
    })

    # Filter to relevant demographic variables
    df = filter_variables(df)

    # Clean and convert values
    df["value"] = df["value"].astype(str).str.strip()
    df["value"] = df["value"].replace("", None)   # empty strings to NaN
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    print(">> Preview:")
    print(df.head())

    print(">> Saving prepared StatsCan CSV")
    df.to_csv(census_path, index=False, encoding="utf-8")
    print(f">> Saved to {census_path}")

    print("\n====================================================")
    print("Census Preparation Complete")
    print(f"Rows saved: {len(df)}")
    print("====================================================")


if __name__ == "__main__":
    main()