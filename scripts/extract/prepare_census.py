import pandas as pd
from pathlib import Path
import os   

from config.config import CONFIG

# Paths
raw_path = os.path.join(CONFIG["pipeline"]["raw_path"], CONFIG["pipeline"]["census_path_raw"])
census_path = CONFIG["pipeline"]["census_path_prepared"]


def filter_variables(df):
    df["variable"] = df["variable"].astype(str).str.strip()

    df = df[
        (df["variable"] == "Population, 2021") |
        (df["variable"] == "Average age of the population") |
        (df["variable"] == "Median total income of household in 2020 ($)")
    ].copy()

    # Map clean names
    df.loc[df["variable"] == "Population, 2021", "variable"] = "population"
    df.loc[df["variable"] == "Average age of the population", "variable"] = "average_age"
    df.loc[df["variable"] == "Median total income of household in 2020 ($)", "variable"] = "median_income"

    return df


def main():
    print(">> Loading raw StatsCan CSV")
    df = pd.read_csv(raw_path, encoding="latin-1")

    print(">> Cleaning up StatsCan CSV")

    # Keep only columns we need 
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

    # Filter only relevant variables
    df = filter_variables(df)

    # Clean values
    df["value"] = df["value"].astype(str).str.strip()
    # Convert empty strings to NaN
    df["value"] = df["value"].replace("", None)
    # Convert to numeric
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    

    print("Preview:")
    print(df.head())

    print(">> Saving prepared StatsCan CSV")
    print(f"Saved to {census_path}")
    df.to_csv(census_path, index=False, encoding="utf-8")

if __name__ == "__main__":
    main()