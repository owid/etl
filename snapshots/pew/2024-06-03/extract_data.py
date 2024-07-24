"""
This code extracts data from the article "Same-Sex Marriage Around the World" from Pew Research Center, in the format country, year.
To update:
    1. Run this code:
        python snapshots/pew/{version}/extract_data.py
    2. Update snapshot:
        python snapshots/pew/{version}/same_sex_marriage.py --path-to-file snapshots/pew/{version}/same_sex_marriage.csv
"""


from pathlib import Path

import pandas as pd

# Define URL to extract
URL = "https://www.pewresearch.org/religion/fact-sheet/gay-marriage-around-the-world/"

# Define directory
DIRECTORY = Path(__file__).parent


def scrape_data(url: str) -> pd.DataFrame:
    df = pd.read_html(url)[0]
    df = df[["Country", "Year"]].rename(columns={"Country": "country", "Year": "year"})
    return df


def main():
    (scrape_data(URL).to_csv(f"{DIRECTORY}/same_sex_marriage.csv", index=False))


if __name__ == "__main__":
    main()
