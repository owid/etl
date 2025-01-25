"""Load a meadow dataset and create a garden dataset."""

import re

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Assign to Autocracires (3)
CUSTOM_REGIMES = {
    ("Democratic Republic of Congo", range(1885, 1900)): 3,
    ("Kenya, Uganda, Tanzania", range(1896, 1901)): 3,
    ("Eastern Europe", range(1945, 1948)): 3,
    ("Tanzania, Mozambique, Rwanda, Burundi", range(1914, 1917)): 3,
    ("Germany, USSR", range(1941, 1945)): 3,
    ("Syria, Lebanon, Israel", range(1915, 1919)): 3,
    ("India, Bangladesh", 1943): 3,
    ("Turkey, Iraq, Iran, Syria", range(1912, 1924)): 3,
    ("Turkey, Armenians", range(1915, 1917)): 3,
    ("Iran", range(1870, 1873)): 3,
    ("Philippines", 1899): 3,
    ("Poland", range(1915, 1918)): 3,
    ("Poland", range(1940, 1944)): 3,
    ("Russia, Kazakhstan", range(1932, 1935)): 3,
    ("Russia, Ukraine", range(1915, 1923)): 3,
    ("Senegal, Burkina Faso, Mali, Niger, Chad", range(1913, 1915)): 3,
    ("Mauritania, Mali, Niger", range(1969, 1975)): 3,
    (
        "Serbia, Albania, Bosnia and Herzegovina, Bulgaria, Greece, Kosovo, Montenegro, North Macedonia, Romania, Croatia, Slovenia",
        range(1914, 1919),
    ): 3,
    ("Sudan, Ethiopia", range(1888, 1893)): 3,
    ("Tanzania", range(1905, 1908)): 3,
    ("Russia, Western Soviet States", range(1941, 1945)): 3,
    ("Moldova, Ukraine, Russia, Belarus", range(1946, 1948)): 3,
    ("Ukraine", range(1931, 1946)): 3,
    ("Vietnam", range(1944, 1946)): 3,
    ("USSR", range(1939, 1947)): 3,
    ("Russia, Western Soviet States", range(1939, 1948)): 3,
    ("Somaliland, African Red Sea Region", range(1910, 2020)): 3,
    ("Sudan", range(1888, 1893)): 3,
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_garden = paths.load_dataset("famines")
    tb_famines = ds_garden["famines"].reset_index()

    # Load regimes.
    ds_regime = paths.load_dataset("vdem")

    # Load GDP.
    ds_gdp = paths.load_dataset("maddison_project_database")
    tb_gdp = ds_gdp["maddison_project_database"].reset_index()
    tb_gdp = tb_gdp[["year", "country", "gdp_per_capita"]]

    # Split the 'date' column into separate rows for each year.
    tb_famines = (
        tb_famines.assign(date=tb_famines["date"].str.split(","))
        .explode("date")
        .drop_duplicates()
        .reset_index(drop=True)
    )
    # Rename date to year.
    tb_famines = tb_famines.rename(columns={"date": "year"})
    tb_famines["year"] = tb_famines["year"].astype(int)

    # Add regime data.
    tb = add_regime(tb_famines, ds_regime)

    # Add GDP data.
    tb = add_gdp(tb, tb_gdp)

    # Extract the text before the year from the famine_name column
    tb["country_name"] = tb["famine_name"].apply(lambda x: re.split(r"\s+\d{4}", x)[0])

    # Remove (Hungerplan) from famine_name
    tb["country_name"] = tb["country_name"].str.replace(r"\s*\(Hungerplan\)", "", regex=True)
    tb["midpoint_year"] = tb["famine_name"].apply(extract_years)

    tb["regime_redux_row_owid"] = tb["regime_redux_row_owid"].replace({3: 0, 2: 1})

    # Drop unused in this dataset columns columns.
    tb = tb.format(["famine_name", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def extract_years(famine_name):
    # Extract start and end years from famine_name and calculate midpoint
    years = re.findall(r"\d{4}", famine_name)
    if len(years) == 2:
        start_year, end_year = map(int, years)
        return (start_year + end_year) // 2
    elif len(years) == 1:
        return int(years[0])
    else:
        return None


def add_regime(tb_famines: Table, ds_regime: Dataset) -> Table:
    """
    Add regime information to the famines table by merging it with the regime dataset and applying custom regime rules.

    Parameters:
    tb_famines (Table): Table containing famine data.
    ds_regime (Dataset): Dataset containing regime data, expected to have a key "vdem" with a DataFrame value.

    Returns:
    Table: The updated Table with regime information added.

    The function performs the following steps:
    1. Extracts the "vdem" DataFrame from the ds_regime dictionary and resets its index.
    2. Reduces the regime DataFrame to only include the columns "country", "year", and "regime_redux_row_owid".
    3. Combines autocracies by setting the "regime_redux_row_owid" value to 3 for rows where it is 0 or 1.
    4. Merges the reduced regime DataFrame with the famines DataFrame on "country" and "year".
    5. Applies custom regime rules defined in the CUSTOM_REGIMES dictionary to assign regime values.
    6. Ensures there are no NaN values in the "regime_redux_row_owid" column.
    """
    tb_regime = ds_regime["vdem"].reset_index()

    reduced_regime = tb_regime[["country", "year", "regime_redux_row_owid"]]

    # Combine autocracies
    reduced_regime.loc[reduced_regime["regime_redux_row_owid"].isin([0, 1]), "regime_redux_row_owid"] = 3

    tb = pr.merge(tb_famines, reduced_regime, on=["country", "year"], how="left")

    # Assign regime values based on the CUSTOM_REGIMES dictionary
    def assign_regime(row):
        for (countries, years), regime in CUSTOM_REGIMES.items():
            # Ensure years is always iterable
            if not isinstance(years, (list, range)):
                years = [years]
            # Check if the country matches and if the year is in the range/list
            if row["country"] == countries and row["year"] in years:
                return regime
        return row["regime_redux_row_owid"]  # Keep the original value if no match found

    # Apply the function to assign regime values
    tb["regime_redux_row_owid"] = tb.apply(assign_regime, axis=1)

    # Ensure there are no NaNs in the 'regime_redux_row_owid' column
    assert not tb["regime_redux_row_owid"].isna().any(), "There are NaN values in the 'regime_redux_row_owid' column"

    return tb


def add_gdp(tb: Table, tb_gdp: Table) -> Table:
    """
    Add GDP information to the famines table by merging it with the GDP dataset and applying custom GDP replacement rules.

    Parameters:
    tb (Table): Table containing famine data.
    tb_gdp (Table): Table containing GDP data.

    Returns:
    Table: The updated Table with GDP information added.

    The function performs the following steps:
    1. Replaces 'Former Sudan' with 'Sudan' in the 'country' column of tb_gdp.
    2. Merges the famine table (tb) with the GDP table (tb_gdp) on 'country' and 'year'.
    3. Defines and applies replacement rules for specific countries and years, using either a reference year or an average GDP over a range of years.
    4. Handles special cases for multiple countries, replacing GDP values for specified years with either a specific year's GDP or an average GDP.
    5. Handles special cases for individual countries, replacing GDP values for the year 2023 with the GDP value from 2022.

    Replacement Rules:
    - For each country and specified years, replace the GDP value with the GDP from a reference year or the average GDP over a specified range of years.
    - Special cases handle multiple countries and individual countries separately, ensuring accurate and consistent GDP data.

    Example:
    - For Cuba, the GDP values for the years 1895 to 1898 are replaced with the GDP value from 1892.
    - For China, the GDP values for the years 1876 to 1879 are replaced with the average GDP from 1870 to 1887.
    - Special cases include handling GDP values for "Russia, Kazakhstan" for the years 1932 to 1934 with the average GDP of the USSR from 1940 to 1946.

    Returns:
    - The updated famine table with the added and replaced GDP information.
    """

    # Replace 'former Sudan' with 'Sudan' in the 'country' column of tb_gdp
    tb_gdp["country"] = tb_gdp["country"].replace("Former Sudan", "Sudan")

    tb = pr.merge(tb, tb_gdp, on=["country", "year"], how="left")

    # Define replacement rules
    replacement_rules = [
        {"country": "Cuba", "years": [1895, 1896, 1897, 1898], "ref_year": 1892},
        {"country": "China", "years": [1876, 1877, 1878, 1879], "year_range": [1870, 1887]},
        {"country": "China", "years": [1897, 1898, 1899], "year_range": [1890, 1900]},
        {"country": "China", "years": [1901], "ref_year": 1900},
        {"country": "China", "years": [1920, 1921], "year_range": [1913, 1929]},
        {"country": "China", "years": [1928], "ref_year": 1929},
        {"country": "India", "years": [1876, 1877, 1878], "year_range": [1870, 1884]},
        {"country": "Turkey", "years": [1894, 1895, 1896], "year_range": [1870, 1913]},
        {"country": "Philippines", "years": [1899, 1900, 1901], "ref_year": 1902},
        {"country": "Poland", "years": [1915, 1916, 1917, 1918], "year_range": [1913, 1920]},
        {"country": "Poland", "years": [1940, 1941, 1942, 1943, 1944, 1945], "year_range": [1938, 1948]},
        {"country": "Iran", "years": [1871, 1872], "ref_year": 1870},
        {"country": "Iran", "years": [1917, 1918, 1919], "ref_year": 1913},
        {"country": "Turkey, Armenians", "years": [1915, 1916], "year_range": [1913, 1918]},
        {"country": "Vietnam", "years": [1944, 1945], "ref_year": 1950},
    ]

    # Apply replacement rules
    for rule in replacement_rules:
        if "ref_year" in rule:
            gdp_value = tb_gdp[(tb_gdp["country"] == rule["country"]) & (tb_gdp["year"] == rule["ref_year"])][
                "gdp_per_capita"
            ].values[0]
        elif "year_range" in rule:
            gdp_value = calculate_average_gdp(tb_gdp, rule["country"], rule["year_range"])
        replace_gdp(tb, tb_gdp, rule["country"], rule["years"], gdp_value)

    # Special cases for multiple countries
    special_cases = {
        "Russia, Kazakhstan": [1932, 1933, 1934],
        "Russia, Ukraine": [1915, 1916, 1917, 1918, 1919, 1920, 1921, 1922],
        "Russia, Western Soviet States": [1941, 1942, 1943, 1944],
        "Moldova, Ukraine, Russia, Belarus": [1946, 1947],
        "Ukraine": [1931, 1932, 1933, 1934, 1941, 1942, 1943, 1944, 1945, 1946, 1947],
    }

    for countries, years in special_cases.items():
        for year in years:
            if year in [1941, 1942, 1943, 1944, 1945]:
                gdp_value = calculate_average_gdp(tb_gdp, "USSR", [1940, 1946])
            if year in [1915, 1916, 1917, 1918, 1919, 1920, 1921, 1922]:
                gdp_value = calculate_average_gdp(tb_gdp, "Russia", [1915, 1922])
            else:
                gdp_value = tb_gdp[(tb_gdp["country"] == "USSR") & (tb_gdp["year"] == year)]["gdp_per_capita"].values[0]
            replace_gdp(tb, tb_gdp, countries, [year], gdp_value)

    # Special cases for individual countries with GDP values for 2023 replaced with GDP values from 2022
    special_individual_cases = [
        {"country": "Central African Republic", "year": 2023, "ref_year": 2022},
        {"country": "Ethiopia", "year": 2023, "ref_year": 2022},
        {"country": "Syria", "year": 2023, "ref_year": 2022},
    ]

    for case in special_individual_cases:
        gdp_value = tb_gdp[(tb_gdp["country"] == case["country"]) & (tb_gdp["year"] == case["ref_year"])][
            "gdp_per_capita"
        ].values[0]
        replace_gdp(tb, tb_gdp, case["country"], [case["year"]], gdp_value)

    return tb


def replace_gdp(tb, tb_gdp, country, years, gdp_value):
    for year in years:
        tb.loc[(tb["country"] == country) & (tb["year"] == year), "gdp_per_capita"] = gdp_value


def calculate_average_gdp(tb_gdp, country, year_range):
    return tb_gdp[(tb_gdp["country"] == country) & (tb_gdp["year"].between(year_range[0], year_range[1]))][
        "gdp_per_capita"
    ].mean()
