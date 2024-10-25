"""Load a meadow dataset and create a garden dataset."""


import owid.catalog.processing as pr

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
    ("Ukraine", range(1932, 1946)): 3,
    ("Vietnam", range(1944, 1946)): 3,
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_garden = paths.load_dataset("famines")

    # Read regions
    ds_regime = paths.load_dataset("vdem")

    # Read GDP
    ds_gdp = paths.load_dataset("maddison_project_database")
    tb_gdp = ds_gdp["maddison_project_database"].reset_index()

    tb_gdp = tb_gdp[["year", "country", "gdp_per_capita"]]

    # Read table from meadow dataset.
    tb_famines = ds_garden["famines"].reset_index()

    tb_famines = (
        tb_famines.assign(date=tb_famines["date"].str.split(","))
        .explode("date")
        .drop_duplicates()
        .reset_index(drop=True)
    )

    tb_famines = tb_famines.rename(columns={"date": "year"})
    tb_famines["year"] = tb_famines["year"].astype(int)

    tb = add_regime(tb_famines, ds_regime)

    tb = add_gdp(tb, tb_gdp)
    tb = tb.drop(columns=["country", "conflict", "government_policy_overall", "external_factors"])
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


def add_regime(tb_famines, ds_regime):
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

    # Ensure there are no NaNs in the 'region' column
    assert not tb["regime_redux_row_owid"].isna().any(), "There are NaN values in the 'regime_redux_row_owid' column"

    return tb


def add_gdp(tb, tb_gdp):
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
        "Ukraine": [1932, 1933, 1934, 1941, 1942, 1943, 1944, 1945, 1946, 1947],
    }

    for countries, years in special_cases.items():
        for year in years:
            if year in [1941, 1942, 1943, 1944, 1945]:
                gdp_value = calculate_average_gdp(tb_gdp, "USSR", [1940, 1946])
            else:
                gdp_value = tb_gdp[(tb_gdp["country"] == "USSR") & (tb_gdp["year"] == year)]["gdp_per_capita"].values[0]
            replace_gdp(tb, tb_gdp, countries, [year], gdp_value)

    # Special cases for individual countries
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
