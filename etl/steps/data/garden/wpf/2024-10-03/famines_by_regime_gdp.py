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

    tb = pr.merge(tb, tb_gdp, on=["country", "year"], how="left")
    print(tb)
    # Identify rows where 'regime_redux_row_owid' is still NaN
    nan_rows = tb[tb["gdp_per_capita"].isna()]
    print(nan_rows)

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
