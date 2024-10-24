"""Load a meadow dataset and create a garden dataset."""


import owid.catalog.processing as pr
import pandas as pd

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
    reduced_regime["regime_redux_row_owid"] = reduced_regime["regime_redux_row_owid"].astype(str)
    # Combine autocracies
    reduced_regime.loc[
        reduced_regime["regime_redux_row_owid"].isin(["0", "1"]), "regime_redux_row_owid"
    ] = "Autocracies"
    reduced_regime.loc[reduced_regime["regime_redux_row_owid"] == "2", "regime_redux_row_owid"] = "Democracies"

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

    tb["regime_redux_row_owid"] = tb["regime_redux_row_owid"].astype(str)
    return tb


def add_gdp(tb, tb_gdp):
    # Replace 'former Sudan' with 'Sudan' in the 'country' column of tb_gdp
    tb_gdp["country"] = tb_gdp["country"].replace("Former Sudan", "Sudan")

    tb = pr.merge(tb, tb_gdp, on=["country", "year"], how="left")

    # Identify rows where country is 'Cuba' and year is in [1895, 1896, 1897, 1898]
    cuba_years_to_replace = [1895, 1896, 1897, 1898]

    # Get the entry for Cuba 1892
    cuba_1892_entry = tb_gdp[(tb_gdp["country"] == "Cuba") & (tb_gdp["year"] == 1892)]["gdp_per_capita"]

    # Replace the rows for Cuba 1895-1898 with the entry for Cuba 1892
    for year in cuba_years_to_replace:
        tb.loc[(tb["country"] == "Cuba") & (tb["year"] == year), "gdp_per_capita"] = cuba_1892_entry.values[0]

    # Identify rows where country is 'China' and year is in [1876, 1877, 1878, 1879]
    china_years_to_replace = [1876, 1877, 1878, 1879]

    # Calculate the average GDP per capita for China for the years 1870 to 1887
    china_avg_gdp_1870_1887 = tb_gdp[(tb_gdp["country"] == "China") & (tb_gdp["year"].between(1870, 1887))][
        "gdp_per_capita"
    ].mean()

    # Replace the rows for China 1876-1879 with the average GDP per capita
    for year in china_years_to_replace:
        tb.loc[(tb["country"] == "China") & (tb["year"] == year), "gdp_per_capita"] = china_avg_gdp_1870_1887

    # Identify rows where country is 'China' and year is in [1897, 1898, 1899]
    china_years_to_replace_1897_1899 = [1897, 1898, 1899]

    # Calculate the average GDP per capita for China for the years 1890 to 1900
    china_avg_gdp_1890_1900 = tb_gdp[(tb_gdp["country"] == "China") & (tb_gdp["year"].between(1890, 1900))][
        "gdp_per_capita"
    ].mean()

    # Replace the rows for China 1897-1899 with the average GDP per capita
    for year in china_years_to_replace_1897_1899:
        tb.loc[(tb["country"] == "China") & (tb["year"] == year), "gdp_per_capita"] = china_avg_gdp_1890_1900

    # Identify rows where country is 'China' and year is 1901
    china_years_to_replace_1901 = [1901]

    # Calculate the average GDP per capita for China for the year 1900
    china_avg_gdp_1900 = tb_gdp[(tb_gdp["country"] == "China") & (tb_gdp["year"] == 1900)]["gdp_per_capita"].mean()

    # Replace the rows for China 1901 with the average GDP per capita for 1900
    for year in china_years_to_replace_1901:
        tb.loc[(tb["country"] == "China") & (tb["year"] == year), "gdp_per_capita"] = china_avg_gdp_1900

    # Identify rows where country is 'China' and year is in [1920, 1921]
    china_years_to_replace_1920_1921 = [1920, 1921]

    # Calculate the average GDP per capita for China for the years 1913 to 1929
    china_avg_gdp_1913_1929 = tb_gdp[(tb_gdp["country"] == "China") & (tb_gdp["year"].between(1913, 1929))][
        "gdp_per_capita"
    ].mean()

    # Replace the rows for China 1920-1921 with the average GDP per capita for 1913-1929
    for year in china_years_to_replace_1920_1921:
        tb.loc[(tb["country"] == "China") & (tb["year"] == year), "gdp_per_capita"] = china_avg_gdp_1913_1929

    # Identify rows where country is 'China' and year is 1928
    china_years_to_replace_1928 = [1928]

    # Calculate the average GDP per capita for China for the year 1929
    china_avg_gdp_1929 = tb_gdp[(tb_gdp["country"] == "China") & (tb_gdp["year"] == 1929)]["gdp_per_capita"].mean()

    # Replace the rows for China 1928 with the average GDP per capita for 1929
    for year in china_years_to_replace_1928:
        tb.loc[(tb["country"] == "China") & (tb["year"] == year), "gdp_per_capita"] = china_avg_gdp_1929

    # Identify rows where country is 'India' and year is in [1876, 1877, 1878]
    india_years_to_replace = [1876, 1877, 1878]

    # Calculate the average GDP per capita for India for the years 1870 and 1884
    india_avg_gdp_1870_1884 = tb_gdp[(tb_gdp["country"] == "India") & (tb_gdp["year"].isin([1870, 1884]))][
        "gdp_per_capita"
    ].mean()

    # Replace the rows for India 1876-1878 with the average GDP per capita for 1870 and 1884
    for year in india_years_to_replace:
        tb.loc[(tb["country"] == "India") & (tb["year"] == year), "gdp_per_capita"] = india_avg_gdp_1870_1884

    # Identify rows where country is 'Turkey' and year is in [1894, 1895, 1896]
    turkey_years_to_replace = [1894, 1895, 1896]

    # Calculate the average GDP per capita for Turkey for the years 1870 to 1913
    turkey_avg_gdp_1870_1913 = tb_gdp[(tb_gdp["country"] == "Turkey") & (tb_gdp["year"].between(1870, 1913))][
        "gdp_per_capita"
    ].mean()

    # Identify rows where country is 'Turkey' and year is in [1894, 1895, 1896]
    turkey_years_to_replace = [1894, 1895, 1896]

    # Calculate the average GDP per capita for Turkey for the years 1870 to 1913
    turkey_avg_gdp_1870_1913 = tb_gdp[(tb_gdp["country"] == "Turkey") & (tb_gdp["year"].between(1870, 1913))][
        "gdp_per_capita"
    ].mean()

    # Replace the rows for Turkey 1894-1896 with the average GDP per capita for 1870 to 1913
    for year in turkey_years_to_replace:
        tb.loc[(tb["country"] == "Turkey") & (tb["year"] == year), "gdp_per_capita"] = turkey_avg_gdp_1870_1913

    # Retrieve the GDP per capita for Philippines for the year 1902
    philippines_gdp_1902 = tb_gdp[(tb_gdp["country"] == "Philippines") & (tb_gdp["year"] == 1902)][
        "gdp_per_capita"
    ].values[0]

    # Replace the rows for Philippines 1899-1901 with the GDP per capita for 1902
    for year in [1899, 1900, 1901]:
        tb.loc[(tb["country"] == "Philippines") & (tb["year"] == year), "gdp_per_capita"] = philippines_gdp_1902

    # Identify rows where country is 'Poland' and year is in [1915, 1916, 1917, 1918]
    poland_years_to_replace_early = [1915, 1916, 1917, 1918]

    # Calculate the average GDP per capita for Poland for the years 1913 to 1920
    poland_avg_gdp_1913_1920 = tb_gdp[(tb_gdp["country"] == "Poland") & (tb_gdp["year"].between(1913, 1920))][
        "gdp_per_capita"
    ].mean()

    # Replace the rows for Poland 1915-1918 with the average GDP per capita for 1913 to 1920
    for year in poland_years_to_replace_early:
        tb.loc[(tb["country"] == "Poland") & (tb["year"] == year), "gdp_per_capita"] = poland_avg_gdp_1913_1920

    # Identify rows where country is 'Poland' and year is in [1940, 1941, 1942, 1943, 1944, 1945]
    poland_years_to_replace_late = [1940, 1941, 1942, 1943, 1944, 1945]

    # Calculate the average GDP per capita for Poland for the years 1938 to 1948
    poland_avg_gdp_1938_1948 = tb_gdp[(tb_gdp["country"] == "Poland") & (tb_gdp["year"].between(1938, 1948))][
        "gdp_per_capita"
    ].mean()

    # Replace the rows for Poland 1940-1945 with the average GDP per capita for 1938 to 1948
    for year in poland_years_to_replace_late:
        tb.loc[(tb["country"] == "Poland") & (tb["year"] == year), "gdp_per_capita"] = poland_avg_gdp_1938_1948

    # Identify rows where country is in ['Russia', 'Kazakhstan', 'Ukraine', 'Western Soviet States', 'Moldova', 'Belarus'] and year is in the specified ranges
    years_to_replace = {
        "Russia, Kazakhstan": [1932, 1933, 1934],
        "Russia, Ukraine": [1915, 1916, 1917, 1918, 1919, 1920, 1921, 1922],
        "Russia, Western Soviet States": [1941, 1942, 1943, 1944],
        "Moldova, Ukraine, Russia, Belarus": [1946, 1947],
        "Ukraine": [1932, 1933, 1934, 1941, 1942, 1943, 1944, 1945, 1946, 1947],
    }

    # Retrieve the GDP per capita for USSR for the specified years
    for countries, years in years_to_replace.items():
        for year in years:
            if year in [1941, 1942, 1943, 1944, 1945]:
                # Calculate the average GDP per capita for USSR for the years 1940 to 1946
                ussr_gdp = tb_gdp[(tb_gdp["country"] == "USSR") & (tb_gdp["year"].between(1940, 1946))][
                    "gdp_per_capita"
                ].mean()

            else:
                ussr_gdp = tb_gdp[(tb_gdp["country"] == "USSR") & (tb_gdp["year"] == year)]["gdp_per_capita"].values[0]

            tb.loc[(tb["country"] == countries) & (tb["year"] == year), "gdp_per_capita"] = ussr_gdp
    # Retrieve the GDP per capita for Central African Republic for the year 2022
    car_gdp_2022 = tb_gdp[(tb_gdp["country"] == "Central African Republic") & (tb_gdp["year"] == 2022)][
        "gdp_per_capita"
    ].values[0]

    # Replace the rows for Central African Republic 2023 with the GDP per capita for 2022
    tb.loc[(tb["country"] == "Central African Republic") & (tb["year"] == 2023), "gdp_per_capita"] = car_gdp_2022

    # Retrieve the GDP per capita for Ethiopia for the year 2022
    ethiopia_gdp_2022 = tb_gdp[(tb_gdp["country"] == "Ethiopia") & (tb_gdp["year"] == 2022)]["gdp_per_capita"].values[0]

    # Replace the rows for Ethiopia 2023 with the GDP per capita for 2022
    tb.loc[(tb["country"] == "Ethiopia") & (tb["year"] == 2023), "gdp_per_capita"] = ethiopia_gdp_2022

    # Retrieve the GDP per capita for Syria for the year 2022
    syria_gdp_2022 = tb_gdp[(tb_gdp["country"] == "Syria") & (tb_gdp["year"] == 2022)]["gdp_per_capita"].values[0]

    # Replace the rows for Syria 2023 with the GDP per capita for 2022
    tb.loc[(tb["country"] == "Syria") & (tb["year"] == 2023), "gdp_per_capita"] = syria_gdp_2022

    # Retrieve the GDP per capita for Iran for the year 1870
    iran_gdp_1870 = tb_gdp[(tb_gdp["country"] == "Iran") & (tb_gdp["year"] == 1870)]["gdp_per_capita"].values[0]

    # Replace the rows for Iran 1871-1872 with the GDP per capita for 1870
    for year in [1871, 1872]:
        tb.loc[(tb["country"] == "Iran") & (tb["year"] == year), "gdp_per_capita"] = iran_gdp_1870

    # Retrieve the GDP per capita for Iran for the year 1913
    iran_gdp_1913 = tb_gdp[(tb_gdp["country"] == "Iran") & (tb_gdp["year"] == 1913)]["gdp_per_capita"].values[0]

    # Identify rows where country is 'Turkey, Armenians' and year is in [1915, 1916]
    turkey_armenians_years_to_replace = [1915, 1916]

    # Calculate the average GDP per capita for Turkey for the years 1913 and 1918
    turkey_avg_gdp_1913_1918 = tb_gdp[(tb_gdp["country"] == "Turkey") & (tb_gdp["year"].isin([1913, 1918]))][
        "gdp_per_capita"
    ].mean()

    # Replace the rows for Iran 1917-1919 with the GDP per capita for 1913
    for year in [1917, 1918, 1919]:
        tb.loc[(tb["country"] == "Iran") & (tb["year"] == year), "gdp_per_capita"] = iran_gdp_1913
    # Replace the rows for Turkey, Armenians 1915-1916 with the average GDP per capita for 1913 and 1918
    for year in turkey_armenians_years_to_replace:
        tb.loc[
            (tb["country"] == "Turkey, Armenians") & (tb["year"] == year), "gdp_per_capita"
        ] = turkey_avg_gdp_1913_1918

    # Identify rows where country is 'Vietnam' and year is in [1944, 1945]
    vietnam_years_to_replace = [1944, 1945]

    # Retrieve the GDP per capita for Vietnam for the year 1950
    vietnam_gdp_1950 = tb_gdp[(tb_gdp["country"] == "Vietnam") & (tb_gdp["year"] == 1950)]["gdp_per_capita"].values[0]

    # Replace the rows for Vietnam 1944-1945 with the GDP per capita for 1950
    for year in vietnam_years_to_replace:
        tb.loc[(tb["country"] == "Vietnam") & (tb["year"] == year), "gdp_per_capita"] = vietnam_gdp_1950

    return tb
