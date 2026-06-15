"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define regions to aggregate
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]

# Define fraction of allowed NaNs per year
FRAC_ALLOWED_NANS_PER_YEAR = 0.2

# Define columns with data
DATA_COLUMNS = [
    "last_decriminalization",
    "last_criminalization",
    "former_decriminalizations",
    "former_criminalizations",
]

# Define years
START_YEAR = 1760
END_YEAR = 2026  # Status reported through 2026 (release year); each country's latest status is carried forward. Most recent events are from 2025.


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow and regions datasets.
    ds_meadow = paths.load_dataset("criminalization_mignot")
    ds_regions = paths.load_dataset("regions")

    # Read table from meadow dataset.
    tb = ds_meadow.read("criminalization_mignot")

    #
    # Process data.
    #
    sanity_check_inputs(tb=tb)

    tb = paths.regions.harmonize_names(tb)
    tb = assign_unique_year_for_columns(tb=tb)
    tb = make_table_long(tb=tb)

    # Only use years between START_YEAR and END_YEAR
    tb = tb[(tb["year"] >= START_YEAR) & (tb["year"] <= END_YEAR)]

    tb = calculate_year_of_decriminalization(tb=tb)

    tb = add_country_counts_and_population_by_status(
        tb=tb, columns=["status"], ds_regions=ds_regions, regions=REGIONS, missing_data_on_columns=True
    )

    sanity_check_outputs(tb=tb)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    """Check assumptions about the raw Mignot table before any processing."""
    expected_columns = {
        "country",
        "last_decriminalization",
        "last_criminalization",
        "former_decriminalizations",
        "former_criminalizations",
    }
    assert set(tb.columns) == expected_columns, (
        f"Unexpected columns in the Mignot table ({set(tb.columns) ^ expected_columns}). The source schema may have changed."
    )
    assert not tb["country"].duplicated().any(), "Duplicate country rows in the Mignot table."
    # The source documents ~203 present-day countries; guard against a parsing regression that drops rows.
    assert tb["country"].nunique() >= 200, f"Only {tb['country'].nunique()} countries — coverage shrank unexpectedly."


def sanity_check_outputs(tb: Table) -> None:
    """Check assumptions about the long, aggregated table before saving.

    NOTE: update-specific facts (e.g. Saint Lucia becoming Legal in 2025) are verified at update
    time via the garden diff, not hard-coded here, so these checks stay valid across future releases.
    """
    # Status is a binary legality flag for country rows; region rows carry NaN status (only counts/population).
    statuses = set(tb["status"].dropna().unique())
    assert statuses <= {"Legal", "Illegal"}, f"Unexpected status values: {statuses}."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in the output."
    assert tb["year"].between(START_YEAR, END_YEAR).all(), (
        f"Years outside [{START_YEAR}, {END_YEAR}]: {sorted(set(tb.loc[~tb['year'].between(START_YEAR, END_YEAR), 'year']))}."
    )
    # Counts are non-negative. (Column names are still mixed-case here — format() lowercases them later.)
    count_cols = [c for c in tb.columns if c.endswith("_count")]
    assert (tb[count_cols].fillna(0) >= 0).all().all(), "Negative count found in a status-count column."
    # The share of the world population living where homosexual acts are legal must be a valid fraction.
    pop_cols = [c for c in tb.columns if c.endswith("_pop")]
    legal_pop_col = next(c for c in pop_cols if "legal" in c.lower() and "illegal" not in c.lower())
    world = tb[tb["country"] == "World"]
    world_total_pop = world[pop_cols].sum(axis=1)
    world_legal_share = world[legal_pop_col] / world_total_pop
    assert world_legal_share.between(0, 1).all(), (
        f"World 'legal' population share outside [0, 1]: {world_legal_share[~world_legal_share.between(0, 1)].tolist()}."
    )
    # Per the source, >75% of people live where homosexual acts are legal since 2020 — guard against a scale/aggregation error.
    latest_world_share = world_legal_share[world["year"] == END_YEAR]
    assert (latest_world_share > 0.5).all(), (
        f"World 'legal' population share in {END_YEAR} is implausibly low: {latest_world_share.tolist()}."
    )
    # No column should be entirely missing.
    fully_nan = list(tb.columns[tb.isna().all()])
    assert not fully_nan, f"Fully-NaN column(s): {fully_nan}."


def assign_unique_year_for_columns(tb: Table) -> Table:
    """
    Standardize format of columns to have a unique year in each one of them.
    """

    tb = tb.copy()

    # Make all columns string
    tb[DATA_COLUMNS] = tb[DATA_COLUMNS].astype("string")

    # When last_decriminalization is "never was illegal", replace value with START_YEAR
    tb.loc[tb["last_decriminalization"] == "never was illegal", "last_decriminalization"] = f"{START_YEAR}"

    # When last_decriminalization is "still illegal", replace value with END_YEAR + 1
    tb.loc[tb["last_decriminalization"] == "still illegal", "last_decriminalization"] = f"{END_YEAR + 1}"

    # Replace value in Germany with the latest year of decriminalization (West Germany, 1969)
    tb.loc[tb["country"] == "Germany", "last_decriminalization"] = "1969"

    # Invert contents of former_decriminalizations and former_criminalizations for Spain
    tb.loc[tb["country"] == "Spain", "former_decriminalizations"] = "1932; 1822"
    tb.loc[tb["country"] == "Spain", "former_criminalizations"] = "1928; 1438"

    # Make years int by selecting the text of the first 4 characters for last_criminalization and the last 4 characters for last_decriminalization
    tb["last_decriminalization"] = tb["last_decriminalization"].str[-4:]
    tb["last_criminalization"] = tb["last_criminalization"].str[:4]

    # When former_decriminalizations is "not illegal before 1873", replace with START_YEAR
    tb.loc[tb["former_decriminalizations"] == "not illegal before 1873", "former_decriminalizations"] = f"{START_YEAR}"

    # Create multiple columns for former_decriminalizations and former_criminalizations when there is a ";" in the value
    tb[["former_decriminalizations1", "former_decriminalizations2"]] = tb["former_decriminalizations"].str.split(
        ";", expand=True
    )
    tb[["former_criminalizations1", "former_criminalizations2"]] = tb["former_criminalizations"].str.split(
        ";", expand=True
    )

    # Drop former_decriminalizations and former_criminalizations columns
    tb = tb.drop(columns=["former_decriminalizations", "former_criminalizations"])

    # Make all columns except country int
    for col in [
        "last_decriminalization",
        "last_criminalization",
        "former_decriminalizations1",
        "former_decriminalizations2",
        "former_criminalizations1",
        "former_criminalizations2",
    ]:
        tb[col] = tb[col].astype("Int64")

    return tb


def make_table_long(tb: Table) -> Table:
    """
    Make the table long by using the different columns of criminalization and decriminalization.
    """

    # Add END_YEAR and START_YEAR as a column
    tb["end_year"] = END_YEAR

    # Create separate tables for decriminalization and criminalization
    tb_last_decriminalization = expand_table(
        tb=tb, start_col="last_decriminalization", end_col="end_year", status="Legal"
    )
    tb_before_last_decriminalization = expand_table(
        tb=tb, start_col="last_criminalization", end_col="last_decriminalization", status="Illegal"
    )
    tb_previous_decriminalization_1 = expand_table(
        tb=tb, start_col="former_decriminalizations1", end_col="last_criminalization", status="Legal"
    )
    tb_previous_criminalization_1 = expand_table(
        tb=tb, start_col="former_criminalizations1", end_col="former_decriminalizations1", status="Illegal"
    )
    tb_previous_decriminalization_2 = expand_table(
        tb=tb, start_col="former_decriminalizations2", end_col="former_criminalizations1", status="Legal"
    )
    tb_previous_criminalization_2 = expand_table(
        tb=tb, start_col="former_criminalizations2", end_col="former_decriminalizations2", status="Illegal"
    )

    # Concatenate all tables
    tb = pr.concat(
        [
            tb_last_decriminalization,
            tb_before_last_decriminalization,
            tb_previous_decriminalization_1,
            tb_previous_criminalization_1,
            tb_previous_decriminalization_2,
            tb_previous_criminalization_2,
        ],
        ignore_index=True,
        short_name=paths.short_name,
    )

    # Fill tb with data from START_YEAR for each country
    tb = fill_countries_to_start_year(tb=tb)

    # Fill empty status of the "Russian Empire" as "Legal" (See P8 of paper)
    tb.loc[
        (
            tb["country"].isin(
                [
                    "Russia",
                    "Estonia",
                    "Latvia",
                    "Lithuania",
                    "Moldova",
                    "Armenia",
                    "Azerbaijan",
                    "Georgia",
                    "Kazakhstan",
                    "Kyrgyzstan",
                    "Tajikistan",
                    "Turkmenistan",
                    "Uzbekistan",
                ]
            )
        )
        & (tb["status"].isna()),
        "status",
    ] = "Legal"

    # Keep only necesary columns
    tb = tb[["country", "year", "status"]]

    return tb


def expand_table(tb: Table, start_col: str, end_col: str, status: str) -> Table:
    """
    Expand the table to have a row for each year between last_decriminalization and last_criminalization.
    """

    tb_ = tb.copy()
    tb_ = tb_[["country", start_col, end_col]]

    # Remove rows with missing values in start_col or end_col
    tb_ = tb_.dropna(subset=[start_col, end_col])

    # Create a long table that has a year column defined between last_decriminalization and last_criminalization for each country
    tbs = []
    for country in tb_["country"].unique():
        country_data = tb_[tb_["country"] == country].copy()

        # Create a table with the years between last_decriminalization and last_criminalization
        if end_col == "end_year":
            years = list(range(country_data[start_col].iloc[0], country_data[end_col].iloc[0] + 1))
        else:
            years = list(range(country_data[start_col].iloc[0], country_data[end_col].iloc[0]))
        country_data = country_data.reindex(country_data.index.repeat(len(years)))
        country_data["year"] = years

        tbs.append(country_data)

    tb_expanded = pr.concat(tbs, ignore_index=True)

    tb_expanded["status"] = status

    # Copy metadata to status
    tb_expanded["status"] = tb_expanded["status"].copy_metadata(tb["country"])

    return tb_expanded


def fill_countries_to_start_year(tb: Table) -> Table:
    """
    Fill data for each country from START_YEAR to the first year with data.
    """

    tb_ = tb.copy()

    # Calculate the minimum year for each country
    tb_ = tb_.groupby("country")["year"].min().reset_index()

    # Make year int
    tb_["year"] = tb_["year"].astype(int)

    # Create start_year column
    tb_["start_year"] = START_YEAR

    tb_ = expand_table(tb=tb_, start_col="start_year", end_col="year", status=pd.NA)  # ty: ignore

    # Concatenate the two tables
    tb = pr.concat([tb, tb_], ignore_index=True)

    return tb


def add_country_counts_and_population_by_status(
    tb: Table, columns: list[str], ds_regions: Dataset, regions: list[str], missing_data_on_columns: bool = False
) -> Table:
    """
    Add country counts and population by status for the columns in the list
    """

    tb_regions = tb.copy()

    tb_regions = paths.regions.add_population(tb=tb_regions, warn_on_missing_countries=False)

    # Define empty dictionaries for each of the columns
    columns_count_dict = {columns[i]: [] for i in range(len(columns))}
    columns_pop_dict = {columns[i]: [] for i in range(len(columns))}
    for col in columns:
        if missing_data_on_columns:
            # Fill nan values with "missing"
            tb_regions[col] = tb_regions[col].fillna("missing")
        # Get the unique values in the column
        status_list = list(tb_regions[col].unique())
        for status in status_list:
            # Calculate count and population for each status in the column
            tb_regions[f"{col}_{status}_count"] = tb_regions[col].apply(lambda x: 1 if x == status else 0)
            tb_regions[f"{col}_{status}_pop"] = tb_regions[f"{col}_{status}_count"] * tb_regions["population"]

            # Add the new columns to the list
            columns_count_dict[col].append(f"{col}_{status}_count")
            columns_pop_dict[col].append(f"{col}_{status}_pop")

    # Create a new list with all the count columns and population columns
    columns_count = [item for sublist in columns_count_dict.values() for item in sublist]
    columns_pop = [item for sublist in columns_pop_dict.values() for item in sublist]

    aggregations = dict.fromkeys(
        columns_count + columns_pop + ["population"],
        "sum",
    )

    # NOTE: This uses the deprecated geo.add_regions_to_table rather than paths.regions.add_aggregates
    # because the two produce different regional population aggregates (they resolve region membership
    # differently), which would shift the world/region population-share indicators across all years —
    # unrelated to this data update. Modernize in a separate, validated PR.
    tb_regions = geo.add_regions_to_table(
        tb=tb_regions,
        ds_regions=ds_regions,
        regions=regions,
        aggregations=aggregations,
        frac_allowed_nans_per_year=FRAC_ALLOWED_NANS_PER_YEAR,
    )

    # Remove population column
    tb_regions = tb_regions.drop(columns=["population"])

    # Add population again
    tb_regions = paths.regions.add_population(tb=tb_regions, warn_on_missing_countries=False)

    # Calculate the missing population for each region
    for col in columns:
        # Calculate the missing population for each column, by subtracting the population of the countries with data from the total population
        tb_regions[f"{col}_missing_pop_other_countries"] = tb_regions["population"] - tb_regions[
            columns_pop_dict[col]
        ].sum(axis=1)
        if missing_data_on_columns:
            tb_regions[f"{col}_missing_pop"] = (
                tb_regions[f"{col}_missing_pop"] + tb_regions[f"{col}_missing_pop_other_countries"]
            )

        else:
            # Rename column
            tb_regions = tb_regions.rename(columns={f"{col}_missing_pop_other_countries": f"{col}_missing_pop"})

            # Append this missing population column to the list of population columns
            columns_pop.append(f"{col}_missing_pop")

    # Keep only the regions in the country column
    tb_regions = tb_regions[tb_regions["country"].isin(REGIONS)].copy().reset_index(drop=True)

    # Keep only the columns I need
    tb_regions = tb_regions[["country", "year"] + columns_count + columns_pop]

    # Merge the two tables
    tb = pr.merge(tb, tb_regions, on=["country", "year"], how="outer")

    return tb


def calculate_year_of_decriminalization(tb: Table) -> Table:
    """
    Calculate the last year of decriminalization for each country.
    """

    tb_last_decriminalization = tb.copy()

    # Sort columns by country and year
    tb_last_decriminalization = tb_last_decriminalization.sort_values(by=["country", "year"])

    # Create the variable "change", that defines the most recent change to "Legal" per year
    # NOTE: I added the fill_value argument to have a True in the first row when the first row is "Legal" or "Illegal" and it does not change during the entire dataset
    tb_last_decriminalization["change"] = tb_last_decriminalization.groupby("country")["status"].transform(
        lambda x: x.ne(x.shift(fill_value="Not legal nor illegal"))
    )

    # Keep only when change is True and status is "Legal"
    tb_last_decriminalization = tb_last_decriminalization[
        (tb_last_decriminalization["change"]) & (tb_last_decriminalization["status"] == "Legal")
    ].reset_index(drop=True)

    # Check duplicates for country and status and keep the last one
    tb_last_decriminalization = tb_last_decriminalization.drop_duplicates(subset=["country", "status"], keep="last")

    # Rename year to last_decriminalization_year
    tb_last_decriminalization = tb_last_decriminalization.rename(columns={"year": "last_decriminalization_year"})

    # Add year as END_YEAR
    tb_last_decriminalization["year"] = END_YEAR

    # Drop status and change columns
    tb_last_decriminalization = tb_last_decriminalization.drop(columns=["status", "change"])

    # Merge the two tables
    tb = pr.merge(tb, tb_last_decriminalization, on=["country", "year"], how="left")

    # When last_decriminalization_year is missing for END_YEAR, fill with END_YEAR + 100
    tb.loc[
        (tb["last_decriminalization_year"].isna()) & (tb["year"] == END_YEAR),
        "last_decriminalization_year",
    ] = END_YEAR + 100

    # Copy metadata to last_decriminalization_year
    tb["last_decriminalization_year"] = tb["last_decriminalization_year"].copy_metadata(tb["country"])

    return tb
