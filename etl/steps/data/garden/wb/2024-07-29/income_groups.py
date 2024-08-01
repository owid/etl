"""Load a meadow dataset and create a garden dataset."""

from typing import List

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

EXPECTED_MISSING_COUNTRIES_IN_LATEST_RELEASE = {
    "Czechoslovakia",
    "Mayotte",
    "Netherlands Antilles",
    "Serbia and Montenegro",
    "USSR",
    "Venezuela",
    "Yugoslavia",
}

# Define regions to aggregate
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]

# Define fraction of allowed NaNs per year
FRAC_ALLOWED_NANS_PER_YEAR = 0.2


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("income_groups")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")
    tb = ds_meadow["income_groups"].reset_index()

    #
    # Process data.
    #
    # Run sanity checks on input data.
    run_sanity_checks_on_inputs(tb=tb)

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Harmonize income group labels.
    tb = harmonize_income_group_labels(tb)

    # Drop unnecessary columns.
    tb = tb.drop(columns=["country_code"], errors="raise")

    # Create an additional table for the classification of the latest year available.
    tb_latest = tb.reset_index().drop_duplicates(subset=["country"], keep="last")

    # Rename new table.
    tb_latest.metadata.short_name = "income_groups_latest"

    # Check that countries without classification for the latest year are as expected.
    missing_countries = set(tb_latest.loc[tb_latest["year"] != tb_latest["year"].max(), "country"])
    assert (
        missing_countries == EXPECTED_MISSING_COUNTRIES_IN_LATEST_RELEASE
    ), f"Unexpected missing countries in latest release. All missing countries: {missing_countries}"

    # Extract data only for latest release (and remove column year).
    tb_latest = tb_latest[tb_latest["year"] == tb_latest["year"].max()].drop(columns=["year"])

    tb = add_country_counts_and_population_by_status(
        tb=tb,
        columns=["classification"],
        ds_regions=ds_regions,
        ds_population=ds_population,
        regions=REGIONS,
        missing_data_on_columns=False,
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year"])

    # Set an appropriate index and sort conveniently.
    tb_latest = tb_latest.format(["country"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_latest], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def run_sanity_checks_on_inputs(tb: Table) -> None:
    # Check that raw labels are as expected.
    assert (labels := set(tb["classification"])) == {
        # No available classification for country-year (maybe country didn't exist yet/anymore).
        "..",
        # High income.
        "H",
        # Low income.
        "L",
        # Lower middle income.
        "LM",
        # Exceptional case of lower middle income.
        "LM*",
        # Upper middle income.
        "UM",
        # Another label for when no classification is available.
        np.nan,
    }, f"Unknown income group label! Check {labels}"


def harmonize_income_group_labels(tb: Table) -> Table:
    # Check if unusual LM* label is still used for Yemen in 1987 and 1988.
    msk = tb["classification"] == "LM*"
    lm_special = set(tb[msk]["country_code"].astype(str) + tb[msk]["year"].astype(str))
    assert lm_special == {"YEM1987", "YEM1988"}, f"Unexpected entries with classification 'LM*': {tb[msk]}"

    # Rename labels.
    classification_mapping = {
        "..": np.nan,
        "L": "Low-income countries",
        "H": "High-income countries",
        "UM": "Upper-middle-income countries",
        "LM": "Lower-middle-income countries",
        "LM*": "Lower-middle-income countries",
    }
    tb["classification"] = tb["classification"].map(classification_mapping)

    # Drop years with no country classification
    tb = tb.dropna(subset="classification").reset_index(drop=True)

    return tb


def add_country_counts_and_population_by_status(
    tb: Table,
    columns: List[str],
    ds_regions: Dataset,
    ds_population: Dataset,
    regions: List[str],
    missing_data_on_columns: bool = False,
) -> Table:
    """
    Add country counts and population by status for the columns in the list
    """

    tb_regions = tb.copy()

    tb_regions = geo.add_population_to_table(
        tb=tb_regions, ds_population=ds_population, warn_on_missing_countries=False
    )

    tb_regions["population"].m.presentation.attribution = None

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
    tb_regions = geo.add_population_to_table(
        tb=tb_regions, ds_population=ds_population, warn_on_missing_countries=False
    )

    tb_regions["population"].m.presentation.attribution = None

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
