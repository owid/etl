"""Load a meadow dataset and create a garden dataset."""
# NOTE: We have manually modified the value for Ethiopia, because, although it is included in the file, it has officially a temporary status of unclassification.
# NOTE: Check this back when it's fixed in the source file.

from typing import List

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Define regions to aggregate
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]

# Define fraction of allowed NaNs per year
FRAC_ALLOWED_NANS_PER_YEAR = 0.2


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_garden = paths.load_dataset("income_groups")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")
    tb = ds_garden.read("income_groups")

    #
    # Process data.
    #

    tb = add_country_counts_and_population_by_status(
        tb=tb,
        columns=["classification"],
        ds_regions=ds_regions,
        ds_population=ds_population,
        regions=REGIONS,
        missing_data_on_columns=False,
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb],
        default_metadata=ds_garden.metadata,
    )
    ds_garden.save()


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

    return tb_regions
