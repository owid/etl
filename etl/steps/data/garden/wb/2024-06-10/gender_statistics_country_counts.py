"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table, VariableMeta, VariablePresentationMeta

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define regions to aggregate
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]

# Define fraction of allowed NaNs per year
FRAC_ALLOWED_NANS_PER_YEAR = 0.2


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load the garden gender statistics datasets.
    ds_garden = paths.load_dataset("gender_statistics")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    tb = ds_garden["gender_statistics"]
    tb = tb.reset_index()

    #
    # Process data.
    #

    indicators_for_sums = []
    for col in tb:
        if "(1=yes; 0=no)" in tb[col].metadata.title:
            indicators_for_sums.append(col)

    # Select only the columns of interest
    tb = tb[indicators_for_sums + ["country", "year"]]
    # Find the minimum year
    min_year = tb["year"].min()

    tb = add_country_counts_and_population_by_status(tb, ds_regions, ds_population)

    # Remove columns that are not needed and are in the original dataset
    columns_to_keep = [col for col in tb.columns if col not in indicators_for_sums]
    tb = tb[columns_to_keep]
    # Filter the Table to include rows from the minimum year in the original dataset onwards
    tb = tb[tb["year"] >= min_year]

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    # Save changes in the new garden dataset.
    ds_garden.save()


def add_country_counts_and_population_by_status(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> Table:
    """
    Add country counts and population by status for the columns in the list
    """

    tb_regions = tb.copy()

    tb_regions = geo.add_population_to_table(
        tb=tb_regions, ds_population=ds_population, warn_on_missing_countries=False
    )
    columns = [col for col in tb.columns if "(1=yes; 0=no)" in tb[col].metadata.title]

    # Define empty dictionaries for each of the columns
    columns_count_dict = {columns[i]: [] for i in range(len(columns))}
    columns_pop_dict = {columns[i]: [] for i in range(len(columns))}
    for col in columns:
        column_title = tb[col].metadata.title
        description_from_producer = tb[col].metadata.description_from_producer
        tb_regions[col] = tb_regions[col].astype(str)
        # Define the mapping dictionary
        value_map = {"nan": "missing", "0.0": "no", "1.0": "yes"}
        tb_regions[col] = tb_regions[col].map(value_map)

        # Get the unique values in the column
        status_list = list(tb_regions[col].unique())

        for status in status_list:
            # Calculate count and population for each status in the column
            tb_regions[f"{col}_{status}_count"] = tb_regions[col].apply(lambda x: 1 if x == status else 0)
            tb_regions[f"{col}_{status}_pop"] = tb_regions[f"{col}_{status}_count"] * tb_regions["population"]

            # Add metadata for the new columns
            tb_regions[f"{col}_{status}_count"].metadata = add_metadata_for_aggregated_columns(
                column_title=column_title,
                description_from_producer=description_from_producer,
                status=status,
                count_or_pop="count",
                origins=tb_regions[f"{col}_{status}_count"].m.origins,
            )
            tb_regions[f"{col}_{status}_pop"].metadata = add_metadata_for_aggregated_columns(
                column_title=column_title,
                description_from_producer=description_from_producer,
                status=status,
                count_or_pop="pop",
                origins=tb_regions[f"{col}_{status}_pop"].m.origins,
            )
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
        regions=REGIONS,
        aggregations=aggregations,
        frac_allowed_nans_per_year=FRAC_ALLOWED_NANS_PER_YEAR,
    )

    # Remove population column
    tb_regions = tb_regions.drop(columns=["population"])

    # Add population again
    tb_regions = geo.add_population_to_table(
        tb=tb_regions, ds_population=ds_population, warn_on_missing_countries=False
    )

    # Calculate the missing population for each region
    for col in columns:
        column_title = tb[col].metadata.title
        description_from_producer = tb[col].metadata.description_from_producer
        # Calculate the missing population for each column, by subtracting the population of the countries with data from the total population
        tb_regions[f"{col}_missing_pop_other_countries"] = tb_regions["population"] - tb_regions[
            columns_pop_dict[col]
        ].sum(axis=1)
        tb_regions[f"{col}_missing_pop"] = (
            tb_regions[f"{col}_missing_pop"] + tb_regions[f"{col}_missing_pop_other_countries"]
        )

        tb_regions[f"{col}_missing_pop"].metadata = add_metadata_for_aggregated_columns(
            column_title=column_title,
            description_from_producer=description_from_producer,
            status="missing",
            count_or_pop="pop",
            origins=tb_regions[f"{col}_missing_pop"].m.origins,
        )

    # Keep only the regions in the country column
    tb_regions = tb_regions[tb_regions["country"].isin(REGIONS)].copy().reset_index(drop=True)

    # Keep only the columns I need
    tb_regions = tb_regions[["country", "year"] + columns_count + columns_pop]

    return tb_regions


def add_metadata_for_aggregated_columns(
    column_title: str, description_from_producer: str, status: str, count_or_pop: str, origins
) -> VariableMeta:
    # Remove the unwanted part "(1=yes; 0=no)" from the column_title
    clean_column_title = column_title.replace(" (1=yes; 0=no)", "")

    if count_or_pop == "count":
        meta = VariableMeta(
            title=f"{column_title} - {status.capitalize()} (Count)",
            description_short=f"Number of countries with the status '{status}' for \"{clean_column_title}\".",
            description_from_producer=description_from_producer,
            unit="countries",
            short_unit="",
            sort=[],
            origins=origins,
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": 0,
            "tolerance": 0,
        }
        meta.presentation = VariablePresentationMeta(title_public=meta.title)
    elif count_or_pop == "pop":
        meta = VariableMeta(
            title=f"{column_title} - {status.capitalize()} (Population)",
            description_short=f"Population of countries with the status '{status}' for \"{clean_column_title}\".",
            description_from_producer=description_from_producer,
            unit="persons",
            short_unit="",
            sort=[],
            origins=origins,
        )
        meta.display = {
            "name": meta.title,
            "numDecimalPlaces": 0,
            "tolerance": 0,
        }
        meta.presentation = VariablePresentationMeta(title_public=meta.title)

    else:
        paths.log.error(f"count_or_pop must be either 'count' or 'pop'. Got {count_or_pop}.")

    return meta  # type: ignore
