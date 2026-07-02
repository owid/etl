"""Load the income_groups garden dataset and build country-count and population aggregates by income status."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Define regions to aggregate
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]

# Define fraction of allowed NaNs per year
FRAC_ALLOWED_NANS_PER_YEAR = 0.2

# (region, year) pairs where the "missing population" residual (region total minus the summed
# classified populations) is knowingly slightly negative — the summed classified population
# marginally exceeds the region total, due to population-estimate mismatches. These are clamped
# to 0 (missing population can't be negative); any *new* case fails for review.
EXPECTED_NEGATIVE_MISSING_POP = {("Europe", 1990)}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_garden = paths.load_dataset("income_groups")
    tb = ds_garden.read("income_groups")

    #
    # Process data.
    #

    tb = add_country_counts_and_population_by_status(
        tb=tb, columns=["classification"], regions=REGIONS, missing_data_on_columns=False
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    # Run sanity checks on the output.
    sanity_check_outputs(tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[tb],
        default_metadata=ds_garden.metadata,
    )
    ds_garden.save()


def sanity_check_outputs(tb: Table) -> None:
    # No column should be entirely empty.
    assert tb.columns[tb.isna().all()].empty, (
        f"Aggregations output has a fully-NaN column: {list(tb.columns[tb.isna().all()])}"
    )

    # Country counts are non-negative integers.
    count_cols = [c for c in tb.columns if c.endswith("_count")]
    assert count_cols, "No '_count' columns found in the aggregations output."
    assert (tb[count_cols] >= 0).all().all(), "Negative country count found in the aggregations output."

    # All population columns are non-negative. The `classification_missing_pop` residual (region total
    # minus the summed classified populations) can compute slightly negative, but it is clamped to 0
    # upstream (see EXPECTED_NEGATIVE_MISSING_POP), so it is included in this check.
    pop_cols = [c for c in tb.columns if c.endswith("_pop")]
    assert pop_cols, "No '_pop' columns found in the aggregations output."
    assert (tb[pop_cols] >= 0).all().all(), "Negative population found in the aggregations output."


def add_country_counts_and_population_by_status(
    tb: Table, columns: list[str], regions: list[str], missing_data_on_columns: bool = False
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

    tb_regions = paths.regions.add_aggregates(
        tb=tb_regions,
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

    # The "missing population" residual can be slightly negative when the summed classified
    # populations exceed the region total. Clamp such residuals to 0 (missing population can't be
    # negative), but fail on any *new* (region, year) case so it's reviewed rather than silently zeroed.
    for c in [col for col in tb_regions.columns if col.endswith("_missing_pop")]:
        neg_mask = tb_regions[c] < 0
        unexpected = {
            (str(country), int(year))
            for country, year in zip(tb_regions.loc[neg_mask, "country"], tb_regions.loc[neg_mask, "year"])
        } - EXPECTED_NEGATIVE_MISSING_POP
        assert not unexpected, f"New negative {c} case(s) — review before clamping to 0: {sorted(unexpected)}"
        tb_regions.loc[neg_mask, c] = 0

    return tb_regions
