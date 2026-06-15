"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Last year the legal-status / count / population series extends to. This tracks the date the
# source data is published for, not the access date: the Pew fact sheet is dated June 2025, so
# the series stops in 2025. Bump this when Pew publishes a more recent edition of the table.
LATEST_YEAR = 2025

# Define regions to aggregate (continents + World + World Bank income groups)
REGIONS = [
    "Europe",
    "Asia",
    "North America",
    "South America",
    "Africa",
    "Oceania",
    "World",
    "High-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "Low-income countries",
]

# Define fraction of allowed NaNs per year
FRAC_ALLOWED_NANS_PER_YEAR = 0.2


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("same_sex_marriage")

    # Read table from meadow dataset.
    tb = ds_meadow.read("same_sex_marriage")

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)

    tb = explode_country_years(tb=tb)

    tb = add_country_counts_and_population_by_status(
        tb=tb,
        columns=["legal_status"],
        regions=REGIONS,
        missing_data_on_columns=False,
    )

    # Redefine legal_status_not_legal_pop as legal_status_not_legal_pop + legal_status_missing_pop
    tb["legal_status_Not legal_pop"] = tb["legal_status_Not legal_pop"] + tb["legal_status_missing_pop"]

    # Drop legal_status_missing_pop and legal_status_not_legal_count
    tb = tb.drop(columns=["legal_status_missing_pop", "legal_status_Not legal_count"])

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def explode_country_years(tb: Table) -> Table:
    """
    Create a row for each country-year from the minimum year to LATEST_YEAR
    """

    # Add category to define legal status
    tb["legal_status"] = "Legal"
    tb["legal_status"] = tb["legal_status"].copy_metadata(tb["country"])

    min_year = tb["year"].min() - 1

    # Define the list of countries
    countries = tb["country"].unique().tolist()

    # Create a table of years and another of countries
    tb_years = Table({"year": range(min_year, LATEST_YEAR + 1)})
    tb_countries = Table({"country": countries})

    # Create a new table with the cartesian product of tb_years and tb_countries
    tb_countries_years = pr.merge(tb_years, tb_countries, how="cross")

    # Merge this table with tb
    tb = pr.merge(tb_countries_years, tb, how="left", on=["country", "year"], short_name=paths.short_name)

    # Fill data forward after the first incidence of "Legal" in each country
    tb["legal_status"] = tb.groupby("country")["legal_status"].ffill()

    # Add "Not legal" for missing legal status
    tb.loc[tb["legal_status"].isnull(), "legal_status"] = "Not legal"

    return tb


def add_country_counts_and_population_by_status(
    tb: Table,
    columns: list[str],
    regions: list[str],
    missing_data_on_columns: bool = False,
) -> Table:
    """
    Add country counts and population by status of the columns in the list
    """

    tb_regions = tb.copy()

    tb_regions = paths.regions.add_population(tb_regions, warn_on_missing_countries=False)

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
        tb_regions,
        regions=regions,
        aggregations=aggregations,
        frac_allowed_nans_per_year=FRAC_ALLOWED_NANS_PER_YEAR,
    )

    # add_aggregates only creates a region row when at least one member country is present in
    # the (legalized-only) data. Regions with no legalized member — e.g. low-income countries,
    # where no country has yet legalized same-sex marriage — are therefore missing entirely.
    # Re-introduce every requested region for every year with zero counts and zero status
    # populations. The total population added below then makes their whole population count as
    # "not legal" (0 legal countries, 0 legal population).
    # NOTE: As of this update, "Low-income countries" is the only region with zero legalized
    # members, so this block effectively just fills in that aggregate. Once a low-income
    # country legalizes same-sex marriage, add_aggregates will produce the row on its own and
    # this injection becomes a no-op for it — at that point re-audit whether this block is
    # still needed (it is harmless either way, since it only adds genuinely missing regions).
    all_region_years = pr.merge(
        Table({"country": regions}),
        Table({"year": sorted(tb_regions["year"].unique())}),
        how="cross",
    )
    present_region_years = tb_regions.loc[tb_regions["country"].isin(regions), ["country", "year"]]
    missing_region_years = all_region_years.merge(
        present_region_years, on=["country", "year"], how="left", indicator=True
    )
    missing_region_years = missing_region_years[missing_region_years["_merge"] == "left_only"].drop(columns="_merge")
    if len(missing_region_years) > 0:
        for count_or_pop_col in columns_count + columns_pop:
            missing_region_years[count_or_pop_col] = 0
        tb_regions = pr.concat([tb_regions, missing_region_years], ignore_index=True)

    # Remove population column
    tb_regions = tb_regions.drop(columns=["population"])

    # Add population again
    tb_regions = paths.regions.add_population(tb_regions, warn_on_missing_countries=False)

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
