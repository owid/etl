"""Garden step that combines Ember's European Electricity Review (EER) and Ember's Yearly Electricity Data (YED).

The YED dataset contains data for all countries in EER. However, YED starts in 2000, while EER starts in 1990.

Therefore, to gather as much data as possible, we combine both datasets.

NOTE: This step used to combine Ember's Global Electricity Review and the EER, but now we have replaced the former by
the YED. However, there may be instances in the code where "global" refers to the YED.

"""

import pandas as pd
from owid import catalog
from owid.catalog import Dataset, Table
from owid.datautils import dataframes

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define aggregates, following their Ember-Electricity-Data-Methodology document:
# https://ember-climate.org/app/uploads/2022/03/GER22-Methodology.pdf
# The European review also has its own methodology document:
# https://ember-climate.org/app/uploads/2022/02/EER-Methodology.pdf
# but it does not explicitly define aggregates. We assume they are consistent with each other.
# This will be also checked, along with other sanity checks, in a separate analysis.
AGGREGATES = {
    "coal__twh": [
        "hard_coal__twh",
        "lignite__twh",
    ],
    "wind_and_solar__twh": ["wind__twh", "solar__twh"],
    "hydro__bioenergy_and_other_renewables__twh": [
        "hydro__twh",
        "bioenergy__twh",
        "other_renewables__twh",
    ],
    "renewables__twh": [
        "wind_and_solar__twh",
        "hydro__bioenergy_and_other_renewables__twh",
    ],
    "clean__twh": [
        "renewables__twh",
        "nuclear__twh",
    ],
    "gas_and_other_fossil__twh": [
        "gas__twh",
        "other_fossil__twh",
    ],
    "fossil__twh": ["gas_and_other_fossil__twh", "coal__twh"],
    "total_generation__twh": [
        "clean__twh",
        "fossil__twh",
    ],
}


def combine_yearly_electricity_data(ds_global: Dataset) -> Table:
    """Combine all tables in Ember's Yearly Electricity Data into one table.

    Parameters
    ----------
    ds_global : Dataset
        Yearly Electricity dataset (containing tables for capacity, electricity demand, generation, imports and
        emissions).

    Returns
    -------
    combined_global : Table
        Combined table containing all data in the Yearly Electricity dataset.

    """
    category_renaming = {
        "capacity": "Capacity - ",
        "electricity_demand": "",
        "electricity_generation": "Generation - ",
        "electricity_imports": "",
        "power_sector_emissions": "Emissions - ",
    }
    error = "Tables in yearly electricity dataset have changed"
    assert set(category_renaming) == set(ds_global.table_names), error
    index_columns = ["country", "year"]
    tables = []
    for category in category_renaming:
        table = ds_global[category].copy()
        table = table.rename(
            columns={
                column: catalog.utils.underscore(category_renaming[category] + column)
                for column in table.columns
                if column not in index_columns
            }
        )
        table = table.reset_index()
        tables.append(table)

    # Merge all tables into one, with an appropriate short name.
    combined_global = dataframes.multi_merge(dfs=tables, on=index_columns, how="outer")
    combined_global.metadata.short_name = "yearly_electricity"

    # Rename certain columns for consistency.
    combined_global = combined_global.rename(
        columns={
            "net_imports__twh": "imports__total_net_imports__twh",
            "demand__twh": "demand__total_demand__twh",
            "demand_per_capita__kwh": "demand__total_demand_per_capita__kwh",
        },
        errors="raise",
    )

    # Sanity check.
    error = "Total generation column in emissions and generation tables are not identical."
    assert all(
        combined_global["emissions__total_generation__twh"] == combined_global["generation__total_generation__twh"]
    ), error

    # Remove unnecessary columns and any possible rows with no data.
    combined_global = combined_global.drop(columns=["population", "emissions__total_generation__twh"]).dropna(how="all")

    # Set a convenient index and sort rows and columns conveniently.
    combined_global = (
        combined_global.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)
    )

    return combined_global


def combine_european_electricity_review_data(ds_european: Dataset) -> Table:
    """Combine tables in Ember's European Electricity Review dataset into one table.

    The tables to be combined are 'country_overview', 'generation', and 'emissions'. The remaining table on net flows
    has a different structure and cannot be combined with the others, so it will remain as a separate table.

    Parameters
    ----------
    ds_european : Dataset
        European Electricity Review dataset.

    Returns
    -------
    combined_european : Table
        Combined table containing all data in the European Electricity Review dataset (except net flows).

    """
    index_columns = ["country", "year"]
    # Extract the necessary tables from the dataset.
    country_overview = ds_european["country_overview"].copy()
    generation = ds_european["generation"].copy()
    emissions = ds_european["emissions"].copy()

    # Create aggregates (defined in AGGREGATES) that are in yearly electricity but not in the european review.
    for aggregate in AGGREGATES:
        generation[aggregate] = pd.DataFrame(generation)[AGGREGATES[aggregate]].sum(axis=1)

    # Create a column for each of those new aggregates, giving percentage share of total generation.
    for aggregate in AGGREGATES:
        column = aggregate.replace("__twh", "__pct")
        generation[column] = pd.DataFrame(generation)[aggregate] / generation["total_generation__twh"] * 100

    # Check that total generation adds up to 100%.
    error = "Total generation does not add up to 100%."
    assert set(generation["total_generation__pct"]) == {100}, error

    # Check that the constructed "total generation" column agrees with the one given in table "country_overview".
    columns = ["country", "year", "total_generation__twh"]
    check = pd.merge(
        ds_european["country_overview"].reset_index()[columns],
        generation.reset_index()[columns],
        on=index_columns,
    )
    # Assert that the percentage change is smaller than 1%
    error = "Total generation does not agree with the on in country_overview."
    assert all(
        (abs(check["total_generation__twh_x"] - check["total_generation__twh_y"]) / check["total_generation__twh_x"])
        < 0.01
    ), error

    # Remove unnecessary columns.
    generation = generation.drop(columns=["total_generation__pct", "total_generation__twh"])

    # Rename all column names to start with the category, before combining all categories.
    generation = generation.rename(columns={column: "generation__" + column for column in generation.columns})
    emissions = emissions.rename(columns={column: "emissions__" + column for column in emissions.columns})
    country_overview = country_overview.rename(
        columns={
            "total_generation__twh": "generation__total_generation__twh",
            "demand__twh": "demand__total_demand__twh",
            "demand_per_capita__kwh": "demand__total_demand_per_capita__kwh",
            "net_imports__twh": "imports__total_net_imports__twh",
        },
        errors="raise",
    )

    # Combine tables into one dataframe.
    combined_european = dataframes.multi_merge(
        [
            country_overview.reset_index(),
            emissions.reset_index(),
            generation.reset_index(),
        ],
        on=index_columns,
        how="outer",
    )

    # Assign an appropriate short name to table.
    combined_european.metadata.short_name = "european_electricity_review"

    # If any column was repeated in the merge, it will have a "_x" at the end of the name.
    # Check that no other columns were repeated.
    error = "There are repeated columns in combined dataframe."
    assert len([column for column in combined_european.columns if column.endswith("_x")]) == 0, error

    # Remove any possible rows with no data.
    combined_european = combined_european.dropna(how="all")

    # Set an appropriate index and sort rows and columns conveniently.
    combined_european = (
        combined_european.set_index(index_columns, verify_integrity=True).sort_index().sort_index(axis=1)
    )

    return combined_european


def combine_yearly_electricity_data_and_european_electricity_review(
    combined_global: Table, combined_european: Table
) -> Table:
    """Combine the combined table of the Yearly Electricity Data with the combined table of the European Electricity
    Review.

    Parameters
    ----------
    combined_global : Table
        Table that combines all tables of the Yearly Electricity Data.
    combined_european : Table
        Table that combines all tables of the European Electricity Review (except net flows).

    Returns
    -------
    combined : Table
        Combined data.

    """
    # Concatenate variables one by one, so that, if one of the two sources does not have data, we can take the
    # source that is complete.
    # When both sources are complete (for european countries), prioritise the european review (since it has more data,
    # and it possibly is more up-to-date than the yearly electricity data).
    combined_global = combined_global.reset_index()
    combined_european = combined_european.reset_index()

    index_columns = ["country", "year"]
    data_columns = sorted(
        [col for col in (set(combined_global.columns) | set(combined_european.columns)) if col not in index_columns]
    )
    # We should not concatenate bp and shift data directly, since there are nans in different places.
    # Instead, go column by column, concatenate, remove nans, and then keep the BP version on duplicated rows.
    combined = pd.DataFrame({column: [] for column in index_columns})
    for variable in data_columns:
        _global_data = pd.DataFrame()
        _european_data = pd.DataFrame()
        if variable in combined_global.columns:
            _global_data = combined_global[index_columns + [variable]].dropna(subset=variable)
        if variable in combined_european.columns:
            _european_data = combined_european[index_columns + [variable]].dropna(subset=variable)
        _combined = pd.concat([_global_data, _european_data], ignore_index=True)
        # On rows where both datasets overlap, give priority to european review data.
        _combined = _combined.drop_duplicates(subset=index_columns, keep="last")
        # Combine data for different variables.
        combined = pd.merge(combined, _combined, on=index_columns, how="outer")
    error = "There are repeated columns when combining yearly electricity data and european electricity review tables."
    assert len([column for column in combined.columns if column.endswith("_x")]) == 0, error

    # Create a table (with no metadata) and sort data appropriately.
    combined = Table(combined, short_name="combined_electricity").set_index(index_columns).sort_index()

    return combined


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read yearly electricity data and european electricity review datasets from garden.
    ds_global: Dataset = paths.load_dependency("yearly_electricity")
    ds_european: Dataset = paths.load_dependency("european_electricity_review")

    #
    # Process data.
    #
    # Combine all tables of the yearly electricity data into one.
    combined_global = combine_yearly_electricity_data(ds_global=ds_global)

    # Combine all tables of the european electricity review into one.
    combined_european = combine_european_electricity_review_data(ds_european=ds_european)

    # Combine yearly electricity and european reviews.
    combined = combine_yearly_electricity_data_and_european_electricity_review(
        combined_global=combined_global, combined_european=combined_european
    )

    # Create an additional table with the electricity net flows (only available in european review).
    net_flows = ds_european["net_flows"].copy()

    #
    # Save outputs.
    #
    # Create new garden dataset.
    ds_garden = create_dataset(
        dest_dir=dest_dir,
        tables=[combined_global, combined_european, combined, net_flows],
        default_metadata=ds_global.metadata,
    )
    ds_garden.save()
