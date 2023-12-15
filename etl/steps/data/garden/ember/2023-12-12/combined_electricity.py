"""Garden step that combines Ember's European Electricity Review (EER) and the latest Ember's Yearly Electricity
Data (YED).

The YED dataset contains data for all countries in EER 2022.
However, YED starts in 2000, while EER 2022 starts in 1990.

Therefore, to gather as much data as possible, we combine both datasets, prioritizing YED.

This way, we'll have data from 1990-1999 from EER 2022, and data from 2000-2022 from YED.

NOTES:
* We don't use the latest EER 2023 because it does not contain data prior to 2000.

"""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table, VariablePresentationMeta, utils
from owid.datautils import dataframes
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def combine_yearly_electricity_data(ds_yed: Dataset) -> Table:
    """Combine all tables in Ember's Yearly Electricity Data into one table.

    Parameters
    ----------
    ds_yed : Dataset
        Yearly Electricity dataset (containing tables for capacity, electricity demand, generation, imports and
        emissions).

    Returns
    -------
    tb_combined : Table
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
    assert set(category_renaming) == set(ds_yed.table_names), error
    index_columns = ["country", "year"]
    tables = []
    for category in category_renaming:
        table = ds_yed[category].reset_index()
        table = table.rename(
            columns={
                column: utils.underscore(category_renaming[category] + column)
                for column in table.columns
                if column not in index_columns
            },
            errors="raise",
        )
        tables.append(table)

    # Merge all tables into one, with an appropriate short name.
    tb_combined = tables[0]
    for table in tables[1:]:
        tb_combined = pr.merge(tb_combined, table, on=index_columns, how="outer", short_name="yearly_electricity")

    # Rename certain columns for consistency.
    tb_combined = tb_combined.rename(
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
        tb_combined["emissions__total_generation__twh"].fillna(-1)
        == tb_combined["generation__total_generation__twh"].fillna(-1)
    ), error

    # Remove unnecessary columns and any possible rows with no data.
    tb_combined = tb_combined.drop(columns=["population", "emissions__total_generation__twh"], errors="raise").dropna(
        how="all"
    )

    # Set a convenient index and sort rows and columns conveniently.
    tb_combined = tb_combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Set an appropriate title for the table.
    tb_combined.metadata.title = "Yearly Electricity Data"

    return tb_combined


def combine_european_electricity_review_data(ds_eer: Dataset) -> Table:
    """Combine tables in Ember's European Electricity Review dataset into one table.

    The tables to be combined are 'country_overview', 'generation', and 'emissions'. The remaining table on net flows
    has a different structure and cannot be combined with the others, so it will remain as a separate table.

    Parameters
    ----------
    ds_eer : Dataset
        European Electricity Review dataset.

    Returns
    -------
    combined_european : Table
        Combined table containing all data in the European Electricity Review dataset (except net flows).

    """
    index_columns = ["country", "year"]
    # Extract the necessary tables from the dataset.
    country_overview = ds_eer["country_overview"].copy()
    generation = ds_eer["generation"].copy()
    emissions = ds_eer["emissions"].copy()

    # Check that the constructed "total generation" column agrees with the one given in table "country_overview".
    columns = ["country", "year", "total_generation__twh"]
    check = pr.merge(
        ds_eer["country_overview"].reset_index()[columns],
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
    generation = generation.drop(columns=["total_generation__pct", "total_generation__twh"], errors="raise")

    # Rename all column names to start with the category, before combining all categories.
    generation = generation.rename(
        columns={column: "generation__" + column for column in generation.columns}, errors="raise"
    )
    emissions = emissions.rename(
        columns={column: "emissions__" + column for column in emissions.columns}, errors="raise"
    )
    country_overview = country_overview.rename(
        columns={
            "total_generation__twh": "generation__total_generation__twh",
            "demand__twh": "demand__total_demand__twh",
            "demand_per_capita__kwh": "demand__total_demand_per_capita__kwh",
            "net_imports__twh": "imports__total_net_imports__twh",
        },
        errors="raise",
    )

    # Combine tables into one.
    combined_european = pr.merge(country_overview.reset_index(), emissions.reset_index(), on=index_columns, how="outer")
    combined_european = pr.merge(
        combined_european,
        generation.reset_index(),
        on=index_columns,
        how="outer",
        short_name="european_electricity_review",
    )

    # If any column was repeated in the merge, it will have a "_x" at the end of the name.
    # Check that no other columns were repeated.
    error = "There are repeated columns in combined dataframe."
    assert len([column for column in combined_european.columns if column.endswith("_x")]) == 0, error

    # Remove any possible rows with no data.
    combined_european = combined_european.dropna(how="all")

    # Ensure that the index is well constructed.
    combined_european = (
        combined_european.set_index(index_columns, verify_integrity=True).sort_index().sort_index(axis=1)
    )

    # Set an appropriate title for the table.
    combined_european.metadata.title = "European Electricity Review"

    return combined_european


def combine_yearly_electricity_data_and_european_electricity_review(tb_yed: Table, tb_eer: Table) -> Table:
    """Combine the combined table of the Yearly Electricity Data with the combined table of the European Electricity
    Review.

    Parameters
    ----------
    tb_yed : Table
        Table that combines all tables of the Yearly Electricity Data.
    combined_eer : Table
        Table that combines all tables of the European Electricity Review (except net flows).

    Returns
    -------
    combined : Table
        Combined data.

    """

    # Combine yearly electricity data with European data, prioritizing the former.
    index_columns = ["country", "year"]
    combined = dataframes.combine_two_overlapping_dataframes(
        df1=tb_yed.reset_index(), df2=tb_eer.reset_index(), index_columns=index_columns
    )

    # TODO: Remove the following block once display and presentation propagate metadata field by field.
    ####################################################################################################################
    for column in combined.columns:
        if not hasattr(combined[column].metadata.presentation, "title_public"):
            title_public_yed = None
            title_public_eer = None
            if column in tb_yed.columns:
                title_public_yed = tb_yed[column].metadata.presentation.title_public
            if column in tb_eer.columns:
                title_public_eer = tb_eer[column].metadata.presentation.title_public
            warning = f"{column}'s title_public in Yearly electricity data differs from European Electricity Review."
            if title_public_yed != title_public_eer:
                log.warning(warning)
            title_public = title_public_yed or title_public_eer
            combined[column].metadata.presentation = VariablePresentationMeta(title_public=title_public)

    ####################################################################################################################

    combined.metadata.short_name = paths.short_name

    # Set an appropriate index and sort conveniently.
    combined = combined.set_index(index_columns, verify_integrity=True).sort_index().sort_index(axis=1)

    return combined


def check_all_variables_have_all_metadata_fields(table: Table) -> None:
    """Check that all variables in a table have all relevant metadata fields."""
    for column in table.columns:
        for field in ["title", "unit", "short_unit", "title_public"]:
            warning = (
                f"Column {column} has no {field}. Ensure all variables have a consistent "
                f"{field} in Yearly electricity data and European Electricity Review datasets."
            )
            if field == "title_public":
                try:
                    if getattr(table[column].metadata.presentation, field) is None:
                        log.warning(warning)
                except AttributeError:
                    log.warning(warning)
            else:
                if not getattr(table[column].metadata, field) is not None:
                    log.warning(warning)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read yearly electricity data and european electricity review datasets from garden.
    ds_yed = paths.load_dataset("yearly_electricity")
    ds_eer = paths.load_dataset("european_electricity_review")

    #
    # Process data.
    #
    # Combine all tables of the yearly electricity data into one.
    tb_yed = combine_yearly_electricity_data(ds_yed=ds_yed)

    # Combine all tables of the european electricity review into one.
    tb_eer = combine_european_electricity_review_data(ds_eer=ds_eer)

    # Combine yearly electricity and european reviews.
    combined = combine_yearly_electricity_data_and_european_electricity_review(tb_yed=tb_yed, tb_eer=tb_eer)

    # Create an additional table with the electricity net flows (only available in european review).
    net_flows = ds_eer["net_flows"].copy()

    # Check that all variables have all metadata fields.
    for table in [tb_yed, tb_eer, combined, net_flows]:
        check_all_variables_have_all_metadata_fields(table)

    #
    # Save outputs.
    #
    # Create new garden dataset.
    ds_garden = create_dataset(
        dest_dir=dest_dir,
        tables=[tb_yed, tb_eer, combined, net_flows],
        default_metadata=ds_yed.metadata,
        check_variables_metadata=True,
    )
    ds_garden.save()
