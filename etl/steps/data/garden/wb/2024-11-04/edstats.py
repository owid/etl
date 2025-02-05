"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.catalog.utils import underscore

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("edstats")
    tb = ds_meadow["edstats"].reset_index()

    # Copy the table with just metadata
    metadata_tb = tb.loc[:, ["indicator_name", "indicator_code", "source_note", "source"]]

    # Load historical literacy
    ds_literacy = paths.load_dataset("literacy_rates")
    tb_literacy = ds_literacy["literacy_rates"]

    # Load historical literacy expenditure data
    ds_expenditure = paths.load_dataset("public_expenditure")
    tb_expenditure = ds_expenditure["public_expenditure"]
    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        excluded_countries_file=paths.excluded_countries_path,
        countries_file=paths.country_mapping_path,
    )
    tb = tb.drop("source", axis=1)

    # Pivot the dataframe so that each indicator is a separate column
    tb = tb.pivot(index=["country", "year"], columns="indicator_code", values="value")
    tb = tb.reset_index()

    # Find the maximum value in the 'HD.HCI.HLOS' column (Harmonized Test Scores)
    max_value = tb["HD.HCI.HLOS"].max()

    # Normalize every value in the 'HD.HCI.HLOS' column by the maximum value (How many years of effective learning do you get for every year of education)
    tb["normalized_hci"] = tb["HD.HCI.HLOS"] / max_value

    # Combine recent literacy estimates and expenditure data with historical estimates from a migrated dataset
    tb = combine_historical_literacy_expenditure(tb, tb_literacy, tb_expenditure)

    #  Rename columns based on metadata
    tb = rename_columns(tb, metadata_tb)

    # Convert the share of the population with no education to a percentage (bug in the data)
    tb[
        "wittgenstein_projection__percentage_of_the_population_age_15plus_by_highest_level_of_educational_attainment__no_education__total"
    ] *= 100
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def combine_historical_literacy_expenditure(tb: Table, tb_literacy: Table, tb_expenditure: Table) -> Table:
    """
    Merge historical and recent literacy and expenditure data into a single Table.

    This function combines data from two separate Tables containing historical literacy rates and
    public expenditure on education with a primary WB Table. The function handles missing data by favoring recent World Bank data; if this is not available,
    it falls back to historical data, which could also be missing (NaN).

    """

    historic_literacy = (
        tb_literacy[["literacy_rates__world_bank__cia_world_factbook__and_other_sources"]].reset_index().copy()
    )
    historic_expenditure = (
        tb_expenditure[["public_expenditure_on_education__tanzi__and__schuktnecht__2000"]].reset_index().copy()
    )
    # Recent literacy rates
    recent_literacy = tb[["year", "country", "SE.ADT.LITR.ZS"]].copy()

    # Recent public expenditure
    recent_expenditure = tb[["year", "country", "SE.XPD.TOTL.GD.ZS"]].copy()

    # Merge the historic and more recent literacy data based on 'year' and 'country'
    combined_df = pr.merge(
        historic_literacy,
        recent_literacy,
        on=["year", "country"],
        how="outer",
        suffixes=("_historic_lit", "_recent_lit"),
    )

    # Merge the historic expenditure with newly created literacy table based on 'year' and 'country'
    combined_df = pr.merge(combined_df, historic_expenditure, on=["year", "country"], how="outer")

    # Merge the recent expenditure with newly created literacy and historic expenditure table based on 'year' and 'country'
    combined_df = pr.merge(
        combined_df, recent_expenditure, on=["year", "country"], how="outer", suffixes=("_historic_exp", "_recent_exp")
    )
    combined_df["combined_literacy"] = combined_df["SE.ADT.LITR.ZS"].fillna(
        combined_df["literacy_rates__world_bank__cia_world_factbook__and_other_sources"]
    )
    combined_df["combined_expenditure"] = combined_df["SE.XPD.TOTL.GD.ZS"].fillna(
        combined_df["public_expenditure_on_education__tanzi__and__schuktnecht__2000"]
    )

    # Now, merge the relevant columns in newly created table that includes both historic and more recent data back into the original tb based on 'year' and 'country'
    tb = pr.merge(
        tb,
        combined_df[["year", "country", "combined_literacy", "combined_expenditure"]],
        on=["year", "country"],
        how="outer",
    )

    return tb


def rename_columns(tb: Table, metadata_tb: Table) -> Table:
    "Rename columns in the table based on the metadata table."
    for column in tb.columns:
        if column not in [
            "country",
            "year",
            "normalized_hci",
            "combined_literacy",
            "combined_expenditure",
        ]:
            # Extract relevant name.
            name = (
                metadata_tb.loc[metadata_tb["indicator_code"] == column, "indicator_name"]
                .str.replace("‚", "")  # commas caused problems when renaming variables later on
                .iloc[0]
            )
            # Truncate the last 5 words if the length of the string exceeds 250 characters
            if len(name) > 250:
                # Separate the string into words and truncate
                words = name.split()
                # Get all words up to the fifth-to-last word
                selected_words = words[:-10]
                # Reconstruct the selected words into a single string
                name = " ".join(selected_words)

            # Convert the name to underscore format
            new_column_name = underscore(name)  # Convert extracted name to underscore format

            # Update the column names and metadata
            tb.rename(columns={column: new_column_name}, inplace=True)
    return tb
