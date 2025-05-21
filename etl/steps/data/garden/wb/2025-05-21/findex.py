"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("findex")

    # Read table from meadow dataset.
    tb = ds_meadow.read("findex")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Convert to %
    tb["value"] = tb["value"] * 100

    # Add metadata to the table.
    tb = add_metadata(tb)

    # Improve table format.
    tb = tb.format(["country", "year"])

    # Calculate the percentage of adults who have both accounts
    tb["both_accounts"] = (
        tb["mobile_money_account__pct_age_15plus"]
        + tb["financial_institution_account__pct_age_15plus"]
        - tb["account__pct_age_15plus"]
    )

    # % only mobile money account
    tb["only_mobile_money_account"] = tb["mobile_money_account__pct_age_15plus"] - tb["both_accounts"]

    # % only financial institution account
    tb["only_financial_institution_account"] = tb["financial_institution_account__pct_age_15plus"] - tb["both_accounts"]

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def add_metadata(tb: Table) -> Table:
    """
    Add metadata to the table.
    """
    # Pivot the table to have the indicators as columns to add descriptions from producer and description_short.
    tb_pivoted = tb.pivot(index=["country", "year"], columns="indicator_name", values="value")

    for column in tb_pivoted.columns:
        meta = tb_pivoted[column].metadata
        long_definition = tb["long_definition"].loc[tb["indicator_name"] == column]
        short_definition = tb["short_definition"].loc[tb["indicator_name"] == column]
        meta.description_from_producer = long_definition.iloc[0]
        meta.description_short = short_definition.iloc[0]
        meta.title = column
        meta.unit = "%"
        meta.short_unit = "%"
    tb_pivoted = tb_pivoted.reset_index()
    return tb_pivoted
