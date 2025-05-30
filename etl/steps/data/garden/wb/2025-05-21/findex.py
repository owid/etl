"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

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

    # combine the two series for Sub-Saharan Africa (WB) and Sub-Saharan Africa (excluding high income) (WB)
    tb = merge_ssa(tb)

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


def merge_ssa(tb: Table) -> Table:
    """
    Merge the two Sub-Saharan Africa series in the given table if they are consistent.

    This function checks that the series for:
      - "Sub-Saharan Africa (WB)"
      - "Sub-Saharan Africa (excluding high income) (WB)"
    have identical values for each (year, indicator) pair, allowing both to be NaN.

    If all overlapping non-null values agree, the function:
      1. Combines the two series by taking the first non-null value for each (year, indicator).
      2. Drops the original separate rows.
      3. Assigns the combined rows to "Sub-Saharan Africa (WB)".
      4. Returns the updated table with a new combined series.

    Raises:
        ValueError: If any (year, indicator) pair has differing non-null values between the two series.

    Args:
        tb (Table): A table containing columns ['country', 'value', 'year', 'indicator_name'].

    Returns:
        Table: A new table with the two SSA series merged.
    """
    # Define the two country labels
    full = "Sub-Saharan Africa (WB)"
    excl = "Sub-Saharan Africa (excluding high income) (WB)"

    # Extract the 'value' series for each label, indexed by (year, indicator_name)
    s_full = (
        tb.loc[tb["country"] == full, ["value", "year", "indicator_name"]]
        .set_index(["year", "indicator_name"])["value"]
        .sort_index()
    )
    s_excl = (
        tb.loc[tb["country"] == excl, ["value", "year", "indicator_name"]]
        .set_index(["year", "indicator_name"])["value"]
        .sort_index()
    )

    # Identify mismatches: both non-null and unequal
    mismatch = s_full.notna() & s_excl.notna() & (s_full != s_excl)
    if mismatch.any():
        # List all (year, indicator_name) pairs where values differ
        bad_pairs = mismatch.loc[mismatch].index.tolist()
        raise ValueError(f"Mismatched values for '{full}' vs '{excl}' at (year, indicator) pairs: {bad_pairs}")

    # Combine the series by taking the first non-null value
    combined_series = s_full.combine_first(s_excl)

    # Assign the country label
    combined_df = combined_series.reset_index()
    combined_df["country"] = full

    # Remove the original two series rows and append the new combined rows
    tb = tb[~tb["country"].isin([full, excl])]
    tb = pr.concat([tb, combined_df], ignore_index=True)

    return tb
