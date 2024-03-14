"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def convert_monthly_to_annual(tb_new: Table) -> Table:
    tb_new = tb_new.copy()

    # Create a year column.
    tb_new["year"] = tb_new["date"].dt.year

    # Create a table with the number of observations per year.
    tb_counts = tb_new.groupby("year", as_index=False).agg(
        {
            "co2_concentration": "count",
            "ch4_concentration": "count",
            "n2o_concentration": "count",
        }
    )
    # Create a table with the average annual values.
    tb_new = tb_new.groupby("year", as_index=False).agg(
        {
            "co2_concentration": "mean",
            "ch4_concentration": "mean",
            "n2o_concentration": "mean",
        }
    )
    # Make nan all data points based on less than 12 observations per year.
    for gas in ["co2", "ch4", "n2o"]:
        tb_new.loc[tb_counts[f"{gas}_concentration"] < 12, f"{gas}_concentration"] = None

    # Drop empty rows.
    tb_new = tb_new.dropna(
        subset=["co2_concentration", "ch4_concentration", "n2o_concentration"], how="all"
    ).reset_index(drop=True)

    return tb_new


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset on long-run GHG concentrations from EPA, and read its main table.
    ds_old = paths.load_dataset("ghg_concentration", namespace="epa")
    tb_old = ds_old["ghg_concentration"].reset_index()

    # Load garden dataset of up-to-date GHG concentrations, and read its main table.
    ds_new = paths.load_dataset("ghg_concentration", namespace="climate")
    tb_new = ds_new["ghg_concentration"].reset_index()

    #
    # Process data.
    #
    # Select columns.
    tb_new = tb_new[["date", "co2_concentration", "ch4_concentration", "n2o_concentration"]].copy()

    # Calculate average annual values.
    tb_new = convert_monthly_to_annual(tb_new=tb_new)

    # Combine old and new data, prioritizing the latter.
    tb = combine_two_overlapping_dataframes(df1=tb_new, df2=tb_old, index_columns=["year"])

    # Rename table.
    tb.metadata.short_name = paths.short_name

    # Add location column.
    tb["location"] = "World"

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["location", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
