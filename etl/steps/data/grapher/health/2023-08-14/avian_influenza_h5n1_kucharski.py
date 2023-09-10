"""Load a garden dataset and create a grapher dataset."""

from typing import cast

from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = cast(Dataset, paths.load_dependency("avian_influenza_h5n1_kucharski"))

    # Read table from garden dataset.
    tb = ds_garden["avian_influenza_h5n1_kucharski"].reset_index()

    #
    # Process data.
    #
    tb = add_num_days(tb)

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def add_num_days(tb: Table) -> Table:
    """Add column with number of days after zero_day.

    Also, drop `date` column.
    """
    column_indicators = ["avian_cases", "avian_deaths"]

    for column_indicator in column_indicators:
        if tb[column_indicator].metadata.display is None:
            tb[column_indicator].metadata.display = {}

        zero_day = tb["date"].min()
        tb[column_indicator].metadata.display["yearIsDay"] = True
        tb[column_indicator].metadata.display["zeroDay"] = zero_day.strftime("%Y-%m-%d")

    # Add column with number of days after zero_day
    tb["year"] = (tb["date"] - zero_day).dt.days  # type: ignore

    # Drop date column
    tb = tb.drop(columns=["date"])

    return tb
