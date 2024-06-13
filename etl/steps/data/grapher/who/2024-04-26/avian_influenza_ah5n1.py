"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("avian_influenza_ah5n1")

    # Read table from garden dataset.
    tb_month = ds_garden["avian_influenza_ah5n1_month"].reset_index()
    tb_year = ds_garden["avian_influenza_ah5n1_year"]

    #
    # Process data.
    #
    # Get zeroDay as the minimum date in the dataset and set it to zeroDay
    tb_month = add_num_days(tb_month)
    tb_month = tb_month.format(["country", "year"])

    #
    # Save outputs.
    #
    tables = [
        tb_month,
        tb_year,
    ]
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=tables, default_metadata=ds_garden.metadata)

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
    column_indicator = "avian_cases_month"

    if tb[column_indicator].metadata.display is None:
        tb[column_indicator].metadata.display = {}

    zero_day = tb["date"].min()
    tb[column_indicator].metadata.display["yearIsDay"] = True
    tb[column_indicator].metadata.display["zeroDay"] = zero_day.strftime("%Y-%m-%d")

    # Add column with number of days after zero_day
    tb["year"] = (tb["date"] - zero_day).dt.days

    # Drop date column
    tb = tb.drop(columns=["date"])

    return tb
