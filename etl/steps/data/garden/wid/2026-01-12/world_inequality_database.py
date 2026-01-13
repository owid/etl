"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("world_inequality_database")

    # Read table from meadow dataset.
    tb = ds_meadow.read("world_inequality_database")
    tb_distribution = ds_meadow.read("world_inequality_database_distribution")
    tb_fiscal = ds_meadow.read("world_inequality_database_fiscal")

    #
    # Process data.
    #

    tb = make_table_long(tb=tb)

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def make_table_long(tb):
    """
    Convert the table to long format, to create dimensionality for indicators.
    """
    tb_long = tb.copy()

    tb_long = tb_long.melt(id_vars=["country", "year"], var_name="indicator", value_name="value")

    # Drop empty values
    tb_long = tb_long.dropna(subset=["value"]).reset_index(drop=True)

    # Now the indicator column contains strings separated by underscores.
    # Split these strings into multiple columns.
    tb_long[["quantile", "indicator", "welfare_type"]] = tb_long["indicator"].str.rsplit("_", n=2, expand=True)

    # Separate the tables between indicators not needing quantiles (quantile = p0p100) and those needing quantiles.
    tb_inequality = tb_long[tb_long["quantile"] == "p0p100"].copy()
    tb_quantiles = tb_long[tb_long["quantile"] != "p0p100"].copy()

    # For the inequality table, drop the quantile column as it is not needed.
    tb_inequality = tb_inequality.drop(columns=["quantile"]).reset_index(drop=True)

    # Make the table wide with the indicators as columns.
    tb_inequality = tb_inequality.pivot_table(
        index=["country", "year", "welfare_type"],
        columns="indicator",
        values="value",
    ).reset_index()

    print(tb_inequality.head())

    return tb_long
