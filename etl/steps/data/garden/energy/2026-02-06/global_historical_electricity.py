"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("global_historical_electricity")

    # Read table on the total electricity production (and consumption).
    tb_total = ds_meadow.read("electricity_production_and_consumption")
    # Read table on the share of electricity production by source.
    tb_share = ds_meadow.read("electricity_production_share_by_source")

    #
    # Process data.
    #
    # Create a combined table with total electricity production, and the share of each source.
    tb = tb_total.drop(columns=["electricity_consumption"]).merge(tb_share, on=["year"], how="outer")

    for column in [column for column in tb.columns if column not in ["year", "electricity_production"]]:
        # Add a column with the electricity produced by each source.
        tb[f"{column}_production"] = tb["electricity_production"] * tb[column]
        # Rename share column conveniently.
        tb = tb.rename(columns={column: f"{column.replace('__', '_')}_share"}, errors="raise")

    # Add a country column.
    tb["country"] = "World"

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
