"""Compilation of minerals data from different origins."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load datasets.
    ds_bgs = paths.load_dataset("world_mineral_statistics")
    # ds_usgs_historical = paths.load_dataset("historical_statistics_for_mineral_and_material_commodities")
    # ds_usgs = paths.load_dataset("mineral_commodity_summaries")

    # Read tables.
    tb_bgs = ds_bgs.read_table("world_mineral_statistics")
    # tb_usgs_historical = ds_usgs_historical.read_table("historical_statistics_for_mineral_and_material_commodities")
    # tb_usgs = ds_usgs.read_table("mineral_commodity_summaries")

    #
    # Process data.
    #
    # Combine tables.
    # TODO: For now, use only BGS data. In the future, include other origins.
    tb = tb_bgs.copy()
    # For now, select only the total for each commodity.
    tb = tb[tb["sub_commodity"] == "Total"].reset_index(drop=True)
    tb = tb.drop(columns=["sub_commodity"])

    # Format table conveniently.
    tb = tb.format(["country", "year", "commodity"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_bgs.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
