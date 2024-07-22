"""Compilation of minerals data from different origins."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load datasets.
    ds_bgs = paths.load_dataset("world_mineral_statistics")
    ds_usgs_historical = paths.load_dataset("historical_statistics_for_mineral_and_material_commodities")
    ds_usgs = paths.load_dataset("mineral_commodity_summaries")

    # Read tables.
    tb_bgs = ds_bgs["world_mineral_statistics"].astype(float).reset_index()
    tb_usgs_historical = (
        ds_usgs_historical["historical_statistics_for_mineral_and_material_commodities"]
        .drop(columns=["unit_value_current"])
        .astype(float)
        .reset_index()
    )
    tb_usgs = (
        ds_usgs["mineral_commodity_summaries"]
        .drop(columns=["reserves_notes", "production_notes"])
        .astype(float)
        .reset_index()
    )

    #
    # Process data.
    #
    # TODO: Combine production from historical USGS data with current USGS data.
    # TODO: Fix unit value conversion.
    # TODO: Unit value is only given for World. For now, use just this.
    # Add a sub-commodity column to the historical data (assume production and unit value correspond to the total of each commodity).
    tb_usgs_historical["sub_commodity"] = "Total"
    tb_usgs_historical = (
        tb_usgs_historical[["country", "year", "commodity", "sub_commodity", "unit_value"]]
        .dropna()
        .reset_index(drop=True)
    )

    # Add a column for the source of each data point, and combine tables.
    tb = pr.concat(
        [tb_bgs.assign(**{"source": "BGS"}), tb_usgs.assign(**{"source": "USGS"})],
        ignore_index=True,
        short_name=paths.short_name,
    )

    # Add unit value to the table.
    tb = tb.merge(
        tb_usgs_historical.assign(**{"source": "USGS"}),
        on=["country", "year", "commodity", "sub_commodity", "source"],
        how="outer",
    )

    # Format table conveniently.
    tb = tb.format(["country", "year", "commodity", "sub_commodity", "source"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
