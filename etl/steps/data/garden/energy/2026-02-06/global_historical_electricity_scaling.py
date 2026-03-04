"""Load a garden dataset and create a garden dataset on scaling of electricity production."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Minimum production threshold (in TWh) to consider a source.
PRODUCTION_THRESHOLD = 100


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("global_historical_electricity")

    # Read table from garden dataset.
    tb = ds_garden.read("global_historical_electricity")

    #
    # Process data.
    #
    # Gather all production and share data since generating over 100TWh for each source.
    tables = []
    for column_production in sorted([col for col in tb.columns if col.endswith("_production")]):
        source = column_production.replace("_production", "")
        column_share = f"{source}_share"

        # Filter to rows where production >= threshold.
        mask = tb[column_production] >= PRODUCTION_THRESHOLD
        if mask.sum() == 0:
            continue

        tb_source = tb[mask].reset_index(drop=True).reset_index()
        tb_source = tb_source.rename(
            columns={"index": "year", "year": "year_since_100_twh", column_production: "production_since_100_twh"},
            errors="raise",
        )

        # Add share column: 100% for total (which doesn't have a share column), from data for all other sources.
        if source == "total":
            assert column_share not in tb_source.columns
            tb_source["share_since_100_twh"] = 100
        else:
            tb_source = tb_source.rename(columns={column_share: "share_since_100_twh"}, errors="raise")

        # Add a source column.
        tb_source["source"] = source.replace("_", " ").capitalize()

        # Select only the relevant columns.
        tables.append(
            tb_source[
                ["country", "source", "year", "year_since_100_twh", "production_since_100_twh", "share_since_100_twh"]
            ]
        )

    tb_scaling = pr.concat(tables, ignore_index=True)

    # Add a country column.
    tb_scaling["country"] = "World"

    # Improve table format.
    tb_scaling = tb_scaling.format(["country", "source", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_scaling = paths.create_dataset(tables=[tb_scaling])

    # Save garden dataset.
    ds_scaling.save()
