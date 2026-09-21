"""Garden step on the scaling of global historical electricity production.

For each electricity source, we track global production from the first year in which it surpassed
100 TWh. The input is the World electricity mix long-run series (Ember + Statistical Review + Pinto
et al.), so this step reuses the already-combined data instead of re-merging sources.
"""

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
    # Load the electricity mix dataset and read its main table (World long-run series).
    ds_electricity = paths.load_dataset("electricity_mix")
    tb = ds_electricity.read("electricity_mix")

    #
    # Process data.
    #
    # Keep only World data.
    tb = tb[tb["country"] == "World"].reset_index(drop=True)

    # Gather production and share data for each source, from the first year it generated over 100 TWh.
    tables = []
    for column_production in sorted([col for col in tb.columns if col.endswith("_generation__twh")]):
        source = column_production.replace("_generation__twh", "")
        column_share = f"{source}_share_of_electricity__pct"

        # Track from the first year production reached the threshold, keeping later dips below it
        # (e.g. bioenergy fell back under 100 TWh during 1991-1993).
        above_threshold = tb[tb[column_production] >= PRODUCTION_THRESHOLD]
        if above_threshold.empty:
            continue
        first_year = above_threshold["year"].min()
        mask = (tb["year"] >= first_year) & tb[column_production].notna()

        tb_source = tb[mask].reset_index(drop=True).reset_index()
        tb_source = tb_source.rename(
            columns={"index": "year", "year": "year_since_100_twh", column_production: "production_since_100_twh"},
            errors="raise",
        )
        # The output "year" is years since the crossing, so the calendar years must be consecutive.
        assert (tb_source["year_since_100_twh"].diff().dropna() == 1).all(), (
            f"Gap in the {source} series after its first year above {PRODUCTION_THRESHOLD} TWh."
        )

        # Add share column: 100% for total (which doesn't have a share column), from data for all other sources.
        if source == "total":
            assert column_share not in tb_source.columns
            tb_source["share_since_100_twh"] = 100
        elif column_share in tb_source.columns:
            tb_source = tb_source.rename(columns={column_share: "share_since_100_twh"}, errors="raise")
        else:
            # Skip sources without a corresponding share column.
            continue

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
