"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("poverty_projections")

    # Read table from meadow dataset.
    tb = ds_meadow.read("poverty_projections")

    #
    # Process data.
    #

    tb = calculate_regional_and_global_aggregates(tb=tb)
    tb = calculate_share_in_poverty_and_rename(tb=tb)

    # Make the povertyline a string with two decimal places.
    tb["povertyline"] = tb["povertyline"].apply(lambda x: f"{x:.2f}")

    # Rename poverty lines
    tb["povertyline"] = tb["povertyline"].replace({"3.00": "300", "4.20": "420", "8.30": "830"})

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Improve table format.
    tb = tb.format(["country", "year", "povertyline"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def calculate_regional_and_global_aggregates(tb):
    """
    Calculate regional and global aggregates for poverty projections.

    The data is shown by country, but it also include the regional column `region_pip`.
    """

    tb = tb.copy()

    # Calculate the sum of pop and poorpop for each region_pip, year, and povertyline.
    tb = (
        tb.groupby(["region_pip", "year", "povertyline"], as_index=False)
        .agg(
            {
                "pop": "sum",
                "poorpop": "sum",
            }
        )
        .reset_index(drop=True)
    )

    # Calculate the global aggregates by summing across all regions.
    tb_global = (
        tb.groupby(["year", "povertyline"], as_index=False)
        .agg(
            {
                "pop": "sum",
                "poorpop": "sum",
            }
        )
        .assign(region_pip="WLD")
    )

    # Concatenate the regional and global aggregates.
    tb = pr.concat([tb, tb_global], ignore_index=True)

    # Make these columns not in millions, but in absolute numbers.
    tb["pop"] *= 1_000_000
    tb["poorpop"] *= 1_000_000

    return tb


def calculate_share_in_poverty_and_rename(tb):
    """
    Calculate the share of the population living in poverty and rename columns.
    """
    tb = tb.copy()

    # Calculate the share of the population living in poverty.
    tb["headcount_ratio"] = tb["poorpop"] / tb["pop"] * 100

    # Rename columns for clarity.
    tb = tb.rename(
        columns={
            "region_pip": "country",
            "poorpop": "headcount",
        }
    )

    # Keep relevant columns.
    tb = tb[["country", "year", "povertyline", "headcount_ratio", "headcount"]]

    return tb
