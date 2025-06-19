"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep in projections and aggregates tables.
COLUMNS_TO_KEEP_PROJECTIONS = ["region_code", "year", "poverty_line", "hc", "poor"]
COLUMNS_TO_KEEP_AGGREGATES = ["region_code", "year", "poverty_line", "headcount", "pop_in_poverty"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("poverty_projections")

    # Read table from meadow dataset.
    tb_projections = ds_meadow.read("poverty_projections")
    tb_aggregates = ds_meadow.read("poverty_aggregates")

    #
    # Process data.
    #
    # Keep only the necessary columns in projections and aggregates tables.
    tb_projections = tb_projections[COLUMNS_TO_KEEP_PROJECTIONS]
    tb_aggregates = tb_aggregates[COLUMNS_TO_KEEP_AGGREGATES]

    # Rename columns in projections table.
    tb_projections = tb_projections.rename(
        columns={"region_code": "country", "hc": "headcount", "poor": "pop_in_poverty"}, errors="raise"
    )
    tb_aggregates = tb_aggregates.rename(columns={"region_code": "country"}, errors="raise")

    # Adjust indicators to be consistent across both tables.
    tb_projections["pop_in_poverty"] *= 1e6
    tb_aggregates["headcount"] *= 100

    # In aggregates table, make the poverty_line a string with two decimal places.
    tb_aggregates["poverty_line"] = tb_aggregates["poverty_line"].apply(lambda x: f"{x:.2f}")

    # Keep only relevant poverty lines
    tb_aggregates = tb_aggregates[tb_aggregates["poverty_line"].isin(["3.00", "4.20", "8.30"])].reset_index(drop=True)

    # Rename poverty lines in aggregates table.
    tb_aggregates["poverty_line"] = tb_aggregates["poverty_line"].replace({"3.00": "300", "4.20": "420", "8.30": "830"})

    # Concatenate projections and aggregates tables.
    tb = pr.concat([tb_projections, tb_aggregates], ignore_index=True)

    # Make the poverty_line column a string
    tb["poverty_line"] = tb["poverty_line"].astype("string")

    # Keep only data from 1990 onwards.
    tb = tb[tb["year"] >= 1990].reset_index(drop=True)

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    )

    # Improve table format.
    tb = tb.format(["country", "year", "poverty_line"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
