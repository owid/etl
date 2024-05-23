"""Load a meadow dataset and create a garden dataset."""

from owid.catalog.tables import Table, concat

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gdelt_v2")

    # Read table from meadow dataset.
    tb = ds_meadow["gdelt_v2"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Remove year 1920
    tb = tb.loc[tb["year"] != 1920]

    # Add relative indicator
    tb = add_relative_indicator(tb, "num_events")

    # Format
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_relative_indicator(tb: Table, colname: str):
    """Add relative indicator.

    E.g. Global share of ? for a given year. Note that we use 'per 100,000' factor.
    """
    EXCLUDE_ENTITIES = [
        "Africa (GDELT)",
        "Asia (GDELT)",
        "Central Asia (GDELT)",
        "Eastern Africa (GDELT)",
        "Europe (GDELT)",
        "Latin America (GDELT)",
        "Middle East (GDELT)",
        "North Africa (GDELT)",
        "North America (GDELT)",
        "Persian Gulf (GDELT)",
        "Southern Africa (GDELT)",
        "South America (GDELT)",
        "South Asia (GDELT)",
        "Scandinavia",
        "Southeast Asia (GDELT)",
        "West Africa (GDELT)",
        "Undetermined",
    ]

    # Split data into regions and countries
    tb_regions = tb.loc[tb["year"].isin(EXCLUDE_ENTITIES)].copy()
    tb_countries = tb.loc[~tb["year"].isin(EXCLUDE_ENTITIES)].copy()

    # Estimate relative values
    tb_countries["total"] = tb_countries.groupby("year")[colname].transform(sum)
    tb_countries[f"{colname}_relative"] = tb_countries[colname] / tb_countries["total"] * 100_000
    tb_countries = tb_countries.drop(columns=["total"])

    # Merge data back
    tb = concat([tb_regions, tb_countries], ignore_index=True)
    return tb
