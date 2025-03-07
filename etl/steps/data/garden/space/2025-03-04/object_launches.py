"""Load a meadow dataset and create a garden dataset.

Adapted from Ed's original code.
"""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def add_world_aggregate(tb):
    """
    Add a row for the world aggregate.
    """
    world = tb.groupby("year", as_index=False).sum(numeric_only=True).assign(country="World")
    return pr.concat([tb, world], ignore_index=True, sort=False)


def clean_entities(tb):
    """
    Clean the entities column.
    """
    tb.loc[tb.country.str.contains(r"[\(\[]for .*[\)\]]$"), "country"] = tb.country.str.extract(
        r"[\(\[]for (.*)[\)\]]$", expand=False
    )
    tb["country"] = tb.country.str.split(r",|&| and ")
    tb = tb.explode("country")
    tb["country"] = tb.country.str.strip()
    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("object_launches")

    # Read table from meadow dataset.
    tb = ds_meadow.read("object_launches")

    #
    # Process data.
    #
    # Filter out data from the current year, which is incomplete.
    current_year = int(tb["annual_launches"].metadata.origins[0].date_published[0:4])
    tb = tb[tb["year"] < current_year].reset_index(drop=True)

    # Add a row for the world aggregate.
    tb = add_world_aggregate(tb)

    # Clean the country column.
    tb = clean_entities(tb)

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Aggregate by country and year
    tb = tb.groupby(["country", "year"], as_index=False).sum(numeric_only=True).sort_values(["country", "year"])

    # Compute cumulative launches.
    tb["cumulative_launches"] = tb[["country", "annual_launches"]].groupby("country", as_index=False).cumsum()
    tb["cumulative_launches"] = tb["cumulative_launches"].copy_metadata(tb["annual_launches"])

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
