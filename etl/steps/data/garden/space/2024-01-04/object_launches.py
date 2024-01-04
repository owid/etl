"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("object_launches")

    # Read table from meadow dataset.
    tb = ds_meadow["object_launches"].reset_index()

    #
    # Process data.
    #
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

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    tb.metadata.short_name = paths.short_name
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
