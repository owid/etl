"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset
import pandas as pd

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def add_world_aggregate(tb):
    """
    Add a row for the world aggregate.
    """
    world = tb.groupby("year", as_index=False).sum(numeric_only=True).assign(country="World")
    return pd.concat([tb, world], ignore_index=True, sort=False)


def clean_entities(tb):
    """
    Clean the entities column.
    """
    tb.loc[tb.country.str.contains("[\(\[]for .*[\)\]]$"), "country"] = tb.country.str.extract(
        "[\(\[]for (.*)[\)\]]$", expand=False
    )
    tb["country"] = tb.country.str.split(",|&| and ")
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
    tb = tb.pipe(add_world_aggregate).pipe(clean_entities)
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Aggregate by country and year
    tb = tb.groupby(["country", "year"], as_index=False).sum(numeric_only=True).sort_values(["country", "year"])

    # Compute cumulative launches
    tb["cumulative_launches"] = tb[["country", "annual_launches"]].groupby("country", as_index=False).cumsum()

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    tb.metadata.short_name = "object_launches"
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
