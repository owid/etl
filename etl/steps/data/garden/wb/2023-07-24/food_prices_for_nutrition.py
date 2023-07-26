"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Expected classifications, sorted from oldest to newest.
EXPECTED_CLASSIFICATIONS = ["FPN 1.0", "FPN 1.1", "FPN 2.0"]
# Classification to adopt (by default, the latest one).
CLASSIFICATION = EXPECTED_CLASSIFICATIONS[-1]


def adapt_units(tb: Table) -> Table:
    # Change units from million people to people.
    for column in [column for column in tb.columns if column.startswith("millions_of_people")]:
        tb[column] *= 1e6
        tb = tb.rename(columns={column: column.replace("millions_of_people", "people")})

    # Convert units expressed as fractions to percentages.
    for column in [column for column in tb.columns if column.startswith(("cost_share_", "affordability_"))]:
        tb[column] *= 100

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = cast(Dataset, paths.load_dependency("food_prices_for_nutrition"))
    tb = ds_meadow["food_prices_for_nutrition"].reset_index()

    #
    # Process data.
    #
    # Sanity check.
    error = "Expected classifications have changed."
    assert set(tb["classification"]) == set(EXPECTED_CLASSIFICATIONS), error

    # Select the latest classification.
    tb = tb[tb["classification"] == CLASSIFICATION].drop(columns=["classification"]).reset_index(drop=True)

    # Rename columns conveniently.
    tb = tb.rename(columns={"economy": "country"})

    # Harmonize country names.
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Adapt units.
    tb = adapt_units(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )
    ds_garden.save()
