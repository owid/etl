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
    ds_meadow = paths.load_dataset("life_in_five_years_and_life_today")

    # Read tables from meadow dataset.
    tb_today = ds_meadow.read("life_satisfaction_today")
    tb_in_5_years = ds_meadow.read("life_satisfaction_in_5_years")

    #
    # Process data.
    #
    # Combine both tables.
    tb = pr.concat([tb_today.assign(**{"when": "today"}), tb_in_5_years.assign(**{"when": "in_5_years"})])

    # Sanity checks.
    error = "Data structure has changed."
    assert set(tb["demographic"]) == {"Aggregate"}, error
    assert set(tb["demographic_value"]) == {"Aggregate"}, error

    # Drop unnecessary columns.
    tb = tb.drop(columns=["demographic", "demographic_value"], errors="raise")

    # Rename columns.
    tb = tb.rename(
        columns={"geography": "country", "time": "year", "value": "satisfaction", "n_size": "n_people_surveyed"},
        errors="raise",
    )

    # Transpose table.
    tb = tb.pivot(index=["country", "year"], columns=["when"], join_column_levels_with="_")

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
