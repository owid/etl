"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ai_bills.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)
    #
    # Process data.
    #
    tb = tb.rename(columns={"Geographic area": "country"})
    tb["year"] = 2023  # The data is actually for 2016-2023 totals

    # Remove duplicate rows for 'SRB' and 'NLD' for the year 2023 (both are zeros)
    tb = tb.drop_duplicates(subset=["country", "year"])
    # Harmonize the country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
