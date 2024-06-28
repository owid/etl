"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("poverty_projections")

    # Read table from meadow dataset.
    tb = ds_meadow["poverty_projections"].reset_index()

    #
    # Process data.
    #
    # Multiply headcount_215 by 1e6 to convert from millions to number of people.
    tb["headcount_215"] *= 1e6
    # Separate estimated data from projected data in headcount_2015.
    # 2019 was the last year with estimated data and I also leave it for projected for visualization purposes.
    tb["headcount_215_estimated"] = tb["headcount_215"].where(tb["year"] <= 2019)
    tb["headcount_215_projected"] = tb["headcount_215"].where(tb["year"] >= 2019)

    # Drop headcount_215 column
    tb = tb.drop(columns=["headcount_215"])

    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
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
