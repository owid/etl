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
    ds_meadow = paths.load_dataset("gco_alcohol")

    # Read table from meadow dataset.
    tb = ds_meadow["gco_alcohol"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # To display on grapher we need to replace "<0.1" with "0.05" and set the decimal places to 1 so that it shows up as <0.1 on the chart.
    tb["value"] = tb["value"].replace("<0.1", "0.05")

    tb = tb.format(["country", "year", "sex", "cancer", "indicator"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
