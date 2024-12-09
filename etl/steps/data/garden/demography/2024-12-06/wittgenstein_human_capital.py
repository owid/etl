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
    ds_meadow = paths.load_dataset("wittgenstein_human_capital")

    # Read table from meadow dataset.
    tb = ds_meadow.read("main")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Fix year

    # Format
    tables = [
        tb.format(["country", "year", "scenario"]),
    ]
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
