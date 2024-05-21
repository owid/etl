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
    ds_meadow = paths.load_dataset("claassen_mood")

    # Read table from meadow dataset.
    tb = ds_meadow["claassen_mood"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Keep relevant columns
    tb = tb[["country", "year", "supdem", "supdem_u95", "supdem_l95"]]

    # Rename columns
    tb = tb.rename(
        columns={
            "supdem": "democracy_support_claassen",
            "supdem_u95": "democracy_support_high_claassen",
            "supdem_l95": "democracy_support_low_claassen",
        }
    )

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
