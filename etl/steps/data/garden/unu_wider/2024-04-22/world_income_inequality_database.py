"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define index variables
INDEX_VARS = [
    "country",
    "year",
    "source_detailed",
    "resource_detailed",
    "scale_detailed",
    "sharing_unit",
    "reference_unit",
    "areacovr_detailed",
    "popcovr_detailed",
    "source_comments",
    "survey",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("world_income_inequality_database")

    # Read table from meadow dataset.
    tb = ds_meadow["world_income_inequality_database"].reset_index()

    #
    # Process data.
    # Select only gini
    tb = tb[INDEX_VARS + ["gini"]]
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    tb = tb.format(INDEX_VARS)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
