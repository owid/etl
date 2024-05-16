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
    ds_meadow = paths.load_dataset("fh")
    # ds_regions = paths.load_dataset("regions")
    # ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb_ratings = ds_meadow["fh_ratings"].reset_index()
    tb_scores = ds_meadow["fh_scores"].reset_index()

    #
    # Process data.
    #
    tb_ratings = geo.harmonize_countries(
        df=tb_ratings,
        countries_file=paths.country_mapping_path,
    )
    tb_scores = geo.harmonize_countries(
        df=tb_scores,
        countries_file=paths.country_mapping_path,
    )

    tables = [
        tb_ratings.format(["country", "year"]),
        tb_scores.format(
            ["country", "year"],
        ),
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
