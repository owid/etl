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
    ds_meadow = paths.load_dataset("urban_agglomerations_largest_cities_history")

    # Read table from meadow dataset.
    tb = ds_meadow["urban_agglomerations_largest_cities_history"].reset_index()
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Select only the first rank order, which is the largest city.
    # Identify countries that have ever been in the top 5
    tb = tb[tb["rank"] < 6]

    tb["population__millions"] = tb["population__millions"] * 1000000
    tb = tb.rename(columns={"population__millions": "population"})
    tb = tb.set_index(["country", "year", "urban_agglomeration"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
