from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("harmonized_scores")

    # Read table from meadow dataset.
    tb = ds_meadow.read("harmonized_scores")
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Find the maximum value in the 'harmonized_test_scores' column
    max_value = tb["harmonized_test_scores"].max()

    # Normalize every value in the 'harmonized_test_scores' column by the maximum value (How many years of effective learning do you get for every year of education)
    tb["normalized_hci"] = tb["harmonized_test_scores"] / max_value

    tb = tb.format(["country", "year", "sex"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
