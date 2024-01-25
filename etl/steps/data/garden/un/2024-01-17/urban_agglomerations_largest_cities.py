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
    ds_meadow = paths.load_dataset("urban_agglomerations_largest_cities")

    # Read table from meadow dataset.
    tb = ds_meadow["urban_agglomerations_largest_cities"].reset_index()
    #
    # Process data.
    #
    country_mapping_path = paths.directory / "urban_agglomerations_shared.countries.json"
    tb = geo.harmonize_countries(df=tb, countries_file=country_mapping_path)
    tb["population__thousands"] = tb["population__thousands"] * 1000
    tb["time_series_of_the_population_of_the_30_largest_urban_agglomerations_in_2018_ranked_by_population_size"] = (
        tb["time_series_of_the_population_of_the_30_largest_urban_agglomerations_in_2018_ranked_by_population_size"]
        * 1000000
    )
    tb = tb.rename(columns={"population__thousands": "population_capital"})

    tb = tb.set_index(["country", "year", "urban_agglomeration", "rank_order"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
