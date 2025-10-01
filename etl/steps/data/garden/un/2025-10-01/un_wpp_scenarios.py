# from deaths import process as process_deaths
# from demographics import process as process_demographics
# from dep_ratio import process as process_depratio
# from fertility import process as process_fertility

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("un_wpp_scenarios")

    # Load tables
    tb = ds_meadow.read("un_wpp_demographic_indicators_scenarios")

    #
    # Process data.

    tb = geo.harmonize_countries(
        tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb.format(["country", "year", "variant"])

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata, repack=False
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
