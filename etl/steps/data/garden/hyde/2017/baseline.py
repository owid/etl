#
#  Hyde baseline 2017
#
#  Harmonize countries in the Hyde baseline.
#

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("baseline")

    # Read table from meadow dataset.
    tb = ds_meadow["population"].reset_index()

    #
    # Process data.
    #
    ## Harmonize country names
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Replace year 0 with year 1
    ## More: https://en.wikipedia.org/wiki/Year_zero#cite_note-7
    tb["year"] = tb["year"].replace(0, 1)

    ## Set index
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
