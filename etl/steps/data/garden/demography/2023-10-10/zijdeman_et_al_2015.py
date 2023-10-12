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
    ds_meadow = paths.load_dataset("zijdeman_et_al_2015")
    # Read table from meadow dataset.
    tb = ds_meadow["zijdeman_et_al_2015"].reset_index()

    #
    # Process data.
    #
    ## Rename columns
    columns = {
        "country_name": "country",
        "year": "year",
        "value": "life_expectancy",
    }
    tb = tb[columns.keys()].rename(columns=columns)

    ## Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    ## Set index
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
