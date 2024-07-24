"""Load a meadow dataset and create a garden dataset."""

import re

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("nutrients")

    # Read table from meadow dataset.
    tb = ds_meadow["nutrients"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.drop(columns=["countrycode", "lat", "lon", "class1", "class2", "class3", "class4", "class5"])
    tb = tb.dropna(subset=["meanvalue"])
    tb["resultuom"] = tb["resultuom"].apply(remove_curly_brackets_from_unit)
    tb["waterbodycategory"] = tb["waterbodycategory"].replace({"GW": "groundwater", "RW": "rivers", "LW": "lakes"})
    tb = tb.rename(
        columns={"waterbodycategory": "water_body_category", "eeaindicator": "indicator", "resultuom": "unit"}
    )
    tb = tb.set_index(["country", "year", "water_body_category", "indicator", "unit"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def remove_curly_brackets_from_unit(text):
    return re.sub(r"\{.*?\}", "", text)
