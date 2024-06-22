"""Load a meadow dataset and create a garden dataset."""

from typing import Dict, Optional

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("education_opri")

    # Read table from meadow dataset.
    tb = ds_meadow["education_opri"].reset_index()

    # Retrieve snapshot with the metadata provided via World Bank.

    snap_wb = paths.load_snapshot("edstats_metadata.xls")
    tb_wb = snap_wb.read()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    long_definition = {}
    for indicator in tb["indicator_label_en"].unique():
        definition = tb_wb[tb_wb["Indicator Name"] == indicator]["Long definition"].values
        if len(definition) > 0:
            long_definition[indicator] = definition[0]
        else:
            long_definition[indicator] = ""

    tb["long_description"] = tb["indicator_label_en"].map(long_definition)

    # Drop columns that are not needed
    tb = tb.drop(columns=["indicator_id", "magnitude", "qualifier"])
    tb = tb.format(["country", "year", "indicator_label_en", "long_description"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
