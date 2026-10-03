"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
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
    ds_meadow = paths.load_dataset("glass_enrolment")

    # Read table from meadow dataset.
    tb = ds_meadow.read("glass_enrolment")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)
    origins = tb["amc"].metadata.origins

    # Make data meaningful.
    tb = tb[["country", "year", "amr", "amc"]]
    # Check there's no weird values, it should be only Y and NA
    assert len(tb["amr"].unique()) == 2, "amr column should have only two unique values"
    assert len(tb["amc"].unique()) == 2, "amc column should have only two unique values"
    tb = combine_data(tb)
    tb = tb.drop(columns=["amr", "amc"])
    tb["enrolment"].metadata.origins = origins

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def combine_data(tb: Table) -> Table:
    """Combine data the amr and amc columns into a single column."""
    # Define conditions
    tb["amr"] = tb["amr"].fillna("")
    tb["amc"] = tb["amc"].fillna("")
    conditions = [
        (tb["amr"] == "Y") & (tb["amc"] == "Y"),  # Both AMR and AMC
        (tb["amr"] == "Y") & (tb["amc"] != "Y"),  # AMR only
        (tb["amr"] != "Y") & (tb["amc"] == "Y"),  # AMC only
        (tb["amr"] != "Y") & (tb["amc"] != "Y"),  # Neither
    ]

    # Define corresponding outputs
    choices = ["Both", "AMR only", "AMC only", "Neither"]

    # Apply row-wise conditions
    tb["enrolment"] = np.select(conditions, choices, default=pd.NA)
    assert all(tb["enrolment"].notna()), "There should be no missing values in the enrolment column"

    return tb
