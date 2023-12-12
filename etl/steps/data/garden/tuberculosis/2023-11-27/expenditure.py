"""Load a meadow dataset and create a garden dataset."""

import numpy as np
from owid.catalog import Table
from shared import add_variable_description_from_producer

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("expenditure")
    snap = paths.load_snapshot("data_dictionary.csv")
    # Read table from meadow dataset.
    tb = ds_meadow["expenditure"].reset_index()
    dd = snap.read()
    #
    # Process data.
    #
    tb = add_variable_description_from_producer(tb, dd)
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = add_values_to_hospital_type(tb)
    tb = replace_zero_with_na(tb)
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_values_to_hospital_type(tb: Table) -> Table:
    """
    Add values from data dictionary to hospital_type variable.
    """
    hosp_dict = {
        2: np.nan,
        140: "Primary-level hospital",
        141: "Secondary-level hospital",
        142: "Tertiary-level hospital",
    }
    tb["hosp_type_mdr"] = tb["hosp_type_mdr"].astype(object)
    tb["hosp_type_mdr"] = tb["hosp_type_mdr"].replace(hosp_dict)

    return tb


def replace_zero_with_na(tb: Table) -> Table:
    """
    Replacing zeros with NAs for variables concerning average cost of drugs per patient

    """

    cols = ["exp_cpp_dstb", "exp_cpp_mdr", "exp_cpp_tpt", "exp_cpp_xdr"]

    tb[cols] = tb[cols].replace(0, np.nan)

    return tb
