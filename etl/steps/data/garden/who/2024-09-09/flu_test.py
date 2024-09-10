"""Load a garden dataset and create a grapher dataset."""


import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("flunet")
    # Read table from garden dataset.
    tb = ds_garden["flunet"]
    # Harmonize countries
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Format date
    tb["date"] = pd.to_datetime(tb["iso_weekstartdate"], format="%Y-%m-%d", utc=True).dt.date.astype(str)

    # Select out only variables that we care about
    tb_test = (
        tb[["country", "date", "origin_source", "spec_processed_nb", "spec_received_nb", "inf_all", "inf_negative"]]
        .dropna(subset=["spec_processed_nb", "spec_received_nb"])
        .copy()
    )
    tb_test["inf_tests"] = tb_test["inf_all"] + tb_test["inf_negative"]
    tb_test = tb_test.drop(columns=["inf_pos", "inf_negative"])
    tb_test = tb_test.format(["country", "date", "origin_source"], short_name="flu_test")
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_test], check_variables_metadata=True)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
