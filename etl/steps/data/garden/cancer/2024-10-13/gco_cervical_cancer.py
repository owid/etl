"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets for Cancer Today and Cancer Over Time datasets.
    ds_meadow_over_time = paths.load_dataset("gco_cancer_over_time_cervical")
    ds_meadow_today = paths.load_dataset("gco_cancer_today_cervical")

    # Read tables from meadow datasets.
    tb_today = ds_meadow_over_time["gco_cancer_over_time_cervical"].reset_index()
    tb_over_time = ds_meadow_today["gco_cancer_today_cervical"].reset_index()
    #
    # Process data.
    #
    tb = pr.merge(tb_today, tb_over_time, on=["country", "year", "asr"], how="outer")

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow_over_time.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
