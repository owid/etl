"""Load a meadow dataset and create a garden dataset."""

import numpy as np

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("lymphatic_filariasis")

    # Read table from meadow dataset.
    tb = ds_meadow["lymphatic_filariasis"].reset_index()
    #
    # Harmonize countries
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Process data.
    # There are separate rows for each combination of drugs used, but this is duplicate for `national_coverage__pct`, so we will extract this column and create a separate table for it

    # In many cases the are two identical values for 'national_coverage__pct', for each country year, this de-duplicates them
    tb_nat = tb[["country", "year", "national_coverage__pct"]].copy().drop_duplicates()
    # There are a few cases with two values for some country-year combos, here we drop them because we are not sure which is the correct value
    tb_nat = tb_nat.drop_duplicates(subset=["country", "year"])
    tb_nat.metadata.short_name = "lymphatic_filariasis_national"
    # Drop `national_coverage_pct` from tb
    tb = tb.drop(columns=["national_coverage__pct", "region", "country_code", "mapping_status"])
    # Replace "No data" with NaN
    tb = tb.replace("No data", np.nan)
    # Format the tables
    tb = tb.format(["country", "year", "type_of_mda"])
    tb_nat = tb_nat.format(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_nat], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
