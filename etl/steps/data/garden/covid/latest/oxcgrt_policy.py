"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("oxcgrt_policy")

    # Read table from meadow dataset.
    tb = ds_meadow["oxcgrt_policy_compact"].reset_index()
    tb_vax = ds_meadow["oxcgrt_policy_vaccines"].reset_index()

    #
    # Process data.
    #
    # Keep only national data
    tb = tb.loc[tb["regioncode"].isnull()].drop(columns="regioncode")
    # Merge tables
    tb = tb.merge(tb_vax, how="outer", on=["countryname", "date"], validate="one_to_one")
    # Column renaming
    tb = tb.rename(
        columns={
            "countryname": "country",
            "stringencyindex_average": "stringency_index",
            "containmenthealthindex_average": "containment_health_index",
        }
    )
    # Harmonize
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    # Parse date
    tb["date"] = pd.to_datetime(tb["date"], format="%Y%m%d")

    # Format
    tb = tb.format(["country", "date"], short_name="oxcgrt_policy")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
