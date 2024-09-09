"""Load a garden dataset and create a grapher dataset."""


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
    # Load garden dataset.
    ds_garden = paths.load_dataset("fluid")
    # Read table from garden dataset.
    tb = ds_garden["fluid"]
    # Harmonize countries
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Filter age-groups
    tb = tb[tb["agegroup_code"] == "All"]

    # Format date
    tb["date"] = pd.to_datetime(tb["iso_weekstartdate"], format="%Y-%m-%d", utc=True).dt.date.astype(str)
    tb["year"] = tb["date"]
    # ZERO_DATE = tb["date"].min()
    # tb = to_grapher_date(tb, ZERO_DATE)
    # Select out only variables that we care about
    tb_test = tb[["country", "year", "inf_tested", "case_info"]].dropna(subset="inf_tested").copy()
    tb_test = tb_test.format(["country", "year", "case_info"], short_name="flu_test")
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_test], check_variables_metadata=True)

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def to_grapher_date(tb: Table, zero_day: str) -> Table:
    """Modify date so Grapher understands it."""
    # Get new 'date', drop old date
    tb["year"] = (pd.to_datetime(tb["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime(zero_day)).dt.days
    tb = tb.drop(columns=["date"])
    return tb
