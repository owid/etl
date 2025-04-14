"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("world_bank_pip")

    # Read tables from meadow dataset.
    # Key indicators
    tb = ds_meadow.read("world_bank_pip")

    # Percentiles
    tb_percentiles = ds_meadow.read("world_bank_pip_percentiles")

    # Region definitions
    tb_region_definitions = ds_meadow.read("world_bank_pip_regions")

    tb = make_shares_and_thresholds_long(tb=tb)

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def show_not_dimensional_data_once(tb: Table) -> Table:
    """
    Make all the columns that are not dimensional (do not depend on dimensions) to be shown once.
    """

    return tb


def make_shares_and_thresholds_long(tb: Table) -> Table:
    """
    Convert decile1, ..., decile10 and decile1_thr, ..., decile9_thr to a long format.
    """
    tb = tb.copy()

    # Define index columns
    index_columns = ["country", "year", "reporting_level", "welfare_type", "ppp_version"]

    # Define share columns
    share_columns = [f"decile{i}" for i in range(1, 11)]
    tb_share = tb.melt(
        id_vars=index_columns,
        value_vars=share_columns,
        var_name="decile",
        value_name="share",
    )

    # Add an empty poverty_line column
    tb_share["poverty_line"] = None

    # Define threshold columns
    thr_columns = [f"decile{i}_thr" for i in range(1, 10)]
    tb_thr = tb.melt(
        id_vars=index_columns,
        value_vars=thr_columns,
        var_name="decile",
        value_name="thr",
    )

    # Add an empty poverty_line column
    tb_thr["poverty_line"] = None

    # Create an empty decile column in tb
    tb["decile"] = None

    # Merge tb and tb_share
    tb = pr.merge(tb, tb_share, on=index_columns + ["decile", "poverty_line"], how="outer")

    # Merge tb and tb_thr
    tb = pr.merge(tb, tb_thr, on=index_columns + ["decile", "poverty_line"], how="outer")

    # Remove share_columns and threshold_columns
    tb = tb.drop(columns=share_columns + thr_columns)

    # Remove "decile" from the decile column
    tb["decile"] = tb["decile"].str.replace("decile", "")

    # Do the same with "_thr"
    tb["decile"] = tb["decile"].str.replace("_thr", "")

    return tb
