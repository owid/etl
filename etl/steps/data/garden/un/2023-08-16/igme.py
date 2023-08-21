"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("igme"))

    # Read table from meadow dataset.
    tb = ds_meadow["igme"].reset_index()

    #
    # Process data.
    #
    tb = fix_sub_saharan_africa(tb)
    tb: Table = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def fix_sub_saharan_africa(tb: Table) -> Table:
    """
    Sub-Saharan Africa appears twice in the Table, as it is defined by two different organisations, UNICEF and SDG.
    This function clarifies this by combining the region and organisation into one.
    """
    tb["country"] = tb["country"].astype(str)

    tb.loc[
        (tb["country"] == "Sub-Saharan Africa") & (tb["regional_group"] == "UNICEF"), "country"
    ] = "Sub-Saharan Africa (UNICEF)"

    tb.loc[
        (tb["country"] == "Sub-Saharan Africa") & (tb["regional_group"] == "SDG"), "country"
    ] = "Sub-Saharan Africa (SDG)"

    return tb


def filter_data(tb: Table) -> Table:
    """
    Filtering out the unnecessary columns and rows from the data.
    We just want the UN IGME estimates, rather than the individual results from the survey data.
    """
    # Keeping only the UN IGME estimates.
    tb = tb.loc[tb["series_name"] == "UN IGME estimate"]

    # Removing the unnecessary columns.
    tb.drop(
        columns=[
            "series_name",
            "regional_group",
            "series_year",
            "time_period",
            "country_notes",
            "connection",
            "status",
            "year_to_achieve",
            "model_used",
            "age_group_of_women",
            "series_method",
            "definition",
            "interval",
        ]
    )
