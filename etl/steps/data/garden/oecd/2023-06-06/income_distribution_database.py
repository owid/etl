"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def process_data(tb: Table) -> Table:
    """
    Removes outliers and pivots table to wide format
    """

    # Check if all values are positive
    negative_values = tb[tb["value"] < 0]

    if len(negative_values) > 0:
        log.warning(f"There are {len(negative_values)} negative values and will be removed:\n {negative_values}")
        tb = tb[~(tb["value"] < 0)].reset_index(drop=True)

    # Check if gini values are all below 1
    # There is currently an issue with the value of GINIG for Latvia in 2021, so I first correct it manually dividing by 100
    latvia_point = tb.loc[
        (tb["country"] == "Latvia") & (tb["year"] == 2021) & (tb["measure"] == "GINIG"), "value"
    ].values[0]
    if latvia_point > 1:
        tb.loc[(tb["country"] == "Latvia") & (tb["year"] == 2021) & (tb["measure"] == "GINIG"), "value"] = (
            latvia_point / 100
        )

    gini_outliers = tb[(tb["measure"].str.contains("GINI")) & (tb["value"] > 1)]

    if len(gini_outliers) > 0:
        log.warning(f"There are {len(gini_outliers)} Gini values greater than 1 and will be removed:\n {gini_outliers}")
        tb = tb[~((tb["measure"].str.contains("GINI")) & (tb["value"] > 1))].reset_index(drop=True)

    # Drop values with the flag "post taxes and before transfers"
    # These are values assigned into the before taxes and transfers category which might generate confusion
    tb = tb[~((tb["measure"] == "GINIB") & (tb["flags"] == "post taxes and before transfers"))].reset_index(drop=True)

    # Drop flags column
    tb = tb.drop(columns=["flags"])

    # Make dataframe wide
    tb = tb.pivot(index=["country", "year"], columns="measure", values="value").reset_index()

    return tb


def run(dest_dir: str) -> None:
    log.info("income_distribution_database.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("income_distribution_database"))

    # Read table from meadow dataset.
    tb = ds_meadow["income_distribution_database"]

    #
    # Process data.
    tb = process_data(tb)

    log.info("income_distribution_database.harmonize_countries")
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb.metadata.short_name = "income_distribution_database"

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("income_distribution_database.end")
