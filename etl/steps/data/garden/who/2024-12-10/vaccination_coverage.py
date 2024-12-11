"""Load a meadow dataset and create a garden dataset."""

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
    ds_meadow = paths.load_dataset("vaccination_coverage")

    # Read table from meadow dataset.
    tb = ds_meadow.read("vaccination_coverage")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = use_only_wuenic_data(tb)

    # antigen_dict = (
    #    tb[["antigen", "antigen_description"]].drop_duplicates().set_index("antigen")["antigen_description"].to_dict()
    # )

    tb = clean_data(tb)

    tb = tb.format(["country", "year", "antigen_description"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def use_only_wuenic_data(tb: Table) -> Table:
    """
    Keep only data that is from WUENIC - estimated by World Health Organization and UNICEF.
    """
    assert "WUENIC" in tb["coverage_category"].unique(), "No data from WUENIC in the table."
    tb = tb[tb["coverage_category"] == "WUENIC"]
    tb = tb.drop(columns=["coverage_category"])
    return tb


def clean_data(tb: Table) -> Table:
    """
    Clean up the data:
    - Remove rows where coverage is NA
    - Remove unneeded columns
    """

    tb = tb[tb["coverage"].notna()]
    tb = tb.drop(columns=["group", "code", "coverage_category_description", "target_number", "doses"])

    return tb
