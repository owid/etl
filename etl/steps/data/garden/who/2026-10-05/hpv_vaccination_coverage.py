"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
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
    # Keep only data from WUENIC (the estimates by World Health Organization and UNICEF).
    tb = use_only_hpv_data(tb)
    tb = paths.regions.harmonize_names(tb, warn_on_unused_countries=False)
    tb = clean_data(tb)
    # Add denominator column

    tb = tb.format(["country", "year", "antigen_description"], short_name="hpv_vaccination_coverage")
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def use_only_hpv_data(tb: Table) -> Table:
    """
    Keep only data that is regarding the HPV vaccine.
    """
    assert "WUENIC" in tb["coverage_category"].unique(), "No data from HPV in the table."
    tb = tb[tb["coverage_category"] == "HPV"]
    tb = tb.drop(columns=["coverage_category"])
    return tb


def clean_data(tb: Table) -> Table:
    """
    Clean up the data:
    - Remove rows where coverage is NA
    - Remove unneeded columns
    """

    tb = tb[tb["coverage"].notna()]
    tb = tb.drop(columns=["group", "code", "coverage_category_description", "target_number", "doses", "antigen"])

    return tb
