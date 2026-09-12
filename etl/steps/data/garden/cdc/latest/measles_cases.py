"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# The CDC's JSON has one record per year, but since 2026-09-10 it leads with a record that has no
# `year` key at all (`{"cases": "5", "filter": "1985-Present*"}`), which pandas reads as a missing
# year. There is nowhere to plot it, and every year from 1985 to the present is already reported by
# its own record, so we drop it. The bound is there so that a producer-side change that starts
# stripping years from real records fails the step instead of silently shrinking the series.
MAX_RECORDS_WITHOUT_YEAR = 1


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("measles_cases")

    # Read table from meadow dataset.
    tb = ds_meadow.read("measles_cases")
    tb = tb[tb["filter"] == "1985-Present*"]
    assert tb["filter"].unique() == ["1985-Present*"]
    tb = tb.drop(columns=["filter", "filter_esp"])
    tb = drop_records_without_year(tb)
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def drop_records_without_year(tb: Table) -> Table:
    """Drop CDC records that carry no year, and check there are no more of them than expected."""
    without_year = tb["year"].isna()
    assert without_year.sum() <= MAX_RECORDS_WITHOUT_YEAR, (
        f"{without_year.sum()} CDC records have no year, expected at most {MAX_RECORDS_WITHOUT_YEAR}. "
        f"Check whether the producer changed the shape of the file before raising this bound."
    )
    return tb[~without_year]
