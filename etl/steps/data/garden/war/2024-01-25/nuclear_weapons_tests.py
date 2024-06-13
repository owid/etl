"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def extract_year_ranges(years_ranges, year_last):
    # Extract years from a string that contains a list of years and year ranges.
    # Examples: "1964-66,72-75,80-", "1980-", "1953-62,82-87", "1970-2003", "1975-90", "2002-07".
    years = []
    for years_range in years_ranges.split(","):
        if "-" in years_range:
            start, end = years_range.split("-")
            if end.lower() == "present":
                end = year_last
            years.extend(list(range(int(start), int(end) + 1)))
        else:
            years.append(int(years_range))

    return years


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("nuclear_weapons_tests")
    tb = ds_meadow["nuclear_weapons_tests"].reset_index()

    #
    # Process data.
    #
    # By looking at the original table, it seems clear that empty cells mean zero.
    tb = tb.astype(object).fillna(0)

    # Temporarily convert all columns to string (to avoid issues with categorical variables).
    tb = tb.astype(str)

    # Remove thousands separator, e.g. 1,000 -> 1000.
    tb = tb.replace(",", "", regex=True)

    # Remove row that contains the total number of tests (and keep it for a later sanity check).
    tb_total = tb[tb["year"] == "Total"].copy()
    tb = tb[tb["year"] != "Total"].reset_index(drop=True)

    # Find out the last informed year in the data. This should be the last full year before the last publication date.
    year_last = int(tb["year"].metadata.origins[0].date_published.split("-")[0]) - 1

    # Adapt years column, so that they are integers, and not ranges, e.g. 2018-2019.
    tb["year"] = tb["year"].astype(str).apply(extract_year_ranges, year_last=year_last)
    tb = tb.explode("year").reset_index(drop=True)

    # All columns should be integers.
    tb = tb.astype(str).astype(int)

    # Sanity check.
    error = "The yearly total number of tests should be equal to the value in the 'total' column."
    assert tb[tb.drop(columns=["year", "total"]).columns].sum(axis=1).equals(tb["total"]), error
    error = "The last row (called 'total') should be equal to the sum of all other rows."
    assert (
        tb_total.drop(columns=["year", "total"]).astype(int).iloc[0] == tb.drop(columns=["year", "total"]).sum(axis=0)
    ).all(), error

    # Remove 'total' column.
    tb = tb.drop(columns=["total"])

    # Transpose table to have a country column.
    tb = tb.melt(id_vars=["year"], var_name="country", value_name="nuclear_weapons_tests")

    # Harmonize country names.
    tb["country"] = tb["country"].str.replace("__", "_").str.replace("_", " ").str.title()
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
