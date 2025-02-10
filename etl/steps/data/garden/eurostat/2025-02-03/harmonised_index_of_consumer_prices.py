"""Load a meadow dataset and create a garden dataset."""

import re

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to keep and how to rename them.
COLUMNS = {
    "coicop": "classification",
    "geo": "country",
    "time": "date",
    "value": "hicp",
}

# NOTE: The description of the flags is given below the main table in
# https://ec.europa.eu/eurostat/databrowser/view/prc_hicp_midx__custom_13200491/default/table?lang=en
# (b) break in time series
# (u) low reliability
# (d) definition differs (see metadata)
FLAGS = {"b": "break in time series", "u": "low reliability", "d": "definition differs"}


def sanity_check_outputs(tb: Table) -> None:
    assert tb["hicp"].notnull().all(), "Some values are missing."
    assert (tb["hicp"] >= 0).all(), "Negative values are not allowed."
    # Extract the base year from the metadata.
    base_year = re.search(r"\b(20\d{2}|19\d{2})\b", tb["hicp"].metadata.description_short)
    assert base_year is not None, "Base year not found in the meadow tb['hicp'].metadata.description_short."
    base_year = base_year.group(0)
    # For all classifications, check that the average HICP in the base year is 100
    # (only for countries that have 12 months in that year for that classification).
    selection = tb["date"].str.startswith(base_year)
    countries_completed = (
        tb[selection].groupby(["country", "classification"], as_index=False, observed=True).agg({"hicp": "count"})
    )
    countries_completed = countries_completed[countries_completed["hicp"] == 12][
        ["classification", "country"]
    ].drop_duplicates()
    check = tb.merge(countries_completed, on=["classification", "country"], how="inner")
    mean_hicp = (
        check[(check["date"].str.startswith(base_year))]
        .groupby(["country", "classification"], as_index=False, observed=True)
        .agg({"hicp": "mean"})
    )
    error = f"The mean HICPC of each country (and classification) does not equal 100 in {base_year}."
    assert (mean_hicp["hicp"].round(1) == 100).all(), error


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its table.
    ds_meadow = paths.load_dataset("harmonised_index_of_consumer_prices")
    tb = ds_meadow.read("harmonised_index_of_consumer_prices")

    #
    # Process data.
    #
    # Select relevant dataset codes, and add a column with the dataset name.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Separate flags from values.
    tb["flag"] = tb["hicp"].astype("string").str.extract(r"([a-z]+)", expand=False)
    tb["flag"] = tb["flag"].map(FLAGS).fillna("")
    tb["hicp"] = tb["hicp"].str.replace(r"[a-z]", "", regex=True).str.strip().astype("Float64")

    # Run sanity checks on outputs.
    sanity_check_outputs(tb=tb)

    # Improve main table format.
    tb = tb.format(keys=["country", "date", "classification"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
