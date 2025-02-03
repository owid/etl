"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to keep and how to rename them.
COLUMNS = {
    "coicop": "coicop",
    "geo": "country",
    "time": "date",
    "value": "value",
}

# NOTE: The description of the flags is given below the main table in
# https://ec.europa.eu/eurostat/databrowser/view/prc_hicp_midx__custom_13200491/default/table?lang=en
# (b) break in time series
# (u) low reliability
# (d) definition differs (see metadata)
FLAGS = {"b": "break in time series", "u": "low reliability", "d": "definition differs"}


def sanity_check_outputs(tb: Table) -> None:
    assert tb["value"].notnull().all(), "Some values are missing."
    assert (tb["value"] >= 0).all(), "Negative values are not allowed."


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
    tb["flag"] = tb["value"].astype("string").str.extract(r"([a-z]+)", expand=False)
    tb["flag"] = tb["flag"].map(FLAGS).fillna("")
    tb["value"] = tb["value"].str.replace(r"[a-z]", "", regex=True).str.strip().astype("Float64")

    # Run sanity checks on outputs.
    sanity_check_outputs(tb=tb)

    # Improve main table format.
    tb = tb.format(keys=["country", "date", "coicop"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
