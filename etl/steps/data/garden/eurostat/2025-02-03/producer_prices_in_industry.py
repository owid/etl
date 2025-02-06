"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to keep and how to rename them.
COLUMNS = {
    # Statistical classification of economic activities in the European Community (NACE Rev. 2)
    "nace_r2": "classification",
    "geo": "country",
    "time": "date",
    "value": "value",
}

# NOTE: The description of the flags is given below the main table in
# https://ec.europa.eu/eurostat/databrowser/view/sts_inpp_m__custom_15280604/default/table?lang=en
# (de) definition differs (see metadata), estimated
# (p) provisional
# (i) value imputed by Eurostat or other receiving agencies
# (e) estimated
FLAGS = {
    "e": "estimated",
    "i": "value imputed by Eurostat or other receiving agencies",
    "p": "provisional",
    "ip": "provisional; value imputed by Eurostat or other receiving agencies",
}


def sanity_check_outputs(tb: Table) -> None:
    assert tb["value"].notnull().all(), "Some values are missing."
    assert (tb["value"] >= 0).all(), "Negative values are not allowed."


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its table.
    ds_meadow = paths.load_dataset("producer_prices_in_industry")
    tb = ds_meadow.read("producer_prices_in_industry")

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
    tb = tb.format(keys=["country", "date", "classification"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
