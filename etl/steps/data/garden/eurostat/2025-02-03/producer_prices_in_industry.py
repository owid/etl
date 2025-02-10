"""Load a meadow dataset and create a garden dataset."""

import re

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
    "value": "ppi",
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
    assert tb["ppi"].notnull().all(), "Some values are missing."
    assert (tb["ppi"] >= 0).all(), "Negative values are not allowed."
    # Extract the base year from the metadata.
    base_year = re.search(r"\b(20\d{2}|19\d{2})\b", tb["ppi"].metadata.description_short)
    assert base_year is not None, "Base year not found in the meadow tb['ppi'].metadata.description_short."
    base_year = base_year.group(0)
    # For all classifications, check that the average PPI in the base year is 100
    # (only for countries that have 12 months in that year for that classification).
    selection = tb["date"].str.startswith(base_year)
    countries_completed = (
        tb[selection].groupby(["country", "classification"], as_index=False, observed=True).agg({"ppi": "count"})
    )
    countries_completed = countries_completed[countries_completed["ppi"] == 12][
        ["classification", "country"]
    ].drop_duplicates()
    check = tb.merge(countries_completed, on=["classification", "country"], how="inner")
    mean_ppi = (
        check[(check["date"].str.startswith(base_year))]
        .groupby(["country", "classification"], as_index=False, observed=True)
        .agg({"ppi": "mean"})
    )
    error = f"The mean PPI of each country (and classification) does not equal 100 in {base_year}."
    # NOTE: For some reason, Hungary's PPI differs by about 2% for some classifications. Assert it separately.
    assert (mean_ppi[mean_ppi["country"] != "Hungary"]["ppi"].round(0) == 100).all(), error
    assert (abs(mean_ppi[mean_ppi["country"] == "Hungary"]["ppi"] - 100) < 3).all(), error


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
    tb["flag"] = tb["ppi"].astype("string").str.extract(r"([a-z]+)", expand=False)
    tb["flag"] = tb["flag"].map(FLAGS).fillna("")
    tb["ppi"] = tb["ppi"].str.replace(r"[a-z]", "", regex=True).str.strip().astype("Float64")

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
