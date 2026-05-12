"""Load a meadow dataset and create a garden dataset."""

import numpy as np

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_meadow = paths.load_dataset("flu_vaccine_policy")
    tb = ds_meadow.read("flu_vaccine_policy")

    # Cast all columns to string for cleaning, then restore the int year.
    tb = tb.astype(str).astype({"year": int})

    tb = paths.regions.harmonize_names(
        tb,
        country_col="country",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    )

    # Replace status/sentinel codes with NaN or descriptive values.
    tb = tb.replace({"ND": np.nan, "nan": np.nan, "<NA>": np.nan, "NR": "Not relevant", "Unknown": np.nan})

    # Strip commas/text from the doses column so it can be parsed as a number.
    tb["how_many_doses_of_influenza_vaccine_were_distributed"] = tb[
        "how_many_doses_of_influenza_vaccine_were_distributed"
    ].str.replace(r"[^0-9\.]", "", regex=True)

    tb = _clean_binary_columns(tb)
    tb = _clean_hemisphere_formulation(tb)
    tb = _remove_erroneous_zeros(tb)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def _clean_binary_columns(tb: Table) -> Table:
    """Restrict binary columns (is_/are_/were_) to 'Yes' / 'No' / NaN."""
    binary_cols = tb.columns[tb.columns.str.startswith(("is", "are", "were"))]
    for col in binary_cols:
        tb[col] = tb[col].where(tb[col].isin(["Yes", "No"]), np.nan)
    return tb


def _clean_hemisphere_formulation(tb: Table) -> Table:
    """Normalise hemisphere labels."""
    tb["what_vaccine_formulation_is_used"] = tb["what_vaccine_formulation_is_used"].replace(
        {"Northern hemisphere": "Northern Hemisphere", "Hemisferio Sur": "Southern Hemisphere"}
    )
    expected = {"Not relevant", "Both", "Northern Hemisphere", "Southern Hemisphere"}
    actual = set(tb["what_vaccine_formulation_is_used"].dropna().unique())
    unexpected = actual - expected
    assert not unexpected, f"Unexpected hemisphere values: {unexpected}"
    return tb


def _remove_erroneous_zeros(tb: Table) -> Table:
    """Zeros in non-numeric columns are ambiguous — drop them."""
    cols = tb.columns.drop("how_many_doses_of_influenza_vaccine_were_distributed")
    tb[cols] = tb[cols].replace(0, np.nan)
    return tb
