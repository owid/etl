"""Load OECD educational attainment data and splice with Lee & Lee historical estimates.

For each country with OECD data, we use OECD as far back as it goes (observed, annual).
For years before the earliest OECD observation, we fill in with Lee & Lee historical
estimates (5-year intervals, 1870-2010). For countries without any OECD data, we keep
the full Lee & Lee series up to 2010.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# The Lee & Lee column that corresponds to the OECD tertiary education share (25-64, both sexes).
LEE_LEE_TERTIARY_COL = "mf_adults__25_64_years__percentage_of_tertiary_education"


def sanity_check_inputs(tb_oecd: Table, tb_lee_lee: Table) -> None:
    assert not tb_oecd.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in OECD data."
    assert tb_oecd["share_tertiary_education"].min() >= 0, "Negative share found in OECD data."
    assert tb_oecd["share_tertiary_education"].max() <= 100, "OECD share exceeds 100%."
    assert LEE_LEE_TERTIARY_COL in tb_lee_lee.columns, f"Missing column {LEE_LEE_TERTIARY_COL} in Lee & Lee data."


def sanity_check_outputs(tb: Table) -> None:
    tb_check = tb.reset_index()
    assert not tb_check.empty, "Output table is empty."
    assert not tb_check.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in output."
    assert tb_check["share_tertiary_education"].min() >= 0, "Negative share in output."
    assert tb_check["share_tertiary_education"].max() <= 100, "Output share exceeds 100%."
    # We should have many more rows than OECD alone (~1200) thanks to Lee & Lee historical data.
    assert len(tb_check) > 1500, f"Unexpectedly few rows in output: {len(tb_check)}."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("education_attainment_distribution")
    tb_oecd = ds_meadow["education_attainment_distribution"].reset_index()

    ds_lee_lee = paths.load_dataset("education_lee_lee")
    tb_lee_lee = ds_lee_lee["education_lee_lee"].reset_index()

    sanity_check_inputs(tb_oecd, tb_lee_lee)

    #
    # Process data.
    #

    # Harmonize OECD country names.
    tb_oecd = paths.regions.harmonize_names(tb_oecd, country_col="country", countries_file=paths.country_mapping_path)

    # Extract the tertiary education share from Lee & Lee and rename to match OECD column.
    tb_ll = tb_lee_lee[["country", "year", LEE_LEE_TERTIARY_COL]].copy()
    tb_ll = tb_ll.rename(columns={LEE_LEE_TERTIARY_COL: "share_tertiary_education"})
    tb_ll = tb_ll.dropna(subset=["share_tertiary_education"])

    # For each country, find the earliest year with OECD data.
    oecd_first_year = tb_oecd.groupby("country")["year"].min().to_dict()

    # Keep Lee & Lee rows only for years before the earliest OECD data point for that country.
    # For countries not in OECD at all, keep the full Lee & Lee series.
    mask = tb_ll.apply(
        lambda row: row["country"] not in oecd_first_year or row["year"] < oecd_first_year[row["country"]],
        axis=1,
    )
    tb_ll = tb_ll[mask]

    # Combine: Lee & Lee historical + OECD observed.
    tb = pr.concat([tb_ll, tb_oecd], short_name=paths.short_name)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    sanity_check_outputs(tb)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
