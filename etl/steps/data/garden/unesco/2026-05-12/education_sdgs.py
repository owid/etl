"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table, VariableMeta

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("education_sdgs")
    ds_expenditure = paths.load_dataset("public_expenditure")

    # Read table from meadow dataset.
    tb = ds_meadow.read("education_sdgs")

    # Load historical expenditure data
    tb_expenditure = ds_expenditure.read("public_expenditure")

    # Retrieve snapshot with the metadata provided via World Bank.
    snap_wb = paths.load_snapshot("edstats_metadata.xls")
    tb_wb = snap_wb.read()

    #
    # Process data.
    #
    country_mapping_path = paths.directory / "education.countries.json"
    excluded_countries_path = paths.directory / "education.excluded_countries.json"
    tb = paths.regions.harmonize_names(
        tb, country_col="country", countries_file=country_mapping_path, excluded_countries_file=excluded_countries_path
    )
    # Drop columns that are not needed
    tb = tb.drop(columns=["magnitude", "qualifier"])

    # Build long-description lookup from World Bank metadata (keyed by indicator label)
    long_definition_map = {}
    for indicator in tb["indicator_label_en"].unique():
        defn = tb_wb[tb_wb["Indicator Name"] == indicator]["Long definition"].values
        long_definition_map[indicator] = defn[0] if len(defn) > 0 else ""

    tb["long_description"] = tb["indicator_label_en"].map(long_definition_map)

    # Drop rows with missing indicator labels
    tb = tb[tb["indicator_label_en"].notna()]
    tb["indicator_label_en"] = tb["indicator_label_en"].astype(str) + ", " + tb["indicator_id"].astype(str)

    # Pivot the table to have indicators as columns
    tb_pivoted = tb.pivot(index=["country", "year"], columns="indicator_label_en", values="value")

    # Assign metadata for every column
    long_desc_lookup = tb.set_index("indicator_label_en")["long_description"]
    for column in tb_pivoted.columns:
        meta = tb_pivoted[column].metadata
        meta.display = {}
        meta.title = column
        if column in long_desc_lookup.index:
            desc = long_desc_lookup[column]
            meta.description_from_producer = desc.iloc[0] if hasattr(desc, "iloc") else desc
        decimals, unit, short_unit = _unit_info(column)
        update_metadata(meta, display_decimals=decimals, unit=unit, short_unit=short_unit)

    tb_pivoted = tb_pivoted.reset_index()

    # Remove Turkey 1998 value for Government expenditure on education as a percentage of GDP (%), XGDP.FSGOV (likely an error)
    mask = (tb_pivoted["country"] == "Turkey") & (tb_pivoted["year"] == 1998)
    tb_pivoted.loc[mask, "Government expenditure on education as a percentage of GDP (%), XGDP.FSGOV"] = None

    tb_pivoted = tb_pivoted.format(["country", "year"])
    # Combine recent literacy estimates and expenditure data with historical estimates from a migrated dataset
    tb_pivoted = combine_historical_expenditure(tb_pivoted, tb_expenditure)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_pivoted], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def combine_historical_expenditure(tb: Table, tb_expenditure: Table) -> Table:
    """
    Merge historical and recent expenditure data into a single Table.

    This function combines data from a Table containing historical public expenditure on education
    with a primary Table. The function handles missing data by favoring recent data; if this is not available,
    it falls back to historical data, which could also be missing (NaN).

    """
    tb = tb.reset_index()

    # Historical expenditure data
    historic_expenditure = tb_expenditure[
        ["year", "country", "public_expenditure_on_education__tanzi__and__schuktnecht__2000"]
    ].copy()

    # Recent public expenditure from main table
    recent_expenditure = tb[
        ["year", "country", "government_expenditure_on_education_as_a_percentage_of_gdp__pct__xgdp_fsgov"]
    ].copy()

    # Merge historic and recent expenditure data based on 'year' and 'country'
    combined_df = pr.merge(historic_expenditure, recent_expenditure, on=["year", "country"], how="outer")

    # Combine expenditure data, favoring recent over historical.
    # Use .loc assignment to avoid unit-mismatch warnings from fillna across columns with
    # different unit metadata ('%' vs 'percent of GDP').
    combined_df["combined_expenditure_share_gdp"] = combined_df[
        "government_expenditure_on_education_as_a_percentage_of_gdp__pct__xgdp_fsgov"
    ].copy()
    mask = combined_df["combined_expenditure_share_gdp"].isna()
    combined_df.loc[mask, "combined_expenditure_share_gdp"] = combined_df.loc[
        mask, "public_expenditure_on_education__tanzi__and__schuktnecht__2000"
    ]
    combined_df["combined_expenditure_share_gdp"].metadata.unit = "%"
    combined_df["combined_expenditure_share_gdp"].metadata.short_unit = "%"
    combined_df["combined_expenditure_share_gdp"].metadata.title = "Government expenditure on education as a percentage of GDP (%)"

    # Merge the combined expenditure data back into the original table
    tb = pr.merge(
        tb,
        combined_df[["year", "country", "combined_expenditure_share_gdp"]],
        on=["year", "country"],
        how="outer",
    )

    tb = tb.format(["country", "year"])
    return tb


def _unit_info(column: str) -> tuple:
    """Return (display_decimals, unit, short_unit) from an indicator label.

    Covers all unit suffixes observed in the UNESCO SDG dataset.
    """
    col = column.lower()
    if "%" in col:
        return 1, "%", "%"
    elif "(days)" in col:
        return 1, "days", ""
    elif "(years)" in col:
        return 1, "years", ""
    elif any(pia in col for pia in ("gpia", "lpia", "wpia", "npia", "dpia", "ltpia")):
        # Gender/learning/wealth/etc. parity index — dimensionless ratio
        return 2, "index", ""
    elif "index" in col:
        return 1, "index", ""
    elif "(current us$)" in col:
        return 0, "current US$", "$"
    elif "ppp$" in col:
        return 0, "constant 2019 US$", "$"
    elif "us$" in col or " usd" in col:
        return 0, "current US$", "$"
    elif "(number)" in col:
        return 0, "number", ""
    else:
        return 0, " ", " "


def update_metadata(meta: VariableMeta, display_decimals: int, unit: str, short_unit: str) -> None:
    """Update metadata unit attributes in-place."""
    meta.display["numDecimalPlaces"] = display_decimals
    meta.unit = unit
    meta.short_unit = short_unit
