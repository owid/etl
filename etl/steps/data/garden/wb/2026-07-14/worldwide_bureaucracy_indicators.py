"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Coverage floors, grounded in WWBI version 6 (2025-02-28): 202 economies × 302 indicators.
# A drop below these values on a future release usually signals a parsing or mapping regression.
EXPECTED_MIN_COUNTRIES = 202
EXPECTED_MIN_INDICATORS = 302


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("worldwide_bureaucracy_indicators")

    # Read tables from meadow dataset.
    tb = ds_meadow.read("worldwide_bureaucracy_indicators")
    tb_series = ds_meadow.read("series_metadata")

    sanity_check_inputs(tb)

    #
    # Process data.
    #
    tb, indicator_dict = reformat_table(tb)

    tb = paths.regions.harmonize_names(tb)

    definitions_dict = extract_producer_definitions(tb_series)

    tb = add_metadata_from_dict(tb, indicator_dict, definitions_dict)

    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def reformat_table(tb: Table) -> tuple[Table, dict]:
    """Reformat table to wide format and drop columns"""

    # Rename columns
    tb = tb.rename(columns={"country_name": "country"})

    # Make indicator_code snake_case
    tb["indicator_code"] = tb["indicator_code"].str.lower().str.replace(".", "_", regex=False)

    # Save a dictionary with the unique values of the indicator_code and the corresponding indicator_name
    indicator_dict = (
        tb[["indicator_code", "indicator_name"]]
        .drop_duplicates()
        .set_index("indicator_code")
        .to_dict()["indicator_name"]
    )

    # Drop columns
    tb = tb.drop(columns=["country_code", "indicator_name"])

    # Make the table long
    tb = tb.melt(id_vars=["country", "indicator_code"], var_name="year", value_name="value")

    # Make table wide again, with the indicator_code as columns
    tb = tb.pivot(index=["country", "year"], columns="indicator_code", values="value").reset_index()

    # Strip the underscore prefix that column underscoring adds to numeric year columns
    tb["year"] = tb["year"].str.replace("_", "").astype(int)

    return tb, indicator_dict


def extract_producer_definitions(tb_series: Table) -> dict:
    """Map indicator codes to the producer's per-indicator metadata from the WWBISeries.csv file.

    Composes the long definition and the producer's source note. The remaining populated fields are
    covered elsewhere or not applicable: "Unit of measure" feeds unit/short_unit in
    add_metadata_from_dict, "Statistical concept and methodology" is a single dataset-level paragraph
    already carried by the origin description, "License Type" lives in the origin license, and
    "Aggregation method" describes the producer's own regional aggregates, which this dataset does not include.
    """

    tb_series = tb_series.copy()
    tb_series["series_code"] = tb_series["series_code"].str.lower().str.replace(".", "_", regex=False)

    # Normalize whitespace in the producer text (some definitions contain double spaces).
    for col in ["long_definition", "source"]:
        tb_series[col] = tb_series[col].str.replace(r"\s+", " ", regex=True).str.strip()

    definitions = {}
    for row in tb_series.itertuples():
        parts = []
        if pd.notna(row.long_definition) and row.long_definition:
            text = str(row.long_definition)
            if not text.endswith("."):
                text += "."
            parts.append(text)
        if pd.notna(row.source) and row.source:
            parts.append(f"Source: {row.source}")
        if parts:
            definitions[row.series_code] = "\n\n".join(parts)

    return definitions


def add_metadata_from_dict(tb: Table, indicator_dict: dict, definitions_dict: dict) -> Table:
    """
    Add metadata to the table from a dictionary.
    The dictionary comes from the previous function, extracted from indicator_code and indicator_name.
    With the title available, we can multiply shares by 100 to have them in percentages.
    """

    for col in indicator_dict:
        meta_title = indicator_dict[col]
        tb[col].metadata.title = meta_title

        if col in definitions_dict:
            tb[col].metadata.description_from_producer = definitions_dict[col]

        # Add units depending on the text of the indicator
        if "share" in meta_title.lower() or "percentage" in meta_title.lower():
            tb[col].metadata.unit = "%"
            tb[col].metadata.short_unit = "%"

            # "share"-titled indicators are stored as fractions in the source and must be scaled to
            # percentages. Indicators titled "as a percentage of ..." (the two wage-bill ratios) already
            # come in percent — scaling them again would inflate them 100-fold, which is exactly what
            # versions of this step up to 2023-11-21 did.
            if "percentage" not in meta_title.lower():
                tb[col] *= 100

        elif "number" in meta_title.lower():
            tb[col].metadata.unit = "employees"
            tb[col].metadata.short_unit = ""

        elif "mean age" in meta_title.lower() or "median age" in meta_title.lower():
            tb[col].metadata.unit = "years"
            tb[col].metadata.short_unit = ""

        else:
            tb[col].metadata.unit = ""
            tb[col].metadata.short_unit = ""

    return tb


def sanity_check_inputs(tb: Table) -> None:
    id_cols = ["country_name", "country_code", "indicator_code", "indicator_name"]
    missing = set(id_cols) - set(tb.columns)
    assert not missing, f"Meadow table is missing expected id columns: {missing}"

    assert not tb.duplicated(subset=["country_name", "indicator_code"]).any(), (
        "Duplicate (country, indicator) rows in meadow table."
    )
    assert tb["country_name"].nunique() >= EXPECTED_MIN_COUNTRIES, (
        f"Country coverage shrank: {tb['country_name'].nunique()} < {EXPECTED_MIN_COUNTRIES}."
    )
    assert tb["indicator_code"].nunique() >= EXPECTED_MIN_INDICATORS, (
        f"Indicator coverage shrank: {tb['indicator_code'].nunique()} < {EXPECTED_MIN_INDICATORS}."
    )

    year_cols = [c for c in tb.columns if c not in id_cols]
    titles = tb["indicator_name"].str.lower()

    # "share"-titled indicators come as fractions in the source; a value above 1 means the producer
    # switched conventions and the x100 scaling in add_metadata_from_dict would become wrong.
    share_vals = tb.loc[titles.str.contains("share") & ~titles.str.contains("percentage"), year_cols]
    assert share_vals.min().min() >= 0, "Negative value found in a share indicator."
    assert share_vals.max().max() <= 1 + 1e-6, (
        "A 'share'-titled indicator exceeds 1 in the raw data — the source may now ship percentages, "
        "which would make the x100 scaling in add_metadata_from_dict wrong."
    )

    # "percentage"-titled indicators (the two wage-bill ratios) come already in percent. If the source
    # switched them to fractions, their maximum would drop to ~1 (observed maximum in percent: ~100.6).
    pct_vals = tb.loc[titles.str.contains("percentage"), year_cols]
    assert pct_vals.min().min() >= 0, "Negative value found in a percentage indicator."
    assert 2 < pct_vals.max().max() <= 150, (
        "A 'percentage'-titled indicator is outside (2, 150] in the raw data — the source may have "
        "switched to fractions, which would require scaling it in add_metadata_from_dict."
    )


def sanity_check_outputs(tb: Table) -> None:
    fully_nan = [c for c in tb.columns if tb[c].isna().all()]
    assert not fully_nan, f"Output has fully-NaN columns: {fully_nan}"

    assert tb["year"].between(2000, 2100).all(), "Year outside the expected range."

    # After scaling, all %-unit indicators should be percentages. The observed maximum is ~100.6
    # (Sao Tome and Principe's wage bill as a percentage of public expenditure in 2000); a value
    # above 150 almost certainly means the fraction-vs-percent scaling regressed.
    pct_cols = [c for c in tb.columns if tb[c].metadata.unit == "%"]
    pct_min = tb[pct_cols].min().min()
    pct_max = tb[pct_cols].max().max()
    assert pct_min >= 0, f"Negative value ({pct_min}) in a percentage indicator."
    assert pct_max <= 150, f"Percentage indicator reaches {pct_max} — fraction-vs-percent scaling likely regressed."

    # Head counts can't be negative.
    emp_cols = [c for c in tb.columns if tb[c].metadata.unit == "employees"]
    assert tb[emp_cols].min().min() >= 0, "Negative value in an employee-count indicator."

    # Mean/median ages of the workforce should be within working-age bounds (observed 19-83).
    age_cols = [c for c in tb.columns if tb[c].metadata.unit == "years"]
    age_min = tb[age_cols].min().min()
    age_max = tb[age_cols].max().max()
    assert age_min >= 15 and age_max <= 100, f"Workforce age outside [15, 100]: min {age_min}, max {age_max}."
