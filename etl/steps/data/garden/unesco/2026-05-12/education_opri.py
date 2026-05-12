"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import VariableMeta

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("education_opri")

    # Read table from meadow dataset.
    tb = ds_meadow.read("education_opri")

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
    tb = tb.drop(columns=["indicator_id", "magnitude", "qualifier"])

    # Build long-description lookup from World Bank metadata (keyed by indicator label)
    long_definition_map = {}
    for indicator in tb["indicator_label_en"].unique():
        defn = tb_wb[tb_wb["Indicator Name"] == indicator]["Long definition"].values
        long_definition_map[indicator] = defn[0] if len(defn) > 0 else ""

    tb["long_description"] = tb["indicator_label_en"].map(long_definition_map)

    # Drop rows where the indicator has no label (new indicators not yet in label file)
    tb = tb.dropna(subset=["indicator_label_en"])

    # Compute derived columns before the metadata loop so they get metadata assigned too.
    # Convert expenditure from millions to full values (multiply by 1,000,000).
    # This is done on the long table level later (after pivot), but the labels are needed now.

    # Pivot the table to have indicators as columns
    tb_pivoted = tb.pivot(index=["country", "year"], columns="indicator_label_en", values="value")

    # Scale expenditure columns from millions to full values
    millions_cols = [col for col in tb_pivoted.columns if "constant PPP$ (millions)" in col]
    tb_pivoted[millions_cols] = tb_pivoted[millions_cols] * 1_000_000

    # Compute derived columns before the metadata loop so they receive metadata
    expenditure_enrollment_mapping = {
        "Government expenditure on pre-primary education, constant PPP$ (millions)": "Enrolment in pre-primary education, both sexes (number)",
        "Government expenditure on primary education, constant PPP$ (millions)": "Enrolment in primary education, both sexes (number)",
        "Government expenditure on lower secondary education, constant PPP$ (millions)": "Enrolment in lower secondary education, both sexes (number)",
        "Government expenditure on upper secondary education, constant PPP$ (millions)": "Enrolment in upper secondary education, both sexes (number)",
        "Government expenditure on tertiary education, constant PPP$ (millions)": "Enrolment in tertiary education, all programmes, both sexes (number)",
    }

    expenditure_cols = [col for col in expenditure_enrollment_mapping if col in tb_pivoted.columns]
    enrollment_cols = [v for k, v in expenditure_enrollment_mapping.items() if k in tb_pivoted.columns and v in tb_pivoted.columns]

    if expenditure_cols and enrollment_cols:
        tb_pivoted["Enrolment in education, total across all levels (number)"] = tb_pivoted[enrollment_cols].sum(
            axis=1, skipna=False
        )
        total_exp_col = "Government expenditure on education, constant PPP$ (millions)"
        total_enrol_col = "Enrolment in education, total across all levels (number)"
        if total_exp_col in tb_pivoted.columns:
            tb_pivoted["Government expenditure on education per student, total across all levels (constant PPP$)"] = (
                tb_pivoted[total_exp_col] / tb_pivoted[total_enrol_col].replace(0, None)
            )

    # Assign metadata for every column (including the two derived columns above)
    long_desc_lookup = tb.set_index("indicator_label_en")["long_description"]
    for column in tb_pivoted.columns:
        meta = tb_pivoted[column].metadata
        meta.display = {}
        meta.title = column
        # Use WB long definition when available; derived columns won't have one
        if column in long_desc_lookup.index:
            meta.description_from_producer = long_desc_lookup[column].iloc[0] if hasattr(long_desc_lookup[column], "iloc") else long_desc_lookup[column]
        decimals, unit, short_unit = _unit_info(column)
        update_metadata(meta, display_decimals=decimals, unit=unit, short_unit=short_unit)

    tb_pivoted = tb_pivoted.reset_index()

    # Remove 2023 data point for Sierra Leone for specific government expenditure indicators (outlier data)
    outlier_indicators = [
        "Government expenditure on primary education as a percentage of GDP (%)",
        "Government expenditure on lower secondary education as a percentage of GDP (%)",
        "Government expenditure on upper secondary education as a percentage of GDP (%)",
        "Government expenditure on tertiary education as a percentage of GDP (%)",
    ]
    columns = tb_pivoted.columns.intersection(outlier_indicators)
    mask = (tb_pivoted["country"] == "Sierra Leone") & (tb_pivoted["year"] == 2023)
    tb_pivoted.loc[mask, columns] = None

    tb_pivoted = tb_pivoted.format(["country", "year"])

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_pivoted], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def _unit_info(column: str) -> tuple:
    """Return (display_decimals, unit, short_unit) from an indicator label.

    Covers all unit suffixes observed in the UNESCO OPRI dataset.
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
