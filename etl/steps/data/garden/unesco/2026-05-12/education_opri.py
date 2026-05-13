"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import VariableMeta

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Curated titles for indicators that have human-readable labels different from the raw UNESCO label.
# Keys are the raw UNESCO indicator labels (as they appear as column names after the pivot).
_TITLE_OVERRIDES = {
    "All staff compensation as a percentage of total expenditure in primary public institutions (%)": "Share of total public education spending allocated to staff compensation in primary education",
    "All staff compensation as a percentage of total expenditure in public institutions (%)": "Share of total public education spending allocated to staff compensation",
    "Duration of compulsory education (years)": "Duration of compulsory education",
    "Government expenditure on education, constant PPP$ (millions)": "Government spending on education",
    "Government expenditure on education, PPP$ (millions)": "Government spending on education in purchasing power parity (PPP) dollars",
    "Government expenditure on education per student, total across all levels (constant PPP$)": "Government spending on education per student across all levels",
    "Government expenditure on lower secondary education, constant PPP$ (millions)": "Government spending on lower secondary education",
    "Government expenditure on lower secondary education as a percentage of GDP (%)": "Government spending on lower secondary education as share of GDP",
    "Government expenditure on pre-primary education, constant PPP$ (millions)": "Government spending on pre-primary education",
    "Government expenditure on pre-primary education as a percentage of GDP (%)": "Government spending on pre-primary education as share of GDP",
    "Government expenditure on primary education, constant PPP$ (millions)": "Government spending on primary education",
    "Government expenditure on primary education as a percentage of GDP (%)": "Government spending on primary education as share of GDP",
    "Government expenditure on secondary education as a percentage of GDP (%)": "Government spending on secondary education as share of GDP",
    "Government expenditure on tertiary education, constant PPP$ (millions)": "Government spending on tertiary education",
    "Government expenditure on tertiary education as a percentage of GDP (%)": "Government spending on tertiary education as share of GDP",
    "Government expenditure on upper secondary education, constant PPP$ (millions)": "Government spending on upper secondary education",
    "Government expenditure on upper secondary education as a percentage of GDP (%)": "Government spending on upper secondary education as share of GDP",
    "Gross enrolment ratio, lower secondary, both sexes (%)": "Gross enrollment ratio in lower secondary education",
    "Gross enrolment ratio, lower secondary, female (%)": "Gross enrollment ratio in lower secondary education among girls",
    "Gross enrolment ratio, lower secondary, male (%)": "Gross enrollment ratio in lower secondary education among boys",
    "Gross enrolment ratio, primary, both sexes (%)": "Gross enrollment ratio in primary education",
    "Gross enrolment ratio, primary, female (%)": "Gross enrollment ratio in primary education among girls",
    "Gross enrolment ratio, primary, male (%)": "Gross enrollment ratio in primary education among boys",
    "Gross enrolment ratio, secondary, both sexes (%)": "Gross enrollment ratio in secondary education",
    "Gross enrolment ratio, upper secondary, both sexes (%)": "Gross enrollment ratio in upper secondary education",
    "Gross enrolment ratio, upper secondary, female (%)": "Gross enrollment ratio in upper secondary education among girls",
    "Gross enrolment ratio, upper secondary, male (%)": "Gross enrollment ratio in upper secondary education among boys",
    "Inbound mobility rate, both sexes (UIS estimate) (%)": "Share of students from abroad",
    "Mean years of schooling (ISCED 1 or higher), population 25+ years, adjusted gender parity index (GPIA)": "Average years of schooling, adjusted gender parity index",
    "Mean years of schooling (ISCED 1 or higher), population 25+ years, female": "Average years of schooling for women",
    "Mean years of schooling (ISCED 1 or higher), population 25+ years, male": "Average years of schooling for men",
    "Official entrance age to compulsory education (years)": "Official entrance age to compulsory education",
    "Official entrance age to pre-primary education (years)": "Official entrance age to pre-primary education",
    "Out-of-school adolescents of lower secondary school age, both sexes (number)": "Out-of-school children of lower secondary school age",
    "Out-of-school adolescents of lower secondary school age, female (number)": "Out-of-school girls of lower secondary school age",
    "Out-of-school adolescents of lower secondary school age, male (number)": "Out-of-school boys of lower secondary school age",
    "Out-of-school children, one year before the official primary entry age, both sexes (number)": "Out-of-school children one year before official primary entry age",
    "Out-of-school children, one year before the official primary entry age, female (number)": "Out-of-school girls one year before official primary entry age",
    "Out-of-school children, one year before the official primary entry age, male (number)": "Out-of-school boys one year before official primary entry age",
    "Out-of-school children of primary school age, both sexes (number)": "Out-of-school children of primary school age",
    "Out-of-school children of primary school age, female (number)": "Out-of-school girls of primary school age",
    "Out-of-school children of primary school age, male (number)": "Out-of-school boys of primary school age",
    "Out-of-school youth of upper secondary school age, both sexes (number)": "Out-of-school children of upper secondary school age",
    "Out-of-school youth of upper secondary school age, female (number)": "Out-of-school girls of upper secondary school age",
    "Out-of-school youth of upper secondary school age, male (number)": "Out-of-school boys of upper secondary school age",
    "Outbound mobility ratio, all regions, both sexes (UIS estimate) (%)": "Share of students studying abroad",
    "Percentage of enrolment in pre-primary education in private institutions, both sexes (%)": "Percentage of enrollment in pre-primary education in private institutions",
    "Percentage of enrolment in primary education in private institutions, both sexes (%)": "Percentage of enrollment in primary education in private institutions",
    "Percentage of teachers in primary education who are female (%)": "Share of primary school teachers who are women",
    "Percentage of teachers in secondary education who are female (%)": "Share of secondary school teachers who are women",
    "Percentage of teachers in tertiary education who are female (%)": "Share of tertiary school teachers who are women",
    "School life expectancy, pre-primary, both sexes (years)": "School life expectancy in pre-primary education",
    "School life expectancy, pre-primary, female (years)": "School life expectancy in pre-primary education among girls",
    "School life expectancy, pre-primary, male (years)": "School life expectancy in pre-primary education among boys",
    "School life expectancy, primary, adjusted gender parity index (GPIA)": "School life expectancy in primary education, adjusted gender parity index",
    "School life expectancy, primary, both sexes (years)": "School life expectancy in primary education",
    "School life expectancy, primary, female (years)": "School life expectancy in primary education among girls",
    "School life expectancy, primary, male (years)": "School life expectancy in primary education among boys",
    "School life expectancy, secondary, both sexes (years)": "School life expectancy in secondary education",
    "School life expectancy, secondary, female (years)": "School life expectancy in secondary education among girls",
    "School life expectancy, secondary, male (years)": "School life expectancy in secondary education among boys",
    "School life expectancy, tertiary, both sexes (years)": "School life expectancy in tertiary education",
    "School life expectancy, tertiary, female (years)": "School life expectancy in tertiary education among girls",
    "School life expectancy, tertiary, male (years)": "School life expectancy in tertiary education among boys",
    "Theoretical duration of pre-primary education (years)": "Theoretical duration of pre-primary education",
    "Total net attendance rate, primary, both sexes (%)": "Total net attendance rate in primary education",
    "Total net enrolment rate, lower secondary, adjusted gender parity index (GPIA)": "Net enrollment rates in lower secondary education, adjusted gender parity index",
    "Total net enrolment rate, lower secondary, both sexes (%)": "Net enrollment rate in lower secondary education",
    "Total net enrolment rate, lower secondary, female (%)": "Net enrollment rate in lower secondary education among girls",
    "Total net enrolment rate, lower secondary, male (%)": "Net enrollment rate in lower secondary education among boys",
    "Total net enrolment rate, primary, adjusted gender parity index (GPIA)": "Net enrollment rates in primary education, adjusted gender parity index",
    "Total net enrolment rate, primary, both sexes (%)": "Net enrollment rate in primary education",
    "Total net enrolment rate, primary, female (%)": "Net enrollment rate in primary education among girls",
    "Total net enrolment rate, primary, male (%)": "Net enrollment rate in primary education among boys",
    "Total net enrolment rate, upper secondary, both sexes (%)": "Net enrollment rate in upper secondary education",
    "Total net enrolment rate, upper secondary, female (%)": "Net enrollment rate in upper secondary education among girls",
    "Total net enrolment rate, upper secondary, male (%)": "Net enrollment rate in upper secondary education among boys",
}


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
    enrollment_cols = [
        v for k, v in expenditure_enrollment_mapping.items() if k in tb_pivoted.columns and v in tb_pivoted.columns
    ]

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
        # Apply curated title where available, otherwise use the raw indicator label
        meta.title = _TITLE_OVERRIDES.get(column, column)
        # Use WB long definition when available; derived columns won't have one
        if column in long_desc_lookup.index:
            meta.description_from_producer = (
                long_desc_lookup[column].iloc[0]
                if hasattr(long_desc_lookup[column], "iloc")
                else long_desc_lookup[column]
            )
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
