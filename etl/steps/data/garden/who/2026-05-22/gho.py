"""Load a meadow dataset and create a garden dataset."""

import json
import re
from typing import Any

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
import structlog
from owid.catalog import Table, VariableMeta
from owid.catalog.meta import VariablePresentationMeta
from owid.catalog.utils import underscore
from owid.repack import repack_series

from etl.helpers import PathFinder

from .gho_omms import create_omms

log = structlog.get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SEX_MAPPING = {
    "Both sexes": "both sexes",
    "Female": "female",
    "Male": "male",
}

# only keep those sections from GHO metadata
HEADINGS_TO_USE = [
    "Rationale",
    "Definition",
    "Method of measurement",
    "Method of estimation",
]

# remove values that are considered as NaN
NAN_VALUES = [
    "No data",
    "Not applicable",
    "not applicable",
    "",
    "-",
    "—",
    ".",
    "–",
    "None",
    "none",
    "Data not available",
    "Not available",
    "Figure not available",
    "...",
    "…",
]

PRIORITY_OF_REGIONS = ["WORLDBANKREGION", "REGION", "UNICEFREGION", "UNREGION", "UNSDGREGION", "FAOREGION", "RCREGION"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gho")

    # Loop over tables in meadow dataset.
    tables = []
    log.info("gho.run.start", n_indicators=len(ds_meadow.table_names))
    for label in ds_meadow.table_names:
        log.debug("gho.run", label=label)

        # Read table from meadow dataset.
        tb = ds_meadow.read(label, safe_types=False)

        # Use safer types
        tb = tb.astype({"country": str})

        # Exclude archived indicators from garden
        if "_archived" in label or tb.m.title.endswith("(archived)"):
            continue

        # Add region suffix.
        tb = add_region_source_suffix(tb)

        #
        # Process data.
        #
        tb = paths.regions.harmonize_names(
            tb,
            warn_on_unused_countries=False,
            warn_on_unknown_excluded_countries=False,
        )

        # Drop excess region sources.
        tb = drop_excess_region_sources(tb, PRIORITY_OF_REGIONS)

        # Standardize dimension values
        if "sex" in tb.columns:
            tb.sex = tb.sex.map(SEX_MAPPING)

        # Normalize year range from xxxx-yyyy to the end of range yyyy
        if tb.year.dtype == "category":
            tb = tb.loc[tb.year.astype(str) != "nan"]
            tb["year"] = normalize_year_range(tb["year"])

        # Remove unused columns & rename
        tb = tb.drop(columns=["spatialdim", "spatialdimtype", "timedimtype", "parentlocationcode"]).rename(
            columns={"numericvalue": "numeric", "value": "display_value"}
        )

        # Set dimensions
        tb = set_dimensions(tb)

        # Check for duplicate index
        tb = check_duplicate_index(tb)

        # Clean up display value
        tb = clean_numeric_column(tb)

        # Remove display_value column
        tb = tb.drop(columns=["display_value"])

        # Change table and indicator short name
        meta = json.loads(tb.numeric.m.description_from_producer)
        assert tb.m.title
        short_name = underscore(tb.m.title)

        # Remove NaN values
        tb = remove_nans(tb)

        # Set indicator metadata
        tb = set_indicator(tb, short_name, meta)

        # Set table metadata
        tb.m.short_name = short_name

        # Check overlapping names
        check_overlapping_names(tb)

        # Special processing for statins indicator
        if "general_availability_of_statins_in_the_public_health_sector" in tb.columns:
            tb = process_statins_indicator(tb)

        tables.append(tb)

    log.info("gho.run.end", n_tables=len(tables), n_indicators=len(ds_meadow.table_names))

    # Merge identical tables
    tables = merge_identical_tables(tables)

    tables_dict = {tb.m.title: tb for tb in tables}
    assert len(tables) == len(tables_dict), "Duplicate titles in tables found"

    # Create OOMs
    create_omms(tables_dict, paths)

    # Apply per-indicator data fixes (moved here from the grapher step so the
    # garden is the canonical place for data cleaning).
    apply_indicator_fixes(tables_dict)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=tables_dict.values(),
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        errors="raise",
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def apply_indicator_fixes(tables_dict: dict[str, Table]) -> None:
    """Per-indicator data fixes.

    These previously lived in the grapher step but they're data-cleaning, not
    grapher-specific, so the canonical place for them is here. The grapher is
    left thin: skip empty tables, drop the `comments` ferry column, upload.
    """
    for title, tb in list(tables_dict.items()):
        # 1. Stunting source-data unit error: WHO labels the column "in millions"
        #    but the underlying values are in thousands. Divide by 1000 so the
        #    unit matches reality.
        col = "stunting_numbers_among_children_under_5_years_of_age__millions__model_based_estimates"
        if col in tb.columns:
            for c in [col, col + "_low", col + "_high"]:
                if c in tb.columns:
                    tb[c] /= 1000

        # 2. Road-traffic alcohol attribution ships some non-numeric placeholder
        #    values in NumericValue. Coerce to NaN so downstream code sees floats.
        col = "attribution_of_road_traffic_deaths_to_alcohol__pct"
        if col in tb.columns:
            tb[col] = pd.to_numeric(tb[col], errors="coerce").copy_metadata(tb[col])

        # 3. Drop noisy DHS/MICS subnational-region dimension entirely (we don't
        #    surface subnational breakdowns in charts).
        if "dhs_mics_subnational_regions__health_equity_monitor" in tb.index.names:
            tb = tb.query("dhs_mics_subnational_regions__health_equity_monitor.isnull()")
            tb = tb.reset_index(["dhs_mics_subnational_regions__health_equity_monitor"], drop=True)
            tables_dict[title] = tb

        # 4. Drop `ghe_cause_of_death_codes*` label columns — they're lookup
        #    metadata, not indicators, and lack unit/title/origins.
        drop_cols = [c for c in tb.columns if c.startswith("ghe_cause_of_death_codes")]
        if drop_cols:
            tables_dict[title] = tb.drop(columns=drop_cols)
            tb = tables_dict[title]

        # 5. Fix typos that WHO ships in description_from_producer across many
        #    indicators. Assert the canonical occurrences still contain the typo
        #    so we notice when WHO fixes them upstream and we can drop the patch.
        error = "Expected typo in description from producer. It may have been fixed, so, remove this patch."
        if "deaths_due_to_tuberculosis_among_hiv_negative_people__per_100_000_population" in tb.columns:
            assert (
                "Millenium"
                in tb[
                    "deaths_due_to_tuberculosis_among_hiv_negative_people__per_100_000_population"
                ].metadata.description_from_producer
            ), error
        if (
            "measles_containing_vaccine_second_dose__mcv2__immunization_coverage_by_the_nationally_recommended_age__pct"
            in tb.columns
        ):
            assert (
                "patters"
                in tb[
                    "measles_containing_vaccine_second_dose__mcv2__immunization_coverage_by_the_nationally_recommended_age__pct"
                ].metadata.description_from_producer
            ), error
        for column in tb.columns:
            dfp = tb[column].metadata.description_from_producer
            if dfp and "Millenium" in dfp:
                tb[column].metadata.description_from_producer = dfp.replace("Millenium", "Millennium")
            if dfp and "patters" in dfp:
                tb[column].metadata.description_from_producer = dfp.replace("patters", "patterns")


def add_region_source_suffix(tb: Table) -> Table:
    """Add region source as suffix to region name, e.g. Africa (WHO)"""
    # I think drop "WORLDBANKINCOMEGROUP" as we are trying to consistently call them 'High-income countries", "Low-income countries",
    # so that they work with the fancy new grapher map.
    tb = tb.copy()

    tb = tb[tb.spatialdimtype != "WORLDBANKINCOMEGROUP"]

    for region_source in tb.spatialdimtype.unique():
        match region_source:
            case "COUNTRY" | "GLOBAL":
                continue  # No suffix for countries and global
            case "FAOREGION":
                suffix = "FAO"
            case "REGION" | "WHOINCOMEREGION":
                suffix = "WHO"
            case "UNREGION":
                suffix = "UN"
            case "WORLDBANKREGION":
                suffix = "WB"
            case "UNSDGREGION":
                suffix = "UN SDG"
            case "UNICEFREGION":
                suffix = "UNICEF"
            case "MGHEREG":
                suffix = "MGH"
            case "GBDREGION":
                suffix = "GBD"
            case "RCREGION":
                suffix = "UN RC"
            case _:
                raise ValueError(f"Unknown region source: {region_source}")

        ix = tb.spatialdimtype == region_source
        tb.loc[ix, "country"] = tb.loc[ix, "country"].astype(str) + " (" + suffix + ")"

    return tb


def clean_numeric_column(tb: Table) -> Table:
    # Detect nans in display_value
    ix = tb["display_value"].isin(NAN_VALUES)
    if ix.any():
        tb["display_value"] = tb["display_value"].astype(str)
        tb.loc[ix, "display_value"] = np.nan

    # If numeric value is all nan (does not exist), use display_value
    if "numeric" not in tb.columns or tb.numeric.isnull().all():
        tb["numeric"] = tb["display_value"]

    # Sometimes numeric is missing, but display_value is present (e.g. in table cholera_0000000001)
    if tb["numeric"].isnull().any():
        if pd.api.types.is_numeric_dtype(tb["numeric"]):
            tb["numeric"] = tb["numeric"].astype(float).fillna(pd.to_numeric(tb["display_value"], errors="coerce"))
        else:
            tb["numeric"] = tb["numeric"].fillna(tb["display_value"])

    return tb


def check_overlapping_names(tb: Table) -> None:
    overlapping_names = set(tb.index.names) & set(tb.columns)
    if overlapping_names:
        raise ValueError(f"index names are overlapping with column names: {overlapping_names}")


def drop_excess_region_sources(tb: Table, priority_regions: list[str]) -> Table:
    """
    Drop specific region sources if there are more than two different sources for a given indicator,
    in the order of preference given in `priority_regions`.
    Keep rows where `region_source` is NaN.
    """
    regions_tb = tb[~tb.spatialdimtype.isin({"COUNTRY", "GLOBAL", "WHOINCOMEREGION"})]

    if regions_tb.empty:
        # If there are no regions, return the original table
        return tb

    region_sources = set(regions_tb.spatialdimtype)
    if len(region_sources) == 1:
        # If there is only one region source, return the original table
        return tb

    missing_priority_regions = set(region_sources) - set(priority_regions)
    assert not missing_priority_regions, (
        f"Some region sources are not in the priority list: {missing_priority_regions}. "
        "Please update the priority_regions list."
    )

    # Keep only two regions
    regions_to_keep = [region for region in priority_regions if region in region_sources][:2]
    regions_to_drop = region_sources - set(regions_to_keep)

    for region_to_remove in regions_to_drop:
        tb = tb[tb.spatialdimtype != region_to_remove]

    return tb.copy()


def merge_identical_tables(tables: list[Table]) -> list[Table]:
    """Some indicators like ncd_ccs_cancerregnational and ncd_ccs_pbcr are the same
    series, but are split into two tables. We merge them here."""
    seen_short_names = set()
    new_tables = []
    for tb in tables:
        if tb.m.short_name not in seen_short_names:
            seen_short_names.add(tb.m.short_name)
            new_tables.append(tb)
            continue

        log.warning("Merging identical tables", short_name=tb.m.short_name)

        # Append to the existing table
        for k, existing_tb in enumerate(new_tables):
            if existing_tb.m.short_name == tb.m.short_name:
                # It's possible that new table has different index
                new_tables[k] = (
                    pr.concat([existing_tb.reset_index(), tb.reset_index()], ignore_index=True)
                    .set_index(existing_tb.index.names)
                    .copy_metadata(existing_tb)
                )
                new_tables[k] = new_tables[k][~new_tables[k].index.duplicated()]
                break

    return new_tables


def _normalize_text(s: str) -> str:
    """Normalize whitespace in GHO source text.

    The metadata-registry API returns text with CRLF line breaks and occasional
    double spaces. Collapse to LF, strip, and squash runs of spaces — leave
    intentional `\n` paragraph breaks intact for markdown rendering.
    """
    if not isinstance(s, str):
        return s
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = "\n".join(line.strip() for line in s.split("\n"))
    return s.strip()


def set_indicator(tb: Table, short_name: str, meta: dict[str, str]) -> Table:
    # create producer description from GHO metadata
    # NOTE: we already use Definition as description_short, so maybe we don't need it here?
    markdown_description = ""
    for heading in HEADINGS_TO_USE:
        if heading in meta:
            markdown_description += f"#### {heading}\n{_normalize_text(meta[heading])}\n\n"
    markdown_description = markdown_description.strip()

    # Make sure the value is not the same as dimension
    if short_name in tb.index.names:
        short_name = short_name + "_value"

    for col, suffix_short, suffix_long in [
        ("numeric", "", ""),
        ("low", "_low", " - Low"),
        ("high", "_high", " - High"),
    ]:
        if col not in tb.columns:
            continue

        new_col = short_name + suffix_short
        tb = tb.rename(columns={col: new_col})
        tb[new_col].m.title = _normalize_text(tb.m.title) + suffix_long
        if "Definition" in meta:
            tb[new_col].m.description_short = _normalize_text(meta["Definition"])

        # Convert description from producer to markdown from JSON
        tb[new_col].m.description_from_producer = markdown_description

        # Set percentage unit automatically
        if is_percentage(meta, tb[new_col].m):
            tb[new_col].m.unit = "%"
        elif meta.get("Data type") == "Rate":
            tb[new_col].m.unit = "rate"
        elif "Unit of Measure" in meta:
            tb[new_col].m.unit = _normalize_text(meta["Unit of Measure"])
        else:
            tb[new_col].m.unit = ""

        # Common defaults that `definitions.common` in the .meta.yml can't reach
        # because these auto-generated tables aren't listed there.
        tb[new_col].m.processing_level = "minor"
        if tb[new_col].m.presentation is None:
            tb[new_col].m.presentation = VariablePresentationMeta()
        tb[new_col].m.presentation.attribution_short = "WHO"
        tb[new_col].m.presentation.topic_tags = ["Global Health"]

    return tb


def is_percentage(meta: dict[str, Any], ind_meta: VariableMeta) -> bool:
    if meta.get("Data type") == "Per Cent":
        return True
    elif meta.get("Unit of Measure") in ("N/A", "%"):
        return True
    elif ind_meta.title and "(%" in ind_meta.title:
        return True
    else:
        return False


def normalize_year_range(year: pd.Series) -> pd.Series:
    """Some years are in the format xxxx-yyyy, we take the end of range to be consistent
    with GHO website. See for instance indicator WHS4_154
    https://www.who.int/data/gho/data/indicators/indicator-details/GHO/antenatal-care-coverage-at-least-four-visits
    """
    assert year.dtype == "category"
    year = year.astype(str)
    ix = year.str.contains(r"\d{4}-\d{4}")
    year[ix] = year[ix].str.extract(r"\d{4}-(\d{4})")[0].values
    return year.astype(int)


def remove_nans(tb: Table) -> Table:
    cols = {"numeric", "low", "high"} & set(tb.columns)
    for col in cols:
        ix = tb[col].isin(NAN_VALUES)
        if ix.any():
            tb.loc[ix, col] = np.nan

    tb = tb.dropna(subset=cols, how="all")

    for col in cols:
        # try to repack category to float first
        if tb[col].dtype == "category":
            try:
                tb[col] = tb[col].astype(str).astype(float)
            except ValueError:
                pass

        tb[col] = repack_series(tb[col])

    return tb


def set_dimensions(tb: Table) -> Table:
    dimensions = [c for c in tb.columns if c not in ("numeric", "low", "high", "comments", "display_value")]

    DEFAULT_ORDER = ["year", "country", "sex", "age_group"]

    # sort dimensions, putting the most important ones first
    dimensions = sorted(dimensions, key=lambda x: DEFAULT_ORDER.index(x) if x in DEFAULT_ORDER else len(DEFAULT_ORDER))

    return tb.set_index(dimensions).sort_index()


def check_duplicate_index(tb: Table) -> Table:
    """Some indicators have duplicate values. The typical reason is multiple providers of
    data (like surveys) or multiple measurement types with different comments.
    If this happens, warn and remove the duplicates. This is potentially dangerous, but it
    doesn't happen that often. We could also take average of the values as an alternative.

    Examples:
        Label `whs4_154`, India for 1995 has multiple values from two different sources
        WHO excludes both data values if there are duplicates like this, see
        https://www.who.int/data/gho/data/indicators/indicator-details/GHO/antenatal-care-coverage-at-least-four-visits

        other labels with duplicates:
            mh_25, ntd_rab2, assistivetech_totalneed
    """

    duplicated_index = tb.index[tb.index.duplicated()]

    if len(duplicated_index) > 0:
        # try to remove duplicated **values**, if values are different, we still raise an error
        index_names = tb.index.names
        tb = tb.reset_index()
        tb = tb.drop_duplicates(subset=[c for c in tb.columns if c not in {"comments"}])
        tb = tb.set_index(index_names)

        duplicated_index = tb.index[tb.index.duplicated()]
        if len(duplicated_index) > 0:
            # TODO: turn this into a hard error once the multi-dim indexing is fixed.
            # Some indicators (e.g. air_17 = "Household air pollution attributable DALYs")
            # ship rows whose `Dim1/Dim2/Dim3` positions shift, so ENVCAUSE values land
            # in different garden columns row-by-row and collapse to `cause=NaN`,
            # producing many rows with the same (year, country, sex, age_group, cause)
            # index but different display_values. Until we fix the dim mapping, we
            # warn and keep the first row; downstream charts may use any of the
            # collapsed values. Tracked in the PR follow-ups.
            log.warning(
                "Duplicated index found",
                indicator=tb.m.short_name,
                duplicated_index=duplicated_index[:10],
            )
            tb = tb[~tb.index.duplicated()]

    return tb


def process_statins_indicator(tb: Table) -> Table:
    """Special processing for statins indicator data."""

    # Asserting that the unique values in the column are a subset of the provided list
    provided_values = ["Yes", "Don't know", "No", "No data received", "No response"]
    unique_values_in_column = tb["general_availability_of_statins_in_the_public_health_sector"].unique()
    assert set(unique_values_in_column).issubset(provided_values)

    # Replace the specified values where there is no data with "NaN" for consistency on grapher charts
    values_to_replace = ["No data received", "No response", "Don't know"]
    tb["general_availability_of_statins_in_the_public_health_sector"] = tb[
        "general_availability_of_statins_in_the_public_health_sector"
    ].replace(values_to_replace, np.nan)

    return tb
