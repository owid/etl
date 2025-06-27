"""Load a meadow dataset and create a garden dataset."""

import json
from typing import Any

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
import structlog
from owid.catalog import Table, VariableMeta
from owid.catalog.utils import underscore
from owid.repack import repack_series

from etl.data_helpers import geo
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

PRIORITY_OF_REGIONS = ["WORLDBANKREGION", "REGION", "UNICEFREGION", "UNREGION", "UNSDGREGION", "FAOREGION"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gho")

    # Loop over tables in meadow dataset.
    tables = []
    for label in ds_meadow.table_names:
        log.info("gho.run", label=label)

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
        tb = geo.harmonize_countries(
            df=tb,
            countries_file=paths.country_mapping_path,
            excluded_countries_file=paths.excluded_countries_path,
            warn_on_unused_countries=False,
            warn_on_missing_countries=True,
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

        tables.append(tb)

    # Merge identical tables
    tables = merge_identical_tables(tables)

    tables_dict = {tb.m.title: tb for tb in tables}
    assert len(tables) == len(tables_dict), "Duplicate titles in tables found"

    # Create OOMs
    ds_population = paths.load_dataset("population")
    ds_regions = paths.load_dataset("regions")
    create_omms(tables_dict, ds_population, ds_regions)  # type: ignore

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


def set_indicator(tb: Table, short_name: str, meta: dict[str, str]) -> Table:
    # create producer description from GHO metadata
    # NOTE: we already use Definition as description_short, so maybe we don't need it here?
    markdown_description = ""
    for heading in HEADINGS_TO_USE:
        if heading in meta:
            markdown_description += f"#### {heading}\n{meta[heading]}\n\n"
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
        tb[new_col].m.title = tb.m.title + suffix_long
        if "Definition" in meta:
            tb[new_col].m.description_short = meta["Definition"]

        # Convert description from producer to markdown from JSON
        tb[new_col].m.description_from_producer = markdown_description

        # Set percentage unit automatically
        if is_percentage(meta, tb[new_col].m):
            tb[new_col].m.unit = "%"
        elif meta.get("Data type") == "Rate":
            tb[new_col].m.unit = "rate"
        elif "Unit of Measure" in meta:
            tb[new_col].m.unit = meta["Unit of Measure"]
        else:
            tb[new_col].m.unit = ""

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
            # warn instead of raising an error, this could be potentially dangerous
            # raise ValueError(f"Duplicated index found for {tb.m.short_name}:\n{tb.loc[duplicated_index[:10]]}")
            log.warning(
                "Duplicated index found",
                indicator=tb.m.short_name,
                duplicated_index=duplicated_index[:10],
            )
            tb = tb[~tb.index.duplicated()]

    return tb
