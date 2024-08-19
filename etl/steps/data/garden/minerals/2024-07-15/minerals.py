"""Compilation of minerals data from different origins.

Currently, the three sources of minerals data are:
* From BGS we extract imports, exports, and production.
* From USGS (current) we extract production and reserves.
* From USGS (historical) we extract production and unit value.

Initially, I thought of creating long tables in the garden steps of USGS current, USGS historical, and BGS data.

However, there is little overlap between the three origins (at the country-year-commodity-subcommodity-unit level).
If we combined all production data into one column (as it would be common to do in a garden step), the resulting data
would show as having 3 origins, whereas in reality most data points would come from just one origin.

So it seems more convenient to create wide tables, where each column has its own origin, and then combine the wide
tables (on those few columns where there is overlap).

"""
import warnings
from typing import List, Optional, Tuple

import owid.catalog.processing as pr
import pandas as pd
import plotly.express as px
from owid.catalog import Table, VariablePresentationMeta
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Ignore PerformanceWarning (that happens when finding the maximum of world data, when creating region aggregates).
warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Prefix used for "share" columns.
SHARE_OF_GLOBAL_PREFIX = "share_of_global_"

# Given that BGS and USGS data often have big discrepancies, we will combine columns only after inspection.
# Specifically, we check that the World aggregate of BGS (constructed in the BGS garden step) coincides reasonably
# well with the World data given in USGS data.
COMBINE_BGS_AND_USGS_COLUMNS = [
    "production|Alumina|Refinery|tonnes",
    "production|Aluminum|Smelter|tonnes",
    "production|Andalusite|Mine|tonnes",
    # Reasonable agreement overall, but disagreement during certain periods, leading to >100% shares of global.
    # TODO: This should be investigated.
    # 'production|Antimony|Mine|tonnes',
    # Somewhat in agreement, but very noisy.
    # 'production|Arsenic|Processing|tonnes',
    # Good agreement after 2012, but prior to that, USGS is significantly larger.
    # "production|Asbestos|Mine|tonnes",
    # Reasonable global agreement, although not for certain countries: Mexico.
    "production|Barite|Mine|tonnes",
    # Reasonable global agreement, although not for certain countries: China, Russia.
    # NOTE: Big drop in Greece 1976 in BGS data, but it has no footnotes. It could be that two zeros where missing? Unclear.
    "production|Bauxite|Mine|tonnes",
    # Reasonable global agreement, although not for certain countries: Denmark, Iran.
    "production|Clays|Mine, bentonite|tonnes",
    # Big global disagreement.
    "production|Clays|Mine, fuller's earth|tonnes",
    # Big global disagreement.
    "production|Clays|Mine, kaolin|tonnes",
    # Reasonable global agreement, except for DRC, that is much larger than World on certain years.
    # TODO: This should be investigated.
    # 'production|Cobalt|Mine|tonnes',
    "production|Copper|Mine|tonnes",
    "production|Copper|Refinery|tonnes",
    # BGS and USGS data are informed on very separated year ranges. It's not possible to assess their agreement.
    # 'production|Diamond|Mine, industrial|tonnes',
    # BGS global data is significantly lower than USGS.
    # 'production|Diatomite|Mine|tonnes',
    # BGS global data is significantly larger than USGS.
    # 'production|Feldspar|Mine|tonnes',
    # Reasonable global agreement, although not for certain countries: Mongolia, Morocco, Pakistan.
    "production|Fluorspar|Mine|tonnes",
    "production|Gallium|Processing|tonnes",
    # Significant global disagreement, leading to >100% shares of global.
    # 'production|Germanium|Refinery|tonnes',
    # Reasonable global agreement, although not for certain countries: Argentina, Brazil, Kazakhstan, Mali, Mexico, Papua New Guinea, Sudan.
    "production|Gold|Mine|tonnes",
    # Significant global disagreement, leading to >100% shares of global.
    # TODO: This should be investigated.
    # 'production|Graphite|Mine|tonnes',
    # Significant global disagreement during certain years.
    # 'production|Gypsum|Mine|tonnes',
    # Significant global disagreement during certain years.
    # TODO: This should be investigated.
    # 'production|Helium|Mine|tonnes',
    # Significant global disagreement, noisy data.
    # 'production|Indium|Refinery|tonnes',
    "production|Iodine|Mine|tonnes",
    # Significant global disagreement during a large range of years.
    # 'production|Iron ore|Mine, crude ore|tonnes',
    # BGS global data is consistently larger than USGS since 1992, and also for Mexico, Iran, India.
    # 'production|Iron|Smelter, pig iron|tonnes',
    # BGS noisy data, and from USGS only US is informed.
    "production|Kyanite|Mine|tonnes",
    # Reasonable global agreement, although not for certain countries: Turkey, Iran.
    "production|Lead|Mine|tonnes",
    # Reasonable global agreement, although not for certain countries: Turkey, Russia.
    "production|Magnesium metal|Smelter|tonnes",
    # Reasonable global agreement, although not for certain countries: Tajikistan.
    "production|Mercury|Mine|tonnes",
    # Reasonable global agreement, although not for certain countries: Armenia, Iran, Mexico.
    # NOTE: between 1970 and 1970, BGS is significantly lower than USGS. This may be because US data was removed.
    #  And US data was removed in the BGS garden step (see explanation there).
    "production|Molybdenum|Mine|tonnes",
    # Reasonable global agreement, although not for certain years.
    "production|Nickel|Mine|tonnes",
    # Significant global disagreement.
    "production|Perlite|Mine|tonnes",
    # Reasonably good agreement, except between 1970 and 1976, where BGS is significantly lower. Noisy data.
    # Also, disagreement for certain countries: Turkey, Syria, Peru, Egypt, Australia.
    "production|Phosphate rock|Mine|tonnes",
    "production|Platinum group metals|Mine, palladium|tonnes",
    "production|Platinum group metals|Mine, platinum|tonnes",
    # Significant global disagreement.
    # 'production|Rhenium|Mine|tonnes',
    # Significant global disagreement.
    # 'production|Selenium|Refinery|tonnes',
    # Reasonably good agreement, except for recent years.
    # Also, significant disagreement for certain countries: Poland, Mexico, Kazakhstan.
    "production|Silver|Mine|tonnes",
    # Significant global disagreement, leading to >100% shares of global.
    # 'production|Soda ash|Natural|tonnes',
    # Reasonably good agreement, except for Iran in 2020, which is an outlier in BGS data (it may be an error).
    # This outlier is fixed after combining with USGS data.
    "production|Steel|Processing, crude|tonnes",
    # Reasonable agreement, except for certain years. Noisy data.
    "production|Strontium|Mine|tonnes",
    # Significant global disagreement.
    # 'production|Tellurium|Refinery|tonnes',
    # Reasonable agreement, except for certain years, which leads to >100% shares.
    # "production|Tin|Mine|tonnes",
    # Significant global disagreement.
    # 'production|Titanium|Mine, ilmenite|tonnes',
    # Reasonable global agreement, but noisy data.
    "production|Titanium|Mine, rutile|tonnes",
    # Reasonable global agreement, except for recent years, where shares can probably be >100%.
    # TODO: Consider discarding.
    "production|Tungsten|Mine|tonnes",
    # Reasonable global agreement. Noisy data.
    "production|Vanadium|Mine|tonnes",
    # Reasonable global agreement. Noisy data.
    "production|Vermiculite|Mine|tonnes",
    # Reasonable global agreement, except for certain countries: Mexico.
    "production|Wollastonite|Mine|tonnes",
    # Reasonable global agreement, except for certain countries: Mexico (big peak in 2021), India (big peak in 2021).
    "production|Zinc|Mine|tonnes",
    # Reasonable global agreement, except for certain countries: China, Sierra Leone.
    "production|Zirconium and hafnium|Mine|tonnes",
]
# The following list contains all columns where USGS (current and historical) overlaps with BGS.
# NOTE: To visually inspect certain columns, the easiest is to redefine COMBINE_BGS_AND_USGS_COLUMNS again here below,
#  only with the columns to inspect. Then, uncomment the line columns_to_plot=COMBINE_BGS_AND_USGS_COLUMNS in run().

# TODO: Consider keeping "World (BGS)" just for run sanity checks and then remove it. This we we will detect >100% shares.


def adapt_flat_table(tb_flat: Table) -> Table:
    tb_flat = tb_flat.copy()
    # For consistency with other tables, rename columns and adapt column types.
    tb_flat = tb_flat.rename(
        columns={
            column: tb_flat[column].metadata.title for column in tb_flat.columns if column not in ["country", "year"]
        },
        errors="raise",
    )
    tb_flat = tb_flat.astype({column: "Float64" for column in tb_flat.columns if column not in ["country", "year"]})
    return tb_flat


def _gather_notes_and_footnotes_from_metadata(tb_flat: Table, column: str) -> Tuple[Optional[str], Optional[str]]:
    # Get notes from description_from_producer field, if given.
    # And gather footnotes from grapher config, if given.
    notes = None
    footnotes = None
    # For "share" columns, we also want to fetch the notes of the original column.
    column = column.replace(SHARE_OF_GLOBAL_PREFIX, "")
    if column in tb_flat.columns:
        notes = tb_flat[column].metadata.description_from_producer
        if tb_flat[column].metadata.presentation.grapher_config is not None:
            footnotes = tb_flat[column].metadata.presentation.grapher_config["note"]

    return notes, footnotes


def _combine_notes(notes_list: List[Optional[str]], separator: str) -> str:
    # Add notes to metadata.
    combined_notes = ""
    notes_exist = False
    for notes in notes_list:
        if notes:
            if notes_exist:
                combined_notes += separator
            combined_notes += notes
            notes_exist = True

    return combined_notes


def improve_metadata(tb: Table, tb_usgs_flat: Table, tb_bgs_flat: Table, tb_usgs_historical_flat: Table) -> Table:
    tb = tb.copy()

    # Improve metadata of new columns.
    for column in tb.drop(columns=["country", "year"]).columns:
        # Extract combination from the column name.
        metric, commodity, sub_commodity, unit = column.split("|")

        # Improve metric, commodity and subcommodity names.
        metric = metric.replace("_", " ").capitalize()
        commodity = commodity.capitalize()

        # Ensure the variable has a title.
        tb[column].metadata.title = column

        # Create a title_public.
        if sub_commodity == "Refinery":
            title_public = f"Refined {commodity.lower()} {metric.lower()}"
        elif sub_commodity.startswith("Refinery, "):
            _sub_commodity = sub_commodity.split(", ")[-1]
            title_public = f"Refined {commodity.lower()} ({_sub_commodity}) {metric.lower()}"
        elif sub_commodity == "Mine":
            title_public = f"{commodity} {metric.lower()}"
        elif sub_commodity.startswith("Mine, "):
            _sub_commodity = sub_commodity.split(", ")[-1]
            title_public = f"{commodity} ({_sub_commodity}) {metric.lower()}"
        else:
            title_public = f"{commodity} {metric.lower()}"
        if tb[column].metadata.presentation is None:
            tb[column].metadata.presentation = VariablePresentationMeta()
        if column.startswith(SHARE_OF_GLOBAL_PREFIX):
            title_public = title_public.replace("share of global ", "") + " as a share of the global total"
        tb[column].metadata.presentation.title_public = title_public

        # Create a short description (that will also appear as the chart subtitle).
        description_short = None
        if metric in [
            "Production",
            "Imports",
            "Exports",
            "Share of global production",
            "Share of global imports",
            "Share of global exports",
        ]:
            if sub_commodity.startswith("Mine"):
                if metric.startswith("Share"):
                    description_short = "Based on mined, rather than [refined](#dod:refined-production), production."
                else:
                    description_short = f"Based on mined, rather than [refined](#dod:refined-production), production. Measured in {unit}."
            elif sub_commodity.startswith("Refinery"):
                if not metric.startswith("Share"):
                    description_short = f"Measured in {unit}. Mineral [refining](#dod:refined-production) takes mined or raw minerals, and separates them into a final product of pure metals and minerals."
                else:
                    description_short = "Mineral [refining](#dod:refined-production) takes mined or raw minerals, and separates them into a final product of pure metals and minerals."
            elif sub_commodity.startswith("Smelter"):
                description_short = f"Measured in {unit}. [Smelting](#dod:smelting) takes raw minerals and produces metals through high-temperature processes."
            elif sub_commodity.startswith("Processing"):
                description_short = "Mineral processing involves the extraction, purification, or production of materials from ores, brines, or other raw sources, often as a byproduct or through specialized industrial processes."
        elif metric in ["Reserves", "Share of global reserves"]:
            description_short = "Mineral [reserves](#dod:mineral-reserve) are resources that have been evaluated and can be mined economically with current technologies."
        elif metric == "Unit value":
            description_short = (
                # f"Value of 1 tonne of {commodity.lower()}, in US dollars per tonne, adjusted for inflation."
                # As suggested by Marcel, the change in USD is quite significant since 1998, so it is worth mentioning
                # the unit in the subtitle, rather than the footnote.
                f"Value of 1 tonne of {commodity.lower()}, in constant 1998 US$ per tonne."
            )
        ################################################################################################################
        # Handle special cases.
        if sub_commodity == "Guano":
            description_short = "Guano is the accumulated excrement of seabirds, bats, and seals. It is rich in nitrogen, phosphate and potassium, making it valuable as a fertilizer."
        ################################################################################################################
        if description_short is not None:
            tb[column].metadata.description_short = description_short

        # Add unit and short_unit.
        tb[column].metadata.unit = unit
        if unit.startswith("tonnes"):
            # Create short unit.
            tb[column].metadata.short_unit = "t"
        elif unit == "constant 1998 US$ per tonne":
            tb[column].metadata.short_unit = "$/t"
        else:
            log.warning(f"Unexpected unit for column: {column}")
            tb[column].metadata.short_unit = ""
        if metric.startswith("Share "):
            tb[column].metadata.unit = "%"
            tb[column].metadata.short_unit = "%"

        # Gather notes and footnotes from USGS' current metadata.
        notes_usgs, footnotes_usgs = _gather_notes_and_footnotes_from_metadata(tb_flat=tb_usgs_flat, column=column)

        # Gather notes and footnotes from USGS' historical metadata.
        notes_usgs_historical, footnotes_usgs_historical = _gather_notes_and_footnotes_from_metadata(
            tb_flat=tb_usgs_historical_flat, column=column
        )

        # Gather notes and footnotes from BGS' metadata.
        notes_bgs, footnotes_bgs = _gather_notes_and_footnotes_from_metadata(tb_flat=tb_bgs_flat, column=column)

        # Add notes to metadata.
        combined_notes = _combine_notes(notes_list=[notes_bgs, notes_usgs, notes_usgs_historical], separator="\n\n")
        if len(combined_notes) > 0:
            tb[column].metadata.description_from_producer = combined_notes

        # Insert (or append) additional footnotes:
        footnotes_additional = ""
        if metric in ["Reserves", "Share of global reserves"]:
            footnotes_additional += "Reserves can increase over time as new mineral deposits are discovered and others become economically feasible to extract."
        elif metric == "Unit value":
            # footnotes_additional += "This data is expressed in constant 1998 US$ per tonne."
            footnotes_additional += "This data is adjusted for inflation."
        elif metric in ["Imports", "Exports", "Share of global imports", "Share of global exports"]:
            footnotes_additional += (
                "After 2002, data is limited to Europe and Turkey; after 2018, only UK data is available."
            )

        # Add footnotes to metadata.
        combined_footnotes = _combine_notes(
            notes_list=[footnotes_bgs, footnotes_usgs, footnotes_usgs_historical, footnotes_additional], separator=" "
        )
        if len(combined_footnotes) > 0:
            if tb[column].metadata.presentation.grapher_config is None:
                tb[column].metadata.presentation.grapher_config = {}
            tb[column].metadata.presentation.grapher_config["note"] = combined_footnotes

    return tb


def inspect_overlaps(
    tb_usgs_flat: Table,
    tb_usgs_historical_flat: Table,
    tb_bgs_flat: Table,
    columns_to_plot: Optional[List[str]] = None,
) -> None:
    # Find the union of all columns.
    columns = (set(tb_usgs_flat.columns) | set(tb_usgs_historical_flat.columns) | set(tb_bgs_flat.columns)) - set(
        ["country", "year"]
    )

    if columns_to_plot is None:
        columns_to_plot = []

    for column in columns:
        if column not in columns_to_plot:
            continue
        # Initialize an empty dataframe, and add data to it from whatever source has data for it.
        _df = pd.DataFrame()
        if column in tb_bgs_flat.columns:
            _df = pd.concat(
                [_df, tb_bgs_flat[["country", "year", column]].assign(**{"source": "BGS"})], ignore_index=True
            )
        if column in tb_usgs_flat.columns:
            _df = pd.concat(
                [_df, tb_usgs_flat[["country", "year", column]].assign(**{"source": "USGS current"})], ignore_index=True
            )
        if column in tb_usgs_historical_flat.columns:
            _df = pd.concat(
                [_df, tb_usgs_historical_flat[["country", "year", column]].assign(**{"source": "USGS historical"})],
                ignore_index=True,
            )
        _df = _df.dropna().reset_index(drop=True)
        for country in _df["country"].unique():
            _df_country = _df[_df["country"] == country]
            if _df_country["source"].nunique() > 1:
                px.line(
                    _df_country,
                    x="year",
                    y=column,
                    color="source",
                    markers=True,
                    title=f"{column} - {country}",
                    range_y=[0, None],
                ).show()


def add_share_of_global_columns(tb: Table) -> Table:
    # Create a table for global data.
    tb_world = tb[tb["country"] == "World"].drop(columns="country").reset_index(drop=True)
    # Replace zeros by nan (to avoid division by zero).
    tb_world = tb_world.replace(0, pd.NA)
    # Create a temporary table with global data for each column in the original table.
    _tb = tb.merge(tb_world, on="year", how="left", suffixes=("", "_world_share"))

    # Create a dictionary to store the new share columns, and another to store each column's metadata.
    new_columns = {}
    metadata = {}
    for column in tb.columns:
        if (column.split("|")[0] in ["exports", "imports", "production", "reserves"]) and (
            tb_world[column].notnull().any()
        ):
            new_columns[f"{SHARE_OF_GLOBAL_PREFIX}{column}"] = _tb[column] / _tb[f"{column}_world_share"] * 100
            metadata[f"{SHARE_OF_GLOBAL_PREFIX}{column}"] = tb[column].metadata

    # Add the new share columns to the original table.
    tb = pr.concat([tb, Table(new_columns)], axis=1)

    for column in tb.drop(columns=["country", "year"]).columns:
        if len(tb[column].metadata.origins) == 0:
            tb[column].metadata = metadata[column].copy()

    return tb


def combine_data(
    tb_usgs_flat: Table, tb_usgs_historical_flat: Table, tb_bgs_flat: Table, columns_to_plot: Optional[List[str]] = None
) -> Table:
    # Compare the data from different origins where they overlap.
    inspect_overlaps(
        tb_usgs_flat=tb_usgs_flat,
        tb_usgs_historical_flat=tb_usgs_historical_flat,
        tb_bgs_flat=tb_bgs_flat,
        columns_to_plot=columns_to_plot,
    )

    # Create a combined flat table.
    # Firstly, combine USGS current and historical for all minerals. Since the former is more up-to-date, prioritize it.
    # NOTE: If a mineral should not be combined, the current solution is to set its COMMODITY_MAPPING value to None
    #  (either in the USGS historical or USGS current garden step).
    tb_usgs_combined_flat = combine_two_overlapping_dataframes(
        df1=tb_usgs_flat, df2=tb_usgs_historical_flat, index_columns=["country", "year"]
    )

    # Create a temporary combined table of BGS and USGS data.
    # After inspection, it seems that, where USGS and BGS data overlap, BGS is usually more complete.
    # All region aggregates from BGS have larger values than USGS' region aggregates (even though
    # data for individual countries agrees reasonably well). However, the latest year is always from USGS.
    # Initially we tried combining both and producing a combined "World" aggregate.
    # But this often led to spurious jumps in global data, simply caused by missing data from different countries.
    # So, the safest option is to combine BGS and USGS only where the "World" aggregate of BGS coincides reasonably well
    # with the "World" data given by USGS.
    # Hence, ignore "World" from BGS data (which was created by us).
    tb_bgs_and_usgs = combine_two_overlapping_dataframes(
        df1=tb_usgs_combined_flat, df2=tb_bgs_flat[tb_bgs_flat["country"] != "World"], index_columns=["country", "year"]
    )

    # There are 22 columns that exist both in BGS and USGS, and where USGS includes "Other".
    # I checked if "Other" is usually small compared to the contribution of all other BGS countries, but it depends.
    # About half of the times, "Other" is larger than the sum of all other BGS countries, and half it's smaller.
    # The safest option is to remove "Other": After combining BGS and USGS, it's impossible to know which countries
    # are included in "Other", and it can therefore be misleading.
    # Note also that initially we created regions for both BGS and USGS, and, when comparing them, the region aggregates
    # from USGS were almost always smaller than those in BGS. This indicates that BGS tends to have data disaggregated
    # for more countries. This does not necessarily mean that "World" is usually smaller in USGS (it isn't).
    # It simply means that "Other" carries an important contribution in USGS data, and it's always missing in regions.
    # Therefore, it doesn't make much sense to keep USGS regions, or to keep "Other" (in columns where BGS and USGS data
    # are combined).
    tb_bgs_and_usgs = tb_bgs_and_usgs[tb_bgs_and_usgs["country"] != "Other"].reset_index(drop=True)

    # Now merge the selected columns from the combined BGS & USGS table with all other columns from the original
    # USGS-only table.
    # Also, include columns that were only in BGS (which do not include "World").
    columns_bgs_only = sorted(set(tb_bgs_flat.columns) - set(tb_usgs_combined_flat.columns))
    tb = tb_bgs_and_usgs[["country", "year"] + COMBINE_BGS_AND_USGS_COLUMNS + columns_bgs_only].merge(
        tb_usgs_combined_flat.drop(columns=COMBINE_BGS_AND_USGS_COLUMNS), on=["country", "year"], how="outer"
    )

    return tb


def run_sanity_checks(tb: Table) -> None:
    # Check that there are no duplicated rows.
    assert tb[tb.duplicated(subset=["country", "year"], keep=False)].empty, "Unexpected duplicated data."
    # Check that there are no missing values.
    for column in [column for column in tb.columns if column.startswith(SHARE_OF_GLOBAL_PREFIX)]:
        if (tb[column] > 100).any():
            log.warning(f"{column} maximum: {tb[column].max():.0f}%")


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load minerals datasets.
    ds_bgs = paths.load_dataset("world_mineral_statistics")
    ds_usgs_historical = paths.load_dataset("historical_statistics_for_mineral_and_material_commodities")
    ds_usgs = paths.load_dataset("mineral_commodity_summaries")

    # Read tables.
    tb_usgs_historical_flat = ds_usgs_historical.read_table(
        "historical_statistics_for_mineral_and_material_commodities_flat"
    )
    tb_usgs_flat = ds_usgs.read_table("mineral_commodity_summaries_flat")
    tb_bgs_flat = ds_bgs.read_table("world_mineral_statistics_flat")

    #
    # Process data.
    #
    # Adapt USGS current flat table.
    tb_usgs_flat = adapt_flat_table(tb_flat=tb_usgs_flat)

    # Adapt USGS historical flat table.
    tb_usgs_historical_flat = adapt_flat_table(tb_flat=tb_usgs_historical_flat)

    # Adapt BGS flat table.
    tb_bgs_flat = adapt_flat_table(tb_flat=tb_bgs_flat)

    # Combine all sources of data.
    tb = combine_data(
        tb_usgs_flat=tb_usgs_flat,
        tb_usgs_historical_flat=tb_usgs_historical_flat,
        tb_bgs_flat=tb_bgs_flat,
        # NOTE: Uncomment to visually inspect columns where BGS and USGS data are combined.
        # columns_to_plot=COMBINE_BGS_AND_USGS_COLUMNS,
    )

    # Create columns for share of world (i.e. production, import, exports and reserves as a share of global).
    tb = add_share_of_global_columns(tb=tb)

    # Improve metadata.
    tb = improve_metadata(
        tb=tb, tb_usgs_flat=tb_usgs_flat, tb_bgs_flat=tb_bgs_flat, tb_usgs_historical_flat=tb_usgs_historical_flat
    )

    # Run sanity checks.
    run_sanity_checks(tb=tb)

    # Format combined table conveniently.
    tb = tb.format(["country", "year"], short_name="minerals").astype("Float64")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
