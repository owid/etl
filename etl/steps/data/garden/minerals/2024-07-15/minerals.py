"""Compilation of minerals data from different origins.

Currently, the three sources of minerals data are:
* From BGS we extract production and disregard imports/exports, which are very sparse.
* From USGS (current) we extract production and reserves.
* From USGS (historical) we extract production and unit value.

For debugging:
* Add columns to PLOT_TO_COMPARE_DATA_SOURCES (defined below) to plot the data for that column separated by data source.
* Comment/uncomment columns in COMBINE_BGS_AND_USGS_COLUMNS (defined below). Two sanity checks will be performed on these columns.
  * First, we will check if the aggregated global data from only BGS (which was calculated in the garden BGS step) is significantly larger than USGS' World data.
  * Second, we will create an aggregated global data after combining BGS and USGS data, and then check if this is larger than USGS' original World data.
  In the two checks above, "significantly larger" means that the aggregated World data is larger than the original USGS's World by more than DEVIATION_MAX_ACCEPTED. If so, an error is raised, and a plot is displayed comparing the two "World" series.
  We have two ways to tackle these issues:
  * Accept the discrepancy between BGS and USGS data. We do this if BGS data is significantly larger on years where we have USGS data, since the latter will replace the former when combining both.
  * Remove data (for all countries except USGS' World) on specific years where BGS data is significantly larger. Ideally, we would not need to do this, and instead we would fetch the rest of USGS' data. However, USGS' data is in a very inconvenient format, and it is a big investment of time to fetch all of it. So, exceptionally, we remove points where BGS data is clearly overestimated, compared to USGS.
  NOTE: We allow this where one or a few BGS data points are significantly larger than USGS (e.g. Lead). But we do not do this if many points in BGS data are consistently larger than USGS (e.g. Graphite).

"""
import warnings
from typing import List, Optional, Tuple

import owid.catalog.processing as pr
import pandas as pd
import plotly.express as px
from owid.catalog import Dataset, Table, VariablePresentationMeta
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Ignore PerformanceWarning (that happens when finding the maximum of world data, when creating region aggregates).
warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Prefix used for "share" columns.
SHARE_OF_GLOBAL_PREFIX = "share of global "

# We use World data from USGS.
# We will also aggregate all other data to create a "World (aggregated)" for sanity check purposes (and then remove it).
# If this aggregated world is larger than USGS' World by more a certain percentage, raise an error.
# We will also raise an error if the "World (BGS)" (aggregated in the BGS garden step) is significantly larger than USGS' World.
# Define that percentage.
DEVIATION_MAX_ACCEPTED = 10

# At the end of the step, we remove certain minerals because they are not as critical, and add a significant amount of
# complexity to the explorer. But we may decide to bring them back in the future.
COLUMNS_TO_DISCARD = [
    "production|Alumina|Refinery|tonnes",
    "unit value|Alumina|Refinery|constant 1998 US$ per tonne",
    "share of global production|Alumina|Refinery|tonnes",
    "production|Bromine|Processing|tonnes",
    "reserves|Bromine|Processing|tonnes",
    "share of global production|Bromine|Processing|tonnes",
    "production|Diatomite|Mine|tonnes",
    "reserves|Diatomite|Mine|tonnes",
    "unit value|Diatomite|Mine|constant 1998 US$ per tonne",
    "share of global production|Diatomite|Mine|tonnes",
    "production|Indium|Refinery|tonnes",
    "unit value|Indium|Refinery|constant 1998 US$ per tonne",
    "share of global production|Indium|Refinery|tonnes",
    "production|Perlite|Mine|tonnes",
    "reserves|Perlite|Mine|tonnes",
    "unit value|Perlite|Mine|constant 1998 US$ per tonne",
    "share of global production|Perlite|Mine|tonnes",
    "production|Pumice and pumicite|Mine|tonnes",
    "unit value|Pumice and pumicite|Mine|constant 1998 US$ per tonne",
    "share of global production|Pumice and pumicite|Mine|tonnes",
    "production|Rhenium|Mine|tonnes",
    "reserves|Rhenium|Mine|tonnes",
    "share of global production|Rhenium|Mine|tonnes",
    "production|Soda ash|Natural and synthetic|tonnes",
    "production|Soda ash|Natural|tonnes",
    "production|Soda ash|Synthetic|tonnes",
    "reserves|Soda ash|Natural|tonnes",
    "unit value|Soda ash|Natural and synthetic|constant 1998 US$ per tonne",
    "share of global production|Soda ash|Natural and synthetic|tonnes",
    "share of global production|Soda ash|Natural|tonnes",
    "share of global production|Soda ash|Synthetic|tonnes",
    "share of global reserves|Soda ash|Natural|tonnes",
    "production|Salt|Brine salt|tonnes",
    "production|Salt|Evaporated salt|tonnes",
    "production|Salt|Other salt|tonnes",
    "production|Salt|Rock salt|tonnes",
    "production|Salt|Salt in brine|tonnes",
    "production|Salt|Sea salt|tonnes",
]

# List of known deviations that should not raise an error.
# The content should be (entity, column, list of years), where entity is either "World (BGS)" or "World (aggregated)".
# Add to this list specific peaks in BGS data that will be overwritten by more recent USGS data.
ACCEPTED_DEVIATIONS = [
    ("World (BGS)", "production|Iodine|Mine|tonnes", [2020, 2022]),
    ("World (BGS)", "production|Zinc|Mine|tonnes", [2021]),
    # NOTE: We decided to discard Wollastonite.
    # ("World (BGS)", "production|Wollastonite|Mine|tonnes", [2022]),
    ("World (BGS)", "production|Tungsten|Mine|tonnes", [2020, 2021, 2022]),
    ("World (BGS)", "production|Steel|Processing, crude|tonnes", [2020]),
    ("World (BGS)", "production|Silver|Mine|tonnes", [2020]),
    ("World (BGS)", "production|Mercury|Mine|tonnes", [2022]),
]

# BGS does not provide any global data. But we created an aggregated World (which we will later on call "World (BGS)").
# This is used mainly for checking purposes (to be able to compare BGS data with USGS data).
# However, for specific minerals, we will keep this global aggregate, since having global data for them is useful.
# We will check that the data is reasonably complete in those cases.
COLUMNS_TO_KEEP_WORLD_BGS = [
    "production|Coal|Mine|tonnes",
    "production|Petroleum|Crude|tonnes",
    "production|Nickel|Processing|tonnes",
    "production|Uranium|Mine|tonnes",
]

# Given that BGS and USGS data often have big discrepancies, we will combine columns only after inspection.
# Specifically, we check that the World aggregate of BGS (constructed in the BGS garden step) coincides reasonably
# well with the World data given in USGS data.
COMBINE_BGS_AND_USGS_COLUMNS = [
    "production|Alumina|Refinery|tonnes",
    "production|Aluminum|Smelter|tonnes",
    # Reasonable agreement overall, but disagreement during certain periods, leading to >100% shares of global.
    # TODO: This should be investigated.
    # "production|Antimony|Mine|tonnes",
    # Somewhat in agreement, but very noisy.
    # 'production|Arsenic|Processing|tonnes',
    # Good agreement after 2012, but prior to that, USGS is significantly larger.
    # "production|Asbestos|Mine|tonnes",
    # Reasonable global agreement, although not for certain countries: Mexico.
    "production|Barite|Mine|tonnes",
    # Reasonable global agreement, although not for certain countries: China, Russia.
    # NOTE: Big drop in Greece 1976 in BGS data, but it has no footnotes. It could be that two zeros where missing? Unclear.
    "production|Bauxite|Mine|tonnes",
    # BGS global data is significantly larger than USGS.
    # "production|Bromine|Processing|tonnes",
    # Reasonable global agreement, although not for certain countries: Denmark, Iran.
    # NOTE: We decided to remove "Clays" altogether.
    # "production|Clays|Mine, bentonite|tonnes",
    # Big global disagreement.
    # NOTE: We decided to remove "Clays" altogether.
    # "production|Clays|Mine, fuller's earth|tonnes",
    # Big global disagreement.
    # NOTE: We decided to remove "Clays" altogether.
    # "production|Clays|Mine, kaolin|tonnes",
    # Reasonable global agreement, except for Turkey and Finland, where USGS is significantly larger.
    "production|Chromium|Mine|tonnes",
    # Reasonable global agreement, except for DRC, that is much larger than World on certain years.
    # TODO: This should be investigated.
    # "production|Cobalt|Mine|tonnes",
    "production|Cobalt|Refinery|tonnes",
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
    # "production|Graphite|Mine|tonnes",
    # Significant global disagreement during certain years.
    # 'production|Gypsum|Mine|tonnes',
    # Significant global disagreement during certain years.
    # Here, it seems that the first years of BGS are very incomplete, leading to a World aggregate significantly lower than USGS' World.
    "production|Helium|Mine|tonnes",
    # Significant global disagreement, noisy data.
    # 'production|Indium|Refinery|tonnes',
    "production|Iodine|Mine|tonnes",
    # Significant global disagreement during a large range of years.
    # 'production|Iron ore|Mine, crude ore|tonnes',
    # BGS global data is consistently larger than USGS since 1992, and also for Mexico, Iran, India.
    # 'production|Iron|Smelter, pig iron|tonnes',
    # Reasonable global agreement, although not for certain countries: Turkey, Iran.
    "production|Lead|Mine|tonnes",
    # Reasonable global agreement, although not for certain countries: Turkey, Russia.
    "production|Magnesium metal|Smelter|tonnes",
    # Reasonable global agreement, except for certain years, e.g. 2002, where share is >150%. Disagreement for countries: Tajikistan.
    "production|Mercury|Mine|tonnes",
    # Reasonable global agreement, except for certain years, and also not for certain countries: Armenia, Iran, Mexico.
    "production|Molybdenum|Mine|tonnes",
    # Reasonable global agreement, but not for certain years, especially 2013, where Indonesia has a large peak.
    # TODO: This should be investigated.
    # "production|Nickel|Mine|tonnes",
    # Significant global disagreement, leading to world shares of 191%.
    # "production|Perlite|Mine|tonnes",
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
    # Reasonable agreement, except for certain years, e.g. 2012, where shares reach 160%. Noisy data.
    # "production|Strontium|Mine|tonnes",
    # Significant global disagreement. We decided to discard Tellurium.
    # 'production|Tellurium|Refinery|tonnes',
    # Reasonable agreement, except for certain years, which leads to >100% shares.
    # "production|Tin|Mine|tonnes",
    # BGS aggregated global data is significantly larger than USGS' World.
    # 'production|Titanium|Mine, ilmenite|tonnes',
    # Reasonable global agreement, but noisy data.
    "production|Titanium|Mine, rutile|tonnes",
    # Reasonable global agreement, except for recent years.
    "production|Tungsten|Mine|tonnes",
    # Reasonable global agreement, except for some years, e.g. 2002, where share becomes 143%. Noisy data.
    # "production|Vanadium|Mine|tonnes",
    # Reasonable global agreement. Noisy data. We decided to discard Vermiculite.
    # "production|Vermiculite|Mine|tonnes",
    # Reasonable global agreement, except for certain countries: Mexico. We decided to discard Wollastonite.
    # "production|Wollastonite|Mine|tonnes",
    # Reasonable global agreement, except for certain countries: Mexico (big peak in 2021), India (big peak in 2021).
    "production|Zinc|Mine|tonnes",
    # Reasonable global agreement, except for certain countries: China, Sierra Leone.
    "production|Zirconium and hafnium|Mine|tonnes",
]
# Columns to plot with the individual data sources differentiated.
PLOT_TO_COMPARE_DATA_SOURCES = [
    # "production|Rhenium|Mine|tonnes",
]


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
        if metric in ["Production", "Share of global production"]:
            if sub_commodity.startswith("Mine"):
                if metric.startswith("Share"):
                    description_short = "Based on mined, rather than [refined](#dod:refined-production), production."
                else:
                    description_short = f"Based on mined, rather than [refined](#dod:refined-production), production. Measured in {unit}."
            elif sub_commodity.startswith("Refinery"):
                if not metric.startswith("Share"):
                    description_short = f"Measured in {unit} of [refined](#dod:refined-production) material. The amounts of refined and raw materials differ because refining removes impurities and adjusts the composition to meet specific standards."
                else:
                    description_short = "The amounts of [refined](#dod:refined-production) and raw materials differ because refining removes impurities and adjusts the composition to meet specific standards."
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
        combined_notes = _combine_notes(notes_list=[notes_usgs_historical, notes_usgs, notes_bgs], separator="\n\n")
        if len(combined_notes) > 0:
            tb[column].metadata.description_from_producer = combined_notes

        # Insert (or append) additional footnotes:
        footnotes_additional = []
        if metric in ["Reserves", "Share of global reserves"]:
            footnotes_additional.append(
                "Reserves can increase over time as new mineral deposits are discovered and others become economically feasible to extract."
            )
        elif metric == "Unit value":
            # footnotes_additional += "This data is expressed in constant 1998 US$ per tonne."
            footnotes_additional.append("This data is adjusted for inflation.")
        if column in COMBINE_BGS_AND_USGS_COLUMNS:
            footnotes_additional.append(
                "The sum of all countries may exceed World data on certain years (by up to 10%), due to discrepancies between data sources."
            )

        # Add footnotes to metadata.
        combined_footnotes = _combine_notes(
            notes_list=[footnotes_bgs, footnotes_usgs, footnotes_usgs_historical] + footnotes_additional, separator=" "
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
        if (column.split("|")[0] in ["production", "reserves"]) and (tb_world[column].notnull().any()):
            new_columns[f"{SHARE_OF_GLOBAL_PREFIX}{column}"] = _tb[column] / _tb[f"{column}_world_share"] * 100
            metadata[f"{SHARE_OF_GLOBAL_PREFIX}{column}"] = tb[column].metadata

    # Add the new share columns to the original table.
    tb = pr.concat([tb, Table(new_columns)], axis=1)

    for column in tb.drop(columns=["country", "year"]).columns:
        if len(tb[column].metadata.origins) == 0:
            tb[column].metadata = metadata[column].copy()

    return tb


def _raise_error_on_large_deviations(tb: Table, ds_regions: Dataset) -> None:
    world_usgs_label = "World (USGS)"
    world_agg_label = "World (aggregated)"
    # The following should coincide with the one defined in combine_data.
    world_bgs_label = "World (BGS)"
    # Add up the contribution from all countries and compare it to USGS' original "World".
    tb = _add_global_data_for_comparison(tb=tb, ds_regions=ds_regions)
    tb["country"] = tb["country"].replace("World", world_agg_label).replace("World (original)", world_usgs_label)
    for entity_to_compare in [world_agg_label, world_bgs_label]:
        _tb = tb[(tb["country"].isin([world_usgs_label, entity_to_compare]))].reset_index(drop=True)
        _tb_pivot = _tb.pivot(
            index=["year"],
            columns="country",
            values=_tb.drop(columns=["country", "year"]).columns,
            join_column_levels_with="_",
        )
        for column in [column for column in _tb.columns if column.startswith("production")]:
            original_column = f"{column}_{world_usgs_label}"
            aggregated_column = f"{column}_{entity_to_compare}"
            # We accept that the aggregated global data often does not add up to 100%, since we lack data for all countries.
            # (For example, historical USGS data is only given for US and World).
            # But we should not accept that the aggregated global data is larger than the World by more than a certain
            # deviation.
            deviation = 100 * (_tb_pivot[aggregated_column] - _tb_pivot[original_column]) / (_tb_pivot[original_column])
            if deviation.isnull().all():
                continue
            deviation_max = deviation.dropna().max()
            years_with_deviation = _tb_pivot[deviation > DEVIATION_MAX_ACCEPTED]["year"].tolist()
            # Raise an error if the maximum deviation of the aggregated world data (above the original world data) is larger
            # than 15%.
            # NOTE: Some of these individual peaks have been tackled above, by simply removing all BGS data.
            # NOTE: An alternative way to detect outliers would be to impose, e.g. maximum deviation > 10% with MAPE > 10%.
            mape = (
                (100 * abs(_tb_pivot[aggregated_column] - _tb_pivot[original_column]) / (_tb_pivot[original_column]))
                .dropna()
                .mean()
            )
            if (deviation_max > DEVIATION_MAX_ACCEPTED) and (
                (entity_to_compare, column, years_with_deviation) not in ACCEPTED_DEVIATIONS
            ):
                message = f"{column}: {entity_to_compare} exceeds {world_usgs_label} by up to {deviation_max:.0f}% (MAPE: {mape:.0f}%)"
                log.error(message + f" on {years_with_deviation}")
                px.line(_tb, x="year", y=column, color="country", markers=True, title=message).show()


def combine_data(
    tb_usgs_flat: Table,
    tb_usgs_historical_flat: Table,
    tb_bgs_flat: Table,
    ds_regions: Dataset,
    columns_to_plot: Optional[List[str]] = None,
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
    # Keep the "World" that was aggregated with BGS data as "World (BGS)" just for sanity checks.
    # This will be removed after sanity checks are completed.
    # NOTE: The only global data from BGS that will be kept is coal and petroleum production.
    tb_bgs_flat["country"] = tb_bgs_flat["country"].astype("string").replace("World", "World (BGS)")
    tb_bgs_and_usgs = combine_two_overlapping_dataframes(
        df1=tb_usgs_combined_flat, df2=tb_bgs_flat, index_columns=["country", "year"]
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

    ####################################################################################################################
    # Remove individual data points where the aggregated world data is significantly larger than the original World aggregate.
    # NOTE: We do this only in cases where this happens in just specific points, and the overall deviation between BGS
    # and USGS is not too high.
    tb.loc[
        (tb["country"] != "World") & (tb["year"].isin([1990, 1993, 1998, 1999, 2001, 2002, 2003, 2007])),
        "production|Mercury|Mine|tonnes",
    ] = None
    tb.loc[
        (tb["country"] != "World") & (tb["year"].isin([1997, 2006, 2008, 2010, 2011])), "production|Barite|Mine|tonnes"
    ] = None
    tb.loc[
        (tb["country"] != "World") & (tb["year"].isin([1972, 1973, 1975])), "production|Fluorspar|Mine|tonnes"
    ] = None
    tb.loc[(tb["country"] != "World") & (tb["year"] < 1975), "production|Gold|Mine|tonnes"] = None
    tb.loc[
        (tb["country"] != "World") & (tb["year"].isin([2014])),
        "production|Lead|Mine|tonnes",
    ] = None
    tb.loc[
        (tb["country"] != "World") & (tb["year"].isin([2008, 2021])),
        "production|Magnesium metal|Smelter|tonnes",
    ] = None
    tb.loc[
        (tb["country"] != "World") & (tb["year"].isin([1992])),
        "production|Cobalt|Refinery|tonnes",
    ] = None
    tb.loc[
        (tb["country"] != "World") & (tb["year"].isin([1977, 1978, 1979, 1983])),
        "production|Iodine|Mine|tonnes",
    ] = None

    ####################################################################################################################

    # Sanity check: Raise an error if the aggregate of all data is significantly larger than the original "World" given
    # by USGS.
    _raise_error_on_large_deviations(tb=tb, ds_regions=ds_regions)

    # It would be useful to have a global total for certain cases, like Coal and Petroleum, and some critical minerals,
    # which come from BGS and are quite complete.
    tb_global = tb[tb["country"] == "World (BGS)"][["country", "year"] + COLUMNS_TO_KEEP_WORLD_BGS].reset_index(
        drop=True
    )
    tb_global["country"] = tb_global["country"].replace("World (BGS)", "World")

    # For all other indicators, remove the "World (BGS)" (aggregated in the garden BGS step), that was only kept for sanity checks.
    tb = tb[tb["country"] != "World (BGS)"].reset_index(drop=True)
    tb = combine_two_overlapping_dataframes(tb, tb_global, index_columns=["country", "year"])

    # # Visually compare the resulting Coal and Oil global data with the ones from the Statistical Review of World Energy.
    # from etl.paths import DATA_DIR
    # tb_sr = Dataset(DATA_DIR / "garden/energy_institute/2024-06-20/statistical_review_of_world_energy").read("statistical_review_of_world_energy")
    # tb_sr = tb_sr[tb_sr["country"]=="World"][["country", "year", 'coal_production_mt', 'oil_production_mt']].rename(columns={"coal_production_mt": "production|Coal|Mine|tonnes", "oil_production_mt": "production|Petroleum|Crude|tonnes"})
    # tb_sr[["production|Coal|Mine|tonnes", "production|Petroleum|Crude|tonnes"]] *= 1e6
    # for column in ["production|Coal|Mine|tonnes", "production|Petroleum|Crude|tonnes"]:
    #     compare = pr.concat([tb_sr.assign(**{"source": "EI"}), tb_global[["country", "year", column]].assign(**{"source": "BGS"})], ignore_index=True)
    #     px.line(compare, x="year", y=column, color="source", markers=True, title=column).show()

    # Indeed, the aggregated global coal and petroleum production obtained from BGS data is reasonable agreement with the Statistical Review.

    return tb


def _add_global_data_for_comparison(tb: Table, ds_regions: Dataset) -> Table:
    _tb = tb.copy()
    # We want to create a "World" aggregate, just for sanity checks:
    # * We will ensure there are no overlaps between historical and successor regions.
    # * We will ensure that the given "World" agrees with the sum of all countries (within a certain error).
    # NOTE: This "World aggregate" will be ignored after sanity checks (and we will keep the original USGS' "World").

    # Known overlaps between historical and successor regions (only on years when the historical region dissolved).
    accepted_overlaps = [
        # {1991: {"USSR", "Russia"}},
        {1992: {"Czechia", "Czechoslovakia"}},
        {1992: {"Slovakia", "Czechoslovakia"}},
        # {1990: {"Germany", "East Germany"}},
        # {1990: {"Germany", "West Germany"}},
        # {1990: {"Yemen", "Yemen People's Republic"}},
    ]
    # Regions to create.
    # NOTE: We only need "World", but we need other regions to construct it.
    regions = {
        "Africa": {},
        "Asia": {},
        "Europe": {},
        "North America": {},
        "Oceania": {},
        "South America": {},
        "World": {"additional_members": ["Other"]},
    }
    _tb = geo.add_regions_to_table(
        tb=_tb[_tb["country"] != "World (BGS)"],
        regions=regions,
        ds_regions=ds_regions,
        min_num_values_per_year=1,
        accepted_overlaps=accepted_overlaps,
        keep_original_region_with_suffix=" (original)",
    )
    # Now that we have a World aggregate (and we are sure there is no double-counting) remove all other regions.
    regions_to_remove = [region for region in regions if region not in ["World", "World (original)"]]
    _tb = _tb.loc[~_tb["country"].isin(regions_to_remove)].reset_index(drop=True)

    # Include the original "World (BGS)" for comparison.
    _tb = pr.concat([_tb, tb[tb["country"] == "World (BGS)"]], ignore_index=True)

    return _tb


def run_sanity_checks(tb: Table) -> None:
    # Check that there are no duplicated rows.
    assert tb[tb.duplicated(subset=["country", "year"], keep=False)].empty, "Unexpected duplicated data."
    # Check that all share columns are <100%.
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
    tb_usgs_historical_flat = ds_usgs_historical.read("historical_statistics_for_mineral_and_material_commodities_flat")
    tb_usgs_flat = ds_usgs.read("mineral_commodity_summaries_flat")
    tb_bgs_flat = ds_bgs.read("world_mineral_statistics_flat")

    # Load regions dataset.
    # NOTE: It will only be used for sanity checks.
    ds_regions = paths.load_dataset("regions")

    #
    # Process data.
    #
    # Adapt USGS current flat table.
    tb_usgs_flat = adapt_flat_table(tb_flat=tb_usgs_flat)

    # Adapt USGS historical flat table.
    tb_usgs_historical_flat = adapt_flat_table(tb_flat=tb_usgs_historical_flat)

    # Adapt BGS flat table.
    tb_bgs_flat = adapt_flat_table(tb_flat=tb_bgs_flat)

    # Given that BGS imports/exports data is very sparse (from 2002 it includes only European countries and Turkey, and
    # from 2018 it includes only UK), remove those columns.
    tb_bgs_flat = tb_bgs_flat.drop(
        columns=[column for column in tb_bgs_flat.columns if column.startswith(("imports", "exports"))]
    )

    # Combine all sources of data.
    tb = combine_data(
        tb_usgs_flat=tb_usgs_flat,
        tb_usgs_historical_flat=tb_usgs_historical_flat,
        tb_bgs_flat=tb_bgs_flat,
        ds_regions=ds_regions,
        columns_to_plot=PLOT_TO_COMPARE_DATA_SOURCES,
    )

    # Create columns for share of world (i.e. production, import, exports and reserves as a share of global).
    tb = add_share_of_global_columns(tb=tb)

    # Improve metadata.
    tb = improve_metadata(
        tb=tb, tb_usgs_flat=tb_usgs_flat, tb_bgs_flat=tb_bgs_flat, tb_usgs_historical_flat=tb_usgs_historical_flat
    )
    # NOTE: Titles, units and descriptions generated with the above function will be overwritten by the content of the
    #  accompanying meta.yaml file.
    #  To regenerate that yaml file, execute the following lines and manually copy the content in the meta.yaml file.
    # from etl.helpers import print_tables_metadata_template
    # print_tables_metadata_template([tb], fields=["title", "unit", "short_unit", "description_short", "presentation.title_public"])

    # Discard some columns, since they are not as critical, and add too much complexity to the explorer.
    tb = tb.drop(columns=COLUMNS_TO_DISCARD, errors="raise")

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
