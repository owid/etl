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

from typing import List, Optional, Tuple

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table, VariablePresentationMeta
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Prefix used for "share" columns.
SHARE_OF_GLOBAL_PREFIX = "share_of_global_"


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
        if notes is not None:
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
        sub_commodity = sub_commodity.lower()

        # Ensure the variable has a title.
        tb[column].metadata.title = column

        # Create a title_public.
        # title_public = f"{metric} of {commodity.lower()} ({sub_commodity.lower()})"
        title_public = f"{commodity} ({sub_commodity}) {metric.lower()}"
        if tb[column].metadata.presentation is None:
            tb[column].metadata.presentation = VariablePresentationMeta()

        if column.startswith(SHARE_OF_GLOBAL_PREFIX):
            title_public = title_public.replace("share of global ", "") + " as a share of the global total"

        tb[column].metadata.presentation.title_public = title_public

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

        # Add footnotes to metadata.
        combined_footnotes = _combine_notes(
            notes_list=[footnotes_bgs, footnotes_usgs, footnotes_usgs_historical], separator=" "
        )
        if len(combined_footnotes) > 0:
            tb[column].metadata.presentation.grapher_config["note"] = combined_footnotes

    return tb


def inspect_overlaps(
    tb: Table,
    tb_usgs_flat: Table,
    tb_usgs_historical_flat: Table,
    tb_bgs_flat: Table,
    minerals: Optional[List[str]] = None,
) -> None:
    import pandas as pd
    import plotly.express as px

    for column in tb.drop(columns=["country", "year"]).columns:
        if minerals:
            mineral = column.split("|")[1]
            if mineral not in minerals:
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
                    _df_country, x="year", y=column, color="source", markers=True, title=f"{column} - {country}"
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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load datasets.
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

    # Create a combined flat table.
    # Firstly, combine USGS current and historical. Since the former is more up-to-date, prioritize it.
    tb = combine_two_overlapping_dataframes(
        df1=tb_usgs_flat, df2=tb_usgs_historical_flat, index_columns=["country", "year"]
    )
    # Then, combine the result with BGS data. After inspection, it seems that, where USGS and BGS data overlap, BGS is
    # usually more complete. All region aggregates from BGS have larger values than USGS' region aggregates (even though
    # data for individual countries agrees reasonably well). However, the latest year is always from USGS.
    # So, I decided to remove region aggregates from USGS, and prioritize USGS over BGS data.
    tb = combine_two_overlapping_dataframes(df1=tb, df2=tb_bgs_flat, index_columns=["country", "year"])

    # Uncomment for debugging purposes, to compare the data from different origins where they overlap.
    # inspect_overlaps(
    #     tb=tb,
    #     tb_usgs_flat=tb_usgs_flat,
    #     tb_usgs_historical_flat=tb_usgs_historical_flat,
    #     tb_bgs_flat=tb_bgs_flat,
    #     minerals=["Mercury"],
    # )

    # Create columns for share of world (i.e. production, import, exports and reserves as a share of global).
    tb = add_share_of_global_columns(tb=tb)

    for column in [column for column in tb.columns if column.startswith(SHARE_OF_GLOBAL_PREFIX)]:
        if (tb[column] > 100).any():
            log.warning(f"{column} maximum: {tb[column].max():.0f}%")

    # Improve metadata.
    tb = improve_metadata(
        tb=tb, tb_usgs_flat=tb_usgs_flat, tb_bgs_flat=tb_bgs_flat, tb_usgs_historical_flat=tb_usgs_historical_flat
    )

    # Format combined table conveniently.
    tb = tb.format(["country", "year"], short_name="minerals").astype("Float64")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
