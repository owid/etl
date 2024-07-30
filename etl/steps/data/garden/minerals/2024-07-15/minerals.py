"""Compilation of minerals data from different origins."""
import ast
from typing import Dict

import pandas as pd
from owid.catalog import Table, VariablePresentationMeta
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from structlog import get_logger
from tqdm.auto import tqdm

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def gather_bgs_notes(tb_bgs: Table) -> Dict[str, str]:
    # Create another table with the same structure, but containing notes.
    tb_bgs_flat_notes = tb_bgs.pivot(
        index=["country", "year"],
        columns=["commodity", "sub_commodity", "unit"],
        values=["notes_exports", "notes_imports", "notes_production"],
        join_column_levels_with="|",
    )
    tb_bgs_flat_notes = tb_bgs_flat_notes.rename(
        columns={column: column.replace("notes_", "") for column in tb_bgs_flat_notes.columns}
    )

    # Gather all notes for each column.
    notes_dict = {}
    for column in tqdm(tb_bgs_flat_notes.drop(columns=["country", "year"]).columns):
        _notes = tb_bgs_flat_notes[column].dropna().tolist()
        if len(_notes) > 0:
            # Gather all notes for this column.
            notes = sum(_notes, [])
            # Get unique notes keeping the order.
            notes = pd.unique(pd.Series(notes)).tolist()
            # Join notes.
            if len(notes) > 0:
                notes_str = "- " + "\n- ".join(notes)
                notes_dict[column] = notes_str

    return notes_dict


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load datasets.
    ds_bgs = paths.load_dataset("world_mineral_statistics")
    ds_usgs_historical = paths.load_dataset("historical_statistics_for_mineral_and_material_commodities")
    ds_usgs = paths.load_dataset("mineral_commodity_summaries")

    # Read tables.
    tb_bgs = ds_bgs.read_table("world_mineral_statistics")
    tb_usgs_historical = (
        ds_usgs_historical["historical_statistics_for_mineral_and_material_commodities"]
        .astype(float)
        .reset_index()
        .rename(columns={"unit_value_constant": "unit_value"}, errors="raise")
    )[["country", "year", "commodity", "sub_commodity", "production", "unit_value"]]
    tb_usgs = (
        ds_usgs["mineral_commodity_summaries"]
        .drop(columns=["reserves_notes", "production_notes"])
        .astype(float)
        .reset_index()
    )[["country", "year", "commodity", "sub_commodity", "reserves", "production"]]

    #
    # Process data.
    #
    # Parse notes as lists of strings.
    for column in [
        "notes_production",
        "notes_imports",
        "notes_exports",
    ]:
        tb_bgs[column] = [notes if len(notes) > 0 else None for notes in [ast.literal_eval(x) for x in tb_bgs[column]]]

    # TODO: Find the actual units of USGS data.
    # TODO: Move this to usgs garden step.
    # For now, assume all USGS data is in tonnes. Later on, Unit value will be changed to USD.
    tb_usgs["unit"] = "tonnes"
    tb_usgs_historical["unit"] = "tonnes"
    #
    # We extract data from three sources:
    # * From BGS we extract imports, exports, and production.
    # * From USGS (current) we extract production and reserves.
    # * From USGS (historical) we extract production and unit value.
    #
    # Ideally (if all commodities and subcommodities were well harmonized) we would need to:
    # * Firstly, combine USGS current and historical production data.
    #   This way, we would have long-run (historical) data for the US and World, and all other countries would have
    #   some years of production and reserves data.
    # * Secondly, combine USGS and BGS production data. We would need to decide which one to prioritize.
    #
    # However, currently, given the lack of harmonization, there is absolutely no overlap between USGS current and
    # historical data; and there is also no overlap between USGS and BGS data.
    # There is overlap, however, between USGS historical and BGS data.
    # If we combine all production data into one column (as it would be the customary thing to do in a garden step), the
    # data will show as having 3 sources, whereas each data point would have just one source (or two, in a few cases).
    # So it seems more convenient to create a wide table where each column has its own source(s), instead of a long one.

    # TODO: Data from BGS and USGS historical are significantly different for Salt (by a factor of 2 or 3).
    #  Other series are similar, but have some range where there are large discrepancies, e.g. Iron ore and Asbestos.
    # country = "World"
    # import owid.catalog.processing as pr
    # check = pr.concat([tb_bgs.assign(**{"source": "BGS"}), tb_usgs_historical.assign(**{"source": "USGS"})], ignore_index=True)
    # check["unit"] = check["unit"].fillna("tonnes")
    # for commodity in tb_usgs["commodity"].unique():
    #     _check = check[(check["country"] == country) & (check["commodity"] == commodity) & (check["sub_commodity"] == "Total")].dropna(subset="production")
    #     if _check["source"].nunique() == 2:
    #         px.line(_check, x="year", y="production", color="source", markers=True, title=f"{commodity} - {country}").show()

    # Create a wide version for each table.

    # Pivot USGS table and remove empty columns.
    tb_usgs_flat = tb_usgs.pivot(
        index=["country", "year"],
        columns=["commodity", "sub_commodity", "unit"],
        values=["production", "reserves"],
        join_column_levels_with="|",
    ).dropna(axis=1, how="all")

    # Pivot USGS historical table and remove empty columns.
    tb_usgs_historical_flat = tb_usgs_historical.pivot(
        index=["country", "year"],
        columns=["commodity", "sub_commodity", "unit"],
        values=["production", "unit_value"],
        join_column_levels_with="|",
    ).dropna(axis=1, how="all")

    # Pivot BGS table.
    tb_bgs_flat = tb_bgs.pivot(
        index=["country", "year"],
        columns=["commodity", "sub_commodity", "unit"],
        values=["exports", "imports", "production"],
        join_column_levels_with="|",
    )

    # Gather notes for BGS data.
    notes_bgs = gather_bgs_notes(tb_bgs)

    # Create a combined flat table.
    tb = combine_two_overlapping_dataframes(
        df1=tb_usgs_flat, df2=tb_usgs_historical_flat, index_columns=["country", "year"]
    )
    tb = combine_two_overlapping_dataframes(df1=tb, df2=tb_bgs_flat, index_columns=["country", "year"])

    # Improve metadata of new columns.
    for column in tb.drop(columns=["country", "year"]).columns:
        metric, commodity, sub_commodity, unit = column.split("|")
        metric = metric.replace("_", " ").capitalize()
        commodity = commodity.capitalize()
        sub_commodity = sub_commodity.lower()
        title_public = f"{metric} of {commodity.lower()} ({sub_commodity.lower()})"
        tb[column].metadata.title = column
        if tb[column].metadata.presentation is None:
            tb[column].metadata.presentation = VariablePresentationMeta()
        tb[column].metadata.presentation.title_public = title_public
        tb[column].metadata.unit = unit
        if unit.startswith("tonnes"):
            # Create short unit.
            tb[column].metadata.short_unit = "t"
        else:
            log.warning(f"Unexpected unit for column: {column}")
            tb[column].metadata.short_unit = ""
        if metric == "Unit value":
            tb[column].metadata.unit = "constant 1998 US$ per tonne"
            tb[column].metadata.short_unit = "$/t"

        if column in notes_bgs:
            combined_note = "Notes found in BGS original data:\n" + notes_bgs[column]
            tb[column].metadata.description_from_producer = combined_note

    # Format combined table conveniently.
    tb = tb.format(["country", "year"], short_name="minerals")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
