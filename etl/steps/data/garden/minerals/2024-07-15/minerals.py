"""Compilation of minerals data from different origins."""
import ast
from typing import Dict, List

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


def gather_notes(tb: Table, notes_columns: List[str]) -> Dict[str, str]:
    # Create another table with the same structure, but containing notes.
    tb_flat_notes = tb.pivot(
        index=["country", "year"],
        columns=["commodity", "sub_commodity", "unit"],
        values=notes_columns,
        join_column_levels_with="|",
    )
    tb_flat_notes = tb_flat_notes.rename(
        columns={column: column.replace("notes_", "") for column in tb_flat_notes.columns}
    )

    # Gather all notes for each column.
    notes_dict = {}
    for column in tqdm(tb_flat_notes.drop(columns=["country", "year"]).columns):
        _notes = tb_flat_notes[column].dropna().tolist()
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


def parse_notes(tb: Table, notes_columns: List[str]) -> Table:
    tb = tb.copy()
    # Parse BGS notes as lists of strings.
    for column in notes_columns:
        tb[column] = [notes if len(notes) > 0 else None for notes in [ast.literal_eval(x) for x in tb[column]]]

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
    tb_bgs = ds_bgs.read_table("world_mineral_statistics")
    tb_usgs_historical = ds_usgs_historical.read_table("historical_statistics_for_mineral_and_material_commodities")
    tb_usgs_flat = ds_usgs.read_table("mineral_commodity_summaries_flat")

    #
    # Process data.
    #
    # Select and rename columns in USGS historical data.
    tb_usgs_historical = tb_usgs_historical.rename(columns={"unit_value_constant": "unit_value"}, errors="raise").drop(
        columns=["unit_value_current"], errors="raise"
    )

    # Parse BGS notes as lists of strings.
    tb_bgs = parse_notes(tb_bgs, notes_columns=["notes_exports", "notes_imports", "notes_production"])

    # We extract data from three sources:
    # * From BGS we extract imports, exports, and production.
    # * From USGS (current) we extract production and reserves.
    # * From USGS (historical) we extract production and unit value.
    #
    # However, there is little overlap between the three sources.
    # If we combine all production data into one column (as it would be the customary thing to do in a garden step), the
    # data will show as having 3 sources, whereas in reality most data points would come from just one source.
    # So it seems more convenient to create a wide table where each column has its own source(s), instead of a long one,
    # and then combine the flat tables.

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

    # For consistency with other tables, rename USGS current columns and adapt column types.
    tb_usgs_flat = tb_usgs_flat.rename(
        columns={
            column: tb_usgs_flat[column].metadata.title
            for column in tb_usgs_flat.columns
            if column not in ["country", "year"]
        },
        errors="raise",
    )
    tb_usgs_flat = tb_usgs_flat.astype(
        {column: "Float64" for column in tb_usgs_flat.columns if column not in ["country", "year"]}
    )

    # Gather notes for BGS data.
    notes_bgs = gather_notes(tb_bgs, notes_columns=["notes_exports", "notes_imports", "notes_production"])

    # Create a combined flat table.
    tb = combine_two_overlapping_dataframes(
        df1=tb_usgs_flat, df2=tb_usgs_historical_flat, index_columns=["country", "year"]
    )
    tb = combine_two_overlapping_dataframes(df1=tb, df2=tb_bgs_flat, index_columns=["country", "year"])

    # Uncomment for debugging purposes.
    # for column in tb.drop(columns=["country", "year"]).columns:
    #     if (column in tb_usgs_flat.columns) and (column in tb_usgs_historical_flat.columns):
    #         log.info(f"Combining overlapping data from USGS current and historical data: {column}")
    #     if (column in tb_usgs_flat.columns) and (column in tb_bgs_flat.columns):
    #         log.info(f"Combining overlapping data from USGS current and BGS data: {column}")
    #     if (column in tb_usgs_historical_flat.columns) and (column in tb_bgs_flat.columns):
    #         log.info(f"Combining overlapping data from USGS historical and BGS data: {column}")

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

        # Get notes from USGS' description_from_producer field.
        notes_usgs = None
        if column in tb_usgs_flat.columns:
            notes_usgs = tb_usgs_flat[column].metadata.description_from_producer

        # Add notes to metadata.
        combined_notes = ""
        if column in notes_bgs:
            combined_notes += "Notes found in BGS original data:\n" + notes_bgs[column]

        if (column in notes_bgs) and (notes_usgs is not None):
            combined_notes += "\n\n"

        if notes_usgs is not None:
            combined_notes += notes_usgs

        if len(combined_notes) > 0:
            tb[column].metadata.description_from_producer = combined_notes

    # Format combined table conveniently.
    tb = tb.format(["country", "year"], short_name="minerals")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
