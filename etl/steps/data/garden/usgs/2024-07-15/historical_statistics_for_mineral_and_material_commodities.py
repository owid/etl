"""Load a meadow dataset and create a garden dataset."""

from typing import Dict

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table, VariablePresentationMeta
from owid.datautils.dataframes import map_series
from tqdm.auto import tqdm

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Harmonize commodity-subcommodity names.
# NOTE: The original data had only commodity (not subcommodity), so we assume subcommodity "Total" for all minerals to
#  begin with. Therefore, all keys in the following dictionary will have "Total" as subcommodity".
#  Those keys should contain all commodities expected in the data.
#  Set the value to None for any commodity-subcommodity that should not be included in the output.
#  To use a subcommodity different than "Total", simply rewrite the value.
COMMODITY_MAPPING = {
    ("Alumina", "Total"): ("Alumina", "Refinery"),
    ("Aluminum", "Total"): ("Aluminum", "Smelter"),
    ("Aluminum oxide", "Total"): None,
    ("Aluminum-zirconium oxide", "Total"): None,
    ("Antimony", "Total"): ("Antimony", "Mine"),
    ("Arsenic", "Total"): ("Arsenic", "Processing"),
    ("Asbestos", "Total"): ("Asbestos", "Mine"),
    ("Ball clay", "Total"): ("Clays", "Ball clay"),
    ("Barite", "Total"): ("Barite", "Mine"),
    ("Bauxite", "Total"): ("Bauxite", "Mine"),
    # NOTE: For consistency with USGS current data, rename the following.
    ("Bentonite", "Total"): ("Clays", "Bentonite"),
    ("Beryllium", "Total"): ("Beryllium", "Mine"),
    ("Bismuth", "Total"): ("Bismuth", "Mine"),
    ("Boron", "Total"): ("Boron", "Mine"),
    ("Boron carbide", "Total"): None,
    ("Cadmium", "Total"): ("Cadmium", "Refinery"),
    ("Cement", "Total"): ("Cement", "Total"),
    ("Cesium", "Total"): ("Cesium", "Total"),
    ("Chromium", "Total"): ("Chromium", "Mine, contained chromium"),
    ("Cobalt", "Total"): ("Cobalt", "Total"),
    ("Construction sand and gravel", "Total"): ("Sand and gravel", "Construction"),
    ("Copper", "Total"): ("Copper", "Mine"),
    ("Crushed stone", "Total"): ("Crushed stone", "Total"),
    ("Diatomite", "Total"): ("Diatomite", "Total"),
    ("Dimension stone", "Total"): ("Dimension stone", "Total"),
    # NOTE: The following could be mapped to ("Iron", "Sponge"). But for now, we decided to exclude it.
    ("Direct Reduced Iron", "Total"): None,
    ("Feldspar", "Total"): ("Feldspar", "Total"),
    ("Fire clay", "Total"): ("Clays", "Fire clay"),
    ("Fluorspar", "Total"): ("Fluorspar", "Total"),
    ("Fuller's earth", "Total"): ("Fuller's earth", "Total"),
    ("Gallium", "Total"): ("Gallium", "Refinery"),
    ("Germanium", "Total"): ("Germanium", "Total"),
    ("Gold", "Total"): ("Gold", "Mine"),
    ("Graphite", "Total"): ("Graphite", "Mine"),
    ("Gypsum", "Total"): ("Gypsum", "Total"),
    ("Helium", "Total"): ("Helium", "Total"),
    ("Indium", "Total"): ("Indium", "Refinery"),
    # NOTE: Industrial diamond production includes natural and synthetic diamonds.
    #  But in USGS current data, industrial diamond production includes only natural diamond.
    ("Industrial diamond", "Total"): ("Diamond", "Mine and synthetic, industrial"),
    ("Industrial garnet", "Total"): ("Industrial garnet", "Total"),
    ("Industrial sand and gravel", "Total"): ("Sand and gravel", "Industrial"),
    ("Iron Oxide Pigments", "Total"): None,
    ("Iron and Steel Slag", "Total"): None,
    ("Iron ore", "Total"): ("Iron ore", "Crude ore"),
    ("Kaolin", "Total"): ("Kaolin", "Total"),
    ("Lead", "Total"): ("Lead", "Mine"),
    ("Lime", "Total"): ("Lime", "Processing"),
    ("Magnesium compounds", "Total"): ("Magnesium compounds", "Total"),
    ("Magnesium metal", "Total"): ("Magnesium metal", "Primary"),
    ("Manganese", "Total"): ("Manganese", "Mine"),
    ("Mercury", "Total"): ("Mercury", "Mine"),
    ("Metallic abrasives", "Total"): None,
    ("Mica (natural), scrap and flake", "Total"): ("Mica", "Natural, scrap and flake"),
    ("Mica (natural), sheet", "Total"): ("Mica", "Natural, sheet"),
    ("Miscellaneous clay", "Total"): ("Clays", "Miscellaneous"),
    ("Molybdenum", "Total"): ("Molybdenum", "Mine"),
    ("Nickel", "Total"): ("Nickel", "Mine"),
    ("Niobium", "Total"): ("Niobium", "Total"),
    ("Nitrogen (Fixed)-Ammonia", "Total"): ("Nitrogen", "Total, fixed ammonia"),
    ("Peat", "Total"): ("Peat", "Total"),
    ("Perlite", "Total"): ("Perlite", "Total"),
    ("Phosphate rock", "Total"): ("Phosphate rock", "Total"),
    ("Pig Iron", "Total"): ("Iron", "Pig iron"),
    ("Pumice and Pumicite", "Total"): ("Pumice and Pumicite", "Total"),
    ("Salt", "Total"): ("Salt", "Total"),
    ("Selenium", "Total"): ("Selenium", "Refinery"),
    ("Silicon", "Total"): ("Silicon", "Processing"),
    ("Silicon carbide", "Total"): None,
    ("Silver", "Total"): ("Silver", "Mine"),
    ("Soda ash", "Total"): ("Soda ash", "Total"),
    ("Steel", "Total"): ("Steel", "Crude"),
    ("Strontium", "Total"): ("Strontium", "Total"),
    ("Sulfur", "Total"): ("Sulfur", "Total"),
    ("Talc and pyrophyllite", "Total"): ("Talc and pyrophyllite", "Total"),
    ("Tantalum", "Total"): ("Tantalum", "Total"),
    ("Tellurium", "Total"): ("Tellurium", "Refinery"),
    ("Tin", "Total"): ("Tin", "Mine"),
    ("Titanium dioxide", "Total"): ("Titanium dioxide", "Total"),
    ("Titanium scrap", "Total"): ("Titanium scrap", "Total"),
    ("Titanium sponge", "Total"): ("Titanium sponge", "Total"),
    # NOTE: After combing data with BGS, USGS' "Total" production is smaller than the sum of all clays production.
    #  To avoid confusion, remove this total.
    ("Total clay", "Total"): None,
    ("Total manufactured abrasives ", "Total"): None,
    ("Tungsten", "Total"): ("Tungsten", "Total"),
    ("Vanadium", "Total"): ("Vanadium", "Total"),
    ("Zinc", "Total"): ("Zinc", "Mine"),
    # Added pairs that were in "world_mine_production" and "world_refinery_production" columns.
    ("Beryllium", "Mine"): ("Beryllium", "Mine"),
    ("Bismuth", "Mine"): ("Bismuth", "Mine"),
    ("Bismuth", "Refinery"): ("Bismuth", "Refinery"),
    ("Cobalt", "Mine"): ("Cobalt", "Mine"),
    ("Cobalt", "Refinery"): ("Cobalt", "Refinery"),
    ("Niobium", "Mine"): ("Niobium", "Mine"),
}

# Units can either be "metric tonnes" or "metric tonnes of gross weight".
# Since this data is later on combined with USGS current data (given in tonnes), we need to ensure that they mean the
# same thing.
# So, to be conservative, go to the explorer and inspect those minerals that come as "tonnes of gross weight"; compare them to the USGS current data (given in "tonnes"); if they are in reasonable agreement, add them to the following list.
# Their unit will be converted to "tonnes", and hence combined with USGS current data.
# NOTE: The names below must coincide with the original names of the commodities (before harmonizing commodity-subcommodity pairs).
MINERALS_TO_CONVERT_TO_TONNES = [
    "Alumina",
    "Asbestos",
    "Barite",
    "Ball clay",
    "Bauxite",
    "Bentonite",
    "Cement",
    "Clays",
    "Construction sand and gravel",
    "Fire clay",
    "Total clay",
    "Miscellaneous clay",
    "Graphite",
    "Industrial diamond",
    "Industrial sand and gravel",
    "Iron ore",
    "Lime",
    "Crushed stone",
]

# Footnotes (that will appear in the footer of charts) to add to the flattened tables (production and unit value).
FOOTNOTES_PRODUCTION = {
    "production|Alumina|Refinery|tonnes": "Values are reported as quantity produced before 1971 and as calcined alumina equivalents afterwards.",
    "production|Bauxite|Mine|tonnes": "Values are reported as dried bauxite equivalents.",
    "production|Barite|Mine|tonnes": "Values are reported as gross weight.",
    "production|Asbestos|Mine|tonnes": "Values are reported as gross weight.",
    "production|Clays|Bentonite|tonnes": "Values are reported as gross weight.",
    "production|Clays|Ball clay|tonnes": "Values are reported as gross weight.",
    "production|Clays|Fire clay|tonnes": "Values are reported as gross weight.",
}
FOOTNOTES_UNIT_VALUE = {}


def harmonize_commodity_subcommodity_pairs(tb: Table) -> Table:
    # Assume "Total" for all those missing subcommodities (e.g. for the US).
    tb["sub_commodity"] = tb["sub_commodity"].fillna("Total")
    missing_mappings = set(
        [tuple(pair) for pair in tb[["commodity", "sub_commodity"]].drop_duplicates().values.tolist()]
    ) - set(COMMODITY_MAPPING)
    assert len(missing_mappings) == 0, f"Missing mappings: {missing_mappings}"
    tb = tb.astype({"commodity": str, "sub_commodity": str}).copy()
    for pair_old, pair_new in COMMODITY_MAPPING.items():
        if pair_old == pair_new:
            # Nothing to do, continue.
            continue

        # Get the old commodity-subcommodity names.
        commodity_old, subcommodity_old = pair_old
        if pair_new is None:
            # Remove rows for this combination.
            index_to_drop = tb.loc[(tb["commodity"] == commodity_old) & (tb["sub_commodity"] == subcommodity_old)].index
            tb = tb.drop(index_to_drop).reset_index(drop=True)
            continue

        # Get the new commodity-subcommodity names.
        commodity_new, subcommodity_new = pair_new
        # Rename the commodity-subcommodity pair.
        tb.loc[
            (tb["commodity"] == commodity_old) & (tb["sub_commodity"] == subcommodity_old),
            ["commodity", "sub_commodity"],
        ] = pair_new

    return tb


def clean_notes(note):
    notes_clean = []
    # After creating region aggregates, some notes become nan.
    # But in all other cases, notes are lists (either empty or filled with strings).
    # Therefore, pd.isnull(notes) returns either a boolean or a numpy array.
    # If it's a boolean, it means that all notes are nan (but just to be sure, also check that the boolean is True).
    is_null = pd.isnull(note)
    if is_null:
        return notes_clean

    # Ensure each note starts with a capital letter, and ends in a single period.
    # NOTE: Using capitalize() would make all characters lower case except the first.
    note = note[0].upper() + (note[1:].replace("\xa0", " ") + ".").replace("..", ".")
    notes_clean.append(note)

    return notes_clean


def gather_notes(tb_combined: Table) -> Dict[str, str]:
    notes_columns = [column for column in tb_combined.columns if column.startswith("notes_")]
    # Create another table with the same structure, but containing notes.
    tb_flat_notes = (
        tb_combined[
            ["commodity", "sub_commodity", "country", "year", "unit"]
            + [c for c in tb_combined.columns if c.startswith("notes_")]
        ]
        .pivot(
            index=["country", "year"],
            columns=["commodity", "sub_commodity", "unit"],
            values=notes_columns,
            join_column_levels_with="|",
        )
        .dropna(axis=1, how="all")
    )
    tb_flat_notes = tb_flat_notes.rename(
        columns={column: column.replace("notes_", "") for column in tb_flat_notes.columns}
    )

    # Gather all notes for each column.
    notes_dict = {}
    for column in tqdm(tb_flat_notes.drop(columns=["country", "year"]).columns, disable=True):
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


def harmonize_units(tb: Table) -> Table:
    # See explanation above, where MINERALS_TO_CONVERT_TO_TONNES is defined.
    assert set(tb["unit"]) == {"metric tonnes", "metric tonnes of gross weight"}
    tb["unit"] = (
        tb["unit"]
        .astype("string")
        .replace({"metric tonnes": "tonnes", "metric tonnes of gross weight": "tonnes of gross weight"})
    )
    tb.loc[tb["commodity"].isin(MINERALS_TO_CONVERT_TO_TONNES), "unit"] = "tonnes"

    return tb


def prepare_us_production(tb: Table, tb_metadata: Table) -> Table:
    # Select columns for US production.
    # NOTE: There are several other columns for production (e.g. "primary_production", "secondary_production", etc.).
    # For now, we'll only keep "production".
    tb_us_production = tb[["commodity", "year", "production", "unit"]].assign(**{"country": "United States"})
    # Remove spurious footnotes like "W".
    tb_us_production["production"] = map_series(
        tb_us_production["production"],
        mapping={"W": None},
        warn_on_missing_mappings=False,
        warn_on_unused_mappings=True,
    ).astype({"production": float})
    # Add notes to the table, using the extracted metadata.
    for column in ["production"]:
        mask = tb_metadata[column].notnull()
        tb_metadata.loc[mask, column] = "Note on United States production: " + tb_metadata[column][mask]
        tb_us_production = tb_us_production.merge(
            tb_metadata[["commodity", column]].rename(
                columns={column: f"notes_{column}" for column in tb_metadata.columns if column != "commodity"},
                errors="ignore",
            ),
            on="commodity",
            how="left",
        )

    # For now, assume "Total" subcommodity.
    tb_us_production["sub_commodity"] = "Total"

    return tb_us_production


def prepare_world_production(tb: Table, tb_metadata: Table) -> Table:
    # NOTE: There are 4 columns for world production, namely:
    # * "world_mine_production" (which exists for Beryllium, Cobalt and Niobium),
    # * "world_mine_production__metal_content" (which exists only for bismuth),
    # * "world_refinery_production" (which exists for Bismuth and Cobalt),
    # * "world_production" (which exists for other commodities).

    # Initialize table for world production.
    tb_world_production = Table()
    for column in [
        "world_production",
        "world_mine_production",
        "world_mine_production__metal_content",
        "world_refinery_production",
    ]:
        if "mine" in column:
            sub_commodity = "Mine"
        elif "refinery" in column:
            sub_commodity = "Refinery"
        else:
            sub_commodity = "Total"

        _tb_production = (
            tb[["commodity", "year", column, "unit"]]
            .rename(columns={column: "production"}, errors="raise")
            .assign(**{"country": "World"})
            .assign(**{"sub_commodity": sub_commodity})
            .astype({"production": float})
            .dropna(subset="production")
            .reset_index(drop=True)
        )
        if "metal_content" in column:
            _tb_production["unit"] = "tonnes of metal content"
        elif "refinery" in column:
            # world_refinery_production is informed only for Bismuth and Cobalt.
            # In the case of Bismuth, the title says "gross weight unless otherwise noted"
            # (and nothing else is noted for world refinery production).
            # However, in both cases, the unit is probably "tonnes of metal content".
            _tb_production["unit"] = "tonnes of metal content"

        # Add notes to the table, using the extracted metadata.
        mask = tb_metadata[column].notnull()
        tb_metadata.loc[mask, column] = "Note on global production: " + tb_metadata[column][mask]
        _tb_production = _tb_production.merge(
            tb_metadata[["commodity", column]].rename(columns={column: "notes_production"}, errors="raise"),
            on="commodity",
            how="left",
        )
        # Combine tables.
        tb_world_production = pr.concat([tb_world_production, _tb_production], ignore_index=True)

    return tb_world_production


def prepare_unit_value(tb: Table, tb_metadata: Table) -> Table:
    # Select columns for unit value.
    tb_unit_value = (
        tb[["commodity", "year", "unit_value_98dollar_t"]]
        .assign(**{"country": "World"})
        .rename(
            columns={"unit_value_98dollar_t": "unit_value"},
            errors="raise",
        )
    )
    # Remove spurious footnotes like "W".
    tb_unit_value["unit_value"] = tb_unit_value["unit_value"].astype("string").replace("W", None).astype(float)
    # Add notes to the table, using the extracted metadata.
    tb_unit_value = tb_unit_value.merge(
        tb_metadata[["commodity", "unit_value_98dollar_t"]].rename(
            columns={"unit_value_98dollar_t": "notes_unit_value"}, errors="raise"
        ),
        on="commodity",
        how="left",
    )

    # Drop empty rows.
    tb_unit_value = tb_unit_value.dropna(subset=["unit_value"], how="all").reset_index(drop=True)

    # Add a generic subcommodity that applies to all commodities.
    # NOTE: This may be problematic for commodities for which there are subcommodities with different unit value series.
    tb_unit_value["sub_commodity"] = "Total"

    # Add a unit.
    tb_unit_value["unit"] = "constant 1998 US$ per tonne"

    return tb_unit_value


def prepare_wide_table(tb: Table, footnotes: Dict[str, str]) -> Table:
    # Gather all notes in a dictionary.
    notes = gather_notes(tb_combined=tb)
    # Identify data columns.
    values_columns = [
        column
        for column in tb.columns
        if column not in ["country", "year", "commodity", "sub_commodity", "unit"] and not column.startswith("notes_")
    ]
    # Create a wide table.
    tb_flat = tb.pivot(
        index=["country", "year"],
        columns=["commodity", "sub_commodity", "unit"],
        values=values_columns,
        join_column_levels_with="|",
    ).dropna(axis=1, how="all")

    # NOTE: Here, I could loop over columns and improve metadata.
    # However, for convenience (since this step is not used separately), this will be done in the garden minerals step.
    # So, for now, simply add titles and descriptions from producer.
    for column in tb_flat.drop(columns=["country", "year"]).columns:
        # Create metadata title (before they become snake-case).
        tb_flat[column].metadata.title = column
        if column in notes:
            tb_flat[column].metadata.description_from_producer = (
                "Notes found in original USGS historical data:\n" + notes[column]
            )

    # Add footnotes.
    for column, note in footnotes.items():
        if not tb_flat[column].metadata.presentation:
            tb_flat[column].metadata.presentation = VariablePresentationMeta(grapher_config={})
        tb_flat[column].metadata.presentation.grapher_config["note"] = note

    tb_flat = tb_flat.astype({column: "Float64" for column in tb_flat.columns if column not in ["country", "year"]})

    return tb_flat


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("historical_statistics_for_mineral_and_material_commodities")

    # Read tables of data and extracted metadata from meadow dataset.
    tb = ds_meadow.read_table("historical_statistics_for_mineral_and_material_commodities")
    tb_metadata = ds_meadow.read_table("historical_statistics_for_mineral_and_material_commodities_metadata").astype(
        "string"
    )

    #
    # Process data.
    #
    # Harmonize units.
    tb = harmonize_units(tb=tb)

    # Remove duplicated rows that have exactly the same data.
    # NOTE: Most columns were called "World Production", but others were called "World Mine Production" (and similar).
    #  We could include them here, but it complicates the processing a bit, so, for now, stick to world production.
    tb = tb.drop_duplicates(
        subset=[
            "commodity",
            "year",
            "production",
            "world_production",
            "unit",
            "unit_value_98dollar_t",
        ]
    ).reset_index(drop=True)

    ####################################################################################################################
    # Fix duplicated rows with different data.
    assert tb[tb.duplicated(subset=["commodity", "year"])].sort_values(by=["commodity", "year"])[
        ["commodity", "year"]
    ].values.tolist() == [["Cadmium", 2021], ["Nickel", 2019]]
    # It happens for Cadmium 2021. By looking at the latest PDF:
    # https://pubs.usgs.gov/periodicals/mcs2024/mcs2024-cadmium.pdf
    # I see that the latest row should correspond to 2022.
    # Manually fix this.
    tb.loc[(tb["commodity"] == "Cadmium") & (tb["year"] == 2021) & (tb["production"] == "212"), "year"] = 2022
    # But also Nickel 2019 is repeated with different values.
    # By looking at the latest PDF:
    # https://pubs.usgs.gov/periodicals/mcs2024/mcs2024-nickel.pdf
    # For example, imports 2020 coincides with the latest row. So it looks like the latest row should be 2020.
    tb.loc[(tb["commodity"] == "Nickel") & (tb["year"] == 2019) & (tb["world_production"] == 2510000.0), "year"] = 2020
    ####################################################################################################################

    # Prepare US production.
    tb_us_production = prepare_us_production(tb=tb, tb_metadata=tb_metadata)

    # Prepare world production.
    tb_world_production = prepare_world_production(tb=tb, tb_metadata=tb_metadata)

    # Prepare unit value.
    tb_unit_value = prepare_unit_value(tb=tb, tb_metadata=tb_metadata)

    # Combine US and world production.
    tb_combined = pr.concat([tb_us_production, tb_world_production], ignore_index=True)

    # Remove empty rows.
    tb_combined = tb_combined.dropna(subset=["production"], how="all").reset_index(drop=True)

    # Harmonize commodity-subcommodity pairs.
    tb_combined = harmonize_commodity_subcommodity_pairs(tb=tb_combined)
    tb_unit_value = harmonize_commodity_subcommodity_pairs(tb=tb_unit_value)

    # Clean notes columns, and combine notes at the individual row level with general table notes.
    for column in [column for column in tb_combined.columns if column.startswith("notes_")]:
        tb_combined[column] = tb_combined[column].apply(clean_notes)
    for column in [column for column in tb_unit_value.columns if column.startswith("notes_")]:
        tb_unit_value[column] = tb_unit_value[column].apply(clean_notes)

    # Create wide tables for production and unit value.
    tb_flat = prepare_wide_table(tb=tb_combined, footnotes=FOOTNOTES_PRODUCTION)
    tb_flat_unit_value = prepare_wide_table(tb=tb_unit_value, footnotes=FOOTNOTES_UNIT_VALUE)
    tb_flat = tb_flat.merge(tb_flat_unit_value, on=["country", "year"], how="outer")

    # Drop empty columns, if any.
    tb_flat = tb_flat.dropna(axis=1, how="all").reset_index(drop=True)

    # Format tables conveniently.
    tb_combined = tb_combined.format(
        ["country", "year", "commodity", "sub_commodity", "unit"], short_name="historical_production"
    )
    tb_combined = tb_combined.astype(
        {column: "string" for column in tb_combined.columns if column.startswith("notes_")}
    )
    tb_unit_value = tb_unit_value.format(
        ["country", "year", "commodity", "sub_commodity", "unit"], short_name="historical_unit_value"
    )
    tb_unit_value = tb_unit_value.astype(
        {column: "string" for column in tb_unit_value.columns if column.startswith("notes_")}
    )
    tb_flat = tb_flat.format(["country", "year"], short_name=paths.short_name + "_flat")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_combined, tb_unit_value, tb_flat], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
