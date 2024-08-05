"""Load a meadow dataset and create a garden dataset."""

from typing import Dict, List

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
    ("Alumina", "Total"): ("Alumina", "Total"),
    ("Aluminum", "Total"): ("Aluminum", "Total"),
    ("Aluminum oxide", "Total"): ("Aluminum oxide", "Total"),
    ("Aluminum-zirconium oxide", "Total"): ("Aluminum-zirconium oxide", "Total"),
    ("Antimony", "Total"): ("Antimony", "Total"),
    ("Arsenic", "Total"): ("Arsenic", "Total"),
    ("Asbestos", "Total"): ("Asbestos", "Total"),
    ("Ball clay", "Total"): ("Clays", "Ball clay"),
    ("Barite", "Total"): ("Barite", "Total"),
    ("Bauxite", "Total"): ("Bauxite", "Total"),
    ("Bentonite", "Total"): ("Bentonite", "Total"),
    ("Beryllium", "Total"): ("Beryllium", "Total"),
    ("Bismuth", "Total"): ("Bismuth", "Total"),
    ("Boron", "Total"): ("Boron", "Total"),
    ("Boron carbide", "Total"): ("Boron carbide", "Total"),
    ("Cadmium", "Total"): ("Cadmium", "Total"),
    ("Cement", "Total"): ("Cement", "Total"),
    ("Cesium", "Total"): ("Cesium", "Total"),
    ("Chromium", "Total"): ("Chromium", "Total"),
    ("Cobalt", "Total"): ("Cobalt", "Total"),
    ("Construction sand and gravel", "Total"): ("Primary aggregates", "Construction sand and gravel"),
    ("Copper", "Total"): ("Copper", "Mine"),
    ("Crushed stone", "Total"): ("Primary aggregates", "Crushed rock"),
    ("Diatomite", "Total"): ("Diatomite", "Total"),
    ("Dimension stone", "Total"): ("Dimension stone", "Total"),
    ("Direct Reduced Iron", "Total"): ("Direct Reduced Iron", "Total"),
    ("Feldspar", "Total"): ("Feldspar", "Total"),
    ("Fire clay", "Total"): ("Clays", "Fire clay"),
    ("Fluorspar", "Total"): ("Fluorspar", "Total"),
    ("Fuller's earth", "Total"): ("Fuller's earth", "Total"),
    ("Gallium", "Total"): ("Gallium", "Total"),
    ("Germanium", "Total"): ("Germanium", "Total"),
    ("Gold", "Total"): ("Gold", "Total"),
    ("Graphite", "Total"): ("Graphite", "Total"),
    ("Gypsum", "Total"): ("Gypsum", "Total"),
    ("Helium", "Total"): ("Helium", "Total"),
    ("Indium", "Total"): ("Indium", "Total"),
    # NOTE: Total diamond production includes natural and synthetic diamonds.
    ("Industrial diamond", "Total"): ("Diamond", "Total, industrial"),
    ("Industrial garnet", "Total"): ("Industrial garnet", "Total"),
    ("Industrial sand and gravel", "Total"): ("Primary aggregates", "Industrial sand and gravel"),
    ("Iron Oxide Pigments", "Total"): ("Iron Oxide Pigments", "Total"),
    ("Iron and Steel Slag", "Total"): ("Iron and Steel Slag", "Total"),
    ("Iron ore", "Total"): ("Iron ore", "Total"),
    ("Kaolin", "Total"): ("Kaolin", "Total"),
    ("Lead", "Total"): ("Lead", "Mine"),
    ("Lime", "Total"): ("Lime", "Total"),
    ("Magnesium compounds", "Total"): ("Magnesium compounds", "Total"),
    ("Magnesium metal", "Total"): ("Magnesium metal", "Primary"),
    ("Manganese", "Total"): ("Manganese", "Total"),
    ("Mercury", "Total"): ("Mercury", "Total"),
    ("Metallic abrasives", "Total"): ("Metallic abrasives", "Total"),
    ("Mica (natural), scrap and flake", "Total"): ("Mica", "Natural, scrap and flake"),
    ("Mica (natural), sheet", "Total"): ("Mica", "Natural, sheet"),
    ("Miscellaneous clay", "Total"): ("Clays", "Miscellaneous clay"),
    ("Molybdenum", "Total"): ("Molybdenum", "Total"),
    ("Nickel", "Total"): ("Nickel", "Mine"),
    ("Niobium", "Total"): ("Niobium", "Total"),
    ("Nitrogen (Fixed)-Ammonia", "Total"): ("Nitrogen", "Total, fixed ammonia"),
    ("Peat", "Total"): ("Peat", "Total"),
    ("Perlite", "Total"): ("Perlite", "Total"),
    ("Phosphate rock", "Total"): ("Phosphate rock", "Total"),
    ("Pig Iron", "Total"): ("Pig Iron", "Total"),
    ("Pumice and Pumicite", "Total"): ("Pumice and Pumicite", "Total"),
    ("Salt", "Total"): ("Salt", "Total"),
    ("Selenium", "Total"): ("Selenium", "Total"),
    ("Silicon", "Total"): ("Silicon", "Total"),
    ("Silicon carbide", "Total"): ("Silicon carbide", "Total"),
    ("Silver", "Total"): ("Silver", "Total"),
    ("Soda ash", "Total"): ("Soda ash", "Total"),
    ("Steel", "Total"): ("Steel", "Total"),
    ("Strontium", "Total"): ("Strontium", "Total"),
    ("Sulfur", "Total"): ("Sulfur", "Total"),
    ("Talc and pyrophyllite", "Total"): ("Talc and pyrophyllite", "Total"),
    ("Tantalum", "Total"): ("Tantalum", "Total"),
    ("Tellurium", "Total"): ("Tellurium", "Total"),
    ("Tin", "Total"): ("Tin", "Mine"),
    ("Titanium dioxide", "Total"): ("Titanium dioxide", "Total"),
    ("Titanium scrap", "Total"): ("Titanium scrap", "Total"),
    ("Titanium sponge", "Total"): ("Titanium sponge", "Total"),
    ("Total clay", "Total"): ("Clays", "Total"),
    ("Total manufactured abrasives ", "Total"): ("Manufactured abrasives ", "Total"),
    ("Tungsten", "Total"): ("Tungsten", "Total"),
    ("Vanadium", "Total"): ("Vanadium", "Total"),
    ("Zinc", "Total"): ("Zinc", "Total"),
}

# Units can either be "metric tonnes" or "metric tonnes of gross weight".
# Since this data is later on combined with USGS current data (given in tonnes), we need to ensure that they mean the
# same thing.
# So, to be conservative, go to the explorer and inspect those minerals that come as "tonnes of gross weight"; compare them to the USGS current data (given in "tonnes"); if they are in reasonable agreement, add them to the following list.
# Their unit will be converted to "tonnes", and hence combined with USGS current data.
MINERALS_TO_CONVERT_TO_TONNES = [
    "Cement",
]

# Footnotes (that will appear in the footer of charts) to add to the flattened output table.
FOOTNOTES = {
    # Example:
    # 'production|Tungsten|Powder|tonnes': "Tungsten includes...",
}


def harmonize_commodity_subcommodity_pairs(tb: Table) -> Table:
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


def gather_notes(tb_combined: Table, notes_columns: List[str]) -> Dict[str, str]:
    # Create another table with the same structure, but containing notes.
    tb_flat_notes = (
        tb_combined[
            ["commodity", "sub_commodity", "country", "year", "unit"]
            + [c for c in tb_combined.columns if c.startswith("notes_")]
        ]
        .rename(columns={"notes_unit_value_constant": "notes_unit_value"}, errors="raise")
        .pivot(
            index=["country", "year"],
            columns=["commodity", "sub_commodity", "unit"],
            values=["notes_production", "notes_unit_value"],
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
        .replace({"metric tonnes": "tonnes", "metric tonnes of gross weight": "tonnes, gross weight"})
    )
    tb.loc[tb["commodity"].isin(MINERALS_TO_CONVERT_TO_TONNES), "unit"] = "tonnes"

    return tb


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
            "unit_value_dollar_t",
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
                columns={column: f"{column}_notes" for column in tb_metadata.columns if column != "commodity"},
                errors="ignore",
            ),
            on="commodity",
            how="left",
        )

    # Select columns for world production.
    # NOTE: There are 4 columns for world production, namely "world_production", "world_mine_production",
    # "world_mine_production__metal_content", and "world_refinery_production".
    # For now, we'll only keep "world_production".
    tb_world_production = (
        tb[["commodity", "year", "world_production", "unit"]]
        .rename(columns={"world_production": "production"}, errors="raise")
        .assign(**{"country": "World"})
        .astype({"production": float})
    )
    # Add notes to the table, using the extracted metadata.
    mask = tb_metadata["world_production"].notnull()
    tb_metadata.loc[mask, "world_production"] = "Note on global production: " + tb_metadata["world_production"][mask]
    tb_world_production = tb_world_production.merge(
        tb_metadata[["commodity", "world_production"]].rename(
            columns={"world_production": "production_notes" for column in tb_metadata.columns if column != "commodity"},
            errors="ignore",
        ),
        on="commodity",
        how="left",
    )

    # Select columns for unit value.
    tb_unit_value = (
        tb[["commodity", "year", "unit_value_dollar_t", "unit_value_98dollar_t"]]
        .assign(**{"country": "World"})
        .rename(
            columns={"unit_value_dollar_t": "unit_value_current", "unit_value_98dollar_t": "unit_value_constant"},
            errors="raise",
        )
    )
    # Remove spurious footnotes like "W".
    for column in ["unit_value_current", "unit_value_constant"]:
        tb_unit_value[column] = tb_unit_value[column].astype("string").replace("W", None).astype(float)
    # Add notes to the table, using the extracted metadata.
    tb_unit_value = tb_unit_value.merge(
        tb_metadata[["commodity", "unit_value_dollar_t", "unit_value_98dollar_t"]].rename(
            columns={
                "unit_value_dollar_t": "unit_value_current_notes",
                "unit_value_98dollar_t": "unit_value_constant_notes",
            },
            errors="ignore",
        ),
        on="commodity",
        how="left",
    )

    # Combine tables.
    tb_combined = pr.concat([tb_us_production, tb_world_production], ignore_index=True)
    tb_combined = tb_combined.merge(tb_unit_value, on=["commodity", "year", "country"], how="outer")

    # Remove empty rows.
    tb_combined = tb_combined.dropna(
        subset=["production", "unit_value_current", "unit_value_constant"], how="all"
    ).reset_index(drop=True)

    # Harmonize commodity-subcommodity pairs.
    # To begin with, assume subcommodity "Total" for all minerals, and then rewrite when needed (using the dictionary
    # COMMODITY_MAPPING defined above).
    tb_combined["sub_commodity"] = "Total"
    tb_combined = harmonize_commodity_subcommodity_pairs(tb=tb_combined)

    # Clean notes columns, and combine notes at the individual row level with general table notes.
    for column in [column for column in tb_combined.columns if "_notes" in column]:
        new_column = f"notes_{column.replace('_notes', '')}"
        tb_combined[new_column] = [clean_notes(note) for note in tb_combined[column]]
        tb_combined[new_column] = tb_combined[new_column].copy_metadata(tb_combined[column])
        # Drop unnecessary columns.
        tb_combined = tb_combined.drop(columns=[column])

    # Gather all notes in a dictionary.
    notes = gather_notes(
        tb_combined=tb_combined, notes_columns=[column for column in tb_combined.columns if column.startswith("notes_")]
    )

    # Create a wide table.
    # For the wide table, select only unit value in constant USD.
    tb_flat = (
        tb_combined.rename(columns={"unit_value_constant": "unit_value"}, errors="raise")
        .drop(columns=["unit_value_current"], errors="raise")
        .pivot(
            index=["country", "year"],
            columns=["commodity", "sub_commodity", "unit"],
            values=["production", "unit_value"],
            join_column_levels_with="|",
        )
        .dropna(axis=1, how="all")
    )

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
    for column, note in FOOTNOTES.items():
        if not tb_flat[column].metadata.presentation:
            tb_flat[column].metadata.presentation = VariablePresentationMeta(grapher_config={})
        tb_flat[column].metadata.presentation.grapher_config["note"] = note

    # Format tables conveniently.
    tb_combined = tb_combined.astype(
        {column: "string" for column in tb_combined.columns if column.startswith("notes_")}
    )
    tb_combined = tb_combined.format(["country", "year", "commodity", "sub_commodity"])
    tb_flat = tb_flat.format(["country", "year"], short_name=paths.short_name + "_flat")
    tb_flat = tb_flat.astype({column: "Float64" for column in tb_flat.columns})

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_combined, tb_flat], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
