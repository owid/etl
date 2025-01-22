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
    # NOTE: The following could be mapped to ("Clays", "Mine, ball clay"). We decided to remove "Clays".
    ("Ball clay", "Total"): None,
    ("Barite", "Total"): ("Barite", "Mine"),
    ("Bauxite", "Total"): ("Bauxite", "Mine"),
    # NOTE: The following could be mapped to ("Clays", "Mine, bentonite"). We decided to remove "Clays".
    ("Bentonite", "Total"): None,
    # NOTE: Extracted from "world_mine_production".
    ("Beryllium", "Total"): ("Beryllium", "Mine"),
    ("Beryllium", "Mine"): ("Beryllium", "Mine"),
    ("Bismuth", "Total"): ("Bismuth", "Mine"),
    # NOTE: Extracted from "world_mine_production".
    ("Bismuth", "Mine"): ("Bismuth", "Mine"),
    # NOTE: Extracted from "world_refinery_production".
    ("Bismuth", "Refinery"): ("Bismuth", "Refinery"),
    ("Boron", "Total"): ("Boron", "Mine"),
    ("Boron carbide", "Total"): None,
    ("Cadmium", "Total"): ("Cadmium", "Refinery"),
    ("Cement", "Total"): ("Cement", "Processing"),
    # NOTE: The following could be mapped to ("Cesium", "Mine"), but it has only global data until 1977.
    ("Cesium", "Total"): None,
    # NOTE: It's not clear what the following means.
    #  According to the Word document in the excel file,
    # "World production is an estimate of world chromite ore mine production measured in contained chromium.
    #  World production reported in gross weight was converted to contained chromium by assuming that its chromic oxide
    #  content was the same as that of chromite ore imported into the United States.
    #  Before content of chromite ore was reported, a time-averaged value was used."
    # So this seems to be chromite (ore of chromium) in tonnes of contained chromium.
    # However, according to the excel file from https://www.usgs.gov/centers/national-minerals-information-center/chromium-statistics-and-information
    # World production of Ferrochromium, in tonnes of gross weight, has similar values to the previous.
    # In any case, USGS current only provides production data for chromite, as:
    # "Units are thousand metric tons, gross weight, of marketable chromite ore."
    # Maybe both USGS historical and current report chromite, but the main difference is that USGS historical is in
    # "contained chromium", whereas USGS current is in "contained chromium".
    # But for now, we will keep only USGS current data.
    ("Chromium", "Total"): None,
    # NOTE: Cobalt total is only used for unit value.
    ("Cobalt", "Total"): ("Cobalt", "Value"),
    # NOTE: Extracted from "world_mine_production".
    ("Cobalt", "Mine"): ("Cobalt", "Mine"),
    # NOTE: Extracted from "world_refinery_production".
    ("Cobalt", "Refinery"): ("Cobalt", "Refinery"),
    ("Construction sand and gravel", "Total"): ("Sand and gravel", "Mine, construction"),
    ("Copper", "Total"): ("Copper", "Mine"),
    # NOTE: The following could be mapped to ("Crushed stone", "Mine"), but it has only US data, not global.
    ("Crushed stone", "Total"): None,
    ("Diatomite", "Total"): ("Diatomite", "Mine"),
    # NOTE: The following could be mapped to ("Dimension stone", "Mine"), but it has only US data, not global.
    ("Dimension stone", "Total"): None,
    # NOTE: The following could be mapped to ("Iron", "Sponge"). But for now, we decided to exclude it.
    ("Direct Reduced Iron", "Total"): None,
    # NOTE: In USGS historical, the notes explicitly say "World production data do not include production data for
    #  nepheline syenite.", whereas in USGS current it's unclear.
    ("Feldspar", "Total"): ("Feldspar", "Mine"),
    # NOTE: The following could be mapped to ("Clays", "Mine, fire clay"). We decided to remove "Clays".
    ("Fire clay", "Total"): None,
    ("Fluorspar", "Total"): ("Fluorspar", "Mine"),
    # NOTE: The following could be mapped to ("Clays", "Mine, fuller's earth"). We decided to remove "Clays".
    ("Fuller's earth", "Total"): None,
    ("Gallium", "Total"): ("Gallium", "Refinery"),
    ("Gemstones", "Total"): ("Gemstones", "Mine"),
    ("Germanium", "Total"): ("Germanium", "Refinery"),
    ("Gold", "Total"): ("Gold", "Mine"),
    ("Graphite", "Total"): ("Graphite", "Mine"),
    ("Gypsum", "Total"): ("Gypsum", "Mine"),
    # In USGS current data, "Hafnium" is not reported, only "Zirconium and Hafnium".
    ("Hafnium", "Total"): None,
    ("Helium", "Total"): ("Helium", "Mine"),
    ("Indium", "Total"): ("Indium", "Refinery"),
    # NOTE: Industrial diamond production includes natural and synthetic diamonds.
    #  But in USGS current data, industrial diamond production includes only natural diamond.
    ("Industrial diamond", "Total"): ("Diamond", "Mine and synthetic, industrial"),
    ("Industrial garnet", "Total"): ("Garnet", "Mine"),
    ("Industrial sand and gravel", "Total"): ("Sand and gravel", "Mine, industrial"),
    ("Iodine", "Total"): ("Iodine", "Mine"),
    ("Iron Oxide Pigments", "Total"): None,
    ("Iron and Steel Scrap", "Total"): None,
    ("Iron and Steel Slag", "Total"): None,
    ("Iron ore", "Total"): ("Iron ore", "Mine, crude ore"),
    # NOTE: The following could be mapped to ("Clays", "Mine, kaolin"). We decided to remove "Clays".
    ("Kaolin", "Total"): None,
    ("Kyanite", "Total"): None,
    ("Lead", "Total"): ("Lead", "Mine"),
    ("Lime", "Total"): ("Lime", "Processing"),
    ("Lithium statistics", "Total"): ("Lithium", "Mine"),
    ("Lumber", "Total"): None,
    ("Magnesium compounds", "Total"): ("Magnesium compounds", "Mine"),
    ("Magnesium metal", "Total"): ("Magnesium metal", "Smelter"),
    ("Manganese", "Total"): ("Manganese", "Mine"),
    ("Mercury", "Total"): ("Mercury", "Mine"),
    ("Metallic abrasives", "Total"): None,
    ("Mica (natural), scrap and flake", "Total"): ("Mica", "Mine, scrap and flake"),
    ("Mica (natural), sheet", "Total"): ("Mica", "Mine, sheet"),
    # NOTE: The following could be mapped to ("Clays", "Mine, miscellaneous"). We decided to remove "Clays".
    ("Miscellaneous clay", "Total"): None,
    ("Molybdenum", "Total"): ("Molybdenum", "Mine"),
    ("Natural & Synthetic Rutile", "Total"): None,
    ("Nickel", "Total"): ("Nickel", "Mine"),
    ("Niobium", "Total"): ("Niobium", "Mine"),
    # NOTE: Extracted from "world_mine_production".
    ("Niobium", "Mine"): ("Niobium", "Mine"),
    ("Nitrogen (Fixed)-Ammonia", "Total"): ("Nitrogen", "Fixed ammonia"),
    ("Other industrial wood products", "Total"): None,
    ("Paper and board", "Total"): None,
    # NOTE: The following could be mapped to ("Peat", "Mine"). We decided to remove "Peat".
    ("Peat", "Total"): None,
    ("Perlite", "Total"): ("Perlite", "Mine"),
    ("Phosphate rock", "Total"): ("Phosphate rock", "Mine"),
    ("Pig Iron", "Total"): ("Iron", "Smelter, pig iron"),
    # In USGS current data, PGM are broken down into palladium and platinum.
    ("Platinum-group metals", "Total"): None,
    ("Plywood and veneer", "Total"): None,
    ("Potash", "Total"): ("Potash", "Mine"),
    ("Pumice and Pumicite", "Total"): ("Pumice and pumicite", "Mine"),
    ("Quartz crystal", "Total"): None,
    ("Rare earths", "Total"): ("Rare earths", "Mine"),
    ("Rhenium", "Total"): ("Rhenium", "Mine"),
    ("Salt", "Total"): ("Salt", "Mine"),
    ("Selenium", "Total"): ("Selenium", "Refinery"),
    ("Silicon", "Total"): ("Silicon", "Processing"),
    ("Silicon carbide", "Total"): None,
    ("Silver", "Total"): ("Silver", "Mine"),
    ("Soda ash", "Total"): ("Soda ash", "Natural and synthetic"),
    ("Sodium sulfate", "Total"): None,
    ("Steel", "Total"): ("Steel", "Processing, crude"),
    ("Strontium", "Total"): ("Strontium", "Mine"),
    ("Sulfur", "Total"): ("Sulfur", "Processing"),
    ("Talc and pyrophyllite", "Total"): ("Talc and pyrophyllite", "Mine"),
    ("Tantalum", "Total"): ("Tantalum", "Mine"),
    # NOTE: The following could be mapped to ("Tellurium", "Refinery"). However, we decided to discard Tellurium.
    ("Tellurium", "Total"): None,
    ("Thallium", "Total"): None,
    ("Thorium", "Total"): None,
    ("Tin", "Total"): ("Tin", "Mine"),
    # NOTE: For titanium there is no global data.
    ("Titanium dioxide", "Total"): None,
    ("Titanium scrap", "Total"): None,
    ("Titanium sponge", "Total"): None,
    # NOTE: After combing data with BGS, USGS' "Total" production is smaller than the sum of all clays production.
    #  To avoid confusion, remove this total.
    ("Total clay", "Total"): None,
    ("Total manufactured abrasives ", "Total"): None,
    ("Total forestry", "Total"): None,
    ("Tungsten", "Total"): ("Tungsten", "Mine"),
    ("Vanadium", "Total"): ("Vanadium", "Mine"),
    ("Vermiculite", "Total"): None,
    ("Wollastonite", "Total"): None,
    ("Wood panel products", "Total"): None,
    ("Zinc", "Total"): ("Zinc", "Mine"),
    # In USGS current data, "Hafnium" is not reported, only "Zirconium and Hafnium".
    ("Zirconium", "Total"): None,
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
    "Feldspar",
    # NOTE: For Cobalt, the conversion to "tonnes" is hardcoded in "prepare_world_production" function.
    "Cobalt",
    "Diatomite",
    "Dimension stone",
    "Fluorspar",
    "Fuller's earth",
    "Gypsum",
    "Industrial garnet",
    "Kaolin",
    "Mica (natural), sheet",
    "Mica (natural), scrap and flake",
    "Peat",
    "Perlite",
    "Phosphate rock",
    "Pumice and Pumicite",
    "Salt",
    "Soda ash",
    "Talc and pyrophyllite",
    # NOTE: Bismuth is in gross weight for the US, but metal content for the World.
    #  However, data for the US contains only nans and zeros, and will be removed later on.
    "Bismuth",
    "Gemstones",
    "Iodine",
]

# Footnotes (that will appear in the footer of charts) to add to the flattened tables (production and unit value).
FOOTNOTES_PRODUCTION = {
    "production|Alumina|Refinery|tonnes": "Values are reported as quantity produced before 1971 and as calcined alumina equivalents afterwards.",
    "production|Bauxite|Mine|tonnes": "Values are reported as dried bauxite equivalents.",
    "production|Barite|Mine|tonnes": "Values are reported as gross weight.",
    "production|Asbestos|Mine|tonnes": "Values are reported as gross weight.",
    # "production|Clays|Mine, bentonite|tonnes": "Values are reported as gross weight.",
    # "production|Clays|Mine, ball clay|tonnes": "Values are reported as gross weight.",
    # "production|Clays|Mine, fire clay|tonnes": "Values are reported as gross weight.",
    # "production|Chromium|Mine|tonnes": "Values are reported as tonnes of contained chromium.",
    "production|Cobalt|Refinery|tonnes": "Values are reported as tonnes of cobalt content.",
    "production|Bismuth|Mine|tonnes": "Values are reported as tonnes of metal content.",
    "production|Lithium|Mine|tonnes": "Values are reported as tonnes of lithium content.",
    "production|Gemstones|Mine|tonnes": "Values are reported as tonnes of gemstone-quality diamonds.",
}
FOOTNOTES_UNIT_VALUE = {
    "unit_value|Silicon|Processing|constant 1998 US$ per tonne": "Values refer to constant 1998 US$ per tonne of silicon content in ferrosilicon or silicon metal.",
}


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
    # Also, zirconiummineral_concentrates contains rows of "< 100,000". It's unclear which value should be assigned here, so we will remove these (two) rows.
    tb_us_production["production"] = map_series(
        tb_us_production["production"],
        mapping={"W": None, "< 100,000": None},
        warn_on_missing_mappings=False,
        warn_on_unused_mappings=True,
    ).astype({"production": "float64[pyarrow]"})
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
            # Instead of changing the unit, we create a footnote.
            _tb_production.loc[_tb_production["commodity"] == "Bismuth", "unit"] = "tonnes"
        elif "refinery" in column:
            # world_refinery_production is informed only for Bismuth and Cobalt.
            # In the case of Bismuth, the title says "gross weight unless otherwise noted"
            # (and nothing else is noted for world refinery production).
            # However, in both cases, the unit is probably "tonnes of metal content".
            # Instead of changing the unit, we create a footnote.
            # _tb_production.loc[_tb_production["commodity"] == "Bismuth", "unit"] = "tonnes"
            # In the case of Cobalt, to harmonize with BGS data, use "tonnes" and add a footnote.
            _tb_production.loc[_tb_production["commodity"] == "Cobalt", "unit"] = "tonnes"

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
        tb_flat[column].metadata.title = column.replace("_", " ")
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
    tb = ds_meadow.read("historical_statistics_for_mineral_and_material_commodities")
    tb_metadata = ds_meadow.read("historical_statistics_for_mineral_and_material_commodities_metadata").astype("string")

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

    ####################################################################################################################
    # Fix some other specific cases.
    # Bismuth in gross weight only has data for the US, and it's all zeros.
    tb_flat.loc[(tb_flat["country"] == "United States"), "production|Bismuth|Mine|tonnes"] = pd.NA

    # Boron mine production data is in gross weight.
    # However, between 1964 and 1975, it is reported as boron oxide content.
    # Apart from creating spurious jumps in the data, this causes the US to have a larger production than the world.
    tb_flat.loc[
        (tb_flat["country"] == "World")
        & (tb_flat["year"].isin([1964, 1965, 1966, 1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975])),
        "production|Boron|Mine|tonnes",
    ] = None
    # Also, data from 2006 onwards excludes the US (as US data is "withheld").
    tb_flat.loc[(tb_flat["country"] == "World") & (tb_flat["year"] >= 2006), "production|Boron|Mine|tonnes"] = None

    # Diatomite mine production data between 1913 and 1918 for US is slightly larger than for the World.
    # There are notes mentioning that US production around that time was incomplete, so I'll remove all those points.
    tb_flat.loc[
        (tb_flat["country"].isin(["World", "United States"]))
        & (tb_flat["year"].isin([1913, 1914, 1915, 1916, 1917, 1918])),
        "production|Diatomite|Mine|tonnes",
    ] = None

    # Helium mine production for the US is larger than the World on specific years.
    # I understand that these issues are within the uncertainty, and that most production the time came from the US.
    # So I'll simply remove those points where US > World.
    tb_flat.loc[
        (tb_flat["country"].isin(["World", "United States"])) & (tb_flat["year"].isin([1990, 1994])),
        "production|Helium|Mine|tonnes",
    ] = None

    # A similar thing happens to Mica mine production.
    tb_flat.loc[
        (tb_flat["country"].isin(["World", "United States"]))
        & (tb_flat["year"].isin([1938, 1940, 1941, 1945, 1947, 1949])),
        "production|Mica|Mine, scrap and flake|tonnes",
    ] = None

    # A similar thing happens to Sulfur processing production.
    tb_flat.loc[
        (tb_flat["country"].isin(["World", "United States"])) & (tb_flat["year"].isin([1920])),
        "production|Sulfur|Processing|tonnes",
    ] = None

    # NOTE: We decided to discard Tellurium.
    # # Tellurium refinery production is very incomplete, as pointed out in the notes.
    # # Specifically, a large range of years exclude US data because of proprietary data.
    # # To be conservative, remove all those years.
    # tb_flat.loc[
    #     (tb_flat["country"].isin(["World"])) & (tb_flat["year"] >= 1976) & (tb_flat["year"] <= 2003),
    #     "production|Tellurium|Refinery|tonnes",
    # ] = None
    # # Also, in some other years in the past, US production was larger than World production.
    # tb_flat.loc[
    #     (tb_flat["country"].isin(["World"])) & (tb_flat["year"].isin([1930, 1933])),
    #     "production|Tellurium|Refinery|tonnes",
    # ] = None

    # Vanadium mine production does not include US production in a range of years.
    # Remove those years.
    tb_flat.loc[
        (tb_flat["country"].isin(["World"])) & (tb_flat["year"].isin([1927, 1928, 1929, 1930, 1931, 1997, 1998, 1999])),
        "production|Vanadium|Mine|tonnes",
    ] = None
    # Also, in some other years in the past, US production was larger than World production.
    tb_flat.loc[
        (tb_flat["country"].isin(["World"])) & (tb_flat["year"].isin([1913, 1914, 1921, 1922])),
        "production|Vanadium|Mine|tonnes",
    ] = None

    # There is a big dip in global Magnesium metal in 1974, because of missing US data.
    # A similar thing happens in 1999.
    tb_flat.loc[
        (tb_flat["country"].isin(["World"])) & (tb_flat["year"].isin([1974, 1999])),
        "production|Magnesium metal|Smelter|tonnes",
    ] = None

    # Lithium production in the US is only given until 1954. From then on, the data is "W" (which means withheld to avoid disclosing company proprietary data). To avoid confusion, simply remove all this data.
    error = "Expected lithium US production data to end in 1954. Remove this part of the code."
    assert (
        tb_flat.loc[
            (tb_flat["country"] == "United States") & (tb_flat["production|Lithium|Mine|tonnes"].notnull()), "year"
        ].max()
        == 1954
    ), error
    tb_flat.loc[(tb_flat["country"] == "United States"), "production|Lithium|Mine|tonnes"] = None
    # Similarly, unit value from 1900 to 1951 is only informed in 1936 (and in terms of production, it's only informed in terms of gross weight, not lithium content). This creates a significant decline in unit value in line charts (between 1936 and 1952) which is unclear if it's real or not. To avoid confusion, ignore that data point and start the series in 1952.
    error = (
        "Expected lithium unit value data to only be informed in 1936 (prior to 1952). Remove this part of the code."
    )
    assert tb_flat[
        (tb_flat["unit_value|Lithium|Mine|constant 1998 US$ per tonne"].notnull()) & (tb_flat["year"] < 1952)
    ]["year"].tolist() == [1936], error
    tb_flat.loc[(tb_flat["year"] < 1952), "unit_value|Lithium|Mine|constant 1998 US$ per tonne"] = None

    # Gemstones unit values is zero in a range of years.
    # The documentation says "Unit value data for 1922–28 were estimated by interpolation of imports value data, and rounded to two significant figures".
    # In practice, the unit values for those years are exactly zero, which are probably spurious.
    # Remove those zeros.
    _years_with_zero_value = [1922, 1923, 1924, 1925, 1926, 1927, 1928]
    error = "Expected gemstones unit value to be zero in a range of years. Remove this part of the code."
    assert set(
        tb_flat.loc[
            (tb_flat["year"].isin(_years_with_zero_value)) & (tb_flat["country"] == "World"),
            "unit_value|Gemstones|Mine|constant 1998 US$ per tonne",
        ]
    ) == {0.0}, error
    tb_flat.loc[
        (tb_flat["year"].isin(_years_with_zero_value)), "unit_value|Gemstones|Mine|constant 1998 US$ per tonne"
    ] = None

    # Unit value of rare earths prior to 1961 is highly unstable, and goes from ~140k to ~40 and then ~100k in a few years. There are many possible reasons for this volatility (it was a small market during tumultuous times).
    # For now, I'll simply remove those years.
    error = "Unit value of rare earths was expected to be highly unstable prior to 1961. Remove this part of the code."
    assert tb_flat.loc[
        (tb_flat["country"] == "World") & (tb_flat["year"] < 1961),
        "unit_value|Rare earths|Mine|constant 1998 US$ per tonne",
    ].describe().loc[["min", "max"]].tolist() == [29.0, 145000.0], error
    tb_flat.loc[(tb_flat["year"] < 1961), "unit_value|Rare earths|Mine|constant 1998 US$ per tonne"] = None
    ####################################################################################################################

    # Format tables conveniently.
    # tb_combined = tb_combined.format(
    #     ["country", "year", "commodity", "sub_commodity", "unit"], short_name="historical_production"
    # )
    # tb_combined = tb_combined.astype(
    #     {column: "string" for column in tb_combined.columns if column.startswith("notes_")}
    # )
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
    ds_garden = create_dataset(dest_dir, tables=[tb_unit_value, tb_flat], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
