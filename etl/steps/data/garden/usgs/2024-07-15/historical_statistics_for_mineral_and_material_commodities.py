"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table, VariablePresentationMeta
from owid.datautils.dataframes import map_series

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
    ("Copper", "Total"): ("Copper", "Total"),
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
    ("Lead", "Total"): ("Lead", "Total"),
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
    ("Nickel", "Total"): ("Nickel", "Total"),
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
    ("Tin", "Total"): ("Tin", "Total"),
    ("Titanium dioxide", "Total"): ("Titanium dioxide", "Total"),
    ("Titanium scrap", "Total"): ("Titanium scrap", "Total"),
    ("Titanium sponge", "Total"): ("Titanium sponge", "Total"),
    ("Total clay", "Total"): ("Clays", "Total"),
    ("Total manufactured abrasives ", "Total"): ("Manufactured abrasives ", "Total"),
    ("Tungsten", "Total"): ("Tungsten", "Total"),
    ("Vanadium", "Total"): ("Vanadium", "Total"),
    ("Zinc", "Total"): ("Zinc", "Total"),
}

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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("historical_statistics_for_mineral_and_material_commodities")

    # Read table from meadow dataset.
    # NOTE: Since the data has spurious footnotes, like "W", some columns were stored as strings.
    # Later on we will remove these footnotes and store data as floats.
    tb = ds_meadow.read_table("historical_statistics_for_mineral_and_material_commodities")

    #
    # Process data.
    #
    # Harmonize units.
    assert set(tb["unit"]) == {"metric tonnes", "metric tonnes of gross weight"}
    tb["unit"] = (
        tb["unit"]
        .astype("string")
        .replace({"metric tonnes": "tonnes", "metric tonnes of gross weight": "tonnes, gross weight"})
    )

    # Remove duplicated rows that have exactly the same data.
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
        # if column in notes:
        #     tb_flat[column].metadata.description_from_producer = "Notes found in original USGS data:\n" + notes[column]

    # Add footnotes.
    for column, note in FOOTNOTES.items():
        if not tb_flat[column].metadata.presentation:
            tb_flat[column].metadata.presentation = VariablePresentationMeta(grapher_config={})
        tb_flat[column].metadata.presentation.grapher_config["note"] = note

    # Format tables conveniently.
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
