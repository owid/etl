"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select and how to rename them.
COLUMNS = {
    "country": "country",
    "year": "year",
    "production_antimony_mine_tonnes": "Antimony",
    "production_asbestos_mine_tonnes": "Asbestos",
    "production_barite_mine_tonnes": "Barite",
    "production_bauxite_mine_tonnes": "Bauxite",
    "production_beryllium_mine_tonnes": "Beryllium",
    "production_bismuth_mine_tonnes": "Bismuth",
    "production_boron_mine_tonnes": "Boron",
    "production_chromium_mine_tonnes": "Chromium",
    "production_coal_mine_tonnes": "Coal",
    "production_cobalt_mine_tonnes": "Cobalt",
    "production_coltan_mine__columbite_tantalite_tonnes": "Coltan (columbite-tantalite)",
    "production_coltan_mine__columbite_tonnes": "Coltan (columbite)",
    "production_coltan_mine__tantalite_tonnes": "Coltan (tantalite)",
    "production_copper_mine_tonnes": "Copper",
    "production_diamond_mine__industrial_tonnes": "Diamond (industrial)",
    "production_diamond_mine_and_synthetic__industrial_tonnes": "Diamond and synthetic (industrial)",
    "production_feldspar_mine_tonnes": "Feldspar",
    "production_fluorspar_mine_tonnes": "Fluorspar",
    "production_garnet_mine_tonnes": "Garnet",
    "production_gemstones_mine_tonnes": "Gemstones",
    "production_gold_mine_tonnes": "Gold",
    "production_graphite_mine_tonnes": "Graphite",
    "production_gypsum_mine_tonnes": "Gypsum",
    "production_helium_mine_tonnes": "Helium",
    "production_iodine_mine_tonnes": "Iodine",
    "production_iron_ore_mine__crude_ore_tonnes": "Iron ore (crude ore)",
    "production_iron_ore_mine__iron_content_tonnes": "Iron ore (iron content)",
    "production_lead_mine_tonnes": "Lead",
    "production_lithium_mine_tonnes": "Lithium",
    "production_magnesium_compounds_mine_tonnes": "Magnesium compounds",
    "production_manganese_mine_tonnes": "Manganese",
    "production_mercury_mine_tonnes": "Mercury",
    "production_mica_mine__scrap_and_flake_tonnes": "Mica (scrap and flake)",
    "production_mica_mine__sheet_tonnes": "Mica (sheet)",
    "production_mica_mine_tonnes": "Mica",
    "production_molybdenum_mine_tonnes": "Molybdenum",
    "production_nickel_mine_tonnes": "Nickel",
    "production_niobium_mine__pyrochlore_tonnes": "Niobium (pyrochlore)",
    "production_niobium_mine_tonnes": "Niobium",
    "production_phosphate_rock_mine__aluminum_phosphate_tonnes": "Phosphate rock (aluminum phosphate)",
    "production_phosphate_rock_mine_tonnes": "Phosphate rock",
    "production_platinum_group_metals_mine__iridium_tonnes": "Platinum group metals (iridium)",
    "production_platinum_group_metals_mine__other_tonnes": "Platinum group metals (other)",
    "production_platinum_group_metals_mine__palladium_tonnes": "Platinum group metals (palladium)",
    "production_platinum_group_metals_mine__platinum_tonnes": "Platinum group metals (platinum)",
    "production_platinum_group_metals_mine__rhodium_tonnes": "Platinum group metals (rhodium)",
    "production_platinum_group_metals_mine__ruthenium_tonnes": "Platinum group metals (ruthenium)",
    "production_potash_mine__chloride_tonnes": "Potash (chloride)",
    "production_potash_mine__polyhalite_tonnes": "Potash (polyhalite)",
    "production_potash_mine__potassic_salts_tonnes": "Potash (potassic salts)",
    "production_potash_mine_tonnes": "Potash",
    "production_rare_earths_mine_tonnes": "Rare earths",
    "production_salt_mine_tonnes": "Salt",
    "production_sand_and_gravel_mine__construction_tonnes": "Sand and gravel (construction)",
    "production_sand_and_gravel_mine__industrial_tonnes": "Sand and gravel (industrial)",
    "production_silver_mine_tonnes": "Silver",
    "production_strontium_mine_tonnes": "Strontium",
    "production_talc_and_pyrophyllite_mine__pyrophyllite_tonnes": "Talc and pyrophyllite (pyrophyllite)",
    "production_talc_and_pyrophyllite_mine_tonnes": "Talc and pyrophyllite",
    "production_tantalum_mine_tonnes": "Tantalum",
    "production_tin_mine_tonnes": "Tin",
    "production_titanium_mine__ilmenite_tonnes": "Titanium (ilmenite)",
    "production_titanium_mine__rutile_tonnes": "Titanium (rutile)",
    "production_tungsten_mine_tonnes": "Tungsten",
    "production_uranium_mine_tonnes": "Uranium",
    "production_vanadium_mine_tonnes": "Vanadium",
    "production_zinc_mine_tonnes": "Zinc",
    "production_zirconium_and_hafnium_mine_tonnes": "Zirconium and hafnium",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its flat table.
    ds_garden = paths.load_dataset("minerals")
    tb = ds_garden.read("minerals")

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[COLUMNS.keys()].rename(columns=COLUMNS, errors="raise")

    # Select global data.
    tb = tb[tb["country"] == "World"].drop(columns=["country"], errors="raise").reset_index(drop=True)

    # Gather all descriptions.
    # NOTE: Footnotes will be gathered and used as description key.
    # description_key = []
    _description_processing = []
    # _description_from_producer = []
    footnotes = []
    for column in tb.drop(columns=["year"]).columns:
        grapher_config = tb[column].m.presentation.grapher_config
        if grapher_config:
            # NOTE: Ignore the frequent footnote saying that "The sum of all countries may exceed World data on certain years (by up to 10%), due to discrepancies between data sources."
            if "The sum of all countries" not in grapher_config["note"]:
                footnotes.append(f"{column} - {grapher_config['note']}")
        # description_key.append(tb[column].metadata.description_short)
        _description_processing.append(tb[column].metadata.description_processing)
        # _description_from_producer.append(f"- {column}\n{tb[column].metadata.description_from_producer}")
    _description_processing = sorted(
        set([tb[column].metadata.description_processing for column in tb.drop(columns=["year"]).columns])
    )
    # By construction, processing description should be the same for all indicators.
    assert len(_description_processing) == 1, "All columns were expected to have the same processing description."
    description_processing = _description_processing[0]
    # Gather all descriptions from producer.
    # description_from_producer = "\n".join(["\n    ".join(description.split("\n")) for description in _description_from_producer if description])

    # Melt table to create a long table with mineral as "country" column.
    tb_long = tb.melt(id_vars=["year"], var_name="country", value_name="mine_production")

    # Drop empty rows.
    tb_long = tb_long.dropna(subset=["mine_production"])

    # Improve metadata.
    tb_long["mine_production"].metadata.title = "Global mine production of different minerals"
    tb_long["mine_production"].metadata.unit = "tonnes"
    tb_long["mine_production"].metadata.short_unit = "t"
    tb_long[
        "mine_production"
    ].metadata.description_short = (
        "Measured in tonnes of mined, rather than [refined](#dod:refined-production) production."
    )
    tb_long["mine_production"].metadata.description_key = footnotes
    tb_long["mine_production"].metadata.description_processing = description_processing
    # NOTE: The following metadata is too long and cannot be inserted in DB.
    # tb_long["mine_production"].metadata.description_from_producer = description_from_producer
    tb_long.metadata.title = "Global mine production by mineral"

    # Improve table format.
    tb_long = tb_long.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_long], check_variables_metadata=True)
    ds_grapher.save()
