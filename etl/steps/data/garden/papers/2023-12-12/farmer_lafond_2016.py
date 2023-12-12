"""Harmonize data from Farmer & Lafond (2016) paper on the evolution of the cost of different technologies.

"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)

# Columns to select from Meadow table, and how to rename them.
COLUMNS = {
    "acrylicfiber": "acrylic_fiber",
    "acrylonitrile": "acrylonitrile",
    "aluminum": "aluminum",
    "ammonia": "ammonia",
    "aniline": "aniline",
    "automotive__us": "automotive_us",
    "beer__japan": "beer_japan",
    "benzene": "benzene",
    "bisphenola": "bisphenol_a",
    "caprolactam": "caprolactam",
    "carbonblack": "carbon_black",
    "carbondisulfide": "carbon_disulfide",
    "ccgt_power": "ccgt_power",
    "concentrating_solar": "concentrating_solar",
    "corn__us": "corn_us",
    "crude_oil": "crude_oil",
    "cyclohexane": "cyclohexane",
    "dna_sequencing": "dna_sequencing",
    "dram": "dram",
    "electric_range": "electric_range",
    "ethanol__brazil": "ethanol_brazil",
    "ethanolamine": "ethanolamine",
    "ethylene": "ethylene",
    "formaldehyde": "formaldehyde",
    "free_standing_gas_range": "free_standing_gas_range",
    "geothermal_electricity": "geothermal_electricity",
    "hard_disk_drive": "hard_disk_drive",
    "hydrofluoricacid": "hydrofluoric_acid",
    "isopropylalcohol": "isopropyl_alcohol",
    "laser_diode": "laser_diode",
    "low_density_polyethylene": "low_density_polyethylene",
    "magnesium": "magnesium",
    "maleicanhydride": "maleic_anhydride",
    "methanol": "methanol",
    "milk__us": "milk_us",
    "monochrome_television": "monochrome_television",
    "motor_gasoline": "motor_gasoline",
    "neoprenerubber": "neoprene_rubber",
    "nuclear_electricity": "nuclear_electricity",
    "onshore_gas_pipeline": "onshore_gas_pipeline",
    "paraxylene": "paraxylene",
    "pentaerythritol": "pentaerythritol",
    "phenol": "phenol",
    "photovoltaics": "photovoltaics",
    "phthalicanhydride": "phthalic_anhydride",
    "polyesterfiber": "polyester_fiber",
    "polyethylenehd": "polyethylene_hd",
    "polyethyleneld": "polyethylene_ld",
    "polypropylene": "polypropylene",
    "polystyrene": "polystyrene",
    "polyvinylchloride": "polyvinylchloride",
    "primary_aluminum": "primary_aluminum",
    "primary_magnesium": "primary_magnesium",
    "refined_cane_sugar": "refined_cane_sugar",
    "sodium": "sodium",
    "sodiumchlorate": "sodium_chlorate",
    "sodiumhydrosulfite": "sodium_hydrosulfite",
    "sorbitol": "sorbitol",
    "styrene": "styrene",
    "titanium_sponge": "titanium_sponge",
    "titanium_dioxide": "titanium_dioxide",
    "transistor": "transistor",
    "urea": "urea",
    "vinylacetate": "vinyl_acetate",
    "vinylchloride": "vinyl_chloride",
    "wind_turbine__denmark": "wind_turbine_denmark",
}


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow and read its main table.
    ds_meadow = paths.load_dataset("farmer_lafond_2016")
    tb_meadow = ds_meadow["farmer_lafond_2016"]

    #
    # Process data.
    #
    # Rename technologies conveniently (both in column names and in metadata).
    tb = tb_meadow[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
