"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from each table, and how to rename them.
COLUMNS_MODIS = {
    "country": "country",
    "year": "year",
    "meangcvi": "yield",
}
COLUMNS_SIF = {
    "country_na": "country",
    "year": "year",
    "mean": "yield",
}
COLUMNS_FAO = {
    "area": "country",
    "year": "year",
    "element": "element",
    "item": "item",
    "value": "yield",
    # Just for sanity checks.
    "unit": "unit",
    "flag_description": "flag",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("crop_yields_in_southern_africa")

    # Read tables from meadow dataset.
    tb_modis = ds_meadow.read("crop_yields_in_southern_africa__modis")
    tb_fao = ds_meadow.read("crop_yields_in_southern_africa__fao")
    tb_sif = ds_meadow.read("crop_yields_in_southern_africa__sif")

    #
    # Process data.
    #
    # Select and rename columns from each table.
    tb_modis = tb_modis[list(COLUMNS_MODIS)].rename(columns=COLUMNS_MODIS, errors="raise")
    tb_sif = tb_sif[list(COLUMNS_SIF)].rename(columns=COLUMNS_SIF, errors="raise")
    tb_fao = tb_fao[list(COLUMNS_FAO)].rename(columns=COLUMNS_FAO, errors="raise")

    # Prepare FAO data.
    # Sanity check.
    assert set(tb_fao["unit"]) == {"100 g/ha"}, "Unexpected units"

    # TODO: There are two items in FAO data, namely "Maize (corn)" and "Green corn (maize)". Decide how to combine with other tables.
    # TODO: Continue here: combine tables, harmonize country names, and improve format.
    # For now, pick just one of them to avoid an error.
    tb = tb_sif.copy()

    # Harmonize country names.
    # tb = paths.regions.harmonize_names(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
