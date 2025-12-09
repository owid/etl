"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("crop_yields_in_southern_africa.zip")

    # Load data from snapshot.
    path_to_files = "southern-africa-trends/data/fig1/"
    with snap.open_archive():
        tb_modis = snap.read_from_archive(f"{path_to_files}CountryYearMeanGCVI.csv")
        tb_sif = snap.read_from_archive(f"{path_to_files}sifMaxCountryMean.csv")
        tb_fao = snap.read_from_archive(f"{path_to_files}FAOSTAT_data_en_12-8-2023.csv")

    #
    # Process data.
    #
    # Improve table formats.
    tb_modis = tb_modis.format(["country", "year"], short_name="crop_yields_in_southern_africa__modis")
    tb_fao = tb_fao.format(["Area", "Year", "Item"], short_name="crop_yields_in_southern_africa__fao")
    tb_sif = tb_sif.format(["country_na", "year"], short_name="crop_yields_in_southern_africa__sif")

    # Improve tables format.
    tables = [tb_modis, tb_fao, tb_sif]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
