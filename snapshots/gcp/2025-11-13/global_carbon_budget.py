"""Script to create snapshots of the Global Carbon Budget data products.

A snapshot will be created for each of the following datasets:
* Global Carbon Budget - Fossil CO2 emissions.
* Global Carbon Budget - Global emissions.
* Global Carbon Budget - Land-use change emissions.
* Global Carbon Budget - National emissions.

"""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Names of input data files to create snapshots for.
DATA_FILES = [
    "global_carbon_budget_fossil_co2_emissions.csv",
    "global_carbon_budget_global_emissions.xlsx",
    "global_carbon_budget_land_use_change_emissions.xlsx",
    "global_carbon_budget_national_emissions.xlsx",
]

# Define common metadata fields (to be written to dvc files).
DATE_PUBLISHED = "2025-11-13"
YEAR_PUBLISHED = "2025"
VERSION_PRODUCER = "v15"
ATTRIBUTION = f"Global Carbon Budget ({YEAR_PUBLISHED})"
# The first line in the full citation can be copied from the Zenodo page (on the right column, under "Citation").
# The rest can be the same, unless GCB notifies that they would prefer to cite a different original paper.
CITATION_FULL = """Andrew, R. M., & Peters, G. P. (2025). The Global Carbon Project's fossil CO2 emissions dataset (2025v15) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.17417124

The data files of the Global Carbon Budget can be found at: https://globalcarbonbudget.org/carbonbudget/

For more details, see the original paper:
Friedlingstein, P., O'Sullivan, M., Jones, M. W., Andrew, R. M., Bakker, D. C. E., Hauck, J., Landschützer, P., Le Quéré, C., Luijkx, I. T., Peters, G. P., Peters, W., Pongratz, J., Schwingshackl, C., Sitch, S., Canadell, J. G., Ciais, P., Jackson, R. B., Alin, S. R., Anthoni, P., Barbero, L., Bates, N. R., Becker, M., Bellouin, N., Decharme, B., Bopp, L., Brasika, I. B. M., Cadule, P., Chamberlain, M. A., Chandra, N., Chau, T.-T.-T., Chevallier, F., Chini, L. P., Cronin, M., Dou, X., Enyo, K., Evans, W., Falk, S., Feely, R. A., Feng, L., Ford, D. J., Gasser, T., Ghattas, J., Gkritzalis, T., Grassi, G., Gregor, L., Gruber, N., Gürses, Ö., Harris, I., Hefner, M., Heinke, J., Houghton, R. A., Hurtt, G. C., Iida, Y., Ilyina, T., Jacobson, A. R., Jain, A., Jarníková, T., Jersild, A., Jiang, F., Jin, Z., Joos, F., Kato, E., Keeling, R. F., Kennedy, D., Klein Goldewijk, K., Knauer, J., Korsbakken, J. I., Körtzinger, A., Lan, X., Lefèvre, N., Li, H., Liu, J., Liu, Z., Ma, L., Marland, G., Mayot, N., McGuire, P. C., McKinley, G. A., Meyer, G., Morgan, E. J., Munro, D. R., Nakaoka, S.-I., Niwa, Y., O'Brien, K. M., Olsen, A., Omar, A. M., Ono, T., Paulsen, M., Pierrot, D., Pocock, K., Poulter, B., Powis, C. M., Rehder, G., Resplandy, L., Robertson, E., Rödenbeck, C., Rosan, T. M., Schwinger, J., Séférian, R., Smallman, T. L., Smith, S. M., Sospedra-Alfonso, R., Sun, Q., Sutton, A. J., Sweeney, C., Takao, S., Tans, P. P., Tian, H., Tilbrook, B., Tsujino, H., Tubiello, F., van der Werf, G. R., van Ooijen, E., Wanninkhof, R., Watanabe, M., Wimart-Rousseau, C., Yang, D., Yang, X., Yuan, W., Yue, X., Zaehle, S., Zeng, J., and Zheng, B.: Global Carbon Budget 2023, Earth Syst. Sci. Data, 15, 5301-5369, https://doi.org/10.5194/essd-15-5301-2023, 2023."""

DESCRIPTION = """The Global Carbon Budget was established by the Global Carbon Project (GCP) to track global carbon emissions and sinks.

This dataset makes it possible to assess whether countries are making progress toward the goals of the Paris Agreement and is widely recognized as the most comprehensive report of its kind.

Since 2001, the GCP has published estimates of global and national fossil CO₂ emissions. Initially, these were simple republished data from other sources, but over time, refinements were made based on feedback and correction of inaccuracies.
"""


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
@click.option("--path-to-folder", prompt=True, type=str, help="Path to local folder where data files are.")
def run(path_to_folder: str, upload: bool) -> None:
    # Create a new snapshot for each dataset.
    for data_file in DATA_FILES:
        snap = Snapshot(f"gcp/{SNAPSHOT_VERSION}/{data_file}")

        # Replace the full citation and description in the metadata.
        snap.metadata.origin.attribution = ATTRIBUTION  # type: ignore
        snap.metadata.origin.citation_full = CITATION_FULL  # type: ignore
        snap.metadata.origin.description = DESCRIPTION  # type: ignore
        snap.metadata.origin.version_producer = VERSION_PRODUCER  # type: ignore
        snap.metadata.origin.date_published = DATE_PUBLISHED  # type: ignore

        # Rewrite metadata to dvc file.
        snap.metadata_path.write_text(snap.metadata.to_yaml())

        # Download data from source, add file to DVC and upload to S3.
        ################################################################################################################
        # snap.create_snapshot(upload=upload)
        # TODO: Once public, remove this, uncomment previous, and remove click.option for path to folder.
        path_to_file = Path(path_to_folder) / data_file
        assert path_to_file.exists(), f"File {path_to_file} does not exist."
        snap.create_snapshot(filename=path_to_file, upload=upload)
        ################################################################################################################


if __name__ == "__main__":
    run()
