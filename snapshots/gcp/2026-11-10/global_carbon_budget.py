"""Script to create snapshots of the Global Carbon Budget data products.

A snapshot will be created for each of the following datasets:
* Global Carbon Budget - Fossil CO2 emissions.
* Global Carbon Budget - Global emissions.
* Global Carbon Budget - Land-use change emissions.
* Global Carbon Budget - National emissions.

"""

from pathlib import Path

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
DATE_PUBLISHED = "2026-11-10"
YEAR_PUBLISHED = "2026"
VERSION_PRODUCER = "v11"
ATTRIBUTION = f"Global Carbon Budget ({YEAR_PUBLISHED})"
# The first line in the full citation can be copied from the Zenodo page (on the right column, under "Citation").
# The rest can be the same, unless GCB notifies that they would prefer to cite a different original paper.
CITATION_FULL = """Andrew, R. M., & Peters, G. P. (2026). The Global Carbon Project's fossil CO2 emissions dataset (2026v11) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.23667912

The data files of the Global Carbon Budget can be found at: https://globalcarbonbudget.org/carbonbudget/

For more details, see the original paper:
Friedlingstein, P., O'Sullivan, M., Jones, M. W., Andrew, R. M., Bakker, D. C. E., Hauck, J., Landschützer, P., Le Quéré, C., Li, H., Luijkx, I. T., Peters, G. P., Peters, W., Pongratz, J., Schwingshackl, C., Sitch, S., Canadell, J. G., Ciais, P., Aas, K., Alin, S. R., Anthoni, P., Barbero, L., Bates, N. R., Bellouin, N., Benoit-Cattin, A., Berghoff, C. F., Bernardello, R., Bopp, L., Brasika, I. B. M., Chamberlain, M. A., Chandra, N., Chevallier, F., Chini, L. P., Collier, N. O., Colligan, T. H., Cronin, M., Djeutchouang, L. M., Dou, X., Enright, M. P., Enyo, K., Erb, M., Evans, W., Feely, R. A., Feng, L., Ford, D. J., Foster, A., Fransner, F., Gasser, T., Gehlen, M., Gkritzalis, T., Goncalves De Souza, J., Grassi, G., Gregor, L., Gruber, N., Guenet, B., Gürses, Ö., Harrington, K., Harris, I., Heinke, J., Hurtt, G. C., Iida, Y., Ilyina, T., Ito, A., Jacobson, A. R., Jain, A. K., Jarníková, T., Jersild, A., Jiang, F., Jones, S. D., Kato, E., Keeling, R. F., Klein Goldewijk, K., Knauer, J., Kong, Y., Korsbakken, J. I., Koven, C., Kunimitsu, T., Lan, X., Liu, J., Liu, Z., Liu, Z., Lo Monaco, C., Ma, L., Marland, G., McGuire, P. C., McKinley, G. A., Melton, J. R., Monacci, N., Monier, E., Morgan, E. J., Munro, D. R., Müller, J. D., Nakaoka, S.-I., Nayagam, L. R., Niwa, Y., Nutzel, T., Olsen, A., Omar, A. M., Pan, N., Pandey, S., Pierrot, D., Qin, Z., Regnier, P., Rehder, G., Resplandy, L., Roobaert, A., Rosan, T. M., Rödenbeck, C., Schwinger, J., Skjelvan, I., Smallman, T. L., Spada, V., Sreeush, M. G., Sun, Q., Sutton, A. J., Sweeney, C., Swingedouw, D., Séférian, R., Takao, S., Tatebe, H., Tian, H., Tian, X., Tilbrook, B., Tsujino, H., Tubiello, F., van Ooijen, E., van der Werf, G. R., van de Velde, S. J., Walker, A. P., Wanninkhof, R., Yang, X., Yuan, W., Yue, X., and Zeng, J.: Global Carbon Budget 2025, Earth Syst. Sci. Data, 18, 3211-3288, https://doi.org/10.5194/essd-18-3211-2026, 2026."""

DESCRIPTION = """The Global Carbon Budget was established by the Global Carbon Project (GCP) to track global carbon emissions and sinks.

This dataset makes it possible to assess whether countries are making progress toward the goals of the Paris Agreement and is widely recognized as the most comprehensive report of its kind.

Since 2001, the GCP has published estimates of global and national fossil CO₂ emissions. Initially, these were simple republished data from other sources, but over time, refinements were made based on feedback and correction of inaccuracies.
"""


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    error = "Pass the path to the local folder where data files are, with --path-to-file."
    assert path_to_file is not None, error

    # Create a new snapshot for each dataset.
    for data_file in DATA_FILES:
        snap = Snapshot(f"gcp/{SNAPSHOT_VERSION}/{data_file}")

        # Replace the full citation and description in the metadata.
        snap.metadata.origin.attribution = ATTRIBUTION  # ty: ignore
        snap.metadata.origin.citation_full = CITATION_FULL  # ty: ignore
        snap.metadata.origin.description = DESCRIPTION  # ty: ignore
        snap.metadata.origin.version_producer = VERSION_PRODUCER  # ty: ignore
        snap.metadata.origin.date_published = DATE_PUBLISHED  # ty: ignore

        # Rewrite metadata to dvc file.
        snap.metadata_path.write_text(snap.metadata.to_yaml())

        # Download data from source, add file to DVC and upload to S3.
        # snap.create_snapshot(upload=upload)
        ################################################################################################################
        # Once all files are public, we could remove this block, add url_download to each snapshot dvc file, uncomment previous line, and remove the path_to_file argument.
        # However, it is convenient to have this block, as normally we update this dataset using local files (a few days before the files are publicly released).
        path_to_data_file = Path(path_to_file) / data_file
        assert path_to_data_file.exists(), f"File {path_to_data_file} does not exist."
        snap.create_snapshot(filename=path_to_data_file, upload=upload)
        ################################################################################################################
