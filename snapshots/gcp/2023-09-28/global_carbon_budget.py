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
CITATION_FULL = """Andrew, R. M., & Peters, G. P. (2022). The Global Carbon Project's fossil CO2 emissions dataset (2022v27) [Data set]. Zenodo. Data and methodology can be found at: https://doi.org/10.5281/zenodo.7215364

The data files of the Global Carbon Budget can be found at: https://globalcarbonbudget.org/carbonbudget/

For more details, see the original paper:
Friedlingstein, P., O'Sullivan, M., Jones, M. W., Andrew, R. M., Gregor, L., Hauck, J., Le Quéré, C., Luijkx, I. T., Olsen, A., Peters, G. P., Peters, W., Pongratz, J., Schwingshackl, C., Sitch, S., Canadell, J. G., Ciais, P., Jackson, R. B., Alin, S. R., Alkama, R., Arneth, A., Arora, V. K., Bates, N. R., Becker, M., Bellouin, N., Bittig, H. C., Bopp, L., Chevallier, F., Chini, L. P., Cronin, M., Evans, W., Falk, S., Feely, R. A., Gasser, T., Gehlen, M., Gkritzalis, T., Gloege, L., Grassi, G., Gruber, N., Gürses, Ö., Harris, I., Hefner, M., Houghton, R. A., Hurtt, G. C., Iida, Y., Ilyina, T., Jain, A. K., Jersild, A., Kadono, K., Kato, E., Kennedy, D., Klein Goldewijk, K., Knauer, J., Korsbakken, J. I., Landschützer, P., Lefèvre, N., Lindsay, K., Liu, J., Liu, Z., Marland, G., Mayot, N., McGrath, M. J., Metzl, N., Monacci, N. M., Munro, D. R., Nakaoka, S.-I., Niwa, Y., O'Brien, K., Ono, T., Palmer, P. I., Pan, N., Pierrot, D., Pocock, K., Poulter, B., Resplandy, L., Robertson, E., Rödenbeck, C., Rodriguez, C., Rosan, T. M., Schwinger, J., Séférian, R., Shutler, J. D., Skjelvan, I., Steinhoff, T., Sun, Q., Sutton, A. J., Sweeney, C., Takao, S., Tanhua, T., Tans, P. P., Tian, X., Tian, H., Tilbrook, B., Tsujino, H., Tubiello, F., van der Werf, G. R., Walker, A. P., Wanninkhof, R., Whitehead, C., Willstrand Wranne, A., Wright, R., Yuan, W., Yue, C., Yue, X., Zaehle, S., Zeng, J., and Zheng, B.: Global Carbon Budget 2022, Earth Syst. Sci. Data, 14, 4811-4900, https://doi.org/10.5194/essd-14-4811-2022, 2022."""

DESCRIPTION = """The Global Carbon Budget 2022 has over 105 contributors from 80 organisations and 18 countries. It was founded by the Global Carbon Project international science team to track the trends in global carbon emissions and sinks and is a key measure of progress towards the goals of the Paris Agreement. It's widely recognised as the most comprehensive report of its kind. The 2022 report was published at COP27 in Egypt on Friday 11th November."""


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Create a new snapshot for each dataset.
    for data_file in DATA_FILES:
        snap = Snapshot(f"gcp/{SNAPSHOT_VERSION}/{data_file}")

        # Replace the full citation and description in the metadata.
        snap.metadata.origin.citation_full = CITATION_FULL  # type: ignore
        snap.metadata.origin.description = DESCRIPTION  # type: ignore

        # Rewrite metadata to dvc file.
        snap.metadata_path.write_text(snap.metadata.to_yaml())

        # Download data from source, add file to DVC and upload to S3.
        snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()
