"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, DatasetMeta
from owid.catalog.datasets import DEFAULT_FORMATS
from owid.datautils.dataframes import combine_two_overlapping_dataframes
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import SnapshotMeta
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Map from columns in the archive dataset to columns in the new dataset.
ARCHIVE_DATA_COLUMNS = {
    "year": "year",
    "household_final_users__food_expenditure_share_of_disposable_personal_income__dpi__1__fah": "Food consumed at home as share of disposable income",
    "household_final_users__food_expenditure_share_of_disposable_personal_income__dpi__fafh": "Food consumed away from home as share of disposable income",
    "household_final_users__food_expenditure_share_of_disposable_personal_income__dpi__all_food": "Food as share of disposable income (at and away from home)",
    "all_purchasers__share_of_food_expenditures__fafh": "Food eaten away from home as share of total food expenditure",
    "all_purchasers__nominal_expenditure_per_capita3__fah": "Food expenditure at home (current prices)",
    "all_purchasers__nominal_expenditure_per_capita__fafh": "Food expenditure away from home (current prices)",
    "all_purchasers__nominal_expenditure_per_capita__all_food": "Food expenditure total (current prices)",
    "all_purchasers__constant_dollar_expenditure_per_capita__1988_100__3__fah": "Food expenditure at home (constant 1988 prices)",
    "all_purchasers__constant_dollar_expenditure_per_capita__1988_100__fafh": "Food expenditure away from home (constant 1988 prices)",
    "all_purchasers__constant_dollar_expenditure_per_capita__1988_100__all_food": "Food expenditure total (constant 1988 prices)",
}

# Map from columns in the recent dataset to columns in the new dataset.
LATEST_DATA_COLUMNS = {
    "year": "year",
    "household_final_users__nominal_food_expenditure_percentage_share_of_disposable_personal_income__dpi__fah": "Food consumed at home as share of disposable income",
    "household_final_users__nominal_food_expenditure_percentage_share_of_disposable_personal_income__dpi__fafh": "Food consumed away from home as share of disposable income",
    "household_final_users__nominal_food_expenditure_percentage_share_of_disposable_personal_income__dpi__all_food": "Food as share of disposable income (at and away from home)",
    "all_purchasers__percentage_share_of_nominal_food_expenditures__fafh": "Food eaten away from home as share of total food expenditure",
    "all_purchasers__nominal_expenditures_per_capita__u_s__dollars__fah": "Food expenditure at home (current prices)",
    "all_purchasers__nominal_expenditures_per_capita__u_s__dollars__fafh": "Food expenditure away from home (current prices)",
    "all_purchasers__nominal_expenditures_per_capita__u_s__dollars__all_food": "Food expenditure total (current prices)",
    "all_purchasers__constant_u_s__dollar_expenditures_per_capita__1988_100__fah": "Food expenditure at home (constant 1988 prices)",
    "all_purchasers__constant_u_s__dollar_expenditures_per_capita__1988_100__fafh": "Food expenditure away from home (constant 1988 prices)",
    "all_purchasers__constant_u_s__dollar_expenditures_per_capita__1988_100__all_food": "Food expenditure total (constant 1988 prices)",
}


def create_dataset_with_combined_metadata(
    dest_dir,
    datasets,
    tables,
    default_metadata=None,  # type: ignore
    underscore_table=True,
    formats=DEFAULT_FORMATS,
):
    """Create a new catalog Dataset with the combination of sources and licenses of a list of datasets.

    This function will:
    * Gather all sources and licenses of a list of datasets (`datasets`).
    * Assign the combined sources and licenses to all variables in a list of tables (`tables`).
    * Create a new dataset (using the function `create_dataset`) with the combined sources and licenses.

    NOTES:
      * The sources and licenses of the default_metadata will be ignored (and the combined sources and licenses of all
        `datasets` will be used instead).
      * If a metadata yaml file exists and contains sources and licenses, the content of the metadata file will
        override the combined sources and licenses.

    Parameters
    ----------
    dest_dir : Union[str, Path]
        Destination directory for the dataset, usually argument of `run` function.
    datasets : List[catalog.Dataset]
        Datasets whose sources and licenses will be gathered and passed on to the new dataset.
    tables : List[catalog.Table]
        Tables to add to the new dataset.
    default_metadata : Optional[Union[SnapshotMeta, catalog.DatasetMeta]]
        Default metadata for the new dataset. If it contains sources and licenses, they will be ignored (and the
        combined sources of the list of datasets passed will be used).
    underscore_table : bool
        Whether to underscore the table name before adding it to the dataset.

    Returns
    -------
    catalog.Dataset
        New dataset with combined metadata.

    """

    # Gather unique sources from the original datasets.
    sources = []
    licenses = []
    for dataset_i in datasets:
        # Get metadata from this dataset or snapshot.
        if isinstance(dataset_i.metadata, SnapshotMeta):
            metadata = convert_snapshot_metadata(dataset_i.metadata)
        else:
            metadata = dataset_i.metadata

        # Gather sources and licenses from this dataset or snapshot.
        for source in metadata.sources:
            if source.name not in [known_source.name for known_source in sources]:
                sources.append(source)
        for license in metadata.licenses:
            if license.name not in [known_license.name for known_license in licenses]:
                licenses.append(license)

    # Assign combined sources and licenses to each of the variables in each of the tables.
    for table in tables:
        index_columns = table.metadata.primary_key
        # If the table has an index, reset it, so that sources and licenses can also be assigned to index columns.
        if len(index_columns) > 0:
            table = table.reset_index()
        # Assign sources and licenses to the metadata of each variable in the table.
        for variable in table.columns:
            table[variable].metadata.sources = sources
            table[variable].metadata.licenses = licenses
        # Bring original index back.
        if len(index_columns) > 0:
            table = table.set_index(index_columns)

    if default_metadata is None:
        # If no default metadata is passed, create new empty dataset metadata.
        default_metadata = DatasetMeta()
    elif isinstance(default_metadata, SnapshotMeta):
        # If a snapshot metadata is passed as default metadata, convert it to a dataset metadata.
        default_metadata: DatasetMeta = convert_snapshot_metadata(default_metadata)

    # Assign combined sources and licenses to the new dataset metadata.
    default_metadata.sources = sources
    default_metadata.licenses = licenses

    # Create a new dataset.
    ds = create_dataset(
        dest_dir=dest_dir,
        tables=tables,
        default_metadata=default_metadata,
        underscore_table=underscore_table,
        formats=formats,
    )

    return ds


def run(dest_dir: str) -> None:
    log.info("food_expenditure_in_us.start")

    #
    # Load inputs.
    #
    # Load meadow datasets of archive data and read its main table.
    ds_archive = cast(Dataset, paths.load_dependency("food_expenditure_in_us_archive"))
    tb_archive = ds_archive["food_expenditure_in_us_archive"].reset_index()

    # Load meadow datasets of latest data and read its main table.
    ds_latest = cast(Dataset, paths.load_dependency("food_expenditure_in_us"))
    tb_latest = ds_latest["food_expenditure_in_us"].reset_index()

    #
    # Process data.
    #
    # Rename columns in table of archive data.
    tb_archive = tb_archive[list(ARCHIVE_DATA_COLUMNS)].rename(columns=ARCHIVE_DATA_COLUMNS, errors="raise")

    # Convert all columns in table or archive data to float.
    tb_archive = tb_archive.astype({column: float for column in tb_archive.columns if column != "year"})

    # Add country column to table of archive data.
    tb_archive = tb_archive.assign(**{"country": "United States"})

    # Rename columns in table of latest data.
    tb_latest = tb_latest[list(LATEST_DATA_COLUMNS)].rename(columns=LATEST_DATA_COLUMNS, errors="raise")

    # Convert all columns in table of latest data to float.
    tb_latest = tb_latest.astype({column: float for column in tb_latest.columns if column != "year"})

    # Add country column to table of latest data.
    tb_latest = tb_latest.assign(**{"country": "United States"})

    # Convert "share" columns in table of latest data into a percentage.
    tb_latest[[column for column in tb_latest.columns if "share" in column]] *= 100

    # Ensure columns in both tables of archive and latest data coincide.
    assert set(tb_archive.columns) == set(tb_latest.columns)

    # Combine both tables, prioritizing latest data over archive data.
    tb_combined = combine_two_overlapping_dataframes(tb_latest, tb_archive, index_columns=["country", "year"])

    # Set an appropriate index and sort conveniently.
    tb_combined = tb_combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset_with_combined_metadata(
        dest_dir, datasets=[ds_archive, ds_latest], tables=[tb_combined], default_metadata=ds_latest.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("food_expenditure_in_us.end")
