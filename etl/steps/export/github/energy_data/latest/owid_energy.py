"""Export step that commits the OWID Energy dataset to the energy-data repository.

The combined datasets include:
* Statistical review of world energy - Energy Institute.
* International energy data - U.S. Energy Information Administration.
* Energy from fossil fuels - The Shift Dataportal.
* Yearly Electricity Data - Ember.
* Primary energy consumption - Our World in Data.
* Fossil fuel production - Our World in Data.
* Energy mix - Our World in Data.
* Electricity mix - Our World in Data.

Additionally, OWID's regions dataset, population dataset and Maddison Project Database on GDP are included.

Outputs that will be committed to a branch in the energy-data repository:
* The main data file (as a .csv file).
* The codebook (as a .csv file).
* The README file.

"""

import re
import tempfile
from pathlib import Path

import git
import pandas as pd
from owid.catalog import Dataset, Origin, Table
from structlog import get_logger

from etl.config import DRY_RUN
from etl.git_api_helpers import GithubApiRepo
from etl.helpers import PathFinder
from etl.paths import BASE_DIR, DATA_DIR

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def remove_details_on_demand(text: str) -> str:
    # Remove references to details on demand from a text.
    # Example: "This is a [description](#dod:something)." -> "This is a description."
    regex = r"\(\#dod\:.*\)"
    if "(#dod:" in text:
        text = re.sub(regex, "", text).replace("[", "").replace("]", "")

    return text


def prepare_codebook(tb: Table) -> pd.DataFrame:
    table = tb.copy()

    # Manually create an origin for the regions dataset.
    regions_origin = [Origin(producer="Our World in Data", title="Regions", date_published=str(table["year"].max()))]

    # Manually edit some of the metadata fields.
    table["country"].metadata.title = "Country"
    table["country"].metadata.description_short = "Geographic location."
    table["country"].metadata.description = None
    table["country"].metadata.unit = ""
    table["country"].metadata.origins = regions_origin
    table["year"].metadata.title = "Year"
    table["year"].metadata.description_short = "Year of observation."
    table["year"].metadata.description = None
    table["year"].metadata.unit = ""
    table["year"].metadata.origins = regions_origin

    if table["population"].metadata.description is not None:
        log.warning("Column population has a description field. Consider removing this part of the code")
        table["population"].metadata.description = None

    # Gather column names, titles, short descriptions, unit and origins from the indicators' metadata.
    metadata = {"column": [], "description": [], "unit": [], "source": []}
    for column in table.columns:
        metadata["column"].append(column)

        if hasattr(table[column].metadata, "description") and table[column].metadata.description is not None:
            log.warning(f"Column {column} still has a 'description' field.")

        # Prepare indicator's description.
        description = ""
        if (
            hasattr(table[column].metadata.presentation, "title_public")
            and table[column].metadata.presentation.title_public is not None
        ):
            description += table[column].metadata.presentation.title_public
        elif table[column].metadata.title:
            description += table[column].metadata.title
        if table[column].metadata.description_short:
            description += f" - {table[column].metadata.description_short}"
            description = remove_details_on_demand(description)
        metadata["description"].append(description)

        # Prepare indicator's unit.
        if table[column].metadata.unit is None:
            log.warning(f"Column {column} does not have a unit.")
            unit = ""
        else:
            unit = table[column].metadata.unit
        metadata["unit"].append(unit)

        # Gather unique origins of current variable.
        unique_sources = []
        for origin in table[column].metadata.origins:
            # Construct the source name from the origin's attribution.
            # If not defined, build it using the default format "Producer - Data product (year)".
            source_name = (
                origin.attribution
                or f"{origin.producer} - {origin.title or origin.title_snapshot} ({origin.date_published.split('-')[0]})"
            )

            # Add url at the end of the source.
            if origin.url_main:
                source_name += f" [{origin.url_main}]"

            # Add the source to the list of unique sources.
            if source_name not in unique_sources:
                unique_sources.append(source_name)

        # Concatenate all sources.
        sources_combined = "; ".join(unique_sources)
        metadata["source"].append(sources_combined)

    # Create a dataframe with the gathered metadata and sort conveniently by column name.
    codebook = pd.DataFrame(metadata).set_index("column").sort_index()
    # For clarity, ensure column descriptions are in the same order as the columns in the data.
    first_columns = ["country", "year", "iso_code", "population", "gdp"]
    codebook = pd.concat([codebook.loc[first_columns], codebook.drop(first_columns, errors="raise")]).reset_index(
        drop=False
    )
    # Note: reset_index() here converts the 'column' index back to a column

    return codebook


def prepare_data(tb: Table) -> Table:
    # Sort rows and columns conveniently.
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)
    first_columns = ["country", "year", "iso_code", "population", "gdp"]
    tb = tb[first_columns + [column for column in sorted(tb.columns) if column not in first_columns]]

    return tb


def validate_data(tb: Table, codebook: pd.DataFrame) -> None:
    """Run validation checks from the original test_make_dataset.py."""
    log.info("Running data validation checks...")

    # All columns in codebook should be in the data
    col_in_data = codebook["column"].isin(tb.columns)
    assert col_in_data.all(), (
        "All codebook columns should be in the data, but the following "
        f"columns are not: {codebook['column'][~col_in_data].tolist()}"
    )

    # All columns in data should be in the codebook
    col_in_codebook = tb.columns.isin(codebook["column"])
    assert col_in_codebook.all(), (
        "All columns should be in the codebook, but the following "
        f"columns are not: {tb.columns[~col_in_codebook].tolist()}"
    )

    # Column names should not contain whitespace
    col_contains_space = tb.columns.str.contains(r"\s", regex=True)
    assert col_contains_space.sum() == 0, (
        "Columns should not contain whitespace, but the following "
        f"columns do: {tb.columns[col_contains_space].tolist()}"
    )

    # All columns should be lowercase
    col_is_lower = tb.columns == tb.columns.str.lower()
    assert col_is_lower.all(), (
        "Columns should not have uppercase characters, but the following "
        f"columns do: {tb.columns[~col_is_lower].tolist()}"
    )

    # No rows should be all NaN (excluding index columns)
    index_cols = ["country", "year", "iso_code"]
    row_all_nan = tb.drop(columns=index_cols).isnull().all(axis=1)
    assert row_all_nan.sum() == 0, (
        "All rows should contain at least one non-NaN value, but " f"{row_all_nan.sum()} row(s) contain all NaN values."
    )

    # Check for deprecated country names
    old_names = ["burma", "macedonia", "swaziland", "czech republic"]
    countries_lower = set(tb["country"].str.lower())
    for old_name in old_names:
        assert (
            old_name not in countries_lower
        ), f"{old_name} is a deprecated country name that should not exist in the dataset."

    # Codebook column order should match data column order
    assert (
        codebook["column"].tolist() == tb.columns.tolist()
    ), "Codebook column descriptions are not in the same order as data columns."

    log.info("All validation checks passed.")


def prepare_and_save_outputs(tb: Table, codebook: Table, temp_dir_path: Path) -> None:
    # Create codebook and save it as a csv file.
    log.info("Creating codebook csv file.")
    pd.DataFrame(codebook).to_csv(temp_dir_path / "owid-energy-codebook.csv", index=False)

    # Create a csv file.
    log.info("Creating csv file.")
    pd.DataFrame(tb).to_csv(temp_dir_path / "owid-energy-data.csv", index=False, float_format="%.3f")

    # Create a README file.
    log.info("Creating README file.")
    readme_path = Path(__file__).parent / "owid_energy_readme.md"
    readme = readme_path.read_text()
    (temp_dir_path / "README.md").write_text(readme)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load the owid_energy dataset from external, and read its main table.
    ds_energy = Dataset(DATA_DIR / "external/energy_data/latest/owid_energy")
    tb = ds_energy.read("owid_energy")

    #
    # Process data.
    #
    # Prepare data and codebook
    tb = prepare_data(tb=tb)
    codebook = prepare_codebook(tb=tb)

    # Run validation checks
    validate_data(tb=tb, codebook=codebook)

    #
    # Save outputs.
    #
    # Check if we're on master branch (if so, force dry run)
    branch = git.Repo(BASE_DIR).active_branch.name
    dry_run = DRY_RUN or (branch == "master")

    if branch == "master":
        log.warning("You are on master branch, using dry mode.")
    else:
        log.info(f"Committing files to branch {branch}")

    # Create a temporary directory for all files to be committed.
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        prepare_and_save_outputs(tb, codebook=codebook, temp_dir_path=temp_dir_path)

        repo = GithubApiRepo(repo_name="energy-data")

        repo.create_branch_if_not_exists(branch_name=branch, dry_run=dry_run)

        # Commit csv files to the repo.
        for file_name in ["owid-energy-data.csv", "owid-energy-codebook.csv", "README.md"]:
            with (temp_dir_path / file_name).open("r") as file_content:
                repo.commit_file(
                    file_content.read(),
                    file_path=file_name,
                    commit_message=":bar_chart: Automated update",
                    branch=branch,
                    dry_run=dry_run,
                )

    if not dry_run:
        log.info(
            f"Files committed successfully to branch {branch}. Create a PR here https://github.com/owid/energy-data/compare/master...{branch}."
        )
