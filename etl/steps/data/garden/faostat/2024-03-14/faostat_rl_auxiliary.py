"""Auxiliary FAOSTAT RL step that does some basic processing and creates a dataset that is used by the population dataset to calculate population density."""

import os
import tempfile
import zipfile

import owid.catalog.processing as pr
import structlog
from owid.catalog import Table, VariablePresentationMeta

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.snapshot import Snapshot

# Initialise log.
log = structlog.get_logger()

paths = PathFinder(__file__)


def load_data(snapshot: Snapshot) -> Table:
    # Unzip data into a temporary folder.
    with tempfile.TemporaryDirectory() as temp_dir:
        z = zipfile.ZipFile(snapshot.path)
        z.extractall(temp_dir)
        (filename,) = list(filter(lambda x: "(Normalized)" in x, os.listdir(temp_dir)))

        # Load data from main file.
        data = pr.read_csv(
            os.path.join(temp_dir, filename),
            encoding="latin-1",
            low_memory=False,
            origin=snapshot.metadata.origin,
            metadata=snapshot.to_table_metadata(),
        )

    return data


def run() -> None:
    #
    # Load data.
    #
    # Load snapshot and read its data.
    snapshot = paths.load_snapshot("faostat_rl")
    tb = load_data(snapshot=snapshot)

    #
    # Process data.
    #
    # Select the only required column (to be used with the final purpose of calculating population density).
    tb = tb[(tb["Item Code"] == 6601) & (tb["Element Code"] == 5110)].reset_index()

    # Adapt units.
    assert set(tb["Unit"]) == {"1000 ha"}, "The unit of land area has changed."
    tb["Value"] *= 1000

    # Create a title for the indicator that is compatible with other FAOSTAT datasets.
    column = "Land area | 00006601 || Area | 005110 || hectares"

    # Prepare table.
    tb = tb[["Area", "Year", "Value"]].rename(columns={"Area": "country", "Year": "year", "Value": column})

    # Improve indicator's metadata.
    title_public = "Land area - Area (hectares)"
    tb[column].metadata.title = column
    tb[column].metadata.unit = "hectares"
    tb[column].metadata.short_unit = "ha"
    tb[
        column
    ].metadata.description_from_producer = "Country area excluding area under inland waters and coastal waters."
    tb[column].metadata.display = {"name": title_public}
    tb[column].metadata.presentation = VariablePresentationMeta(title_public=title_public)

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path.parent / "faostat.countries.json",
        excluded_countries_file=paths.excluded_countries_path.parent / "faostat.excluded_countries.json",
        warn_on_missing_countries=True,
        warn_on_unknown_excluded_countries=False,
        warn_on_unused_countries=False,
    )

    # NOTE: Ideally, we would add region aggregates here. However, that would introduce new circular dependencies. So region aggregates will be added in the population step.

    # Improve table format.
    tb = tb.format(short_name="faostat_rl_auxiliary")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=snapshot.to_table_metadata())

    # Save garden dataset.
    ds_garden.save()
