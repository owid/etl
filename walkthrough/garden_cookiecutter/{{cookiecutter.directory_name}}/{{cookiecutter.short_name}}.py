"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
{% if cookiecutter.load_countries_regions == "True" %}


def load_countries_regions() -> Table:
    """Load countries-regions table from reference dataset (e.g. to map from iso codes to country names)."""
    ds_reference = cast(Dataset, paths.load_dependency("regions"))
    tb_countries_regions = ds_reference["regions"]

    return tb_countries_regions
{% endif -%}
{% if cookiecutter.load_population == "True" %}


def load_population() -> Table:
    """Load population table from population OMM dataset."""
    ds_indicators = cast(Dataset, paths.load_dependency(channel="garden", namespace="demography", short_name="population"))
    tb_population = ds_indicators["population"]

    return tb_population
{% endif -%}


def run(dest_dir: str) -> None:
    log.info("{{cookiecutter.short_name}}.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("{{cookiecutter.short_name}}"))

    # Read table from meadow dataset.
    tb = ds_meadow["{{cookiecutter.short_name}}"]

    #
    # Process data.
    #
    log.info("{{cookiecutter.short_name}}.harmonize_countries")
    tb: Table = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("{{cookiecutter.short_name}}.end")
