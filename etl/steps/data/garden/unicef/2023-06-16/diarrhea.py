"""Load a meadow dataset and create a garden dataset."""

from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("diarrhea.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("diarrhea")

    # Read table from meadow dataset.
    tb = ds_meadow["diarrhea"]

    #
    # Process data.
    #
    log.info("diarrhea.harmonize_countries")
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # The source data contains a single duplicate (Rwanda 2000 ORS) where one row has a value and another is NaN.
    # Drop NaN rows so each (country, year, indicator) is unique, then pivot while preserving metadata.
    tb = tb.dropna(subset=["value"]).drop_duplicates(subset=["country", "year", "indicator"])
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value", join_column_levels_with="")
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("diarrhea.end")
