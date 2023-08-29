from typing import cast

from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    ds_meadow = paths.load_dataset()

    tb = cast(Table, ds_meadow["excess_mortality_economist"])

    # Set index
    tb = tb.reset_index()
    # Set type
    tb = tb.astype({"country": str})

    regions = cast(Dataset, paths.load_dependency("regions"))["regions"]

    tb = _harmonize_countries(tb, regions)

    tb = cast(Table, tb.drop(columns=["known_excess_deaths"]))

    # Set index
    tb = tb.set_index(["country", "date"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata, formats=["csv", "feather"])

    # Save changes in the new garden dataset.
    ds_garden.save()


def _harmonize_countries(tb: Table, regions: Table) -> Table:
    # Map codes to country names, keep originals if no match.
    tb["country"] = tb["country"].map(regions["name"].astype(str)).fillna(tb["country"])

    # Map KSV to Kosovo
    tb.loc[tb["country"] == "KSV", "country"] = "Kosovo"

    assert set(tb["country"]) - set(regions["name"]) == set(), "Unknown countries"

    return tb
