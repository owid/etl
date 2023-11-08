"""Load a snapshot and create a meadow dataset."""

import re

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("modal_age_death.xlsx")

    # Load data from snapshot.
    tb = snap.read(header=1)

    #
    # Process data.
    #
    tb = tb.dropna(how="all", axis=1)

    # Iterate over triplets and format them accordingly
    tbs = []
    num_triplets = int(len(tb.columns) / 3)
    for i in range(num_triplets):
        columns = tb.columns[i * 3 : (i + 1) * 3]
        tb_ = format_subtable(tb[columns])
        tbs.append(tb_)
    tb = pr.concat(tbs)

    # Drop NaNs
    tb = tb.dropna(subset="modal_age_death")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year", "sex"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def format_subtable(tb: Table) -> Table:
    tb_ = tb.copy()
    # Sanity checks
    assert "year" in tb.columns[0], "First column in triplet should be the year!"
    assert "M_F" in tb.columns[1], "Second column in triplet should be M_M (modal age for males)!"
    assert "M_M" in tb.columns[2], "Third column in triplet should be M_F (modal age for females)!"

    # Extract country name
    regex = r"M_[M|F]_([A-Z]{3})"
    country_F = re.match(regex, tb.columns[1])
    if country_F:
        country_F = country_F.group(1)
    else:
        raise ValueError("Could not extract country name from column name (Female)!")
    country_M = re.match(regex, tb.columns[1])
    if country_M:
        country_M = country_M.group(1)
    else:
        raise ValueError("Could not extract country name from column name (Male)!")
    assert country_F == country_M, "Country names in triplet should be the same!"

    # Clean column names
    tb_.columns = [
        "year",
        "females",
        "males",
    ]

    # Unpivot
    tb_ = tb_.melt(id_vars=["year"], var_name="sex", value_name="modal_age_death")

    # Add country column
    tb_["country"] = country_F

    return tb_
