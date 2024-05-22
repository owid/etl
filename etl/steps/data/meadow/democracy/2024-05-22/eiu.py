"""Load a snapshot and create a meadow dataset."""

from owid.catalog.tables import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots
    shortnames = [
        "eiu_gapminder",
        "eiu_2021",
        "eiu_2022",
        "eiu_2023",
    ]
    tbs = []
    for name in shortnames:
        snap = paths.load_snapshot(f"{name}.csv")
        tb = snap.read()
        tbs.append(tb)

    # Concatenate all tables.
    tb = concat(tbs, ignore_index=True, short_name="eiu")

    #
    # Process data.
    #
    tb = tb.rename(
        columns={
            "country_name": "country",
        }
    )

    tb["rank_eiu"] = tb["rank_eiu"].str.replace("=", "")
    tb["rank_eiu"] = tb["rank_eiu"].astype("float")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
