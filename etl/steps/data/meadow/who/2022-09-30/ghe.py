from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    snap = paths.load_snapshot()
    tb = snap.read()

    # clean and transform data
    tb = clean_data(tb)

    # format
    tb = tb.format(["country", "year", "age_group", "sex", "cause"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def clean_data(tb: Table) -> Table:
    return tb.rename(
        columns={
            "DIM_COUNTRY_CODE": "country",
            "DIM_YEAR_CODE": "year",
            "DIM_AGEGROUP_CODE": "age_group",
            "DIM_SEX_CODE": "sex",
            "DIM_GHECAUSE_TITLE": "cause",
            "VAL_DALY_RATE100K_NUMERIC": "daly_rate100k",
            "VAL_DALY_COUNT_NUMERIC": "daly_count",
            "VAL_DEATHS_RATE100K_NUMERIC": "death_rate100k",
            "VAL_DEATHS_COUNT_NUMERIC": "death_count",
        }
    )
