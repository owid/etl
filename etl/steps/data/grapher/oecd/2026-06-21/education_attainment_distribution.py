"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

AGGREGATES_TO_EXCLUDE = ["European Union (25 countries)", "G20", "OECD"]

TABLE_NAMES = [
    "education_attainment_distribution",
    "education_no_formal_combined",
    "education_attainment_distribution_oecd",
    "education_attainment_distribution_oecd_sex",
    "education_attainment_distribution_wc",
    "education_no_formal_wc",
    "education_some_formal_wc",
    "education_no_formal_by_sex_wc",
    "education_formal_combined",
    "education_no_formal_three_sources",
]


def run() -> None:
    ds_garden = paths.load_dataset("education_attainment_distribution")

    tables = []
    for name in TABLE_NAMES:
        tb = ds_garden.read(name)

        # Remove non-country aggregates that don't map to grapher entities.
        if "country" in tb.columns:
            tb = tb[~tb["country"].isin(AGGREGATES_TO_EXCLUDE)]

        # Re-format with the appropriate index columns.
        index_cols = [c for c in ["country", "year", "sex"] if c in tb.columns]
        tb = tb.format(index_cols)

        tables.append(tb)

    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata)
    ds_grapher.save()
