#
#  __init__.py
#  owid/latest/key_indicators
#

from owid.catalog import Dataset, DatasetMeta, Table

from etl.steps.data.garden.owid.latest.key_indicators import (
    table_land_area,
    table_population,
    table_population_density,
)


def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = DatasetMeta(
        namespace="owid",
        short_name="key_indicators",
        title="Key Indicators",
        description="The most important handful of indicators for use directly and in transforming other statistics.",
        version="latest",
    )

    # Add main tables
    sources = []
    table_modules = [table_land_area, table_population]
    for module in table_modules:
        t: Table = module.make_table()
        ds.add(t)
        # Collect sources from variables
        sources.extend([source for col in t.columns for source in t[col].metadata.sources])

    # Add derived table
    t = table_population_density.make_table(ds)
    ds.add(t)
    sources.extend([source for col in t.columns for source in t[col].metadata.sources])

    # Add sources from variables (ensure sources are not duplicated)
    ds.metadata.sources = [dict(ss) for ss in set(frozenset(s.items()) for s in sources)]

    ds.save()
