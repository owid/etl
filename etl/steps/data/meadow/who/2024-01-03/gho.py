"""Load a snapshot and create a meadow dataset.

## Development & debugging:

Processing all datasets in this step can take a long time. For debugging it is recommended
to use `SUBSET` env to process only a subset of the datasets. Look up label of your
indicator in https://apps.who.int/gho/athena/api/GHO?format=json.

Then run the following ETL command to process it
```
SUBSET=LIFE_0000000030 etl run who/2024-01-03/gho --grapher
```
This will upsert the indicator to MySQL and don't delete other indicators that are already in MySQL,
making it useful for adding new / updating indicators.
"""

import os
import warnings
import zipfile

import owid.catalog.processing as pr
import structlog
from owid.catalog import Table
from owid.catalog.utils import underscore

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

log = structlog.get_logger()

# Subset of indicators to process separated by comma. Case insensitive.
SUBSET = os.environ.get("SUBSET")


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gho.zip")

    tables = {}

    with zipfile.ZipFile(snap.path) as z:
        with z.open("indicators.feather") as f:
            indicators = pr.read_feather(f, metadata=snap.to_table_metadata(), origin=snap.metadata.origin)

        for zip_info in z.filelist:
            if zip_info.filename == "indicators.feather":
                continue

            label = zip_info.filename.removesuffix(".feather")

            if SUBSET and underscore(label) not in underscore(SUBSET):
                continue

            ind_meta = indicators[indicators.label == label].iloc[0]

            log.info("gho.run", label=label, display=ind_meta["display"])

            with z.open(zip_info.filename) as f:
                tb = pr.read_feather(f, metadata=snap.to_table_metadata(), origin=snap.metadata.origin)

            for col in tb.columns:
                # Set metadata from WHO website as description_from_producer.
                tb[col].m.description_from_producer = ind_meta["metadata"]
                # Use `indicator's name - column` as title
                tb[col].m.title = f"{ind_meta['display']} - {col}"

            #
            # Process data.
            #
            tb = tb.rename(
                columns={
                    "Countries, territories and areas": "country",
                    "Year": "year",
                }
            )

            if "country" not in tb.columns:
                tb["country"] = None
            else:
                tb.country = tb.country.astype(str).replace("nan", None)

            tb = _remove_voided_rows(tb)

            tb = _remove_invalid_data_source_values(tb)

            if "World Bank income group" in tb.columns and tb["World Bank income group"].isnull().all():
                del tb["World Bank income group"]

            # tb = tb[(tb.country == "Honduras") & (tb.year == 2014)]

            tb = _fill_country_from_regions(tb)

            tb = tb.underscore()

            tb = _exclude_invalid_rows(tb)

            tb = tb.drop(
                columns=[
                    "who_region",
                    "un_region",
                    "un_sdg_region",
                    "unicef_region",
                    "world_bank_region",
                    "world_bank_income_group",
                ],
                errors="ignore",
            )

            # Drop low and high estimates - we could add them to the dataset, but we don't use them yet
            # and they're quite noisy.
            tb = tb.drop(
                columns=["low", "high"],
                errors="ignore",
            )

            assert tb.country.notnull().all()
            tb.m.short_name = label
            tb.m.title = ind_meta["display"]
            del tb.m.description

            tables[label] = tb.reset_index(drop=True)

    tables = list(tables.values())

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    with warnings.catch_warnings():
        # Ignore warning about missing primary key, we set dimensions in garden step
        warnings.simplefilter("ignore", category=UserWarning)
        ds_meadow = create_dataset(
            dest_dir, tables=tables, check_variables_metadata=True, default_metadata=snap.metadata
        )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def _remove_voided_rows(tb: Table) -> Table:
    # Exclude `Void approved` rows
    if "PUBLISH STATES" in tb.columns:
        tb = tb.loc[tb["PUBLISH STATES"] != "Void approved", :]
    return tb


def _remove_invalid_data_source_values(tb: Table) -> Table:
    # Remove const Data source values
    if "Data Source" not in tb.columns:
        return tb

    if tb["Data Source"].isnull().all() or set(tb["Data Source"].str.lower()) == {"data source"}:
        del tb["Data Source"]

    return tb


def _exclude_invalid_rows(tb: Table) -> Table:
    # Sudan (former) belongs to both Eastern Mediterranean and Africa regions, pick only one
    if "who_region" in tb.columns:
        exclude = (tb.country == "Sudan (former)") & (tb.who_region == "Eastern Mediterranean")
        tb = tb[~exclude]
    return tb


def _fill_country_from_regions(tb: Table) -> Table:
    # Ordered by priority
    REGION_SOURCES = [
        "WHO region",
        "UN Region",
        "UN SDG Region",
        "UNICEF region",
        "World Bank Region",
    ]

    # Start with countries
    tbs = [tb[tb.country.notnull()]]

    # Add all regions to country column
    for region_source in REGION_SOURCES:
        if region_source in tb.columns:
            ix = tb.country.isnull() & tb[region_source].notnull()
            tbs.append(
                tb[ix].assign(
                    region_source=region_source.replace(" region", "").replace(" Region", ""),
                    country=tb[ix][region_source],
                )
            )

    # Concatenate all tables
    tb = pr.concat(tbs)

    # Add origin to `region_source`
    if "region_source" in tb.columns:
        tb.region_source.m.origins = tb["Indicator"].m.origins

    # Exclude `Global` which is not an income group and would introduce duplicate rows.
    if "World Bank income group" in tb.columns:
        tb = tb[tb["World Bank income group"] != "Global"]

    # Move `World Bank income group` to `country` column.
    if "World Bank income group" in tb.columns:
        ix = (
            tb["country"].isnull()
            & tb["World Bank income group"].notnull()
            & (tb["World Bank income group"] != "Global")
        )
        if ix.any():
            tb.loc[ix, "country"] = tb.loc[ix, "World Bank income group"]

    return tb
