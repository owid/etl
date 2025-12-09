"""Load a snapshot and create a meadow dataset.

## Development & debugging:

Processing all datasets in this step can take a long time. For debugging it is recommended
to use `SUBSET` env to process only a subset of the datasets. Look up label of your
indicator in https://apps.who.int/gho/athena/api/GHO?format=json.

Then run the following ETL command to process it
```
SUBSET=AIR_11 etl run who/2025-05-19/gho --grapher
```
This will upsert the indicator to MySQL and don't delete other indicators that are already in MySQL,
making it useful for adding new / updating indicators.
"""

import os
import warnings
import zipfile

import owid.catalog.processing as pr
import structlog
from owid.catalog.utils import underscore

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

log = structlog.get_logger()

# Subset of indicators to process separated by comma. Case insensitive.
SUBSET = os.environ.get("SUBSET")
if SUBSET:
    # These are required by OOMs in the garden step
    subset_list = [
        "WHS3_45",
        "PHE_HHAIR_POP_CLEAN_FUELS",
        "WHS3_56",
        "PHE_HHAIR_PROP_POP_CLEAN_FUELS",
        "NTD_YAWSNUM",
        "carep",
        "NTD_7",
        "NTD_8",
        "NTD_TRA5",
        "NTD_ONCHEMO",
        "NTD_ONCTREAT",
        "NCD_BMI_25A",
        "MDG_0000000026",
        "MDG_0000000032",
        "MORT_MATERNALNUM",
        "NUTSTUNTINGPREV",
        "R_Total_tax",
        "O_Group",
        "E_Group",
        "P_count_places_sf",
        "R_afford_gdp",
        "SDGNTDTREATMENT",
        "MH_1",
    ]
    SUBSET += "," + ",".join(subset_list)


def run() -> None:
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

            ind_meta = indicators[indicators.label == label].iloc[0]

            if SUBSET:
                if (
                    "smoking" in ind_meta["display"].lower()
                    or "tobacco" in ind_meta["display"].lower()
                    or "tax" in ind_meta["display"].lower()
                ) and "estimate" in ind_meta["display"].lower():
                    pass
                elif "stunting" in ind_meta["display"].lower():
                    pass
                elif "cooking" in ind_meta["display"].lower():
                    pass
                elif underscore(label) in underscore(SUBSET):
                    pass
                else:
                    continue

            log.info("gho.run", label=label, display=ind_meta["display"])

            with z.open(zip_info.filename) as f:
                tb = pr.read_feather(f, metadata=snap.to_table_metadata(), origin=snap.metadata.origin)

            for col in tb.columns:
                # Set metadata from WHO website as description_from_producer.
                tb[col].m.description_from_producer = ind_meta["metadata"]
                # Use `indicator's name - column` as title
                tb[col].m.title = f"{ind_meta['display']} - {col}"

            # Drop unused columns
            tb = tb.drop(columns=["ParentLocation", "TimeDimensionValue"])

            #
            # Process data.
            #
            tb = tb.rename(
                columns={
                    "Country": "country",
                    "Year": "year",
                }
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
        ds_meadow = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
