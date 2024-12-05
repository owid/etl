"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("paternal_ages.rdata")

    # Load data from snapshot.
    tbs = snap.read_rda_multiple()

    #
    # Process data.
    #
    # Read counts & rates
    tb_counts = [tb.assign(tname=tname) for tname, tb in tbs.items() if tname.startswith("counts_")]
    tb_counts = pr.concat(tb_counts, ignore_index=True)
    tb_counts["code"] = tb_counts["tname"].str.split("_").str[1].str[:-1]
    tb_rates = [tb.assign(tname=tname) for tname, tb in tbs.items() if tname.startswith("rates_")]
    tb_rates = pr.concat(tb_rates, ignore_index=True)
    tb_rates["code"] = tb_rates["tname"].str.split("_").str[1].str[:-1]

    # Drop duplicates
    flag_nld = (tb_counts["tname"] == "counts_NLD4") & (tb_counts["year"] >= 1996) & (tb_counts["year"] <= 2014)
    flag_dnk = (tb_counts["tname"] == "counts_DNK3") & (tb_counts["year"] >= 2007) & (tb_counts["year"] <= 2015)
    flag_che = (tb_counts["tname"] == "counts_CHE4") & (tb_counts["year"] >= 2007) & (tb_counts["year"] <= 2014)
    tb_counts = tb_counts.loc[~(flag_nld | flag_dnk | flag_che)]

    # Drop duplicates
    flag_isl1 = (tb_rates["tname"] == "rates_ISL5") & (tb_rates["year"] >= 1981) & (tb_rates["year"] <= 2013)
    flag_isl2 = (tb_rates["tname"] == "rates_ISL3") & (tb_rates["year"] >= 1981)
    flag_gbr = (tb_rates["tname"] == "rates_GBREW4") & (tb_rates["year"] >= 1964) & (tb_rates["year"] <= 2013)
    tb_rates = tb_rates.loc[~(flag_isl1 | flag_isl2 | flag_gbr)]

    # Dtypes
    str_types = ["sourcelong", "tname", "GMI", "country", "type", "source"]
    tb_counts = tb_counts.astype({col: "string" for col in str_types})
    tb_rates = tb_rates.astype({col: "string" for col in str_types})

    # Drop spurious data
    tb_counts = tb_counts.loc[tb_counts["source"] != "True"]
    tb_rates = tb_rates.loc[~tb_rates["source"].isna()]

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [
        tb_counts.format(["code", "year", "source", "type"], short_name="counts"),
        tb_rates.format(["code", "year", "source", "type"], short_name="rates"),
    ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


COLUMNS_COUNTS = [
    "country",
    "year",
    "type",
    "mean age at childbirth as arithmetic mean",
    "mean age at childbirth based demographic rates",
    "source",
]
