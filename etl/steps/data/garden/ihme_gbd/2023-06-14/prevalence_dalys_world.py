"""Load a meadow dataset and create a garden dataset."""
from typing import cast

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("prevalence_dalys_world: start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_dalys = cast(Dataset, paths.load_dependency("gbd_cause"))
    ds_mh_prevalence = cast(Dataset, paths.load_dependency("gbd_mental_health_prevalence_rate"))

    #
    # Process data.
    #
    log.info("prevalence_dalys_world: build tables")
    tb_dalys = make_table_dalys(ds_dalys)
    tb_prevalence = make_table_prevalence(ds_mh_prevalence)
    # Merge tables
    tb = pd.merge(tb_dalys, tb_prevalence, left_index=True, right_index=True, how="outer")
    tb.metadata.short_name = "prevalence_dalys_world"

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb])

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("prevalence_dalys_world: end")


def make_table_dalys(ds: Dataset) -> Table:
    """Build table with DALYs for mental health causes."""
    # Reset index
    # We are only interested in mental health causes
    causes_mental_health = [
        "anxiety_disorders__both_sexes__age_standardized",
        "bipolar_disorder__both_sexes__age_standardized",
        "depressive_disorders__both_sexes__age_standardized",
        "eating_disorders__both_sexes__age_standardized",
        "schizophrenia__both_sexes__age_standardized",
    ]
    tb = Table()
    for cause in causes_mental_health:
        tb_cause = ds[cause].reset_index()
        tb_cause = tb_cause.drop(columns=["sex", "age", "cause", "rei"], errors="ignore")
        tb_cause = tb_cause.dropna(axis=1, how="all")
        if tb.shape == (0, 0):
            tb = tb_cause
        else:
            tb = pr.merge(tb, tb_cause, on=["country", "year"])

    # Filter dimensions
    tb = tb[(tb["country"] == "World")].drop(columns=["country"])

    cause_rename = {
        "dalys_from_anxiety_disorders_per_100_000_people_in__both_sexes_aged_age_standardized": "Anxiety disorders",
        "dalys_from_bipolar_disorder_per_100_000_people_in__both_sexes_aged_age_standardized": "Bipolar disorder",
        "dalys_from_depressive_disorders_per_100_000_people_in__both_sexes_aged_age_standardized": "Depressive disorders",
        "dalys_from_eating_disorders_per_100_000_people_in__both_sexes_aged_age_standardized": "Eating disorders",
        "dalys_from_schizophrenia_per_100_000_people_in__both_sexes_aged_age_standardized": "Schizophrenia",
    }
    tb = tb.melt(id_vars=["year"])
    list_keys = list(cause_rename.keys())
    tb = tb[tb["variable"].isin(list_keys)]
    assert len(tb["variable"].drop_duplicates()) == len(cause_rename)
    tb["variable"] = tb["variable"].replace(cause_rename)

    tb = tb.rename(columns={"variable": "cause", "value": "dalys_rate"})

    tb = tb.set_index(["cause", "year"], verify_integrity=True).sort_index()
    return tb


def make_table_prevalence(ds: Dataset) -> Table:
    # Reset index
    tb = ds["gbd_mental_health_prevalence_rate"].reset_index()
    # Filter dimensions
    tb = tb[(tb["country"] == "World") & (tb["sex"] == "Both") & (tb["age"] == "Age-standardized")]
    # Only keep prevalence rates, change names and keep relevant columns
    column_rename = {
        "share_anxiety_disorders": "Anxiety disorders",
        "share_bipolar_disorders": "Bipolar disorder",
        "share_depressive_disorders": "Depressive disorders",
        "share_eating_disorders": "Eating disorders",
        "share_schizophrenia_disorders": "Schizophrenia",
    }
    tb = tb.rename(columns=column_rename)[set(column_rename.values()) | {"year"}]
    # Unpivot
    tb = tb.melt(id_vars=["year"], var_name="cause", value_name="share_rate")

    # Filter columns and sort rows
    tb = tb[["cause", "year", "share_rate"]].set_index(["cause", "year"]).sort_index()
    return tb
