"""Load a snapshot and create a meadow dataset."""

from zipfile import ZipFile

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("maternal_mortality.zip")
    snap_meta = snap.to_table_metadata()

    # Load data from snapshot.
    zf = ZipFile(snap.path)
    folder_name = zf.namelist()[0]
    tb = pr.read_csv(
        zf.open(f"{folder_name}Estimates/estimates.csv"), metadata=snap_meta, origin=snap.metadata.origin
    ).rename(columns={"iso_alpha_3_code": "country", "year_mid": "year"})
    tb_income = (
        pr.read_csv(
            zf.open(f"{folder_name}Estimates/aggregates/aggregate_estimates_rt_World_Bank_Income.csv"),
            metadata=snap_meta,
            origin=snap.metadata.origin,
        )
        .drop(columns="Unnamed: 0")
        .rename(columns={"group": "country", "year_mid": "year"})
    )
    tb_world = (
        pr.read_csv(
            zf.open(f"{folder_name}Estimates/aggregates/aggregate_estimates_rt_WORLD.csv"),
            metadata=snap_meta,
            origin=snap.metadata.origin,
        )
        .drop(columns="Unnamed: 0")
        .rename(columns={"group": "country", "year_mid": "year"})
    )

    tb = pr.concat([tb, tb_income, tb_world], ignore_index=True)
    tb = fix_data_issues(tb)
    # drop unneeded column
    tb = tb.drop(columns=["estimate_version"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "parameter"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def fix_data_issues(tb):
    # Fix specific data issues in the table
    # For regions the maternal deaths is provided in two forms : "maternal_deaths_aggregated_samples" and "maternal_deaths_summation_of_country_estimates"
    # The WHO reports the aggregated samples in their report so we'll go forward using that one. I can't find an explanation of what the differences are.
    tb = tb.replace(
        {"parameter": {"coviddeaths1549": "coviddeaths", "maternal_deaths_aggregated_samples": "maternal_deaths"}}
    )
    # Drop the country estimates maternal deaths rows
    tb = tb[tb["parameter"] != "maternal_deaths_summation_of_country_estimates"]

    return tb
