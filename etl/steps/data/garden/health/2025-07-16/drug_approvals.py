"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load inputs.
    #
    # Load meadow dataset.
    ds_cder = paths.load_dataset("cder_approvals")
    ds_purple_book = paths.load_dataset("purple_book")

    # Read table from meadow dataset.
    tb_cder = ds_cder.read("cder_approvals")
    tb_purple_book = ds_purple_book.read("purple_book")

    # Normalize relevant column names.
    tb_cder = tb_cder.rename(
        columns={
            "application_number__1": "application_number",
            "proprietary__name": "proprietary_name",
            "approval_date": "approval_date",
            "nda_bla": "application_type",
        }
    )
    tb_cder = tb_cder.replace({"BLA": "New biologics", "NDA": "New chemical drugs"})
    tb_purple_book = tb_purple_book.rename(
        columns={
            "bla_number": "application_number",
            "proprietary_name": "proprietary_name",
            "approval_date": "approval_date",
            "bla_type": "application_type",
        }
    )
    # add approval year to purple book
    tb_purple_book["approval_year"] = pd.to_datetime(
        tb_purple_book["approval_date"], format="%Y-%m-%d %H:%M:%S"
    ).dt.year

    # restrict purple book to a) CBER approvals and b) 351(a) applications (new biologics)
    tb_purple_book = tb_purple_book[tb_purple_book["center"] == "CBER"].copy()
    tb_purple_book = tb_purple_book[tb_purple_book["application_type"] == "351(a)"]

    # get approvals per year (NMEs, biologics, vaccines)
    tb = get_approvals_per_year(tb_cder, tb_purple_book)

    # get approvals per designation
    tb_designations = get_approvals_per_designation(tb_cder)

    # Format tables
    tb = tb.rename(
        columns={
            "approval_year": "year",
            "approvals": "total_approvals",
        }
    )

    tb_designations = tb_designations.rename(
        columns={
            "approval_year": "year",
        }
    )
    # Improve table format.
    tb = tb.format(["year", "application_type"], short_name="total_drug_approvals")
    tb_designations = tb_designations.format(["year", "designation"], short_name="drug_approvals_designations")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_designations], default_metadata=ds_cder.metadata)

    # Save garden dataset.
    ds_garden.save()


def get_approvals_per_year(tb_cder, tb_purple_book):
    # copy table to avoid modifying original data
    tb_cber_biologics = tb_purple_book.copy()
    tb_cder = tb_cder.copy()

    # get vaccine approvals from CBER
    tb_cber_biologics["vaccine"] = tb_cber_biologics["proper_name"].str.contains("vaccine", case=False, na=False)
    tb_cber_biologics["vaccine"] = tb_cber_biologics["vaccine"].apply(
        lambda x: "New vaccines" if x else "Other new biologics"
    )
    tb_cber_biologics["application_type"] = tb_cber_biologics["vaccine"]
    tb_cder = tb_cder.replace(
        {
            "New biologics": "Other new biologics",
        }
    )

    tb = pr.concat(
        [
            tb_cder[["approval_year", "application_type"]],
            tb_cber_biologics[["approval_year", "application_type"]],
        ],
        ignore_index=True,
    )

    tb_gb = tb.groupby(["approval_year", "application_type"]).size().reset_index(name="approvals").copy_metadata(tb)

    tb_gb["approvals"].m.origins = tb_gb["approval_year"].m.origins.copy()

    # get total approvals per year
    tb_cum = tb_gb.groupby("approval_year").sum().reset_index()
    tb_cum["application_type"] = "All new drug approvals"

    tb = pr.concat([tb_gb, tb_cum], ignore_index=True)

    return tb


def get_approvals_per_designation(tb_cder):
    # copy table to avoid modifying original data
    tb_cder_des = tb_cder.copy()

    # sum over designation columns
    designation_columns = [
        "orphan_drug_designation",
        "accelerated_approval",
        "breakthrough_therapy_designation",
        "fast_track_designation",
        "qualified_infectious_disease_product",
    ]

    # all possible positive indications in those columns
    pos_indications = [
        "Yes",
        "Yes (indication [B] only)",
        "Yes (indication [A] only)",
        "Yes (indication [B] and [C] only)",
        "Yes (indications [B] and [C] only)",
    ]

    # convert to 1/0
    tb_cder_des[designation_columns] = tb_cder_des[designation_columns].isin(pos_indications).astype(int)

    # sum designation columns per year
    designations = tb_cder_des.groupby("approval_year")[designation_columns].sum().reset_index()
    # sum all designations
    all_designations = (
        tb_cder_des[["approval_year", "orphan_drug_designation"]].groupby("approval_year").count().reset_index()
    )
    all_designations = all_designations.rename(columns={"orphan_drug_designation": "all_approvals"})

    designations = pr.merge(
        designations,
        all_designations,
        on="approval_year",
        how="left",
    ).copy_metadata(tb_cder_des)

    # add metadata (i don't know why this is necessary, but it is)
    for col in designation_columns + ["all_approvals"]:
        designations[col].m.origins = tb_cder_des["approval_year"].m.origins.copy()

    # melt table
    designations = pr.melt(
        designations,
        id_vars=["approval_year"],
        value_vars=designation_columns + ["all_approvals"],
        var_name="designation",
        value_name="approvals",
    )

    # rename designation values
    designations = designations.replace(
        {
            "orphan_drug_designation": "Orphan drug approvals",
            "accelerated_approval": "Accelerated approvals",
            "breakthrough_therapy_designation": "Breakthrough therapy approvals",
            "fast_track_designation": "Fast track approvals",
            "qualified_infectious_disease_product": "Qualified infectious disease product approvals",
            "all_approvals": "All new drug approvals",
        }
    )

    return designations
