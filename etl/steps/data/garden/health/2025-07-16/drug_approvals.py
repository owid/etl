"""Load a meadow dataset and create a garden dataset."""

from datetime import datetime

from matplotlib import pyplot as plt
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def get_approvals_per_year(tb_cder, tb_purple_book):
    # copy table to avoid modifying original data
    tb_cber = tb_purple_book.copy()
    tb_cber["application_type"] = "BLA"

    tb = pr.concat(
        [
            tb_cder[["approval_year", "application_type"]],
            tb_cber[["approval_year", "application_type"]],
        ],
        ignore_index=True,
    )

    tb = tb.groupby(["approval_year", "application_type"]).size().reset_index(name="approvals")
    tb["approval_year"] = tb["approval_year"].astype(int)
    return tb


def get_approvals_per_designation(tb_cder, tb_purple_book):
    # copy table to avoid modifying original data
    tb_cder_des = tb_cder.copy()
    tb_cber = tb_purple_book.copy()

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
        "Yes (indication [B] and [C] only)" "Yes (indications [B] and [C] only)",
    ]

    # convert to 1/0
    tb_cder_des[designation_columns] = tb_cder_des[designation_columns].apply(
        lambda x: 1 if str(x) in pos_indications else 0
    )
    designations = tb_cder_des.groupby("approval_year")[designation_columns].sum().reset_index()

    tb_cber["vaccine"] = (tb_cber["proper_name"].str.contains("vaccine", case=False, na=False)).astype(int)
    vaccines = tb_cber.groupby("approval_year")["vaccine"].sum().reset_index()

    tb_designation = pr.merge(
        designations,
        vaccines,
        on="approval_year",
        how="outer",
        suffixes=("", "_vaccine"),
    )

    return tb_designation


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
    tb_purple_book = tb_purple_book.rename(
        columns={
            "bla_number": "application_number",
            "proprietary_name": "proprietary_name",
            "approval_date": "approval_date",
            "bla_type": "application_type",
        }
    )
    # add approval year to purple book
    tb_purple_book["approval_year"] = tb_purple_book["approval_date"].apply(
        lambda x: datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").year
    )

    # restrict purple book to a) CBER approvals and b) 351(a) applications
    tb_purple_book = tb_purple_book[tb_purple_book["center"] == "CBER"].copy()
    tb_purple_book = tb_purple_book[tb_purple_book["application_type"] == "351(a)"]

    # get approvals per year (NDAs, BLAs)
    tb = get_approvals_per_year(tb_cder, tb_purple_book)

    # get approvals per designation
    tb_designations = get_approvals_per_designation(tb_cder, tb_purple_book)

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
            "orphan_drug_designation": "orphan_drug_approvals",
            "accelerated_approval": "accelerated_approvals",
            "breakthrough_therapy_designation": "breakthrough_therapy_approvals",
            "fast_track_designation": "fast_track_approvals",
            "qualified_infectious_disease_product": "qualified_infectious_disease_approvals",
            "vaccine": "vaccine_approvals",
        }
    )

    # Improve table format.
    tb = tb.format(["year", "application_type"], short_name="total_drug_approvals")
    tb_designations = tb_designations.format(["year"], short_name="drug_approvals_designations")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_designations], default_metadata=ds_cder.metadata)

    # Save garden dataset.
    ds_garden.save()


def plot_per_source(tb, source, by="application_type", color=None, title=None):
    tb_source = tb[tb["source"] == source][["approval_year", "approvals", by]].copy()
    tb_source = tb_source.groupby(["approval_year", by]).sum().reset_index()

    tb_pv = tb_source.pivot(index="approval_year", columns=by, values="approvals")

    if color:
        ordered_series = list(color.keys())
        tb_pv = tb_pv[ordered_series]

    tb_pv.plot(kind="bar", figsize=(8, 5), stacked=True, color=color)
    plt.xlabel("Approval Year")
    plt.ylabel("Approvals")
    if title:
        plt.title(f"{title}")
    else:
        plt.title(f"Drug Approvals per Year ({source})")
    plt.legend(title=by)
    plt.tight_layout()
    plt.show()
